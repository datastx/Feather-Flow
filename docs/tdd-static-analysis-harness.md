# TDD Static Analysis Test Harness — Implementation Plan

This document turns [`docs/static-analysis-test-plan.md`](static-analysis-test-plan.md) into an
actionable, phased implementation plan using red-green-refactor TDD discipline.
Every fixture, test function, and Makefile target is specified here so the definition of done
is unambiguous.

---

## Table of Contents

1. [Principles](#1-principles)
2. [Infrastructure](#2-infrastructure)
3. [Phase 1 — Type Inference (A001–A005)](#3-phase-1--type-inference-a001a005)
4. [Phase 2 — Nullability (A010–A012)](#4-phase-2--nullability-a010a012)
5. [Phase 3 — Unused Columns (A020)](#5-phase-3--unused-columns-a020)
6. [Phase 4 — Join Keys (A030–A033)](#6-phase-4--join-keys-a030a033)
7. [Phase 5 — Cross-Model Consistency (A040–A041)](#7-phase-5--cross-model-consistency-a040a041)
8. [Phase 6 — Schema Propagation Engine](#8-phase-6--schema-propagation-engine)
9. [Phase 7 — DataFusion Bridge](#9-phase-7--datafusion-bridge)
10. [Phase 8 — DuckDB-Specific SQL & Types](#10-phase-8--duckdb-specific-sql--types)
11. [Phase 9 — DuckDB Function Stubs](#11-phase-9--duckdb-function-stubs)
12. [Phase 10 — Multi-Model DAG Scenarios](#12-phase-10--multi-model-dag-scenarios)
13. [Phase 11 — Error Handling & Edge Cases](#13-phase-11--error-handling--edge-cases)
14. [Phase 12 — CLI Integration](#14-phase-12--cli-integration)
15. [Phase 13 — Regression Guard Rails](#15-phase-13--regression-guard-rails)
16. [Phase 14 — User-Defined Functions (FN001–FN012)](#16-phase-14--user-defined-functions)
17. [Tracking & Definition of Done](#17-tracking--definition-of-done)

---

## 1. Principles

### TDD Cycle (Red-Green-Refactor)

Every test case follows this discipline:

1. **RED** — Write the test function first. It references a fixture that doesn't exist yet (or
   asserts behavior the code doesn't support). Run `make test-sa` and confirm it fails.
2. **GREEN** — Create the fixture project (sources YAML, model SQL/YAML, featherflow.yml).
   If the code has a bug, fix the analysis pass. Run `make test-sa` and confirm the test passes.
3. **REFACTOR** — Clean up. Extract shared helpers, remove duplication, ensure `make lint` passes.

### Fixture Naming Convention

```
crates/ff-cli/tests/fixtures/sa_<category>_<pass|fail>_<scenario>/
```

Categories: `type`, `null`, `unused`, `join`, `xmodel`, `prop`, `bridge`, `duckdb`,
`fn`, `dag`, `edge`, `cli`, `guard`.

- `sa_type_fail_union_mismatch/` — A002 tests, expects diagnostics
- `sa_null_pass_guarded/` — A010 tests, expects zero diagnostics
- `sa_dag_pass_diamond/` — multi-model DAG, expects clean propagation

### Test File Organization

| File | Purpose |
|------|---------|
| `sa_integration_tests.rs` | CLI-level tests (`ff analyze`, `ff validate`, `ff compile`) |
| `integration_tests.rs` | Rust-level tests using `build_analysis_pipeline()` + `PlanPassManager` |

New tests go in the file that matches their level. CLI-level tests invoke the `ff` binary
via `Command`. Rust-level tests use the library API directly.

### Shared Test Helpers

**CLI-level** (in `sa_integration_tests.rs`):
```rust
fn ff_bin() -> String { env!("CARGO_BIN_EXE_ff").to_string() }

fn run_analyze_json(fixture: &str) -> Vec<serde_json::Value> {
    // Runs `ff analyze --output json --project-dir <fixture>`
    // Returns parsed JSON diagnostic array
}

fn assert_diagnostics(diagnostics: &[serde_json::Value], code: &str, expected_count: usize) {
    // Counts diagnostics matching code, asserts count
}

fn assert_no_diagnostics_with_code(diagnostics: &[serde_json::Value], code: &str) {
    // Asserts zero diagnostics with given code
}

fn assert_no_error_severity(diagnostics: &[serde_json::Value]) {
    // Asserts no diagnostic has severity "error"
}
```

**Rust-level** (in `integration_tests.rs`):

Already exists: `build_analysis_pipeline(fixture_path) -> AnalysisPipeline`

Add:
```rust
fn run_all_passes(pipeline: &AnalysisPipeline) -> Vec<Diagnostic> {
    let mgr = PlanPassManager::with_defaults();
    mgr.run(&pipeline.order, &pipeline.propagation.model_plans, &pipeline.ctx, None)
}

fn run_single_pass(pipeline: &AnalysisPipeline, pass_name: &str) -> Vec<Diagnostic> {
    let mgr = PlanPassManager::with_defaults();
    let filter = vec![pass_name.to_string()];
    mgr.run(&pipeline.order, &pipeline.propagation.model_plans, &pipeline.ctx, Some(&filter))
}

fn diagnostics_with_code(diags: &[Diagnostic], code: DiagnosticCode) -> Vec<&Diagnostic> {
    diags.iter().filter(|d| d.code == code).collect()
}
```

### Makefile Targets

```makefile
test-sa: ## Run static analysis integration tests only
	cargo test -p ff-cli --test sa_integration_tests -- --test-threads=1

test-sa-rust: ## Run Rust-level analysis tests only
	cargo test -p ff-cli --test integration_tests -- test_analysis --test-threads=1

test-sa-all: test-sa test-sa-rust ## Run all static analysis tests
```

### Minimal Fixture Template

Every fixture project follows this structure:

```
sa_<name>/
  featherflow.yml
  sources/
    raw_sources.yml
  models/
    <model_name>/
      <model_name>.sql
      <model_name>.yml
```

**Minimal `featherflow.yml`:**
```yaml
name: sa_<name>
version: "1.0.0"
model_paths: ["models"]
source_paths: ["sources"]
target_path: "target"
materialization: view
schema: analytics
dialect: duckdb
database:
  type: duckdb
  path: "target/dev.duckdb"
```

---

## 2. Infrastructure

### Task 2.1 — Add `test-sa` Makefile targets

**File:** `Makefile`

Add three targets under a new `# Static Analysis Tests` section:
- `test-sa` — runs `sa_integration_tests` (CLI-level)
- `test-sa-rust` — runs analysis-prefixed tests in `integration_tests.rs`
- `test-sa-all` — runs both

**Done when:** `make test-sa` runs the existing 16 SA integration tests and passes.

### Task 2.2 — Extract CLI test helpers into `sa_integration_tests.rs`

Extract the repeated patterns from existing tests into:
- `run_analyze_json(fixture: &str) -> Vec<serde_json::Value>`
- `assert_diagnostics(diags, code, count)`
- `assert_no_diagnostics_with_code(diags, code)`
- `assert_no_error_severity(diags)`

Refactor existing tests to use these helpers. All existing tests must still pass.

**Done when:** `make test-sa` passes, no test logic duplicated.

### Task 2.3 — Extract Rust-level test helpers into `integration_tests.rs`

Add `run_all_passes()`, `run_single_pass()`, and `diagnostics_with_code()` helpers.
Refactor existing `test_analysis_*` tests to use them.

**Done when:** `make test-sa-rust` passes, helpers exist.

---

## 3. Phase 1 — Type Inference (A001–A005)

### Fixture: `sa_type_fail_union_mismatch`

**Tests:** A002 (2.2.1–2.2.7)

```
sa_type_fail_union_mismatch/
  sources/raw_sources.yml    # raw_ints (id INT), raw_strings (name VARCHAR)
  models/
    int_source/int_source.sql   # SELECT id FROM raw_ints
    int_source/int_source.yml
    str_source/str_source.sql   # SELECT name FROM raw_strings
    str_source/str_source.yml
    union_model/union_model.sql # SELECT id FROM int_source UNION ALL SELECT name FROM str_source
    union_model/union_model.yml
```

**Test functions:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_union_type_mismatch_a002` | Rust | A002 emitted for union_model, INT vs VARCHAR |
| `test_sa_union_type_mismatch_cli` | CLI | JSON output contains code "A002" |

**Done when:** Both tests pass. A002 fires on the INT/VARCHAR UNION mismatch.

---

### Fixture: `sa_type_pass_union_compatible`

**Tests:** A002 should-pass (2.2.8–2.2.12)

```
sa_type_pass_union_compatible/
  sources/raw_sources.yml    # raw_data (id INTEGER, val FLOAT, name VARCHAR)
  models/
    source_a/source_a.sql    # SELECT id FROM raw_data
    source_a/source_a.yml
    source_b/source_b.sql    # SELECT id FROM raw_data (same type)
    source_b/source_b.yml
    union_model/union_model.sql # SELECT id FROM source_a UNION ALL SELECT id FROM source_b
    union_model/union_model.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_union_compatible_no_a002` | Rust | Zero A002 diagnostics |

**Done when:** Zero A002 diagnostics emitted for compatible UNION.

---

### Fixture: `sa_type_fail_agg_on_string`

**Tests:** A004 (2.4.1–2.4.3)

```
sa_type_fail_agg_on_string/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, amount DECIMAL)
  models/
    bad_agg/bad_agg.sql      # SELECT SUM(name) AS name_sum FROM raw_data
    bad_agg/bad_agg.yml
```

**Test functions:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_sum_on_string_a004` | Rust | A004 emitted on `name` column |
| `test_sa_sum_on_string_a004_cli` | CLI | JSON output contains A004 |

**Done when:** A004 fires for SUM on VARCHAR column.

---

### Fixture: `sa_type_pass_agg_on_numeric`

**Tests:** A004 should-pass (2.4.4–2.4.8)

```
sa_type_pass_agg_on_numeric/
  sources/raw_sources.yml    # raw_data (id INT, amount DECIMAL, name VARCHAR)
  models/
    good_agg/good_agg.sql    # SELECT SUM(amount), COUNT(name), MIN(name), MAX(name) FROM raw_data
    good_agg/good_agg.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_agg_on_numeric_no_a004` | Rust | Zero A004 diagnostics |

**Done when:** No A004 for SUM on numeric or COUNT/MIN/MAX on string.

---

### Fixture: `sa_type_fail_lossy_cast`

**Tests:** A005 (2.5.1–2.5.6)

```
sa_type_fail_lossy_cast/
  sources/raw_sources.yml    # raw_data (price FLOAT, amount DECIMAL, created_at TIMESTAMP)
  models/
    lossy/lossy.sql          # SELECT CAST(price AS INTEGER), CAST(amount AS INTEGER), CAST(created_at AS DATE) FROM raw_data
    lossy/lossy.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_lossy_cast_a005` | Rust | A005 emitted (>=2 instances) |

**Done when:** A005 fires for FLOAT→INT and DECIMAL→INT casts.

---

### Fixture: `sa_type_pass_safe_cast`

**Tests:** A005 should-pass (2.5.7–2.5.11)

```
sa_type_pass_safe_cast/
  sources/raw_sources.yml    # raw_data (id INTEGER, d DATE)
  models/
    safe/safe.sql            # SELECT CAST(id AS BIGINT), CAST(id AS FLOAT), CAST(d AS TIMESTAMP) FROM raw_data
    safe/safe.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_safe_cast_no_a005` | Rust | Zero A005 diagnostics |

**Done when:** No A005 for widening or safe casts.

---

### Phase 1 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_type_fail_union_mismatch` | 2 | 2.2.1–2.2.7 |
| `sa_type_pass_union_compatible` | 1 | 2.2.8–2.2.12 |
| `sa_type_fail_agg_on_string` | 2 | 2.4.1–2.4.3 |
| `sa_type_pass_agg_on_numeric` | 1 | 2.4.4–2.4.8 |
| `sa_type_fail_lossy_cast` | 1 | 2.5.1–2.5.6 |
| `sa_type_pass_safe_cast` | 1 | 2.5.7–2.5.11 |
| **Total** | **8 tests, 6 fixtures** | |

**Phase 1 Done when:** All 8 tests pass, `make test-sa-all` green, `make lint` clean.

---

## 4. Phase 2 — Nullability (A010–A012)

### Fixture: `sa_null_fail_left_join_unguarded`

**Tests:** A010 (3.1.1–3.1.5)

```
sa_null_fail_left_join_unguarded/
  sources/raw_sources.yml    # raw_orders (id INT, cid INT, amount DECIMAL)
                             # raw_customers (id INT, name VARCHAR, email VARCHAR)
  models/
    stg_orders/stg_orders.sql     # SELECT id, cid, amount FROM raw_orders
    stg_orders/stg_orders.yml
    stg_customers/stg_customers.sql   # SELECT id, name, email FROM raw_customers
    stg_customers/stg_customers.yml
    joined/joined.sql             # SELECT o.id, c.name, c.email FROM stg_orders o LEFT JOIN stg_customers c ON o.cid = c.id
    joined/joined.yml
```

**Test functions:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_left_join_unguarded_a010` | Rust | A010 on `name` and/or `email` |
| `test_sa_left_join_unguarded_a010_cli` | CLI | JSON output contains A010 |

**Done when:** A010 fires for unguarded nullable columns from LEFT JOIN.

---

### Fixture: `sa_null_pass_coalesce_guarded`

**Tests:** A010 should-pass (3.1.6–3.1.9)

```
sa_null_pass_coalesce_guarded/
  sources/raw_sources.yml    # same as above
  models/
    stg_orders/...
    stg_customers/...
    guarded/guarded.sql      # SELECT o.id, COALESCE(c.name, 'Unknown') AS name FROM stg_orders o LEFT JOIN stg_customers c ON o.cid = c.id
    guarded/guarded.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_coalesce_guard_no_a010` | Rust | Zero A010 for guarded columns |

---

### Fixture: `sa_null_fail_yaml_not_null`

**Tests:** A011 (3.2.1–3.2.3)

```
sa_null_fail_yaml_not_null/
  sources/raw_sources.yml
  models/
    stg_orders/...
    stg_customers/...
    contradiction/contradiction.sql   # LEFT JOIN, right side `name` becomes nullable
    contradiction/contradiction.yml   # Declares name as NOT NULL (not_null: true or similar)
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_yaml_not_null_contradiction_a011` | Rust | A011 on `name` |

---

### Fixture: `sa_null_fail_redundant_check`

**Tests:** A012 (3.3.1–3.3.3)

```
sa_null_fail_redundant_check/
  sources/raw_sources.yml    # raw_data (id INT NOT NULL via tests: [not_null])
  models/
    redundant/redundant.sql  # SELECT id FROM raw_data WHERE id IS NOT NULL
    redundant/redundant.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_redundant_null_check_a012` | Rust | A012 on `id` |

---

### Phase 2 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_null_fail_left_join_unguarded` | 2 | 3.1.1–3.1.5 |
| `sa_null_pass_coalesce_guarded` | 1 | 3.1.6–3.1.9 |
| `sa_null_fail_yaml_not_null` | 1 | 3.2.1–3.2.3 |
| `sa_null_fail_redundant_check` | 1 | 3.3.1–3.3.3 |
| **Total** | **5 tests, 4 fixtures** | |

**Phase 2 Done when:** All 5 tests pass.

---

## 5. Phase 3 — Unused Columns (A020)

### Fixture: `sa_unused_fail_extra_columns`

**Tests:** A020 (4.1.1–4.1.2)

```
sa_unused_fail_extra_columns/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, internal_code VARCHAR, debug_flag BOOLEAN)
  models/
    stg/stg.sql              # SELECT id, name, internal_code, debug_flag FROM raw_data
    stg/stg.yml
    fct/fct.sql              # SELECT id, name FROM stg
    fct/fct.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_unused_columns_a020` | Rust | A020 on `internal_code` and `debug_flag` in `stg` |

---

### Fixture: `sa_unused_pass_all_consumed`

**Tests:** A020 should-pass (4.1.3–4.1.7)

Diamond DAG where all columns consumed across dependents:

```
sa_unused_pass_all_consumed/
  sources/raw_sources.yml    # raw_data (id INT, a VARCHAR, b VARCHAR)
  models/
    stg/stg.sql              # SELECT id, a, b FROM raw_data
    stg/stg.yml
    fct_a/fct_a.sql          # SELECT id, a FROM stg
    fct_a/fct_a.yml
    fct_b/fct_b.sql          # SELECT id, b FROM stg
    fct_b/fct_b.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_all_columns_consumed_no_a020` | Rust | Zero A020 (all consumed across diamond) |

---

### Fixture: `sa_unused_pass_terminal`

**Tests:** A020 should-pass — terminal model (4.1.6, 4.1.8)

```
sa_unused_pass_terminal/
  sources/raw_sources.yml
  models/
    terminal/terminal.sql    # SELECT id, name, extra FROM raw_data
    terminal/terminal.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_terminal_model_no_a020` | Rust | Zero A020 (terminal model skipped) |

---

### Phase 3 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_unused_fail_extra_columns` | 1 | 4.1.1–4.1.2 |
| `sa_unused_pass_all_consumed` | 1 | 4.1.3–4.1.5 |
| `sa_unused_pass_terminal` | 1 | 4.1.6, 4.1.8 |
| **Total** | **3 tests, 3 fixtures** | |

---

## 6. Phase 4 — Join Keys (A030–A033)

### Fixture: `sa_join_fail_type_mismatch`

**Tests:** A030 (5.1.1–5.1.5)

```
sa_join_fail_type_mismatch/
  sources/raw_sources.yml    # raw_orders (id INT, code VARCHAR), raw_items (id INT, order_code INT)
  models/
    stg_orders/...           # SELECT id, code FROM raw_orders
    stg_items/...            # SELECT id, order_code FROM raw_items
    bad_join/bad_join.sql    # SELECT o.id FROM stg_orders o JOIN stg_items i ON o.code = i.order_code
                             # code is VARCHAR, order_code is INT → A030
    bad_join/bad_join.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_join_key_type_mismatch_a030` | Rust | A030 emitted |

---

### Fixture: `sa_join_fail_cross_join`

**Tests:** A032 (5.2.1–5.2.4)

```
sa_join_fail_cross_join/
  sources/raw_sources.yml    # raw_a (id INT), raw_b (id INT)
  models/
    source_a/...             # SELECT id FROM raw_a
    source_b/...             # SELECT id FROM raw_b
    crossed/crossed.sql      # SELECT a.id, b.id AS b_id FROM source_a a CROSS JOIN source_b b
    crossed/crossed.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_cross_join_a032` | Rust | A032 emitted |

---

### Fixture: `sa_join_fail_non_equi`

**Tests:** A033 (5.3.1–5.3.6)

```
sa_join_fail_non_equi/
  sources/raw_sources.yml    # raw_a (id INT, val INT), raw_b (id INT, val INT)
  models/
    source_a/...
    source_b/...
    range_join/range_join.sql # SELECT a.id FROM source_a a JOIN source_b b ON a.val > b.val
    range_join/range_join.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_non_equi_join_a033` | Rust | A033 emitted for `>` condition |

---

### Fixture: `sa_join_pass_equi`

**Tests:** A030/A032/A033 should-pass (5.1.6–5.1.10, 5.2.5–5.2.6, 5.3.7–5.3.8)

```
sa_join_pass_equi/
  sources/raw_sources.yml    # raw_a (id INT, code INT), raw_b (id INT, code INT)
  models/
    source_a/...
    source_b/...
    clean_join/clean_join.sql # SELECT a.id FROM source_a a JOIN source_b b ON a.id = b.id AND a.code = b.code
    clean_join/clean_join.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_equi_join_no_join_diagnostics` | Rust | Zero A030, A032, A033 |

---

### Phase 4 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_join_fail_type_mismatch` | 1 | 5.1.1–5.1.5 |
| `sa_join_fail_cross_join` | 1 | 5.2.1–5.2.4 |
| `sa_join_fail_non_equi` | 1 | 5.3.1–5.3.6 |
| `sa_join_pass_equi` | 1 | 5.1.6–5.3.8 |
| **Total** | **4 tests, 4 fixtures** | |

---

## 7. Phase 5 — Cross-Model Consistency (A040–A041)

### Fixture: `sa_xmodel_fail_extra_in_sql`

**Tests:** A040 ExtraInSql (6.1.1, 6.1.4)

```
sa_xmodel_fail_extra_in_sql/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, bonus INT)
  models/
    extra/extra.sql          # SELECT id, name, bonus FROM raw_data
    extra/extra.yml          # Only declares id, name — bonus is extra
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_extra_in_sql_a040` | Rust | A040 warning on `bonus` |

---

### Fixture: `sa_xmodel_fail_missing_from_sql`

**Tests:** A040 MissingFromSql (6.1.2, 6.1.5)

```
sa_xmodel_fail_missing_from_sql/
  sources/raw_sources.yml    # raw_data (id INT)
  models/
    missing/missing.sql      # SELECT id FROM raw_data
    missing/missing.yml      # Declares id AND phantom_col — phantom_col missing from SQL
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_missing_from_sql_a040` | Rust | A040 error on `phantom_col` |

---

### Fixture: `sa_xmodel_fail_type_mismatch`

**Tests:** A040 TypeMismatch (6.1.3, 6.1.7–6.1.8)

Uses existing `sa_diagnostic_project` fixture (already has `amount` declared as VARCHAR in YAML
but SQL infers DECIMAL from source). Add a test that specifically checks for A040.

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_type_mismatch_a040` | Rust | A040 on `amount` (VARCHAR vs DECIMAL) |
| `test_sa_missing_from_sql_a040_diagnostic_project` | CLI | A040 error on `extra_col` |

---

### Fixture: `sa_xmodel_pass_exact_match`

**Tests:** A040/A041 should-pass (6.1.9–6.1.11, 6.2.4–6.2.6)

Uses existing `sa_clean_project` fixture.

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_clean_project_no_a040_a041` | Rust | Zero A040, A041 |

---

### Phase 5 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_xmodel_fail_extra_in_sql` | 1 | 6.1.1, 6.1.4 |
| `sa_xmodel_fail_missing_from_sql` | 1 | 6.1.2, 6.1.5 |
| `sa_diagnostic_project` (existing) | 2 | 6.1.3, 6.1.7–6.1.8 |
| `sa_clean_project` (existing) | 1 | 6.1.9–6.2.6 |
| **Total** | **5 tests, 2 new fixtures** | |

---

## 8. Phase 6 — Schema Propagation Engine

### Fixture: `sa_prop_pass_linear_chain`

**Tests:** Linear chain (7.1–7.3)

```
sa_prop_pass_linear_chain/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, amount DECIMAL)
  models/
    stg/stg.sql              # SELECT id, name, amount FROM raw_data
    stg/stg.yml
    int/int.sql              # SELECT id, name FROM stg  (narrows columns)
    int/int.yml
    mart/mart.sql            # SELECT id, name FROM int
    mart/mart.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_propagation_linear_chain` | Rust | All 3 models plan successfully, zero failures |

---

### Fixture: `sa_prop_pass_diamond`

**Tests:** Diamond DAG (7.6)

```
sa_prop_pass_diamond/
  sources/raw_sources.yml    # raw_data (id INT, a VARCHAR, b VARCHAR)
  models/
    stg/stg.sql              # SELECT id, a, b FROM raw_data
    stg/stg.yml
    branch_a/branch_a.sql    # SELECT id, a FROM stg
    branch_a/branch_a.yml
    branch_b/branch_b.sql    # SELECT id, b FROM stg
    branch_b/branch_b.yml
    joined/joined.sql        # SELECT a.id, a.a, b.b FROM branch_a a JOIN branch_b b ON a.id = b.id
    joined/joined.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_propagation_diamond_dag` | Rust | All 4 models plan successfully |

---

### Fixture: `sa_prop_fail_unknown_table`

**Tests:** Error recovery (7.11)

```
sa_prop_fail_unknown_table/
  sources/raw_sources.yml    # (empty or minimal)
  models/
    broken/broken.sql        # SELECT id FROM nonexistent_table
    broken/broken.yml
    good/good.sql            # SELECT 1 AS val
    good/good.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_propagation_unknown_table_partial_failure` | Rust | `broken` fails, `good` succeeds |

---

### Phase 6 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_prop_pass_linear_chain` | 1 | 7.1–7.3 |
| `sa_prop_pass_diamond` | 1 | 7.6 |
| `sa_prop_fail_unknown_table` | 1 | 7.11, 7.14 |
| **Total** | **3 tests, 3 fixtures** | |

---

## 9. Phase 7 — DataFusion Bridge

### Fixture: `sa_bridge_pass_basic_sql`

**Tests:** SQL-to-Plan conversion (9.1–9.4, 9.6)

```
sa_bridge_pass_basic_sql/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, ts TIMESTAMP, amount DECIMAL)
  models/
    simple_select/simple_select.sql   # SELECT id, name FROM raw_data
    simple_select/simple_select.yml
    with_join/with_join.sql           # SELECT a.id, b.name FROM raw_data a JOIN raw_data b ON a.id = b.id
    with_join/with_join.yml
    with_agg/with_agg.sql             # SELECT COUNT(*) AS cnt FROM raw_data
    with_agg/with_agg.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_bridge_basic_sql_plans` | Rust | All 3 models plan successfully, correct output column count |

---

### Fixture: `sa_bridge_fail_unknown_table`

**Tests:** AE008 (9.5)

```
sa_bridge_fail_unknown_table/
  sources/raw_sources.yml    # (minimal)
  models/
    bad_ref/bad_ref.sql      # SELECT * FROM nonexistent_table_xyz
    bad_ref/bad_ref.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_bridge_unknown_table_fails` | Rust | `bad_ref` appears in propagation failures |

---

### Phase 7 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_bridge_pass_basic_sql` | 1 | 9.1–9.6 |
| `sa_bridge_fail_unknown_table` | 1 | 9.5 |
| **Total** | **2 tests, 2 fixtures** | |

---

## 10. Phase 8 — DuckDB-Specific SQL & Types

### Fixture: `sa_duckdb_pass_syntax`

**Tests:** DuckDB SQL syntax (10.1–10.2)

```
sa_duckdb_pass_syntax/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR)
  models/
    cast_shorthand/cast_shorthand.sql  # SELECT 42::INTEGER AS val, id FROM raw_data
    cast_shorthand/cast_shorthand.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_duckdb_cast_shorthand_plans` | Rust | Model plans successfully |

---

### Fixture: `sa_duckdb_pass_all_types`

**Tests:** DuckDB type coverage (10.14–10.56)

```
sa_duckdb_pass_all_types/
  sources/raw_sources.yml    # raw_typed (
                             #   tiny TINYINT, small SMALLINT, med INTEGER, big BIGINT,
                             #   f FLOAT, d DOUBLE, dec DECIMAL(10,2),
                             #   s VARCHAR, b BOOLEAN, dt DATE, ts TIMESTAMP, t TIME,
                             #   bin BLOB, j JSON, u UUID, iv INTERVAL
                             # )
  models/
    typed_model/typed_model.sql  # SELECT tiny, small, med, big, f, d, dec, s, b, dt, ts, t, bin, j, u, iv FROM raw_typed
    typed_model/typed_model.yml  # All columns with matching data_type declarations
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_duckdb_all_types_plan` | Rust | Model plans, all columns in output schema |
| `test_sa_duckdb_all_types_no_a040` | Rust | Zero A040 (YAML matches SQL types) |

---

### Phase 8 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_duckdb_pass_syntax` | 1 | 10.1–10.2 |
| `sa_duckdb_pass_all_types` | 2 | 10.14–10.56 |
| **Total** | **3 tests, 2 fixtures** | |

---

## 11. Phase 9 — DuckDB Function Stubs

### Fixture: `sa_duckdb_pass_scalar_functions`

**Tests:** Scalar function stubs (12.1–12.27)

```
sa_duckdb_pass_scalar_functions/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, ts TIMESTAMP, amount DECIMAL)
  models/
    fn_date/fn_date.sql      # SELECT date_trunc('month', ts) AS trunc_ts, date_part('year', ts) AS yr FROM raw_data
    fn_date/fn_date.yml
    fn_string/fn_string.sql  # SELECT regexp_matches(name, '^A') AS matches, md5(name) AS hash FROM raw_data
    fn_string/fn_string.yml
    fn_null/fn_null.sql      # SELECT coalesce(name, 'unknown') AS safe_name, ifnull(name, 'n/a') AS if_name FROM raw_data
    fn_null/fn_null.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_duckdb_scalar_functions_plan` | Rust | All 3 models plan successfully |

---

### Fixture: `sa_duckdb_pass_agg_functions`

**Tests:** Aggregate function stubs (12.28–12.43)

```
sa_duckdb_pass_agg_functions/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, amount DECIMAL, active BOOLEAN)
  models/
    agg_model/agg_model.sql  # SELECT string_agg(name, ',') AS names, bool_and(active) AS all_active, median(amount) AS med FROM raw_data
    agg_model/agg_model.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_duckdb_agg_functions_plan` | Rust | Model plans successfully |

---

### Phase 9 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_duckdb_pass_scalar_functions` | 1 | 12.1–12.27 |
| `sa_duckdb_pass_agg_functions` | 1 | 12.28–12.43 |
| **Total** | **2 tests, 2 fixtures** | |

---

## 12. Phase 10 — Multi-Model DAG Scenarios

### Fixture: `sa_dag_pass_ecommerce`

**Tests:** Full clean e-commerce project (13.1)

A realistic 6-model project that exercises the full pipeline with zero diagnostics:

```
sa_dag_pass_ecommerce/
  sources/raw_sources.yml    # raw_orders (id, customer_id, amount, status, created_at)
                             # raw_customers (id, name, email)
                             # raw_products (id, name, price, category)
  models/
    stg_orders/...           # SELECT id, customer_id, amount, status, created_at FROM raw_orders
    stg_customers/...        # SELECT id, name, email FROM raw_customers
    stg_products/...         # SELECT id, name, price, category FROM raw_products
    int_enriched/...         # JOIN stg_orders + stg_customers (INNER JOIN, no nullable issues)
    dim_products/...         # SELECT id, name, price, category FROM stg_products
    fct_sales/...            # JOIN int_enriched + dim_products (INNER JOIN)
```

**Test functions:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_dag_ecommerce_all_plan` | Rust | All 6 models plan, zero failures |
| `test_sa_dag_ecommerce_zero_diagnostics` | Rust | Zero diagnostics from all passes |
| `test_sa_dag_ecommerce_cli` | CLI | `ff analyze --output json` returns empty array |

**Done when:** Full e-commerce DAG produces zero diagnostics.

---

### Fixture: `sa_dag_fail_mixed`

**Tests:** Mixed diagnostics across DAG (13.11)

A 4-model project with deliberate issues at each layer:

```
sa_dag_fail_mixed/
  sources/raw_sources.yml    # raw_data (id INT, name VARCHAR, amount DECIMAL, code VARCHAR)
  models/
    stg/stg.sql              # SELECT id, name, amount, code FROM raw_data
    stg/stg.yml              # Missing `code` from YAML → A040
    int/int.sql              # SELECT s.id, s.name, s.amount FROM stg s LEFT JOIN stg s2 ON s.id = s2.id
                             # name becomes nullable after LEFT JOIN → A010
    int/int.yml
    fct/fct.sql              # SELECT id FROM int (only uses id, name+amount unused) → A020
    fct/fct.yml
```

**Test functions:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_dag_mixed_diagnostics` | Rust | Has A040, A010, A020 |
| `test_sa_dag_mixed_diagnostics_cli` | CLI | JSON output contains all 3 codes |

---

### Phase 10 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_dag_pass_ecommerce` | 3 | 13.1 |
| `sa_dag_fail_mixed` | 2 | 13.11 |
| **Total** | **5 tests, 2 fixtures** | |

---

## 13. Phase 11 — Error Handling & Edge Cases

### Fixture: `sa_edge_pass_literal_query`

**Tests:** SELECT with no FROM (14.9), boolean/NULL literals (14.19–14.20)

```
sa_edge_pass_literal_query/
  sources/raw_sources.yml    # (minimal — at least one source so project loads)
  models/
    literal/literal.sql      # SELECT 1 AS val, TRUE AS flag, NULL AS empty_col
    literal/literal.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_literal_query_plans` | Rust | Model plans, schema has 3 columns |

---

### Fixture: `sa_edge_pass_self_join`

**Tests:** Self-join (14.10)

```
sa_edge_pass_self_join/
  sources/raw_sources.yml    # raw_data (id INT, parent_id INT, name VARCHAR)
  models/
    self_join/self_join.sql  # SELECT t1.id, t2.name AS parent_name FROM raw_data t1 JOIN raw_data t2 ON t1.parent_id = t2.id
    self_join/self_join.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_self_join_plans` | Rust | Plans successfully |

---

### Fixture: `sa_edge_pass_deep_expression`

**Tests:** Deeply nested expressions (14.18)

```
sa_edge_pass_deep_expression/
  sources/raw_sources.yml    # raw_data (id INT, amount DECIMAL, name VARCHAR)
  models/
    deep/deep.sql            # SELECT CAST(COALESCE(CASE WHEN amount > 0 THEN amount ELSE 0 END, 0) AS BIGINT) AS safe_amount FROM raw_data
    deep/deep.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_deep_expression_plans` | Rust | Plans, type resolved to BIGINT |

---

### Phase 11 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_edge_pass_literal_query` | 1 | 14.9, 14.19–14.20 |
| `sa_edge_pass_self_join` | 1 | 14.10 |
| `sa_edge_pass_deep_expression` | 1 | 14.18 |
| **Total** | **3 tests, 3 fixtures** | |

---

## 14. Phase 12 — CLI Integration

### Tests against existing fixtures

These tests verify the CLI commands work correctly with analysis. No new fixtures needed.

| Function | Fixture | Command | Asserts |
|----------|---------|---------|---------|
| `test_cli_validate_clean` | `sa_clean_project` | `ff validate` | Exit 0 |
| `test_cli_validate_strict_with_warnings` | `sa_dag_fail_mixed` | `ff validate --strict` | Exit non-zero |
| `test_cli_compile_with_sa_error` | `sa_xmodel_fail_missing_from_sql` | `ff compile` | Exit non-zero (SA01) |
| `test_cli_compile_skip_sa` | `sa_xmodel_fail_missing_from_sql` | `ff compile --skip-static-analysis` | Exit 0 |
| `test_cli_analyze_json_structure` | `sa_dag_fail_mixed` | `ff analyze --output json` | Valid JSON array, each item has code/severity/message/model |
| `test_cli_analyze_model_filter` | `sa_dag_fail_mixed` | `ff analyze --model stg` | Only diagnostics for `stg` |

**Phase 12 Summary:** 6 tests, 0 new fixtures. All use fixtures from prior phases.

---

## 15. Phase 13 — Regression Guard Rails

### Tests against existing fixtures

| Function | Fixture | Asserts |
|----------|---------|---------|
| `test_guard_sample_project_zero_diagnostics` | `sample_project` | Zero diagnostics (already exists) |
| `test_guard_sample_project_zero_a001` | `sample_project` | Zero A001 (already exists in CLI form) |
| `test_guard_clean_project_zero_diagnostics` | `sa_clean_project` | Zero diagnostics |
| `test_guard_ecommerce_zero_diagnostics` | `sa_dag_pass_ecommerce` | Zero diagnostics |

**Phase 13 Summary:** 4 tests, 0 new fixtures. Ensures regressions are caught.

---

## 16. Phase 14 — User-Defined Functions

### Existing fixtures are sufficient for basic function tests

The function test infrastructure already exists in `function_tests.rs` with fixtures
`sa_fn_pass_scalar_basic` and `sa_fn_fail_duplicate_name`.

### Fixture: `sa_fn_pass_multi_function`

**Tests:** 17.9.3 — function-to-function deps, multiple stubs

```
sa_fn_pass_multi_function/
  featherflow.yml            # function_paths: ["functions"]
  sources/raw_sources.yml    # raw_orders (id INT, revenue DECIMAL, cost DECIMAL, amount_cents BIGINT)
  functions/
    safe_divide.sql          # CASE WHEN b = 0 THEN NULL ELSE a / b END
    safe_divide.yml          # scalar, args: (a DECIMAL, b DECIMAL), returns: DECIMAL
    cents_to_dollars.sql     # amount / 100.0
    cents_to_dollars.yml     # scalar, args: (amount BIGINT), returns: DECIMAL(10,2)
    margin_pct.sql           # safe_divide(revenue - cost, revenue) * 100
    margin_pct.yml           # scalar, args: (revenue DECIMAL, cost DECIMAL), returns: DECIMAL
  models/
    margins/margins.sql      # SELECT id, cents_to_dollars(amount_cents) AS dollars, margin_pct(revenue, cost) AS margin FROM raw_orders
    margins/margins.yml
```

**Test functions:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_fn_multi_function_plans` | Rust | Model plans with all 3 UDF stubs |
| `test_sa_fn_multi_function_no_errors` | Rust | Zero error-severity diagnostics |

---

### Fixture: `sa_fn_fail_model_calls_undefined`

**Tests:** 17.9.15 — model calls nonexistent function

```
sa_fn_fail_model_calls_undefined/
  featherflow.yml
  sources/raw_sources.yml    # raw_data (id INT, amount BIGINT)
  models/
    bad_call/bad_call.sql    # SELECT id, nonexistent_func(amount) AS val FROM raw_data
    bad_call/bad_call.yml
```

**Test function:**
| Function | Level | Asserts |
|----------|-------|---------|
| `test_sa_fn_undefined_call_fails` | Rust | `bad_call` appears in propagation failures |

---

### Phase 14 Summary

| Fixture | Tests | Test Plan Refs |
|---------|-------|----------------|
| `sa_fn_pass_multi_function` | 2 | 17.9.3 |
| `sa_fn_fail_model_calls_undefined` | 1 | 17.9.15 |
| **Total** | **3 tests, 2 fixtures** | |

---

## 17. Tracking & Definition of Done

### Grand Summary

| Phase | Fixtures | Tests | Focus |
|-------|----------|-------|-------|
| Infrastructure | 0 | 0 | Makefile targets, test helpers |
| Phase 1 — Types | 6 | 8 | A002, A004, A005 |
| Phase 2 — Nullability | 4 | 5 | A010, A011, A012 |
| Phase 3 — Unused | 3 | 3 | A020 |
| Phase 4 — Joins | 4 | 4 | A030, A032, A033 |
| Phase 5 — Cross-Model | 2 | 5 | A040, A041 |
| Phase 6 — Propagation | 3 | 3 | Schema propagation engine |
| Phase 7 — Bridge | 2 | 2 | DataFusion planning |
| Phase 8 — DuckDB Types | 2 | 3 | Type coverage |
| Phase 9 — DuckDB Funcs | 2 | 2 | Function stubs |
| Phase 10 — DAG | 2 | 5 | End-to-end multi-model |
| Phase 11 — Edge Cases | 3 | 3 | Literals, self-join, nesting |
| Phase 12 — CLI | 0 | 6 | ff validate/compile/analyze |
| Phase 13 — Guards | 0 | 4 | Regression prevention |
| Phase 14 — UDFs | 2 | 3 | Function stubs in analysis |
| **Total** | **35 new fixtures** | **56 new tests** | |

### Definition of Done — Per Phase

A phase is complete when:

1. All fixture projects for the phase exist under `crates/ff-cli/tests/fixtures/`
2. All test functions for the phase exist in the appropriate test file
3. Every test follows TDD: was written RED first, then made GREEN
4. `make test-sa-all` passes (all SA tests green)
5. `make lint` passes (no clippy warnings, formatting clean)
6. No existing tests regressed (`make test` all green)

### Definition of Done — Overall

The harness is complete when:

1. All 14 phases are individually complete
2. `make test-sa-all` runs all 56+ new tests and passes
3. `make test` runs all 750+ tests (existing + new) and passes
4. `make lint` is clean
5. Every diagnostic code (A001–A041) has at least one should-fail and one should-pass test
6. The `sample_project` and `sa_clean_project` regression guards still assert zero diagnostics
7. No test uses `#[ignore]` — every test is active

### Execution Order

Phases should be completed in order (1→14) because later phases depend on fixtures and
helpers introduced in earlier phases. Within each phase, follow TDD:

```
For each test in the phase:
  1. Write the test function (RED) — references fixture path, asserts expected diagnostics
  2. Run `make test-sa-all` — confirm it fails (RED)
  3. Create the fixture project files (GREEN)
  4. Run `make test-sa-all` — confirm it passes (GREEN)
  5. If the test fails because the analysis pass has a bug, fix the pass
  6. Refactor — extract helpers, clean up, `make lint`
```

### Makefile Targets Reference

```makefile
# Run all tests (existing behavior, unchanged)
make test

# Run only static analysis CLI tests
make test-sa

# Run only Rust-level analysis tests
make test-sa-rust

# Run both SA test suites
make test-sa-all

# Standard quality checks
make lint
make ci
```
