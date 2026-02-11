# Feather-Flow Code Review Report

**Date:** 2026-02-08
**Scope:** All 6 crates (`ff-cli`, `ff-core`, `ff-db`, `ff-jinja`, `ff-sql`, `ff-test`)
**Total findings:** 74

---

## Summary by Severity

| Severity | Count | Categories |
|----------|-------|------------|
| HIGH     | ~25   | Mutex panics, SQL injection, `.unwrap()` on Options |
| MEDIUM   | ~35   | Error swallowing, `process::exit()`, deduplication, stringly-typed enums, async patterns |
| LOW      | ~14   | Dead code, visibility, comments, magic numbers |

---

## Batch 1: Mutex panics in ff-db (HIGH)

9 calls to `self.conn.lock().unwrap()` will panic if the mutex is poisoned.

| # | File | Line | Issue |
|---|------|------|-------|
| 1 | `ff-db/src/duckdb.rs` | 57 | `execute_sync`: `.lock().unwrap()` |
| 2 | `ff-db/src/duckdb.rs` | 64 | `execute_batch_sync`: `.lock().unwrap()` |
| 3 | `ff-db/src/duckdb.rs` | 71 | `query_count_sync`: `.lock().unwrap()` |
| 4 | `ff-db/src/duckdb.rs` | 82 | `relation_exists_sync`: `.lock().unwrap()` |
| 5 | `ff-db/src/duckdb.rs` | 212 | `infer_csv_schema`: `.lock().unwrap()` |
| 6 | `ff-db/src/duckdb.rs` | 245 | `query_sample_rows`: `.lock().unwrap()` |
| 7 | `ff-db/src/duckdb.rs` | 274 | `query_one`: `.lock().unwrap()` |
| 8 | `ff-db/src/duckdb.rs` | 366 | `get_table_schema`: `.lock().unwrap()` |
| 9 | `ff-db/src/duckdb.rs` | 394 | `describe_query`: `.lock().unwrap()` |

**Fix:** Add `MutexPoisoned(String)` variant (D006) to `DbError`. Add `lock_conn()` helper method.

---

## Batch 2: Mutex panics in ff-jinja (HIGH)

7 calls to `.lock().unwrap()` on `config_capture` mutex.

| # | File | Line | Issue |
|---|------|------|-------|
| 1 | `ff-jinja/src/environment.rs` | 74 | `render`: `.lock().unwrap()` |
| 2 | `ff-jinja/src/environment.rs` | 91 | `render_with_config`: `.lock().unwrap()` |
| 3 | `ff-jinja/src/environment.rs` | 97 | `get_captured_config`: `.lock().unwrap()` |
| 4 | `ff-jinja/src/environment.rs` | 103 | `get_materialization`: `.lock().unwrap()` |
| 5 | `ff-jinja/src/environment.rs` | 111 | `get_schema`: `.lock().unwrap()` |
| 6 | `ff-jinja/src/environment.rs` | 121 | `get_tags`: `.lock().unwrap()` |
| 7 | `ff-jinja/src/functions.rs` | 77 | `make_config_fn`: `.lock().unwrap()` |

**Fix:** Use `map_err` where return type is `JinjaResult`, `unwrap_or_else(|p| p.into_inner())` elsewhere.

---

## Batch 3: SQL injection via format!() (HIGH)

User-controlled identifiers interpolated directly into SQL via `format!()`.

| # | File | Line | Issue |
|---|------|------|-------|
| 1 | `ff-db/src/duckdb.rs` | 91 | `relation_exists_sync`: schema/table in format string |
| 2 | `ff-db/src/duckdb.rs` | 374 | `get_table_schema`: schema/table in format string |
| 3 | `ff-db/src/duckdb.rs` | 213 | `infer_csv_schema`: path in format string |
| 4 | `ff-cli/src/commands/diff.rs` | 154 | Unquoted identifiers in SQL |
| 5 | `ff-cli/src/commands/diff.rs` | 258-263 | Unquoted identifiers in SQL |
| 6 | `ff-cli/src/commands/freshness.rs` | 222-224 | Unquoted identifiers in SQL |
| 7 | `ff-cli/src/commands/seed.rs` | 124 | Unquoted table name in SQL |
| 8 | `ff-cli/src/commands/run.rs` | 1576 | Unquoted qualified_name in SQL |

**Fix:** Use parameterized queries for `information_schema` lookups. Use `quote_ident()` for DDL.

---

## Batch 4: .unwrap() on Options in non-test code (HIGH)

| # | File | Line | Issue |
|---|------|------|-------|
| 1 | `ff-cli/src/commands/ls.rs` | 35 | `project.get_model(name).unwrap()` |
| 2 | `ff-cli/src/commands/compile.rs` | 537 | `project.get_model(name).unwrap()` |
| 3 | `ff-core/src/model.rs` | 662 | `unwrap_or("unknown")` should return error |
| 4 | `ff-core/src/snapshot.rs` | 224 | `unwrap_or(&"id".to_string())` |
| 5 | `ff-core/src/snapshot.rs` | 299 | `unwrap_or(&"id".to_string())` |
| 6 | `ff-db/src/duckdb.rs` | 540 | `unwrap_or("id")` |
| 7 | `ff-jinja/src/custom_tests.rs` | 66 | `Regex::new().unwrap()` |

**Fix:** Use `.ok_or_else()`, `.expect()` with justification, or `OnceLock` as appropriate.

---

## Batch 5: Silent error swallowing + duplicate error codes (MEDIUM)

| # | File | Line | Issue |
|---|------|------|-------|
| 1 | `ff-core/src/error.rs` | - | `Io` and `YamlParse` share codes with other variants |
| 2 | `ff-core/src/model.rs` | 672 | `ModelSchema::load().ok()` silently swallows errors |
| 3 | `ff-core/src/source.rs` | 228-233 | Silent `continue` on error |
| 4 | `ff-core/src/query_comment.rs` | 79 | `unwrap_or_default()` hides serialization failure |
| 5 | `ff-core/src/manifest.rs` | 335-352 | Returns `io::Error` instead of `CoreResult` |
| 6 | `ff-db/src/duckdb.rs` | 232-235 | `let _ =` on DROP TABLE error |
| 7 | `ff-cli/src/commands/run.rs` | 1984 | `let _ =` on JoinError |

**Fix:** Renumber error codes, propagate errors, add logging where appropriate.

---

## Batch 6: std::process::exit() replacement (MEDIUM)

15 calls to `std::process::exit()` bypass RAII destructors and prevent proper cleanup.

| # | File | Approx count |
|---|------|-------------|
| 1 | `ff-cli/src/commands/run.rs` | 2 |
| 2 | `ff-cli/src/commands/compile.rs` | 2 |
| 3 | `ff-cli/src/commands/test.rs` | 1 |
| 4 | `ff-cli/src/commands/validate.rs` | 1 |
| 5 | `ff-cli/src/commands/diff.rs` | 1 |
| 6 | `ff-cli/src/commands/seed.rs` | 1 |
| 7 | `ff-cli/src/commands/snapshot.rs` | 1 |
| 8 | `ff-cli/src/commands/analyze.rs` | 1 |
| 9 | `ff-cli/src/commands/freshness.rs` | 1 |
| 10 | `ff-cli/src/commands/run_operation.rs` | 1 |

**Fix:** Create `ExitCode(pub i32)` error type, handle in `main.rs`.

---

## Batch 7: Code deduplication (MEDIUM)

| # | File | Issue |
|---|------|-------|
| 1 | `ff-cli/src/commands/run.rs` + `compile.rs` | Duplicate `parse_hooks_from_config` |
| 2 | `ff-cli/src/commands/compile.rs` + `parse.rs` | Duplicate `filter_models` |
| 4 | `ff-jinja/src/environment.rs` | `with_macros` / `with_incremental_context` share init logic |
| 5 | `ff-test/src/generator.rs` | 3 duplicated `TestType` match blocks |
| 6 | `ff-db/src/duckdb.rs` | `merge_into` / `delete_insert` near-identical |
| 7 | `ff-core/src/manifest.rs` | `add_model` / `add_model_relative` + custom `chrono_lite_now()` |

**Fix:** Extract shared helpers to `commands/common.rs` and local helper methods.

---

## Batch 8: Stringly-typed enums (MEDIUM)

| # | File | Field | Should be |
|---|------|-------|-----------|
| 1 | `ff-cli/src/commands/run.rs` | `ModelRunResult.status: String` | `RunStatus` enum |
| 2 | `ff-cli/src/commands/compile.rs` | `ModelCompileResult.status: String` | `RunStatus` enum |
| 3 | `ff-cli/src/commands/test.rs` | `TestResultOutput.status: String` | `TestStatus` enum |
| 4 | `ff-cli/src/commands/snapshot.rs` | `SnapshotRunResult.status: String` | `RunStatus` enum |
| 5 | `ff-cli/src/commands/diff.rs` | `RowDifference.diff_type: String` | `DiffType` enum |
| 6 | `ff-core/src/config.rs` | `DatabaseConfig.db_type: String` | `DatabaseType` enum |
| 7 | `ff-core/src/exposure.rs` | `Exposure.kind: String` | `ExposureKind` enum |
| 8 | `ff-core/src/metric.rs` | `MetricFile.kind: String` | `MetricKind` enum |

**Fix:** Define proper enums with `Serialize`/`Deserialize` derives.

---

## Batch 9: Mutex unwrap in ff-cli parallel run + async patterns (MEDIUM)

| # | File | Line | Issue |
|---|------|------|-------|
| 1 | `ff-cli/src/commands/run.rs` | - | `std::sync::Mutex` used in async context for `run_results` |
| 2 | `ff-cli/src/commands/run.rs` | - | `std::sync::Mutex` used for `completed` counter |
| 3 | `ff-cli/src/commands/run.rs` | 1386 | `.expect()` on compiled_models lookup |
| 4 | `ff-cli/src/commands/run.rs` | 1721 | `.expect()` on compiled_models lookup |

**Fix:** Replace `std::sync::Mutex` with `tokio::sync::Mutex`, `.expect()` with `.ok_or_else()?`.

---

## Batch 10: Nesting, too_many_arguments, context structs (LOW)

| # | File | Issue |
|---|------|-------|
| 1 | `ff-cli/src/commands/compile.rs` | `#[allow(clippy::too_many_arguments)]` — needs `CompileContext` struct |
| 2 | `ff-cli/src/commands/test.rs` | 4x `#[allow(clippy::too_many_arguments)]` — needs `TestRunContext` |
| 3 | `ff-cli/src/commands/run.rs` | 4x `#[allow(clippy::too_many_arguments)]` — needs `ExecutionContext` |
| 4 | `ff-cli/src/commands/validate.rs` | 470-490: nested `if let` → `let-else` |
| 5 | `ff-core/src/model.rs` | 757-766: nested `if let` → `and_then` |
| 6 | `ff-core/src/config.rs` | 414-422: nested `if let` → `and_then` |

---

## Batch 11: Dead code, visibility, comments (LOW)

| # | File | Issue |
|---|------|-------|
| 1 | `ff-core/src/selector.rs:424-433` | Unused `compute_model_checksum` |
| 2 | `ff-core/src/dag.rs:78` | Unused `_node_name` |
| 3 | `ff-core/src/metric.rs:82-83` | `#[allow(dead_code)]` on unused `version` |
| 4 | `ff-core/src/source.rs:146-149` | Unreachable kind validation |
| 5 | `ff-cli/src/commands/ls.rs:409` | Unused `_order` computation |
| 6 | `ff-cli/src/commands/mod.rs:3-20` | `pub mod` should be `pub(crate) mod` |
| 7 | `ff-core/src/model.rs:288` | `parse_test_definition` should be `pub(crate)` |
| 8 | `ff-core/src/error.rs:104` | Wrong comment (says "YAML" for JSON error) |
| 9 | `ff-sql/src/inline.rs:71` | Magic number `4` → `"WITH".len()` |
| 10 | `ff-sql/src/lineage.rs:399` | Stale comment |

---

## Batch 12: ff-sql and ff-test remaining findings (LOW)

| # | File | Issue |
|---|------|-------|
| 1 | `ff-sql/src/parser.rs:32-36` | Add `UnknownDialect(String)` to `SqlError` |
| 2 | `ff-sql/src/extractor.rs:63,118` | Replace `unwrap_or` with `.expect()` |
| 3 | `ff-sql/src/extractor.rs:123-126` | Document why unknowns go to `external_deps` |
| 4 | `ff-sql/src/lineage.rs:192` | Avoid cloning full `models` HashMap |
| 5 | `ff-sql/src/suggestions.rs:254` | Remove redundant `.to_lowercase()` |
| 6 | `ff-test/src/generator.rs:148` | Remove unused `_registry` parameter |
| 7 | `ff-test/src/generator.rs:62-67` | Escape single quotes in accepted_values test |
| 8 | `ff-jinja/src/builtins.rs:278-299` | Cache `get_builtin_macros()` with `OnceLock` |
