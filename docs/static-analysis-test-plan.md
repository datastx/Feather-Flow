# Static Analysis Test Plan — Comprehensive DuckDB Test Harness

This document defines every test case needed for a comprehensive static analysis test
harness running against DuckDB. Each section maps to a diagnostic code, an analysis pass,
or an infrastructure component. Tests are grouped by **should-pass** (clean SQL, no
diagnostics expected) and **should-fail** (specific diagnostics must fire).

---

## Table of Contents

1. [Fixture Architecture](#1-fixture-architecture)
2. [Type Inference Pass (A001-A005)](#2-type-inference-pass-a001-a005)
3. [Nullability Pass (A010-A012)](#3-nullability-pass-a010-a012)
4. [Unused Column Pass (A020-A021)](#4-unused-column-pass-a020-a021)
5. [Join Key Pass (A030-A033)](#5-join-key-pass-a030-a033)
6. [Cross-Model Consistency (A040-A041)](#6-cross-model-consistency-a040-a041)
7. [Schema Propagation Engine](#7-schema-propagation-engine)
8. [IR Lowering](#8-ir-lowering)
9. [DataFusion Bridge](#9-datafusion-bridge)
10. [DuckDB-Specific SQL Features](#10-duckdb-specific-sql-features)
11. [DuckDB Type Coverage](#11-duckdb-type-coverage)
12. [DuckDB Function Stubs](#12-duckdb-function-stubs)
13. [Multi-Model DAG Scenarios](#13-multi-model-dag-scenarios)
14. [Error Handling & Edge Cases](#14-error-handling--edge-cases)
15. [CLI Integration](#15-cli-integration)
16. [Regression Guard Rails](#16-regression-guard-rails)

---

## 1. Fixture Architecture

Each test scenario should be a self-contained fixture project following the
directory-per-model layout:

```
tests/fixtures/sa_<scenario>/
  featherflow.yml              # project config
  sources/
    raw_sources.yml            # external table definitions
  models/
    <model_name>/
      <model_name>.sql         # model SQL
      <model_name>.yml         # 1:1 YAML schema
```

Fixture categories:
- `sa_pass_*` — projects that should produce zero errors (warnings/info OK)
- `sa_fail_*` — projects that must produce specific diagnostics
- `sa_type_*` — type-system-focused fixtures
- `sa_dag_*` — multi-model DAG topology fixtures
- `sa_duckdb_*` — DuckDB-specific SQL/type fixtures

---

## 2. Type Inference Pass (A001-A005)

### A001 — Unknown Column Type

**Should-fail: column with no type information emits A001 (Info)**

| # | Test Case | SQL Pattern | YAML Schema | Expected |
|---|-----------|-------------|-------------|----------|
| 2.1.1 | Column from unknown-type source | `SELECT mystery_col FROM src` | `src` has `mystery_col` typed as `Unknown` | A001 on `mystery_col` |
| 2.1.2 | Multiple unknown columns | `SELECT a, b, c FROM src` | All three typed `Unknown` | 3x A001 |
| 2.1.3 | Mix of known and unknown | `SELECT id, data FROM src` | `id` is INT, `data` is Unknown | A001 only on `data` |

**Should-pass: all columns have known types, no A001**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.1.4 | All columns typed in YAML | `SELECT id, name FROM src` where both have `data_type` | No A001 |
| 2.1.5 | Computed column with known inputs | `SELECT id + 1 AS inc FROM src` | No A001 (type inferred from INT + INT) |

### A002 — UNION Type Mismatch

**Should-fail: incompatible types across UNION arms emit A002 (Warning)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.2.1 | INT vs VARCHAR in UNION | `SELECT id FROM a UNION ALL SELECT name FROM b` where `id` INT, `name` VARCHAR | A002 on `id`/`name` |
| 2.2.2 | BOOLEAN vs INTEGER | `SELECT active FROM a UNION ALL SELECT count FROM b` | A002 |
| 2.2.3 | DATE vs VARCHAR | `SELECT created_date FROM a UNION ALL SELECT status FROM b` | A002 |
| 2.2.4 | INTERSECT with type mismatch | `SELECT id FROM a INTERSECT SELECT name FROM b` | A002 |
| 2.2.5 | EXCEPT with type mismatch | `SELECT id FROM a EXCEPT SELECT name FROM b` | A002 |
| 2.2.6 | Three-way UNION, middle arm mismatches | `SELECT id FROM a UNION ALL SELECT name FROM b UNION ALL SELECT id FROM c` | A002 on the mismatch pair |
| 2.2.7 | Nested UNION with mismatch | `(SELECT id FROM a UNION ALL SELECT name FROM b) UNION ALL SELECT id FROM c` | A002 |

**Should-pass: compatible types across UNION arms, no A002**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.2.8 | INT32 UNION INT64 | Both numeric, different widths | No A002 (numeric family compatible) |
| 2.2.9 | FLOAT UNION DECIMAL | Both numeric | No A002 |
| 2.2.10 | VARCHAR(50) UNION VARCHAR(100) | Both string family | No A002 |
| 2.2.11 | DATE UNION TIMESTAMP | Both temporal family | No A002 |
| 2.2.12 | Identical types UNION ALL | Same types both sides | No A002 |

### A003 — UNION Column Count Mismatch

**Should-fail: different column counts emit A003 (Error)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.3.1 | Left has 2, right has 1 | `SELECT a, b FROM t1 UNION ALL SELECT c FROM t2` | A003 with left=2, right=1 |
| 2.3.2 | Left has 1, right has 3 | `SELECT a FROM t1 UNION ALL SELECT b, c, d FROM t2` | A003 with left=1, right=3 |
| 2.3.3 | INTERSECT with count mismatch | Same pattern with INTERSECT | A003 |
| 2.3.4 | EXCEPT with count mismatch | Same pattern with EXCEPT | A003 |

**Should-pass: matching column counts**

| # | Test Case | Expected |
|---|-----------|----------|
| 2.3.5 | Both sides have 3 columns | No A003 |
| 2.3.6 | Single column UNION | No A003 |

### A004 — SUM/AVG on String Column

**Should-fail: aggregate on string emits A004 (Warning)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.4.1 | SUM on VARCHAR | `SELECT SUM(name) FROM users` | A004 on `name` |
| 2.4.2 | AVG on VARCHAR | `SELECT AVG(status) FROM orders` | A004 on `status` |
| 2.4.3 | SUM on TEXT alias | `SELECT SUM(description) FROM items` where description is TEXT | A004 |

**Should-pass: aggregate on numeric columns**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.4.4 | SUM on INTEGER | `SELECT SUM(amount) FROM orders` | No A004 |
| 2.4.5 | AVG on DECIMAL | `SELECT AVG(price) FROM products` | No A004 |
| 2.4.6 | SUM on FLOAT | `SELECT SUM(weight) FROM items` | No A004 |
| 2.4.7 | COUNT on VARCHAR | `SELECT COUNT(name) FROM users` | No A004 (COUNT is valid on any type) |
| 2.4.8 | MIN/MAX on VARCHAR | `SELECT MIN(name) FROM users` | No A004 (MIN/MAX valid on strings) |

### A005 — Lossy Cast

**Should-fail: lossy cast emits A005 (Info)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.5.1 | FLOAT to INTEGER | `SELECT CAST(price AS INTEGER) FROM products` (price FLOAT) | A005 |
| 2.5.2 | DECIMAL to INTEGER | `SELECT CAST(amount AS INTEGER) FROM orders` (amount DECIMAL) | A005 |
| 2.5.3 | VARCHAR to INTEGER | `SELECT CAST(code AS INTEGER) FROM items` (code VARCHAR) | A005 |
| 2.5.4 | VARCHAR to FLOAT | `SELECT CAST(rating AS FLOAT) FROM reviews` (rating VARCHAR) | A005 |
| 2.5.5 | TIMESTAMP to DATE | `SELECT CAST(created_at AS DATE) FROM events` (created_at TIMESTAMP) | A005 |
| 2.5.6 | Nested lossy cast | `SELECT CAST(CAST(name AS FLOAT) AS INTEGER) FROM t` | 2x A005 (both casts lossy) |

**Should-pass: safe casts, no A005**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 2.5.7 | INTEGER to BIGINT (widening) | `SELECT CAST(id AS BIGINT) FROM t` | No A005 |
| 2.5.8 | INTEGER to FLOAT | `SELECT CAST(id AS FLOAT) FROM t` | No A005 |
| 2.5.9 | DATE to TIMESTAMP | `SELECT CAST(d AS TIMESTAMP) FROM t` | No A005 |
| 2.5.10 | INTEGER to VARCHAR | `SELECT CAST(id AS VARCHAR) FROM t` | No A005 |
| 2.5.11 | No cast at all | `SELECT id FROM t` | No A005 |

---

## 3. Nullability Pass (A010-A012)

### A010 — Nullable Column from JOIN Without Guard

**Should-fail: unguarded nullable column emits A010 (Warning)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 3.1.1 | LEFT JOIN, right column unguarded | `SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.cid = c.id` | A010 on `name` (and `id` from right) |
| 3.1.2 | RIGHT JOIN, left column unguarded | `SELECT o.id, c.name FROM orders o RIGHT JOIN customers c ON o.cid = c.id` | A010 on `o.id` |
| 3.1.3 | FULL OUTER JOIN, both sides unguarded | `SELECT a.x, b.y FROM a FULL OUTER JOIN b ON a.id = b.id` | A010 on both `x` and `y` |
| 3.1.4 | LEFT JOIN chain (double left) | `a LEFT JOIN b LEFT JOIN c` — `c` columns unguarded | A010 on `c` columns |
| 3.1.5 | Multiple columns from right side | `SELECT o.id, c.name, c.email, c.phone FROM orders o LEFT JOIN customers c ON ...` | A010 on `name`, `email`, `phone` |

**Should-pass: guarded nullable columns, no A010**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 3.1.6 | COALESCE wraps nullable | `SELECT o.id, COALESCE(c.name, 'Unknown') FROM orders o LEFT JOIN customers c ON ...` | No A010 for `name` |
| 3.1.7 | IS NOT NULL in WHERE | `SELECT o.id, c.name FROM o LEFT JOIN c ON ... WHERE c.name IS NOT NULL` | No A010 for `name` |
| 3.1.8 | INNER JOIN (no nullability change) | `SELECT o.id, c.name FROM orders o JOIN customers c ON ...` | No A010 |
| 3.1.9 | COALESCE in CASE expression | `SELECT CASE WHEN c.name IS NULL THEN 'N/A' ELSE c.name END FROM o LEFT JOIN c ON ...` | No A010 for `name` (guarded by CASE) |

### A011 — YAML NOT NULL vs JOIN Nullable

**Should-fail: YAML stricter than inferred emits A011 (Warning)**

| # | Test Case | Setup | Expected |
|---|-----------|-------|----------|
| 3.2.1 | YAML NOT NULL, LEFT JOIN nullable | YAML: `name NOT NULL`. SQL: LEFT JOIN makes `name` nullable | A011 on `name` |
| 3.2.2 | Multiple YAML NOT NULL columns contradicted | YAML: `name NOT NULL, email NOT NULL`. SQL: both from right of LEFT JOIN | A011 on both |
| 3.2.3 | FULL OUTER JOIN contradicts YAML | YAML: `id NOT NULL`. SQL: FULL OUTER JOIN makes `id` nullable | A011 on `id` |

**Should-pass: YAML matches or is more lenient**

| # | Test Case | Setup | Expected |
|---|-----------|-------|----------|
| 3.2.4 | YAML declares Nullable, JOIN makes nullable | YAML: `name Nullable`. SQL: LEFT JOIN | No A011 |
| 3.2.5 | YAML NOT NULL, INNER JOIN preserves | YAML: `name NOT NULL`. SQL: INNER JOIN | No A011 |
| 3.2.6 | No YAML schema for model | Model has no YAML columns defined | No A011 |

### A012 — Redundant IS NULL Check

**Should-fail: redundant null check emits A012 (Info)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 3.3.1 | IS NULL on NOT NULL column | `SELECT id FROM t WHERE id IS NULL` (id NOT NULL) | A012 on `id` |
| 3.3.2 | IS NOT NULL on NOT NULL column | `SELECT id FROM t WHERE id IS NOT NULL` (id NOT NULL) | A012 on `id` |
| 3.3.3 | Compound WHERE with redundant check | `SELECT * FROM t WHERE active = true AND id IS NOT NULL` (id NOT NULL) | A012 on `id` |

**Should-pass: null check on nullable column**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 3.3.4 | IS NULL on Nullable column | `SELECT id FROM t WHERE name IS NULL` (name Nullable) | No A012 |
| 3.3.5 | IS NOT NULL on Nullable column | `SELECT id FROM t WHERE name IS NOT NULL` | No A012 |
| 3.3.6 | No null checks at all | `SELECT id FROM t WHERE id > 0` | No A012 |

---

## 4. Unused Column Pass (A020-A021)

### A020 — Column Produced but Never Consumed Downstream

**Should-fail: unused column emits A020 (Info)**

| # | Test Case | DAG Setup | Expected |
|---|-----------|-----------|----------|
| 4.1.1 | Single unused column | `stg` outputs `id, name, internal_code`. `fct` only uses `id, name` | A020 on `internal_code` in `stg` |
| 4.1.2 | Multiple unused columns | `stg` outputs 5 cols, `fct` uses 2 | A020 on each unused col |
| 4.1.3 | Unused column in diamond DAG | `stg` outputs `id, a, b`. `fct_a` uses `id, a`. `fct_b` uses `id, b` | No A020 (all consumed across dependents) |
| 4.1.4 | Column used in WHERE but not SELECT | `stg` outputs `id, status`. `fct`: `SELECT id FROM stg WHERE status = 'active'` | No A020 for `status` (used in WHERE counts as consumed) |
| 4.1.5 | Column used in JOIN ON but not projected | `stg` outputs `id, fk`. `fct`: `SELECT stg.id FROM stg JOIN other ON stg.fk = other.fk` | No A020 for `fk` (used in JOIN condition) |

**Should-pass: all columns consumed or terminal model**

| # | Test Case | Expected |
|---|-----------|----------|
| 4.1.6 | Terminal model (no dependents) | No A020 (terminal models skipped) |
| 4.1.7 | All columns consumed by downstream | No A020 |
| 4.1.8 | Single model in project | No A020 (terminal) |

### A021 — SELECT * Blocks Detection

**Should-fail: SELECT * in non-terminal model emits A021 (Info)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 4.2.1 | SELECT * in staging model | `stg: SELECT * FROM raw`. `fct` depends on `stg` | A021 on `stg` |
| 4.2.2 | SELECT t.* in non-terminal | `stg: SELECT t.* FROM raw t`. Has dependents | A021 on `stg` |

**Should-pass: no SELECT * or terminal model**

| # | Test Case | Expected |
|---|-----------|----------|
| 4.2.3 | Explicit column list in non-terminal | No A021 |
| 4.2.4 | SELECT * in terminal model | No A021 (terminal skipped) |

---

## 5. Join Key Pass (A030-A033)

### A030 — Join Key Type Mismatch

**Should-fail: incompatible join key types emit A030 (Warning)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 5.1.1 | INT = VARCHAR | `JOIN ON o.id = c.code` (id INT, code VARCHAR) | A030 |
| 5.1.2 | BOOLEAN = INTEGER | `JOIN ON a.flag = b.count` | A030 |
| 5.1.3 | DATE = VARCHAR | `JOIN ON a.dt = b.dt_str` | A030 |
| 5.1.4 | UUID = INTEGER | `JOIN ON a.uuid_id = b.int_id` | A030 |
| 5.1.5 | Compound join with one mismatch | `JOIN ON a.id = b.id AND a.name = b.count` | A030 on the `name = count` pair |

**Should-pass: compatible join key types**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 5.1.6 | INT32 = INT64 | Same numeric family | No A030 |
| 5.1.7 | FLOAT = DECIMAL | Same numeric family | No A030 |
| 5.1.8 | VARCHAR = VARCHAR | Exact match | No A030 |
| 5.1.9 | DATE = TIMESTAMP | Temporal family | No A030 |
| 5.1.10 | Unknown type on one side | One side Unknown, other INT | No A030 (Unknown is compatible with all) |

### A032 — Cross Join Detected

**Should-fail: cross join / missing ON emits A032 (Info)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 5.2.1 | Explicit CROSS JOIN | `FROM a CROSS JOIN b` | A032 |
| 5.2.2 | INNER JOIN without ON | `FROM a JOIN b` (no condition) | A032 |
| 5.2.3 | LEFT JOIN without ON | `FROM a LEFT JOIN b` (no condition) | A032 |
| 5.2.4 | Comma join (implicit cross) | `FROM a, b` (if lowered as cross join) | A032 |

**Should-pass: join with proper condition**

| # | Test Case | Expected |
|---|-----------|----------|
| 5.2.5 | INNER JOIN with ON | No A032 |
| 5.2.6 | LEFT JOIN with ON | No A032 |

### A033 — Non-Equi Join

**Should-fail: non-equality join condition emits A033 (Info)**

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 5.3.1 | Greater-than join | `JOIN ON a.val > b.val` | A033 |
| 5.3.2 | Less-than join | `JOIN ON a.start_date < b.end_date` | A033 |
| 5.3.3 | Not-equals join | `JOIN ON a.id <> b.id` | A033 |
| 5.3.4 | Greater-or-equal | `JOIN ON a.rank >= b.min_rank` | A033 |
| 5.3.5 | Range join (BETWEEN via AND) | `JOIN ON a.val >= b.low AND a.val <= b.high` | A033 for each inequality |
| 5.3.6 | Mixed equi + non-equi | `JOIN ON a.id = b.id AND a.rank > b.rank` | A033 for the `>` condition only |

**Should-pass: equi-join conditions**

| # | Test Case | Expected |
|---|-----------|----------|
| 5.3.7 | Simple equality | `JOIN ON a.id = b.id` | No A033 |
| 5.3.8 | Compound equi-join | `JOIN ON a.id = b.id AND a.code = b.code` | No A033 |

---

## 6. Cross-Model Consistency (A040-A041)

### A040 — Schema Mismatch (ExtraInSql / MissingFromSql / TypeMismatch)

**Should-fail: mismatches emit A040**

| # | Test Case | Setup | Severity | Expected |
|---|-----------|-------|----------|----------|
| 6.1.1 | Extra column in SQL | SQL: `SELECT id, name`. YAML: only `id` | Warning | A040 ExtraInSql on `name` |
| 6.1.2 | Missing column from SQL | SQL: `SELECT id`. YAML: `id, name` | **Error** | A040 MissingFromSql on `name` |
| 6.1.3 | Type mismatch | SQL infers `name` as VARCHAR. YAML: `name: INTEGER` | Warning | A040 TypeMismatch on `name` |
| 6.1.4 | Multiple extras | SQL: `SELECT a, b, c`. YAML: only `a` | Warning | A040 on `b` and `c` |
| 6.1.5 | Multiple missing | SQL: `SELECT a`. YAML: `a, b, c` | Error | A040 on `b` and `c` |
| 6.1.6 | Combo extra + missing | SQL: `SELECT a, c`. YAML: `a, b` | Warning + Error | A040 extra `c`, missing `b` |
| 6.1.7 | INT vs VARCHAR type mismatch | SQL infers INT. YAML: VARCHAR | Warning | A040 TypeMismatch |
| 6.1.8 | BOOLEAN vs INTEGER type mismatch | SQL infers BOOLEAN. YAML: INTEGER | Warning | A040 TypeMismatch |

**Should-pass: schemas match exactly**

| # | Test Case | Expected |
|---|-----------|----------|
| 6.1.9 | SQL and YAML columns match exactly | No A040 |
| 6.1.10 | Compatible types (INT32 sql, INT64 yaml) | No A040 (numeric family compatible) |
| 6.1.11 | No YAML schema defined | No A040 (comparison skipped) |

### A041 — Nullability Mismatch

**Should-fail: YAML NOT NULL but SQL infers nullable emits A041 (Warning)**

| # | Test Case | Setup | Expected |
|---|-----------|-------|----------|
| 6.2.1 | LEFT JOIN makes column nullable | YAML: `name NOT NULL`. SQL: from right side of LEFT JOIN | A041 on `name` |
| 6.2.2 | UNION introduces nullability | YAML: `id NOT NULL`. SQL: one UNION arm has nullable `id` | A041 on `id` |
| 6.2.3 | Aggregate produces nullable | YAML: `total NOT NULL`. SQL: `SUM(amount)` which can return NULL for empty group | A041 on `total` |

**Should-pass: nullability matches**

| # | Test Case | Expected |
|---|-----------|----------|
| 6.2.4 | YAML Nullable, SQL nullable | No A041 |
| 6.2.5 | YAML NOT NULL, SQL NOT NULL | No A041 |
| 6.2.6 | YAML Nullable, SQL NOT NULL (lenient) | No A041 (only checks stricter direction) |

---

## 7. Schema Propagation Engine

### Linear Chain Propagation

| # | Test Case | DAG Shape | Expected |
|---|-----------|-----------|----------|
| 7.1 | Two-model chain | `source → stg` | stg infers schema from source |
| 7.2 | Three-model chain | `source → stg → mart` | mart uses stg's inferred schema |
| 7.3 | Long chain (5+ models) | `raw → stg → int → dim → fct` | Each model inherits correct upstream schema |

### Branching DAG

| # | Test Case | DAG Shape | Expected |
|---|-----------|-----------|----------|
| 7.4 | Fan-out (one-to-many) | `source → A, B, C` | All three plan successfully |
| 7.5 | Fan-in (many-to-one) | `A, B → C (join)` | C sees schemas of both A and B |
| 7.6 | Diamond DAG | `source → B, C; B + C → D` | D plans with both B and C schemas |
| 7.7 | Complex DAG (mixed fan-in/fan-out) | 8+ models with mixed topology | All plan successfully |

### Column Projection Narrowing

| # | Test Case | Expected |
|---|-----------|----------|
| 7.8 | Upstream has 5 cols, downstream selects 2 | Downstream schema has 2 cols |
| 7.9 | Column rename (AS alias) | Downstream sees aliased name, not original |
| 7.10 | Column computed expression | `SELECT a + b AS total` — downstream sees `total` |

### Error Recovery

| # | Test Case | Expected |
|---|-----------|----------|
| 7.11 | Unknown table reference | Failure recorded, propagation continues for other models |
| 7.12 | SQL syntax error in one model | That model fails, others still plan |
| 7.13 | Missing rendered SQL | Failure recorded with "No rendered SQL available" |
| 7.14 | Upstream failure doesn't cascade | If model B fails, model C (depends on different upstream) still plans |

---

## 8. IR Lowering

### Supported SQL Constructs (Should-Pass)

| # | Test Case | SQL Pattern | Expected IR Node |
|---|-----------|-------------|-----------------|
| 8.1 | Simple SELECT | `SELECT a, b FROM t` | Project(Scan) |
| 8.2 | SELECT with WHERE | `SELECT a FROM t WHERE b > 0` | Project(Filter(Scan)) |
| 8.3 | SELECT with alias | `SELECT a AS alias_a FROM t` | Project with renamed column |
| 8.4 | SELECT * | `SELECT * FROM t` | Project with expanded columns |
| 8.5 | INNER JOIN | `FROM a JOIN b ON a.id = b.id` | Join(Inner) |
| 8.6 | LEFT JOIN | `FROM a LEFT JOIN b ON ...` | Join(LeftOuter) |
| 8.7 | RIGHT JOIN | `FROM a RIGHT JOIN b ON ...` | Join(RightOuter) |
| 8.8 | FULL OUTER JOIN | `FROM a FULL OUTER JOIN b ON ...` | Join(FullOuter) |
| 8.9 | CROSS JOIN | `FROM a CROSS JOIN b` | Join(Cross) |
| 8.10 | GROUP BY | `SELECT status, COUNT(*) FROM t GROUP BY status` | Aggregate |
| 8.11 | HAVING | `SELECT status, COUNT(*) FROM t GROUP BY status HAVING COUNT(*) > 1` | Filter(Aggregate) |
| 8.12 | ORDER BY | `SELECT * FROM t ORDER BY id` | Sort |
| 8.13 | LIMIT/OFFSET | `SELECT * FROM t LIMIT 10 OFFSET 5` | Limit |
| 8.14 | UNION ALL | `SELECT a FROM t1 UNION ALL SELECT b FROM t2` | SetOp(UnionAll) |
| 8.15 | UNION | `SELECT a FROM t1 UNION SELECT b FROM t2` | SetOp(Union) |
| 8.16 | INTERSECT | `SELECT a FROM t1 INTERSECT SELECT b FROM t2` | SetOp(Intersect) |
| 8.17 | EXCEPT | `SELECT a FROM t1 EXCEPT SELECT b FROM t2` | SetOp(Except) |
| 8.18 | CAST expression | `SELECT CAST(a AS INTEGER) FROM t` | TypedExpr::Cast |
| 8.19 | CASE expression | `SELECT CASE WHEN a > 0 THEN 'yes' ELSE 'no' END FROM t` | TypedExpr::Case |
| 8.20 | Scalar subquery in SELECT | `SELECT (SELECT MAX(id) FROM t2) AS max_id FROM t` | Allowed (not banned) |
| 8.21 | Scalar subquery in WHERE | `SELECT id FROM t WHERE id > (SELECT MIN(id) FROM t2)` | Allowed |
| 8.22 | Multi-table join | `FROM a JOIN b ON ... JOIN c ON ...` | Nested Join nodes |
| 8.23 | IS NULL / IS NOT NULL | `WHERE name IS NULL` | TypedExpr::IsNull |
| 8.24 | BETWEEN | `WHERE price BETWEEN 10 AND 100` | BinaryOp chain |
| 8.25 | IN list | `WHERE status IN ('active', 'pending')` | Lowered to OR chain or InList |
| 8.26 | LIKE | `WHERE name LIKE '%test%'` | BinaryOp or FunctionCall |
| 8.27 | COALESCE | `SELECT COALESCE(a, b, 'default') FROM t` | FunctionCall(COALESCE) |
| 8.28 | Aliased table | `SELECT t.id FROM table_name t` | Scan with alias |

### Unsupported SQL Constructs (Should-Fail)

| # | Test Case | SQL Pattern | Expected Error |
|---|-----------|-------------|---------------|
| 8.29 | CTE (WITH clause) | `WITH cte AS (SELECT ...) SELECT * FROM cte` | S005 / AE002 |
| 8.30 | Derived table (subquery in FROM) | `SELECT * FROM (SELECT id FROM t) sub` | S006 / AE002 |
| 8.31 | INSERT statement | `INSERT INTO t VALUES (1)` | LoweringFailed (not SELECT) |
| 8.32 | UPDATE statement | `UPDATE t SET a = 1` | LoweringFailed |
| 8.33 | DELETE statement | `DELETE FROM t WHERE id = 1` | LoweringFailed |
| 8.34 | CREATE TABLE | `CREATE TABLE t (id INT)` | LoweringFailed |

### Nullability Tracking Through IR

| # | Test Case | Expected |
|---|-----------|----------|
| 8.35 | LEFT JOIN: right columns nullable | `c.name` becomes Nullable after LEFT JOIN |
| 8.36 | RIGHT JOIN: left columns nullable | `o.id` becomes Nullable after RIGHT JOIN |
| 8.37 | FULL OUTER JOIN: both nullable | Both sides become Nullable |
| 8.38 | INNER JOIN: preserves nullability | No nullability changes |
| 8.39 | Chained LEFT JOINs | Each successive right side adds nullable columns |
| 8.40 | LEFT JOIN then INNER JOIN | Right side of LEFT still nullable after subsequent INNER JOIN |

---

## 9. DataFusion Bridge

### SQL-to-Plan Conversion (Planner)

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 9.1 | Simple SELECT plans successfully | `SELECT id, name FROM source` | LogicalPlan produced |
| 9.2 | JOIN plans with correct schema | `FROM a JOIN b ON a.id = b.id` | Plan has merged schema |
| 9.3 | Aggregate plans correctly | `SELECT COUNT(*) FROM t` | Plan with aggregate node |
| 9.4 | DuckDB function resolves | `SELECT date_trunc('month', ts) FROM t` | Plan resolves `date_trunc` stub |
| 9.5 | Unknown table fails | `SELECT * FROM nonexistent` | AE008 PlanningError |
| 9.6 | DuckDB dialect syntax | DuckDB-specific SQL like `SELECT 1::INTEGER` | Plans successfully |

### Type Conversion Roundtrips

| # | SqlType | Arrow Type | Roundtrip? | Notes |
|---|---------|------------|------------|-------|
| 9.7 | Boolean | Boolean | Yes | |
| 9.8 | Integer(I8) | Int8 | Yes | TINYINT |
| 9.9 | Integer(I16) | Int16 | Yes | SMALLINT |
| 9.10 | Integer(I32) | Int32 | Yes | INTEGER |
| 9.11 | Integer(I64) | Int64 | Yes | BIGINT |
| 9.12 | HugeInt | Decimal128(38,0) | Yes | Special-case detection |
| 9.13 | Float(F32) | Float32 | Yes | FLOAT |
| 9.14 | Float(F64) | Float64 | Yes | DOUBLE |
| 9.15 | Decimal(10,2) | Decimal128(10,2) | Yes | |
| 9.16 | String | Utf8 | Yes | VARCHAR/TEXT |
| 9.17 | Date | Date32 | Yes | |
| 9.18 | Time | Time64(us) | Yes | |
| 9.19 | Timestamp | Timestamp(us,None) | Yes | |
| 9.20 | Interval | Interval(DayTime) | Yes | |
| 9.21 | Binary | Binary | Yes | BLOB |
| 9.22 | Json | Utf8 | **Lossy** | Loses JSON semantics |
| 9.23 | Uuid | Utf8 | **Lossy** | Loses UUID semantics |
| 9.24 | Array(Int32) | List(Int32) | Yes | INTEGER[] |
| 9.25 | Struct(fields) | Struct(fields) | Yes | |
| 9.26 | Map(Varchar,Int) | Map(Utf8,Int32) | Yes | |
| 9.27 | Unknown | Utf8 | **Fallback** | Defaults to string |

### Unsigned Integer Widening (Arrow → SqlType)

| # | Arrow Type | Expected SqlType | Notes |
|---|------------|-----------------|-------|
| 9.28 | UInt8 | Integer(I16) | Widened to fit unsigned range |
| 9.29 | UInt16 | Integer(I32) | Widened |
| 9.30 | UInt32 | Integer(I64) | Widened |
| 9.31 | UInt64 | HugeInt | Widened to 128-bit |

### Schema Extraction from LogicalPlan

| # | Test Case | Expected |
|---|-----------|----------|
| 9.32 | Extract column names | Correct names from plan output |
| 9.33 | Extract types | Types match Arrow-to-SqlType conversion |
| 9.34 | Extract nullability | Nullable fields marked correctly |
| 9.35 | Computed column naming | `SELECT a + b AS total` yields column named "total" |

---

## 10. DuckDB-Specific SQL Features

These test cases ensure the static analysis engine can plan and analyze SQL
that uses DuckDB-specific syntax and extensions.

### DuckDB SQL Syntax

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 10.1 | Type cast shorthand | `SELECT 42::INTEGER` | Plans successfully |
| 10.2 | String escape | `SELECT 'it''s a test'` | Plans successfully |
| 10.3 | EXCLUDE clause | `SELECT * EXCLUDE (col_a) FROM t` | Plans if supported, error if not |
| 10.4 | REPLACE clause | `SELECT * REPLACE (col_a * 2 AS col_a) FROM t` | Plans if supported |
| 10.5 | QUALIFY clause | `SELECT *, ROW_NUMBER() OVER () AS rn FROM t QUALIFY rn = 1` | Plans if supported |
| 10.6 | SAMPLE clause | `SELECT * FROM t USING SAMPLE 10%` | Plans if supported |
| 10.7 | Positional join | `SELECT * FROM t1 POSITIONAL JOIN t2` | Plans if supported |
| 10.8 | STRUCT literal | `SELECT {'name': 'Alice', 'age': 30}` | Plans successfully |
| 10.9 | LIST literal | `SELECT [1, 2, 3]` | Plans successfully |
| 10.10 | MAP literal | `SELECT MAP {'key': 'value'}` | Plans successfully |
| 10.11 | Lambda expression | `SELECT list_transform([1,2,3], x -> x + 1)` | Plans if supported |
| 10.12 | PIVOT | `PIVOT t ON col USING SUM(val)` | Plans if supported, else graceful error |
| 10.13 | UNPIVOT | `UNPIVOT t ON col1, col2 INTO NAME key VALUE val` | Plans if supported |

### DuckDB Data Types in YAML

| # | Test Case | YAML data_type | Expected SqlType |
|---|-----------|---------------|-----------------|
| 10.14 | TINYINT | `TINYINT` | Integer(I8) |
| 10.15 | SMALLINT | `SMALLINT` | Integer(I16) |
| 10.16 | INTEGER | `INTEGER` | Integer(I32) |
| 10.17 | BIGINT | `BIGINT` | Integer(I64) |
| 10.18 | HUGEINT | `HUGEINT` | HugeInt |
| 10.19 | UTINYINT | `UTINYINT` | Parsed correctly (unsigned) |
| 10.20 | USMALLINT | `USMALLINT` | Parsed correctly |
| 10.21 | UINTEGER | `UINTEGER` | Parsed correctly |
| 10.22 | UBIGINT | `UBIGINT` | Parsed correctly |
| 10.23 | FLOAT | `FLOAT` | Float(F32) |
| 10.24 | DOUBLE | `DOUBLE` | Float(F64) |
| 10.25 | DECIMAL(10,2) | `DECIMAL(10,2)` | Decimal { precision: 10, scale: 2 } |
| 10.26 | VARCHAR | `VARCHAR` | String { max_length: None } |
| 10.27 | VARCHAR(255) | `VARCHAR(255)` | String { max_length: Some(255) } |
| 10.28 | TEXT | `TEXT` | String { max_length: None } |
| 10.29 | BOOLEAN | `BOOLEAN` | Boolean |
| 10.30 | DATE | `DATE` | Date |
| 10.31 | TIMESTAMP | `TIMESTAMP` | Timestamp |
| 10.32 | TIMESTAMPTZ | `TIMESTAMPTZ` | Timestamp (no timezone preserved) |
| 10.33 | TIME | `TIME` | Time |
| 10.34 | INTERVAL | `INTERVAL` | Interval |
| 10.35 | BLOB | `BLOB` | Binary |
| 10.36 | JSON | `JSON` | Json |
| 10.37 | UUID | `UUID` | Uuid |
| 10.38 | INTEGER[] | `INTEGER[]` | Array(Integer(I32)) |
| 10.39 | VARCHAR[] | `VARCHAR[]` | Array(String) |
| 10.40 | Nested array | `INTEGER[][]` | Array(Array(Integer(I32))) |
| 10.41 | STRUCT | `STRUCT(name VARCHAR, age INTEGER)` | Struct([("name", String), ("age", Integer)]) |
| 10.42 | MAP | `MAP(VARCHAR, INTEGER)` | Map { key: String, value: Integer } |
| 10.43 | Nested STRUCT | `STRUCT(addr STRUCT(city VARCHAR, zip VARCHAR))` | Nested Struct |
| 10.44 | INT1 alias | `INT1` | Integer(I8) |
| 10.45 | INT2 alias | `INT2` | Integer(I16) |
| 10.46 | INT4 alias | `INT4` | Integer(I32) |
| 10.47 | INT8 alias | `INT8` | Integer(I64) |
| 10.48 | INT128 alias | `INT128` | HugeInt |
| 10.49 | LONG alias | `LONG` | Integer(I64) |
| 10.50 | REAL alias | `REAL` | Float(F32) |
| 10.51 | DOUBLE PRECISION | `DOUBLE PRECISION` | Float(F64) |
| 10.52 | CHAR | `CHAR` | String { max_length: None } |
| 10.53 | BYTEA alias | `BYTEA` | Binary |
| 10.54 | JSONB alias | `JSONB` | Json |
| 10.55 | DATETIME alias | `DATETIME` | Timestamp |
| 10.56 | Unrecognized type | `MY_CUSTOM_TYPE` | Unknown("MY_CUSTOM_TYPE") |

---

## 11. DuckDB Type Coverage

### Type Compatibility Matrix

Test that `is_compatible_with` returns correct results for all pairings:

| Left Type | Right Type | Expected Compatible? |
|-----------|------------|---------------------|
| Integer(I32) | Integer(I64) | Yes (numeric family) |
| Integer(I32) | Float(F64) | Yes (numeric family) |
| Integer(I32) | Decimal(10,2) | Yes (numeric family) |
| Float(F32) | Decimal(10,2) | Yes (numeric family) |
| HugeInt | Integer(I64) | Yes (numeric family) |
| String | String | Yes |
| String(50) | String(100) | Yes |
| Date | Timestamp | Yes (temporal) |
| Timestamp | Date | Yes (temporal) |
| Date | Date | Yes |
| Boolean | Boolean | Yes |
| Binary | Binary | Yes |
| Json | Json | Yes |
| Uuid | Uuid | Yes |
| Interval | Interval | Yes |
| Array(Int) | Array(Int) | Yes |
| Array(Int) | Array(Str) | No (element mismatch) |
| Struct(a Int) | Struct(a Int) | Yes |
| Struct(a Int) | Struct(a Str) | No (field mismatch) |
| Struct(2 fields) | Struct(3 fields) | No (count mismatch) |
| Map(Str,Int) | Map(Str,Int) | Yes |
| Map(Str,Int) | Map(Int,Int) | No (key mismatch) |
| Unknown | anything | Yes (always compatible) |
| anything | Unknown | Yes (always compatible) |
| Integer | String | **No** |
| Integer | Boolean | **No** |
| Integer | Date | **No** |
| String | Boolean | **No** |
| String | Date | **No** |
| Boolean | Date | **No** |

---

## 12. DuckDB Function Stubs

### Scalar Functions (Should Plan Successfully)

| # | Function | SQL Pattern | Expected Return Type |
|---|----------|-------------|---------------------|
| 12.1 | date_trunc | `SELECT date_trunc('month', ts) FROM t` | Timestamp |
| 12.2 | date_part | `SELECT date_part('year', ts) FROM t` | Int64 |
| 12.3 | date_diff | `SELECT date_diff('day', ts1, ts2) FROM t` | Int64 |
| 12.4 | date_add | `SELECT date_add(ts, INTERVAL '1' DAY) FROM t` | Timestamp |
| 12.5 | datediff | `SELECT datediff('day', ts1, ts2) FROM t` | Int64 |
| 12.6 | dateadd | `SELECT dateadd('day', 1, ts) FROM t` | Timestamp |
| 12.7 | strftime | `SELECT strftime(ts, '%Y-%m-%d') FROM t` | Utf8 |
| 12.8 | strptime | `SELECT strptime('2024-01-01', '%Y-%m-%d')` | Timestamp |
| 12.9 | epoch | `SELECT epoch(ts) FROM t` | Int64 |
| 12.10 | epoch_ms | `SELECT epoch_ms(1234567890) FROM t` | Timestamp |
| 12.11 | regexp_matches | `SELECT regexp_matches(name, '^A.*') FROM t` | Boolean |
| 12.12 | regexp_replace | `SELECT regexp_replace(name, 'foo', 'bar') FROM t` | Utf8 |
| 12.13 | regexp_extract | `SELECT regexp_extract(name, '(\d+)') FROM t` | Utf8 |
| 12.14 | coalesce | `SELECT coalesce(a, b, 'default') FROM t` | Utf8 |
| 12.15 | ifnull | `SELECT ifnull(name, 'unknown') FROM t` | Utf8 |
| 12.16 | nullif | `SELECT nullif(a, b) FROM t` | Utf8 |
| 12.17 | struct_pack | `SELECT struct_pack(a, b, c) FROM t` | Utf8 |
| 12.18 | struct_extract | `SELECT struct_extract(s, 'name') FROM t` | Utf8 |
| 12.19 | list_value | `SELECT list_value(1, 2, 3)` | Utf8 |
| 12.20 | list_extract | `SELECT list_extract(arr, 1) FROM t` | Utf8 |
| 12.21 | unnest | `SELECT unnest(arr) FROM t` | Utf8 |
| 12.22 | generate_series | `SELECT generate_series(1, 10)` | Int64 |
| 12.23 | hash | `SELECT hash(name) FROM t` | Int64 |
| 12.24 | md5 | `SELECT md5(name) FROM t` | Utf8 |
| 12.25 | format | `SELECT format('Hello, {}', name) FROM t` | Utf8 |
| 12.26 | printf | `SELECT printf('%s: %d', name, id) FROM t` | Utf8 |
| 12.27 | string_split | `SELECT string_split(name, ',') FROM t` | Utf8 |

### Aggregate Functions (Should Plan Successfully)

| # | Function | SQL Pattern | Expected Return Type |
|---|----------|-------------|---------------------|
| 12.28 | sum | `SELECT sum(amount) FROM t` | Float64 |
| 12.29 | avg | `SELECT avg(price) FROM t` | Float64 |
| 12.30 | min | `SELECT min(name) FROM t` | Utf8 |
| 12.31 | max | `SELECT max(name) FROM t` | Utf8 |
| 12.32 | count | `SELECT count(id) FROM t` | Int64 |
| 12.33 | string_agg | `SELECT string_agg(name, ',') FROM t` | Utf8 |
| 12.34 | group_concat | `SELECT group_concat(name) FROM t` | Utf8 |
| 12.35 | array_agg | `SELECT array_agg(name) FROM t` | Utf8 |
| 12.36 | bool_and | `SELECT bool_and(active) FROM t` | Boolean |
| 12.37 | bool_or | `SELECT bool_or(active) FROM t` | Boolean |
| 12.38 | approx_count_distinct | `SELECT approx_count_distinct(name) FROM t` | Int64 |
| 12.39 | approx_quantile | `SELECT approx_quantile(amount, 0.5) FROM t` | Float64 |
| 12.40 | median | `SELECT median(amount) FROM t` | Float64 |
| 12.41 | mode | `SELECT mode(status) FROM t` | Utf8 |
| 12.42 | arg_min | `SELECT arg_min(name, amount) FROM t` | Utf8 |
| 12.43 | arg_max | `SELECT arg_max(name, amount) FROM t` | Utf8 |

### Missing DuckDB Functions (Should-Fail to Plan)

| # | Function | Notes | Expected |
|---|----------|-------|----------|
| 12.44 | list_transform | Lambda-based, not stubbed | AE008 or equivalent |
| 12.45 | list_filter | Lambda-based, not stubbed | AE008 |
| 12.46 | read_csv | Table function, not stubbed | AE008 |
| 12.47 | read_parquet | Table function, not stubbed | AE008 |
| 12.48 | range | May not be stubbed | AE008 |
| 12.49 | row_number (window) | Window functions may need stubs | Verify behavior |
| 12.50 | lag/lead (window) | Window functions | Verify behavior |

---

## 13. Multi-Model DAG Scenarios

### End-to-End Fixture Projects

| # | Fixture Name | Topology | Models | What It Tests |
|---|-------------|----------|--------|---------------|
| 13.1 | `sa_pass_clean_ecommerce` | raw → stg → int → dim/fct | 8-10 models | Full clean project, zero diagnostics |
| 13.2 | `sa_pass_simple_chain` | src → stg → mart | 3 models | Basic linear propagation |
| 13.3 | `sa_pass_diamond` | src → A, B → C | 4 models | Diamond dependency, schema merge |
| 13.4 | `sa_pass_wide_fanout` | src → A, B, C, D, E | 6 models | One source, many consumers |
| 13.5 | `sa_pass_deep_chain` | raw → stg → int1 → int2 → dim → fct | 6 models | Deep linear chain, schema narrowing |
| 13.6 | `sa_fail_type_mismatch_chain` | src → stg → fct | 3 models | A002 in stg UNION, A030 in fct JOIN |
| 13.7 | `sa_fail_null_violations` | src → stg → fct | 3 models | A010, A011, A041 across chain |
| 13.8 | `sa_fail_schema_drift` | src → stg → fct | 3 models | A040 extras and missing |
| 13.9 | `sa_fail_unused_columns` | src → stg → fct | 3 models | A020, A021 in staging layer |
| 13.10 | `sa_fail_join_issues` | src → stg → fct | 3 models | A030, A032, A033 in fact layer |
| 13.11 | `sa_fail_mixed_diagnostics` | 6+ models | Various | Multiple diagnostic codes across DAG |
| 13.12 | `sa_pass_all_duckdb_types` | src → model | 2 models | Every DuckDB type in YAML + SQL |
| 13.13 | `sa_pass_all_duckdb_functions` | src → model | 2 models | Every stubbed DuckDB function |
| 13.14 | `sa_pass_complex_joins` | 5+ sources → mart | 6 models | Multiple join types, compound keys |
| 13.15 | `sa_pass_aggregations` | src → stg → metrics | 3 models | GROUP BY, HAVING, various aggregates |

### Incremental Model Considerations

| # | Test Case | Expected |
|---|-----------|----------|
| 13.16 | Incremental model with WHERE clause | Static analysis applies to the full SQL (not just the incremental part) |
| 13.17 | Incremental model with unique_key | YAML unique_key columns should exist in SQL output |
| 13.18 | Snapshot model | Analysis should handle snapshot SQL patterns |

---

## 14. Error Handling & Edge Cases

### Infrastructure Errors (AE001-AE008)

| # | Code | Test Case | Trigger | Expected |
|---|------|-----------|---------|----------|
| 14.1 | AE001 | Lowering failure | Unsupported SQL construct | LoweringFailed error |
| 14.2 | AE002 | CTE in model SQL | `WITH cte AS (...) SELECT ...` | UnsupportedConstruct error |
| 14.3 | AE002 | Derived table in FROM | `SELECT * FROM (SELECT ...)` | UnsupportedConstruct error |
| 14.4 | AE003 | Unknown table in catalog | Model references table not in catalog | UnknownTable error |
| 14.5 | AE004 | Unresolved column | `SELECT nonexistent_col FROM t` | UnresolvedColumn error |
| 14.6 | AE005 | SQL parse error | `SELECT FROM WHERE` (garbage SQL) | SqlParse error |
| 14.7 | AE008 | DataFusion planning error | Table not found during planning | PlanningError error |

### Edge Cases

| # | Test Case | SQL Pattern | Expected |
|---|-----------|-------------|----------|
| 14.8 | Empty SELECT list | `SELECT FROM t` | Parse error |
| 14.9 | SELECT with no FROM | `SELECT 1 AS val` | Plans successfully (literal query) |
| 14.10 | Self-join | `FROM t t1 JOIN t t2 ON t1.id = t2.parent_id` | Plans with correct schema |
| 14.11 | Same column name from two tables | `FROM a JOIN b ON a.id = b.id` where both have `name` | Ambiguous column handled |
| 14.12 | Very long column name | 128+ char column name | Handled without truncation |
| 14.13 | Special characters in alias | `SELECT id AS "my column!" FROM t` | Handled correctly |
| 14.14 | Unicode column names | `SELECT id AS "日本語" FROM t` | Handled correctly |
| 14.15 | Empty model (just SELECT 1) | `SELECT 1 AS dummy` | Plans, schema has one column |
| 14.16 | Column aliased to same name | `SELECT id AS id FROM t` | No confusion |
| 14.17 | Multiple aggregates | `SELECT SUM(a), AVG(b), COUNT(*), MIN(c), MAX(d) FROM t` | All plan correctly |
| 14.18 | Deeply nested expression | `SELECT CAST(COALESCE(CASE WHEN ... END, 0) AS BIGINT) FROM t` | Plans, type inferred |
| 14.19 | NULL literal | `SELECT NULL AS empty_col FROM t` | Type is Unknown/Null |
| 14.20 | Boolean literal | `SELECT TRUE AS flag FROM t` | Type is Boolean |
| 14.21 | Numeric overflow in literal | `SELECT 99999999999999999999 FROM t` | Handled (HugeInt or error) |
| 14.22 | Case sensitivity | `SELECT ID FROM t` vs schema has `id` | Case-insensitive match |
| 14.23 | Mixed case YAML vs SQL | YAML: `MyColumn`, SQL: `mycolumn` | Case-insensitive comparison |

---

## 15. CLI Integration

### `ff validate` Command

| # | Test Case | Expected |
|---|-----------|----------|
| 15.1 | Clean project, no diagnostics | Exit code 0, "Validation passed" |
| 15.2 | Project with warnings only | Exit code 0, warnings displayed |
| 15.3 | Project with errors | Exit code non-zero, errors displayed |
| 15.4 | `--strict` flag with warnings | Exit code non-zero (warnings treated as errors) |
| 15.5 | `--pass type_inference` filter | Only A001-A005 diagnostics shown |
| 15.6 | `--skip-pass nullability` | A010-A012 suppressed |

### `ff compile` Command

| # | Test Case | Expected |
|---|-----------|----------|
| 15.7 | Static analysis runs during compile | Diagnostics shown after compilation |
| 15.8 | SA errors block compile | SA01 (MissingFromSql) prevents successful compile |
| 15.9 | `--skip-static-analysis` bypasses | Compile succeeds even with SA errors |

### `ff run` Command

| # | Test Case | Expected |
|---|-----------|----------|
| 15.10 | SA errors block run | A040 Error prevents execution |
| 15.11 | SA warnings allow run | Warnings shown, execution proceeds |
| 15.12 | `--skip-static-analysis` bypasses | Run proceeds even with SA errors |
| 15.13 | Pre-execution analysis | `run_pre_execution_analysis()` runs schema propagation |

### `ff analyze` Command

| # | Test Case | Expected |
|---|-----------|----------|
| 15.14 | Full analysis output | All diagnostics from all passes |
| 15.15 | JSON output format | Structured diagnostic JSON |
| 15.16 | Filter by model | Only diagnostics for specified model |
| 15.17 | Filter by severity | Only errors, or errors + warnings |

---

## 16. Regression Guard Rails

### Previously Fixed Bugs (Add a Test for Each)

| # | Regression | Test Case |
|---|-----------|-----------|
| 16.1 | sqlparser version mismatch | Ensure SQL re-parsed through DataFusion's 0.59 parser, not Feather-Flow's 0.60 |
| 16.2 | HugeInt roundtrip | Verify `HugeInt → Decimal128(38,0) → HugeInt` doesn't become `Decimal(38,0)` |
| 16.3 | Case-insensitive column matching | Schema comparison should be case-insensitive |
| 16.4 | NULL propagation through UNION | If one arm produces nullable, result should be nullable |
| 16.5 | Empty YAML columns list | Model with empty `columns: []` should not crash |
| 16.6 | Circular dependency detection | Circular DAG should fail at project load, not during analysis |

### Performance Guard Rails

| # | Test Case | Expected |
|---|-----------|----------|
| 16.7 | 50+ model project | Completes analysis in < 5 seconds |
| 16.8 | Model with 100+ columns | No performance degradation in schema comparison |
| 16.9 | Deep join chain (10+ joins) | IR lowering completes without stack overflow |

---

## Summary Statistics

| Category | Should-Pass | Should-Fail | Total |
|----------|-------------|-------------|-------|
| Type Inference (A001-A005) | 16 | 19 | 35 |
| Nullability (A010-A012) | 10 | 11 | 21 |
| Unused Columns (A020-A021) | 5 | 4 | 9 |
| Join Keys (A030-A033) | 8 | 12 | 20 |
| Cross-Model (A040-A041) | 5 | 11 | 16 |
| Schema Propagation | 10 | 4 | 14 |
| IR Lowering | 28 | 6 | 34 |
| DataFusion Bridge | 35 | 0 | 35 |
| DuckDB SQL Features | 13 | 0 | 13 |
| DuckDB Type Coverage | 43 | 0 | 43 |
| DuckDB Function Stubs | 43 | 7 | 50 |
| Multi-Model DAG | 18 | 0 | 18 |
| Error Handling & Edge Cases | 16 | 7 | 23 |
| CLI Integration | 17 | 0 | 17 |
| Regression Guard Rails | 9 | 0 | 9 |
| **Total** | **~276** | **~81** | **~357** |
