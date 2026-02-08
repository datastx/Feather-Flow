# Static Analysis

Featherflow includes a compiler-style static analysis engine that catches errors before any SQL hits the database. This is the core differentiator from dbt, which has zero understanding of what your SQL actually does.

## How It Works

### The Pipeline

```
.sql file
  → Jinja rendering (ff-jinja)
  → SQL parsing (sqlparser-rs via ff-sql)
  → AST validation (no CTEs, no derived tables)
  → IR lowering (ff-analysis)
  → Analysis passes (type inference, nullability, joins, unused columns)
  → Diagnostics
```

### Intermediate Representation (IR)

SQL AST nodes are lowered into a typed relational algebra:

**Relational operators (`RelOp`):**

- `Scan` -- read from a table (leaf node)
- `Project` -- column selection and expression evaluation (SELECT)
- `Filter` -- row filtering (WHERE)
- `Join` -- all join types with typed conditions
- `Aggregate` -- GROUP BY with aggregate functions
- `Sort` -- ORDER BY
- `Limit` -- LIMIT/OFFSET
- `SetOp` -- UNION, INTERSECT, EXCEPT

**Typed expressions (`TypedExpr`):**

Every expression carries its resolved `SqlType` and `Nullability`:

- `ColumnRef` -- reference to a table column with resolved type
- `Literal` -- typed constant value
- `BinaryOp` -- arithmetic, comparison, logical operators
- `UnaryOp` -- NOT, negation
- `FunctionCall` -- function with typed arguments and return type
- `Cast` -- explicit type cast
- `Case` -- CASE/WHEN with typed branches
- `IsNull` -- NULL check
- `Wildcard` -- SELECT *
- `Subquery` -- scalar subquery

**Type system (`SqlType`):**

```
Boolean
Integer { bits: 8|16|32|64|128 }
Float { bits: 32|64 }
Decimal { precision: Option<u16>, scale: Option<u16> }
String { max_length: Option<u32> }
Date
Time
Timestamp
Interval
Binary
Unknown(String)
```

**Nullability:**

```
NotNull   -- guaranteed non-null
Nullable  -- may be null
Unknown   -- nullability not determined
```

### Schema Catalog

The analysis processes models in topological order. Each model's output schema is added to the catalog so downstream models can resolve column types:

```
catalog = {}
for model in topological_order:
    ir = lower(model.sql, catalog)
    catalog[model.name] = ir.output_schema
```

The `.yml` schema definitions seed the catalog with declared types. When a model's SQL produces a different type than declared, the type inference pass flags it.

## Analysis Passes

### Type Inference (A001-A005)

Checks type compatibility across expressions.

| Code | Severity | What it detects |
| --- | --- | --- |
| A001 | Info | Unknown column type (could not resolve) |
| A002 | Warning | Type mismatch in UNION/INTERSECT/EXCEPT columns |
| A003 | Error | UNION operands have different column counts |
| A004 | Warning | Aggregate function (SUM/AVG) applied to string column |
| A005 | Info | Potentially lossy cast (e.g., FLOAT to INTEGER) |

**Example:** A model uses `SUM()` on a VARCHAR column. The pass emits A004. A `CAST(price AS INTEGER)` where `price` is FLOAT emits A005.

### Nullability (A010-A012)

Tracks how nullability propagates through operations.

| Code | Severity | What it detects |
| --- | --- | --- |
| A010 | Warning | Nullable column from JOIN used without a null guard (e.g., COALESCE) |
| A011 | Warning | Column declared NOT NULL in YAML but becomes nullable after JOIN |
| A012 | Info | Redundant IS NULL / IS NOT NULL check on always-NotNull column |

Key rules:
- `LEFT JOIN` makes all right-side columns nullable
- `RIGHT JOIN` makes all left-side columns nullable
- `FULL OUTER JOIN` makes all columns nullable
- `COALESCE(nullable_col, default)` produces non-null if default is non-null
- `IS NOT NULL` in WHERE guards a column against nullability warnings

### Join Key Analysis (A030-A033)

Examines JOIN conditions for correctness.

| Code | Severity | What it detects |
| --- | --- | --- |
| A030 | Warning | Join key type mismatch (e.g., INT = VARCHAR) |
| A032 | Info | Cross join detected (no join condition) |
| A033 | Info | Non-equi join (using >, <, etc. instead of =) |

**Example:** Joining `orders.customer_id` (INTEGER) with `customers.id` (VARCHAR) produces A030. This would silently fail or produce wrong results at runtime.

Compatible types (like INT32 = INT64) do NOT produce diagnostics.

### Unused Column Detection (A020-A021)

DAG-level pass that finds columns produced by a model but never consumed by any downstream model.

| Code | Severity | What it detects |
| --- | --- | --- |
| A020 | Info | Column produced but never used downstream |
| A021 | Info | Model uses SELECT * (cannot determine unused columns) |

Terminal models (no downstream dependents) are skipped since they are final outputs.

## Running Analysis

```bash
# Run all passes on all models
ff analyze

# Run specific passes
ff analyze --pass type_inference,join_keys

# Only show warnings and errors
ff analyze --severity warning

# JSON output for CI integration
ff analyze --output json

# Analyze specific models
ff analyze --models fct_orders,dim_customers
```

## SQL Validation (Pre-Analysis)

Before IR lowering, `ff-sql` enforces structural constraints:

| Code | Error | Description |
| --- | --- | --- |
| S001 | Parse error | SQL syntax error with line/column location |
| S002 | Empty SQL | File contains no SQL statements |
| S003 | Unsupported statement | Non-SELECT statement (INSERT, UPDATE, etc.) |
| S004 | Validation error | Generic validation failure |
| S005 | CTE not allowed | WITH clause detected |
| S006 | Derived table not allowed | Subquery in FROM clause detected |

These are hard errors. The model will not compile. S005 and S006 are not restrictions we plan to relax -- they are the foundation of the entire tool. Featherflow enforces readable, traceable SQL by removing the constructs that let engineers hide complexity. If you need to compose logic, create a model. The DAG handles composition. Every model stays a single readable SELECT, every dependency is explicit, and every column is traceable end-to-end.

## Column-Level Lineage

The lineage engine (separate from analysis passes) traces data flow at the column level:

```
extract_column_lineage(sql_ast, model_name) → ModelLineage
  - For each output column:
    - Which source columns feed into it
    - Whether the mapping is direct (pass-through) or transformed
    - Expression type: column, function, literal, cast, etc.

ProjectLineage::resolve_edges(known_models)
  - Connects cross-model column references using table aliases
  - Produces LineageEdge records with source/target model+column
```

Lineage supports classification propagation: if a column is tagged as PII in its `.yml`, that classification follows it through transformations to downstream models.
