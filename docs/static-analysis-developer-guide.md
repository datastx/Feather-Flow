# Static Analysis in Feather Flow — Developer Guide

## Table of Contents

1. [What Is Static Analysis?](#what-is-static-analysis)
2. [How It Works: The DataFusion Logical Planner](#how-it-works-the-datafusion-logical-planner)
3. [Strict Mode (The Only Mode)](#strict-mode-the-only-mode)
4. [The Schema Propagation Pipeline](#the-schema-propagation-pipeline)
5. [Handling Non-SQL Node Kinds](#handling-non-sql-node-kinds)
   - [Seeds](#seeds)
   - [Sources](#sources)
   - [Python Models](#python-models)
   - [Functions](#functions)
6. [Phantom SQL: Bridging Non-SQL Nodes into the Planner](#phantom-sql-bridging-non-sql-nodes-into-the-planner)
7. [YAML Schema Declarations](#yaml-schema-declarations)
8. [What Gets Checked](#what-gets-checked)
9. [Diagnostic Codes Reference](#diagnostic-codes-reference)
10. [Configuration](#configuration)
11. [CLI Usage](#cli-usage)
12. [Developer Workflow](#developer-workflow)
13. [Comparison with dbt](#comparison-with-dbt)

---

## What Is Static Analysis?

Static analysis in Feather Flow validates your SQL models, schema contracts, and
cross-model dependencies **without ever connecting to a database**. It catches:

- Type mismatches between your YAML declarations and what the SQL actually produces
- Missing or extra columns in model output vs. contract
- Nullable columns that violate NOT NULL constraints
- Join key type incompatibilities
- Unused columns across the DAG
- Lossy casts and invalid aggregations

Think of it as a compiler for your data pipeline. Just as `rustc` catches type
errors before your code runs, `ff analyze` catches schema errors before your
models hit the warehouse.

---

## How It Works: The DataFusion Logical Planner

Feather Flow's static analysis is powered by [Apache DataFusion](https://datafusion.apache.org/),
used purely as a **planning engine** — no queries are ever executed.

The core insight: DataFusion's `SqlToRel` planner performs **full type inference**
when converting SQL text into a `LogicalPlan`. Every column in the plan's output
schema carries a concrete Arrow `DataType` and a `nullable` flag. Feather Flow
harnesses this by:

1. Parsing each model's compiled SQL through DataFusion's SQL planner
2. Providing a `ContextProvider` that resolves table names to known schemas
3. Extracting the inferred output schema from the resulting `LogicalPlan`
4. Cross-checking inferred schemas against YAML declarations
5. Feeding each model's inferred schema forward for downstream models

```
                    ┌─────────────────────────┐
                    │   Model YAML + SQL       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  Jinja template render   │
                    │  (ff-jinja)              │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  DataFusion SQL planner  │
                    │  SQL text → LogicalPlan  │
                    │  (resolves tables,       │
                    │   infers all types)      │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  Extract inferred schema │
                    │  Arrow types → SqlType   │
                    └────────────┬────────────┘
                                 │
                ┌────────────────┼────────────────┐
                │                                 │
   ┌────────────▼────────────┐     ┌──────────────▼──────────────┐
   │  Compare vs YAML schema │     │  Register in catalog for    │
   │  → emit diagnostics     │     │  downstream models          │
   └─────────────────────────┘     └─────────────────────────────┘
```

---

## Strict Mode (The Only Mode)

Unlike dbt, which offers three analysis modes (`on`, `unsafe`, `off`), Feather
Flow supports **only strict (ahead-of-time) analysis**. There is no JIT mode,
no `unsafe` escape hatch, and no way to turn analysis off for individual models.

**Why strict-only?**

- **Predictability**: Every model in your DAG is analyzed the same way, every
  time. No "this model is special" exceptions that erode trust in the pipeline.
- **Propagation guarantee**: Static analysis works by propagating schemas through
  the DAG in topological order. If any model opts out, all downstream models
  lose type information. Strict mode ensures the chain is never broken.
- **Simplicity**: One mode means one mental model. You don't need to reason about
  which models are analyzed and which aren't.

The only escape hatch is `--skip-static-analysis` at the CLI level, which
disables analysis for the **entire** run. This is intentionally all-or-nothing.

### The Propagation Constraint

A key architectural constraint: **a model can only be statically analyzed if all
of its parents have known schemas**. In dbt's world, this means disabling
analysis on one model can cascade and disable it for all descendants. Feather
Flow sidesteps this problem entirely — since analysis is always on, the schema
catalog is always complete.

---

## The Schema Propagation Pipeline

Static analysis walks the DAG in **topological order** (dependencies before
dependents). At each step:

```
catalog = { seeds, sources, external tables }     ← initial schemas

for model in topological_order:
    provider = FeatherFlowProvider(catalog)        ← current catalog state
    plan = sql_to_plan(model.compiled_sql, provider)
    inferred = extract_schema(plan)                ← Arrow → SqlType
    mismatches = compare(yaml_schema, inferred)    ← emit diagnostics
    catalog[model.name] = inferred                 ← feed forward
```

This means:
- The first model in the DAG resolves its table references against seed/source schemas
- Each subsequent model sees the **inferred** (not declared) schemas of its upstreams
- Type information flows transitively through the entire graph
- A type change in a staging model is detected in all downstream marts

---

## Handling Non-SQL Node Kinds

Feather Flow's unified `NodeKind` system means the DAG contains more than just
SQL models. Seeds, sources, python models, and functions all participate as
first-class nodes. But the DataFusion planner only understands SQL. So how do
non-SQL nodes enter the schema catalog?

The answer: **every non-SQL node must declare its schema in YAML**, and that
declared schema becomes the node's entry in the `SchemaCatalog`. Non-SQL nodes
never go through the DataFusion planner — their schemas are taken as ground truth
and injected directly into the catalog before propagation begins.

### Seeds

Seeds are CSV files with an optional YAML schema. Their schema enters the catalog
in one of two ways:

1. **Explicit YAML declaration** (preferred): Column names and types declared in
   the `.yml` file are parsed into a `RelSchema` and registered.

2. **CSV header inference**: If no column types are declared, column names come
   from the CSV header and types default to `VARCHAR` (since CSV is inherently
   untyped). The `column_types` map in the YAML can override individual columns.

```yaml
# seeds/raw_orders/raw_orders.yml
kind: seed
columns:
  - name: order_id
    data_type: INTEGER
  - name: customer_id
    data_type: INTEGER
  - name: amount_cents
    data_type: BIGINT
  - name: status
    data_type: VARCHAR
  - name: created_at
    data_type: TIMESTAMP
```

**What the planner sees**: A table named `raw_orders` with exactly these columns
and types. When a SQL model writes `SELECT * FROM raw_orders`, DataFusion resolves
it against this schema.

### Sources

Sources represent external tables that exist outside Feather Flow (e.g., raw
tables loaded by an ingestion pipeline). They have no companion data file — only
a `.yml` definition.

```yaml
# sources/raw_ecommerce/raw_ecommerce.yml
kind: source
schema: raw
tables:
  - name: orders
    columns:
      - name: id
        data_type: INTEGER
      - name: user_id
        data_type: INTEGER
      - name: total
        data_type: DECIMAL(10,2)
      - name: created_at
        data_type: TIMESTAMP
  - name: customers
    columns:
      - name: id
        data_type: INTEGER
      - name: email
        data_type: VARCHAR
      - name: name
        data_type: VARCHAR
```

**What the planner sees**: Tables named `orders` and `customers` with the
declared schemas. If a source table has no `columns` declared, it registers with
an empty schema — any column reference against it will produce a planning error,
which is the desired behavior (it forces you to declare what you expect from
external systems).

### Python Models

Python models execute arbitrary Python code (e.g., pandas, polars, or ML
pipelines) that produces a table. The static analysis engine cannot parse Python
to infer output schemas, so **Python models must fully declare their output
schema in YAML**.

```yaml
# nodes/ml_predictions/ml_predictions.yml
kind: python
columns:
  - name: customer_id
    data_type: INTEGER
    constraints:
      - not_null
  - name: churn_probability
    data_type: DOUBLE
  - name: predicted_segment
    data_type: VARCHAR
  - name: prediction_date
    data_type: DATE
schema:
  depends_on:
    - stg_customers
    - fct_order_history
```

Unlike SQL models where the planner infers the schema from the SQL, Python model
schemas are **authoritative by declaration**. The YAML schema is injected directly
into the catalog. Downstream SQL models that `SELECT FROM ml_predictions` will
resolve columns against this declared schema.

The `depends_on` field is also critical — since the analysis engine can't extract
dependencies from Python code, they must be explicitly declared. This ensures the
DAG topology is correct and the python model's upstream schemas are available.

### Functions

User-defined functions (SQL macros defined via `.yml` + `.sql` pairs) participate
in static analysis differently. They are not table-producing nodes — they are
**registered as stub UDFs** in the DataFusion planner so that SQL models calling
them can be planned successfully.

```yaml
# functions/cents_to_dollars/cents_to_dollars.yml
kind: function
function_type: scalar
arguments:
  - name: amount
    data_type: BIGINT
returns:
  data_type: DECIMAL(18,2)
```

**What the planner sees**: A scalar function `cents_to_dollars(BIGINT) → DECIMAL(18,2)`.
When a model writes `SELECT cents_to_dollars(amount_cents) AS dollars`, DataFusion
resolves the function stub, infers the return type as `DECIMAL(18,2)`, and
propagates that type to the `dollars` column.

Table functions work similarly but register as table sources:

```yaml
# functions/recent_orders/recent_orders.yml
kind: function
function_type: table
arguments:
  - name: days_back
    data_type: INTEGER
    default: "30"
returns:
  columns:
    - name: order_id
      data_type: INTEGER
    - name: amount
      data_type: DECIMAL(10,2)
    - name: order_date
      data_type: DATE
```

**What the planner sees**: A table function `recent_orders(INTEGER)` returning
rows with `(order_id INTEGER, amount DECIMAL(10,2), order_date DATE)`.

---

## Phantom SQL: Bridging Non-SQL Nodes into the Planner

Under the hood, non-SQL nodes need to produce something the DataFusion catalog
can understand. The approach is to generate **phantom SQL** — synthetic `SELECT`
statements that represent the output schema of a non-SQL node. These are never
executed; they exist solely so the planner can construct a `LogicalPlan` and
extract a typed schema.

### How Phantom SQL Works

For a seed with this schema:

```yaml
columns:
  - name: order_id
    data_type: INTEGER
  - name: amount
    data_type: DECIMAL(10,2)
  - name: status
    data_type: VARCHAR
```

The system generates:

```sql
SELECT
  CAST(NULL AS INTEGER) AS order_id,
  CAST(NULL AS DECIMAL(10,2)) AS amount,
  CAST(NULL AS VARCHAR) AS status
WHERE 1 = 0
```

This phantom SQL:
- Produces exactly the declared column names and types
- Returns zero rows (`WHERE 1 = 0`) — it's never meant to produce data
- Uses `CAST(NULL AS <type>)` to get DataFusion to assign concrete Arrow types
- Can be planned by DataFusion just like any other SQL

### When Phantom SQL Is Generated

| Node Kind | Schema Source | Phantom SQL? |
|-----------|-------------|--------------|
| `sql` | Inferred from actual SQL via DataFusion planner | No — uses real SQL |
| `seed` | YAML `columns` or CSV header + `column_types` | Yes — generated from declared schema |
| `source` | YAML `columns` on each table | Yes — generated from declared schema |
| `python` | YAML `columns` (required) | Yes — generated from declared schema |
| `function` | Registered as UDF stub (scalar) or table source (table) | No — uses stub registration |

### Why Not Just Inject Schemas Directly?

You might ask: if we already parse the YAML into a `RelSchema`, why bother
generating SQL at all? Why not just insert the `RelSchema` into the catalog?

In practice, **both approaches are used**:

- **Direct catalog injection** is the primary path. Seed, source, and python
  model schemas are parsed from YAML → `RelSchema` → inserted into the
  `SchemaCatalog` before propagation begins. This is fast and avoids unnecessary
  round-trips through the planner.

- **Phantom SQL** serves as a **validation path**. By round-tripping the schema
  through DataFusion (`YAML → SqlType → Arrow → DataFusion plan → Arrow → SqlType`),
  we verify that our type mappings are consistent and that the declared types are
  valid SQL types that DataFusion recognizes. If a YAML schema declares a type
  that doesn't map cleanly to Arrow, the phantom SQL will fail to plan — catching
  the error early.

Think of phantom SQL as a schema contract test: it proves that the YAML
declaration is expressible in SQL and that the type bridge handles it correctly.

---

## YAML Schema Declarations

Every node kind uses a YAML configuration file. For static analysis to work,
column schemas must be declared. Here's the anatomy of a schema declaration:

```yaml
# nodes/stg_orders/stg_orders.yml
kind: sql
description: Cleaned and standardized orders
columns:
  - name: order_id
    data_type: INTEGER
    description: Primary key
    constraints:
      - not_null
      - primary_key
  - name: customer_id
    data_type: INTEGER
    constraints:
      - not_null
  - name: total_amount
    data_type: DECIMAL(10,2)
  - name: order_date
    data_type: DATE
    constraints:
      - not_null
  - name: status
    data_type: VARCHAR
contract:
  enforced: true
```

### Column Properties

| Property | Required | Description |
|----------|----------|-------------|
| `name` | Yes | Column name (case-insensitive matching) |
| `data_type` | Recommended | SQL type string (e.g., `INTEGER`, `VARCHAR`, `DECIMAL(10,2)`) |
| `description` | No | Human-readable description |
| `constraints` | No | List of `not_null`, `unique`, `primary_key` |

### Contract Enforcement

When `contract.enforced: true` is set:
- **Missing columns** (declared in YAML but absent from SQL output) become **errors**
  that block execution
- **Type mismatches** produce warnings
- **Extra columns** (in SQL but not in YAML) produce warnings

Without a contract, mismatches are still detected and reported, but they don't
block execution.

### Supported Data Types

All DuckDB-compatible SQL types are supported in `data_type`:

| Category | Types |
|----------|-------|
| Integer | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `HUGEINT`, `INT1`-`INT8`, `INT128` |
| Unsigned | `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` |
| Float | `FLOAT`, `DOUBLE`, `REAL`, `DOUBLE PRECISION` |
| Decimal | `DECIMAL(p,s)`, `NUMERIC(p,s)` |
| String | `VARCHAR`, `VARCHAR(n)`, `TEXT`, `CHAR`, `STRING` |
| Boolean | `BOOLEAN`, `BOOL` |
| Temporal | `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIME`, `INTERVAL` |
| Binary | `BLOB`, `BYTEA`, `BINARY` |
| Semi-structured | `JSON`, `JSONB`, `UUID` |
| Complex | `INTEGER[]`, `STRUCT(name VARCHAR, age INTEGER)`, `MAP(VARCHAR, INTEGER)` |

---

## What Gets Checked

### Per-Model Checks

| Code | Check | Severity | Description |
|------|-------|----------|-------------|
| A001 | Unknown column type | Info | Column type couldn't be determined |
| A002 | UNION type mismatch | Warning | Incompatible types across UNION arms |
| A003 | UNION column count | Error | Different column counts in UNION arms |
| A004 | SUM/AVG on string | Warning | Numeric aggregate on a string column |
| A005 | Lossy cast | Info | Potentially lossy type cast (e.g., FLOAT→INT) |
| A010 | Unguarded nullable | Warning | Column nullable from JOIN, used without COALESCE/IS NOT NULL |
| A011 | YAML NOT NULL conflict | Warning | YAML declares NOT NULL but column is nullable from JOIN |
| A012 | Redundant null check | Info | IS NULL on a NOT NULL column |
| A030 | Join key type mismatch | Warning | Join keys have incompatible types |
| A032 | Cross join detected | Info | Cartesian product (no join condition) |
| A033 | Non-equi join | Info | Join uses inequality operator |

### Cross-Model Checks (DAG-Wide)

| Code | Check | Severity | Description |
|------|-------|----------|-------------|
| A020 | Unused column | Info | Column produced but never consumed downstream |
| A040 | Extra column in SQL | Warning | Column in SQL output but not in YAML |
| A040 | Missing column from SQL | **Error** | Column in YAML but missing from SQL output |
| A040 | Type mismatch | Warning | YAML type differs from inferred type |
| A041 | Nullability mismatch | Warning | YAML NOT NULL but SQL infers nullable |

### The Critical Error: Missing Column (A040)

The only cross-model diagnostic that is **Error** severity (and thus blocks
execution) is a column declared in YAML that doesn't appear in the SQL output.
This represents a broken schema contract — downstream models depending on that
column will fail at runtime.

---

## Diagnostic Codes Reference

### Severity Levels

| Level | Behavior |
|-------|----------|
| **Error** | Blocks `ff run` and `ff compile` (unless `--skip-static-analysis`) |
| **Warning** | Reported but does not block execution |
| **Info** | Reported at verbose levels, informational only |

### Diagnostic Output Format

Each diagnostic includes:

```
[A040] ERROR in stg_orders (cross_model_consistency):
  Column 'order_date' declared in YAML but missing from SQL output
  Hint: Add 'order_date' to the SELECT clause or remove it from the YAML schema
```

Fields: code, severity, model name, pass name, message, optional column, optional hint.

---

## Configuration

### Project-Level (`featherflow.yml`)

Static analysis runs automatically during `ff compile` and `ff run`. There is
no per-model opt-out — it's strict mode only.

```yaml
# featherflow.yml
name: my_project
version: "1.0.0"
node_paths: ["nodes"]

# Analysis is always on. These settings control behavior:
analysis:
  # Severity overrides for specific diagnostic codes
  severity_overrides:
    A020: off      # suppress unused-column warnings
    A005: warning  # promote lossy-cast from info to warning
```

### Severity Overrides

You can adjust the severity of any diagnostic code:

| Override Value | Effect |
|---------------|--------|
| `off` | Suppress the diagnostic entirely |
| `info` | Downgrade to info level |
| `warning` | Set to warning level |
| `error` | Promote to error (will block execution) |

---

## CLI Usage

### `ff analyze` — Run Static Analysis

```bash
# Full analysis, table output
ff analyze

# JSON output (for CI/CD integration)
ff analyze --output json

# Filter by severity
ff analyze --min-severity warning

# Filter by specific model
ff analyze --select stg_orders
```

### `ff compile` — Compile with Analysis

```bash
# Compile runs static analysis automatically
ff compile

# Skip static analysis during compile
ff compile --skip-static-analysis
```

### `ff run` — Execute with Pre-Flight Analysis

```bash
# Static analysis runs before execution
ff run

# Skip analysis (use when iterating quickly)
ff run --skip-static-analysis
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No errors (warnings/info may be present) |
| 1 | One or more Error-severity diagnostics found |

---

## Developer Workflow

### 1. Write Your Model

Create a SQL model with its YAML schema:

```sql
-- nodes/stg_orders/stg_orders.sql
SELECT
    id AS order_id,
    customer_id,
    CAST(amount AS DECIMAL(10,2)) AS total_amount,
    created_at::DATE AS order_date,
    status
FROM raw_orders
```

```yaml
# nodes/stg_orders/stg_orders.yml
kind: sql
columns:
  - name: order_id
    data_type: INTEGER
    constraints: [not_null]
  - name: customer_id
    data_type: INTEGER
  - name: total_amount
    data_type: DECIMAL(10,2)
  - name: order_date
    data_type: DATE
  - name: status
    data_type: VARCHAR
contract:
  enforced: true
```

### 2. Run Analysis

```bash
$ ff analyze

  Model         Code   Severity  Message
  ──────────────────────────────────────────────────────────────
  stg_orders    A040   Warning   Column 'total_amount' type mismatch:
                                 YAML=DECIMAL(10,2), inferred=DOUBLE

  1 warning, 0 errors
```

The analysis caught that `CAST(amount AS DECIMAL(10,2))` doesn't produce exactly
`DECIMAL(10,2)` after the DuckDB → Arrow → SqlType round-trip — it's a compatible
numeric type, but flagged as a potential drift.

### 3. Add a Non-SQL Node

When adding a seed:

```yaml
# nodes/product_categories/product_categories.yml
kind: seed
columns:
  - name: category_id
    data_type: INTEGER
  - name: category_name
    data_type: VARCHAR
  - name: parent_category_id
    data_type: INTEGER
```

The seed's schema is immediately available to downstream SQL models:

```sql
-- nodes/dim_products/dim_products.sql
SELECT
    p.product_id,
    p.name AS product_name,
    c.category_name
FROM stg_products p
JOIN product_categories c ON p.category_id = c.category_id
```

Static analysis will verify that `c.category_id` and `c.category_name` exist
in the seed's declared schema and that the join key types are compatible.

### 4. Add a Python Model

```yaml
# nodes/ml_churn_scores/ml_churn_scores.yml
kind: python
columns:
  - name: customer_id
    data_type: INTEGER
    constraints: [not_null]
  - name: churn_score
    data_type: DOUBLE
  - name: score_date
    data_type: DATE
schema:
  depends_on:
    - stg_customers
```

Now downstream SQL models can reference `ml_churn_scores`:

```sql
-- nodes/fct_customer_health/fct_customer_health.sql
SELECT
    c.customer_id,
    c.name,
    m.churn_score,
    CASE WHEN m.churn_score > 0.8 THEN 'high_risk' ELSE 'healthy' END AS risk_tier
FROM stg_customers c
LEFT JOIN ml_churn_scores m ON c.customer_id = m.customer_id
```

Analysis will:
- Verify `m.churn_score` and `m.customer_id` exist in the python model's declared schema
- Flag that `m.churn_score` and `m.customer_id` become nullable from the LEFT JOIN
- Check join key compatibility (`INTEGER = INTEGER`)

### 5. Iterate

The typical development loop is:

```
Edit SQL/YAML → ff analyze → fix diagnostics → ff analyze → ff run
```

Since analysis is fast (no database connection required), you can run it
frequently as part of your inner development loop.

---

## Comparison with dbt

| Aspect | dbt (Fusion Engine) | Feather Flow |
|--------|-------------------|--------------|
| Analysis modes | `on`, `unsafe`, `off` | **Strict only** (always `on`) |
| Per-model opt-out | Yes (`static_analysis: off`) | No — analysis is all-or-nothing |
| JIT analysis | Yes (`unsafe` mode) | No — always ahead-of-time |
| CLI override | `--static-analysis off` | `--skip-static-analysis` |
| Propagation rule | "Only eligible if all parents eligible" | Always eligible (all parents always analyzed) |
| Engine | dbt Fusion (proprietary) | DataFusion LogicalPlan (open source) |
| Node kinds | SQL models only | SQL, seeds, sources, python, functions |
| Non-SQL handling | N/A (models only) | Phantom SQL + direct catalog injection |
| Schema source for non-SQL | N/A | YAML declarations (required) |

### Why No `off` Mode?

dbt introduced `off` primarily to handle models that use **introspective queries**
(queries that inspect the database schema at runtime, like `information_schema`
lookups). These can't be analyzed statically because their behavior depends on
runtime state.

Feather Flow takes a different stance: if a model's output schema can't be
determined statically, it must be **declared in YAML**. This keeps the schema
catalog complete and preserves type safety for all downstream models. The tradeoff
is that developers must maintain accurate YAML schemas for any model whose output
can't be inferred from its SQL — but this is arguably a good practice regardless.

### Why No `unsafe` (JIT) Mode?

dbt's `unsafe` mode defers analysis to just-in-time, running it closer to
execution time when more context is available. Feather Flow doesn't need this
because:

1. All analysis is against declared schemas (seeds, sources, python models) or
   inferred schemas (SQL models) — there's no runtime state to wait for
2. The DataFusion planner resolves everything at planning time using the schema
   catalog
3. If a type can't be determined, it's marked `Unknown` and treated as compatible
   with everything (avoiding false positives without needing a JIT escape hatch)
