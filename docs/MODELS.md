# Models

## What is a Model?

A model in Featherflow is a single `SELECT` statement in a `.sql` file paired with a `.yml` schema definition. When executed, it materializes as a table or view in DuckDB.

## Directory Structure

Every model must follow the directory-per-model layout:

```
models/
  stg_orders/
    stg_orders.sql       # The SQL
    stg_orders.yml       # The schema definition
  stg_customers/
    stg_customers.sql
    stg_customers.yml
  fct_orders/
    fct_orders.sql
    fct_orders.yml
```

**Hard rules:**

- Each model gets its own directory under `models/`
- The directory name, `.sql` file name, and `.yml` file name must all match
- No other files in the model directory
- No nesting (no `models/staging/stg_orders/`)
- No loose `.sql` files at the `models/` root

Violations produce errors E011 (loose file) or E012 (name mismatch).

## The SQL File

A model's `.sql` file contains a single `SELECT` statement. It may optionally start with a `{{ config(...) }}` block.

```sql
{{ config(materialized='table', schema='analytics') }}

SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.amount,
    o.status
FROM staging.stg_orders o
LEFT JOIN staging.stg_customers c
    ON o.customer_id = c.customer_id
```

**What's allowed:**

- Plain `SELECT` statements
- JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
- WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- Scalar subqueries in SELECT, WHERE, or HAVING
- Jinja template expressions (`{{ config() }}`, `{{ var() }}`, built-in macros)
- DuckDB-specific functions and syntax

**What's NOT allowed:**

- CTEs (`WITH ... AS`) -- error S005
- Derived tables / subqueries in FROM (`SELECT * FROM (SELECT ...)`) -- error S006
- INSERT, UPDATE, DELETE, DROP, TRUNCATE -- error S003

The CTE and derived table restrictions are not negotiable. They are the entire reason Featherflow exists. If you think you need a CTE, create a model instead. The DAG is your composition mechanism. Every model stays readable, every dependency stays visible, and every column stays traceable. Featherflow builds the guardrails so you never write complicated SQL that hides logic inside nested queries.

## The Schema File (.yml)

Every model requires a matching `.yml` file that declares the model's contract.

```yaml
version: 1
description: "Fact table for customer orders"
owner: "data-engineering"

columns:
  - name: order_id
    data_type: INTEGER
    description: "Primary key"
    tests:
      - unique
      - not_null

  - name: customer_id
    data_type: INTEGER
    description: "FK to stg_customers"
    tests:
      - not_null
    references:
      model: stg_customers
      column: customer_id

  - name: customer_name
    data_type: VARCHAR
    tests:
      - not_null

  - name: amount
    data_type: DECIMAL(10,2)
    tests:
      - not_null
      - non_negative

  - name: status
    data_type: VARCHAR
    tests:
      - accepted_values:
          values: ['pending', 'completed', 'cancelled']

config:
  materialized: table
  schema: analytics

freshness:
  warn_after: "24 hours"
  error_after: "48 hours"
  loaded_at_field: updated_at

tags:
  - finance
  - critical
```

### Schema Fields

| Field | Required | Description |
| --- | --- | --- |
| `version` | Yes | Schema version (currently `1`) |
| `description` | No | Human-readable model description |
| `owner` | No | Team or person responsible |
| `columns` | Yes | Column definitions (see below) |
| `config` | No | Materialization config (overrides SQL `config()`) |
| `freshness` | No | Data freshness rules |
| `tags` | No | Arbitrary tags for filtering |
| `contract` | No | Contract enforcement settings |
| `deprecated` | No | Mark model as deprecated |
| `deprecation_message` | No | Message shown when deprecated model is referenced |

### Column Fields

| Field | Required | Description |
| --- | --- | --- |
| `name` | Yes | Column name (must match SQL output) |
| `data_type` | No | Expected SQL type (used by static analysis) |
| `description` | No | Human-readable description |
| `primary_key` | No | Mark as primary key |
| `tests` | No | List of schema tests to run |
| `constraints` | No | Column constraints (not_null, etc.) |
| `references` | No | Foreign key reference (model + column) |
| `classification` | No | Data classification (PII, sensitive, etc.) |

## Materialization Types

Set via `{{ config(materialized='...') }}` in SQL or `config.materialized` in YAML.

| Type | Behavior |
| --- | --- |
| `table` | `CREATE TABLE AS SELECT ...` -- full rebuild every run |
| `view` | `CREATE VIEW AS SELECT ...` -- no data stored |
| `incremental` | Inserts new rows on subsequent runs (requires `is_incremental()` logic) |
| `ephemeral` | Not materialized; inlined as CTE into downstream models |

### Incremental Models

Incremental models insert only new data on subsequent runs. Use `is_incremental()` in Jinja:

```sql
{{ config(materialized='incremental') }}

SELECT
    order_id,
    customer_id,
    amount,
    created_at
FROM raw.orders
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

`is_incremental()` returns `true` only when ALL three conditions are met:
1. The model is configured as incremental
2. The target table already exists in DuckDB
3. The `--full-refresh` flag was NOT passed

## Dependency Resolution

**There is no `ref()` function. There is no `source()` function. There never will be.**

Dependencies are extracted automatically from the SQL AST. When Featherflow parses:

```sql
SELECT o.order_id, c.name
FROM staging.stg_orders o
JOIN staging.stg_customers c ON o.customer_id = c.customer_id
```

It walks the AST with `visit_relations()` and discovers references to `staging.stg_orders` and `staging.stg_customers`. These are matched against known models (case-insensitive) to build the dependency graph.

Every table reference in your SQL must resolve to one of:
1. **A known model** -- discovered from `models/` directories
2. **A declared external table** -- listed in `external_tables` in `featherflow.yml`

If a table reference matches neither, **compilation fails**. This is intentional. You either define all your dependencies or the tool refuses to run. This hard failure replaces the need for `ref()` -- the parser knows what tables you're reading from because it parses the SQL, and the config declares which of those tables exist outside the project.

## Schema Tests

Tests declared in the `.yml` file generate SQL that returns failing rows. A count of zero means the test passed.

Available test types:

| Test | What it checks |
| --- | --- |
| `unique` | No duplicate values in the column |
| `not_null` | No NULL values |
| `positive` | All values > 0 |
| `non_negative` | All values >= 0 |
| `accepted_values` | Values within an allowed set |
| `min_value` | Values >= minimum |
| `max_value` | Values <= maximum |
| `regex` | Values match a pattern |
| `relationship` | Foreign key integrity (referenced row exists) |

Custom tests can be defined as Jinja macros in macro files.

## Config Extraction

The `{{ config() }}` block at the top of a model captures metadata without affecting the SQL output. Valid keys:

- `materialized` -- table, view, incremental, ephemeral
- `schema` -- target schema name
- `tags` -- array of tags

The `config()` function returns an empty string so it doesn't affect the rendered SQL. Its arguments are captured by the Jinja environment for use during compilation.
