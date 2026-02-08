# Why Featherflow

Featherflow is not a dbt clone. It is a ground-up reimagining of what a SQL transformation framework should be when you remove the legacy constraints.

## What's Wrong with dbt

dbt was built in 2016 as a Python wrapper around Jinja templates. Every design decision flows from that origin:

- **`ref()` and `source()` are the dependency system.** Dependencies are encoded in Jinja function calls, which means the tool must render every template just to build the DAG. This is slow, fragile, and means your SQL isn't actually SQL — it's a Jinja program that happens to emit SQL.
- **CTEs everywhere.** dbt encourages massive CTE chains within single models. This makes models impossible to analyze independently and hides data flow in nested subqueries.
- **No type system.** dbt has zero understanding of what your SQL actually does. It can't tell you that you're joining an INT to a VARCHAR, or that a LEFT JOIN makes a column nullable. It just renders text and throws it at the database.
- **Python overhead.** Every dbt invocation boots a Python runtime, parses YAML, renders Jinja, and builds the DAG. For a 500-model project, this takes 30+ seconds before any SQL executes.
- **Database-agnostic abstraction tax.** dbt abstracts across Postgres, Snowflake, BigQuery, Redshift, and Databricks. This lowest-common-denominator approach means you can't use database-specific features without escape hatches.

## What Featherflow Does Differently

### SQL is the Source of Truth

Models are plain SQL. Write `SELECT * FROM stg_orders` and Featherflow parses the AST to discover that this model depends on `stg_orders`. No `ref()`. No `source()`. No magic. **These functions will never be added.**

The dependency system works because the parser understands SQL. Every table reference in your query is either:
1. A known model (resolved from `models/` directories)
2. A declared external table (listed in `external_tables` in `featherflow.yml`)

If a table reference matches neither, **compilation fails**. There is no "maybe it'll work at runtime" -- you either declare your dependencies explicitly or the tool won't run. This is the enforcement mechanism that replaces `ref()`: instead of wrapping every table name in a function call, you write plain SQL and the tool validates that every table reference resolves to something real.

```sql
-- This IS the model. No Jinja needed for dependencies.
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.amount
FROM staging.stg_orders o
LEFT JOIN staging.stg_customers c
    ON o.customer_id = c.customer_id
```

### No CTEs, No Derived Tables -- This Is the Point

Every model is a single `SELECT`. If you need to compose logic, create another model. This is enforced at parse time -- CTEs produce error S005, derived tables produce S006.

This is not a technical limitation we plan to relax. It is the core design principle. CTEs and derived tables are how engineers build 500-line queries that nobody can read, nobody can trace, and no tool can analyze. Featherflow removes that option entirely -- the guardrails exist so you never build complicated SQL in the first place.

When every model is a single `SELECT`:
- Every query is readable by anyone on the team
- Every dependency is visible in the DAG
- Every column can be traced from source to output
- The tool can run static analysis on each model independently
- Models are small, testable, and reusable

If you think you need a CTE, you need a model. If you think you need a derived table, you need a model. There are no exceptions.

### True Static Analysis

Featherflow lowers SQL into a typed intermediate representation (IR) and runs compiler-style analysis passes:

- **Type inference** catches `INT = VARCHAR` join mismatches before you run anything
- **Nullability tracking** tells you that a LEFT JOIN makes right-side columns nullable
- **Unused column detection** finds columns that are produced but never consumed downstream
- **Join key analysis** flags cross joins, non-equi joins, and incompatible key types

This is what dbt fundamentally cannot do because it never parses the SQL — it just renders and ships text.

### Schema Contracts are Mandatory

Every model has a `.yml` file that declares its columns, types, and tests. This isn't optional. `Model::from_file()` returns an error if the `.yml` is missing.

```yaml
version: 1
description: "Enriched orders with customer data"
columns:
  - name: order_id
    data_type: INTEGER
    tests: [unique, not_null]
  - name: customer_name
    data_type: VARCHAR
    tests: [not_null]
  - name: amount
    data_type: DECIMAL(10,2)
    tests: [non_negative]
```

The static analysis passes use these declarations to validate the SQL. If your `.yml` says `amount` is `DECIMAL(10,2)` but your SQL produces an `INTEGER`, the type inference pass catches it.

### Column-Level Lineage

Because Featherflow parses the actual SQL, it builds true column-level lineage:

```
ff lineage --model fct_orders --column amount --direction upstream
```

This traces `amount` back through every model to its source, showing exactly which columns feed into it and whether any transformations (functions, casts) were applied. dbt can only do table-level lineage because it doesn't parse SQL.

### DuckDB Native

We target DuckDB and only DuckDB. This means:
- Sub-second startup (no Python, no database connection handshake)
- In-process execution (DuckDB is embedded via `duckdb-rs`)
- Full access to DuckDB-specific features (GENERATE_SERIES, regexp_matches, etc.)
- File-based databases that are trivially portable

### Rust Performance

The entire tool is a single compiled binary. There is no runtime, no package manager, no virtual environment. `ff run` on a 100-model project completes in under a second for compilation and validation.

## The Non-Negotiables

These are not configuration options. They are hard constraints.

| Rule | Error | Rationale |
|------|-------|-----------|
| Every `.sql` model must have a matching `.yml` | `MissingSchemaFile` | Schema contracts enable static analysis |
| No CTEs in model SQL | S005 | Composition through DAG, not query nesting |
| No derived tables (subqueries in FROM) | S006 | Same as above |
| Directory-per-model layout | E011, E012 | Clean project structure, no ambiguity |
| DuckDB only | By design | No abstraction tax |

## Who This Is For

Featherflow is for teams that:
- Want compile-time guarantees about their SQL transformations
- Are willing to enforce strict project structure for correctness
- Use DuckDB (or are willing to)
- Value fast iteration cycles over database portability
- Believe that the transformation tool should understand the SQL, not just shuttle text
