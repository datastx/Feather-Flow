# Featherflow Architecture

Featherflow is a Rust-native SQL transformation framework for DuckDB. It replaces dbt-core with a fundamentally different philosophy: **SQL is the source of truth**. There are no `ref()` or `source()` functions. Dependencies are extracted directly from the SQL AST via `sqlparser-rs`, enabling true static analysis that guarantees correctness before runtime.

## Design Philosophy

1. **SQL is SQL.** Models are plain `.sql` files. No magic functions, no proprietary DSL. Dependencies are extracted by parsing the AST, not by injecting Jinja macros. **There is no `ref()`. There is no `source()`. There never will be.** You write `FROM stg_orders` and the parser figures out the dependency. If a table doesn't exist as a model, you declare it in `external_tables` in the config -- otherwise compilation fails. This is by design: the SQL you write is the SQL that runs, and the tool understands it because it actually parses it.
2. **One model, one directory, one schema.** Every model lives at `models/<name>/<name>.sql` with a matching `<name>.yml`. No exceptions. No nesting. No loose files.
3. **No CTEs. No derived tables. Ever.** Each model is a single `SELECT` statement. This is not a missing feature -- it is the feature. CTEs and derived tables let engineers hide complexity inside a single query, producing unreadable, untraceable SQL that no tool can analyze. Featherflow removes that option entirely. Composition happens through the DAG: if you need to stage data, create a model. Every model is a single readable SELECT, every dependency is visible in the graph, and every column can be traced from source to output. There is no argument for "just this one CTE" -- the guardrails exist so you never build complicated SQL in the first place.
4. **DuckDB only.** We don't abstract away the database. We target DuckDB directly and exploit its capabilities fully. We will never add a second database backend.
5. **Compile-time correctness.** The IR and analysis passes catch type mismatches, nullable join keys, and unused columns before a single query hits the database.

## System Overview

```
                          featherflow.yml
                               |
                          +----v----+
                          | ff-core |  Config, Project, Model, DAG
                          +----+----+
                               |
              +--------+-------+-------+--------+
              |        |               |        |
         +----v---+ +--v---+     +----v----+ +-v--------+
         |ff-jinja| |ff-sql|     |  ff-db  | | ff-test  |
         +--------+ +--+---+     +---------+ +----------+
          Template    Parse &      DuckDB      Schema test
          rendering   analyze      backend     generation
                       |
                  +----v------+
                  |ff-analysis|
                  +-----------+
                   IR, type system,
                   static analysis
                       |
                  +----v----+
                  | ff-cli  |  18 subcommands
                  +---------+
```

## Crate Breakdown

### `ff-core` — Project Model & DAG

The foundation crate. Owns the project structure, configuration, model discovery, and dependency graph.

**Key types:**
- `Project` — loaded project with all models, config, and sources
- `Config` — deserialized from `featherflow.yml`
- `Model` — a single SQL model with its raw SQL, schema, dependencies, and metadata
- `ModelSchema` — parsed from the `.yml` file (columns, tests, constraints, freshness rules)
- `ModelDag` — directed acyclic graph built from model dependencies via `petgraph`
- `Manifest` — serialized project state for CI diffing and deferred execution
- `RunState` — tracks partial execution for resume-on-failure
- `Source` — external data source definitions

**Critical invariants enforced here:**
- `discover_models_flat()` rejects loose `.sql` files (E011) and directory/file name mismatches (E012)
- `Model::from_file()` unconditionally requires a matching `.yml` file (`CoreError::MissingSchemaFile`)
- `ModelDag::build()` detects circular dependencies

### `ff-sql` — SQL Parsing & Analysis

Parses SQL into an AST, extracts dependencies, validates constraints, and builds column-level lineage.

**Key modules:**
- `parser` — wraps `sqlparser-rs` with DuckDB dialect support
- `extractor` — `extract_dependencies()` uses `visit_relations()` to walk the AST and collect table references, filtering out CTE names
- `validator` — enforces the no-CTE (S005) and no-derived-table (S006) rules. Scalar subqueries in SELECT/WHERE/HAVING are permitted
- `lineage` — `extract_column_lineage()` traces data flow through SELECT expressions, JOINs, and functions. `ProjectLineage::resolve_edges()` connects cross-model column references using table aliases
- `inline` — handles ephemeral model inlining by prepending CTEs (the only place CTEs appear in compiled SQL)
- `suggestions` — analyzes column names and usage patterns to suggest schema tests (unique, not_null, relationship, etc.)

### `ff-analysis` — IR & Static Analysis

Lowers SQL AST into a typed intermediate representation and runs analysis passes.

**IR types:**
- `RelOp` — relational algebra operators: Scan, Project, Filter, Join, Aggregate, Sort, Limit, SetOp
- `TypedExpr` — expressions with resolved types and nullability
- `RelSchema` — output schema with typed columns
- `SqlType` — Boolean, Integer, Float, Decimal, String, Date, Time, Timestamp, Interval, Binary, Unknown
- `Nullability` — NotNull, Nullable, Unknown

**Analysis passes (via `PassManager`):**
| Pass | Scope | Codes | What it checks |
|------|-------|-------|----------------|
| `type_inference` | per-model | A001-A005 | Unknown types, UNION type mismatches, lossy casts, aggregate on wrong type |
| `nullability` | per-model | A010-A012 | Nullable columns from JOINs without null guards, YAML/SQL nullability conflicts, redundant null checks |
| `join_keys` | per-model | A030-A033 | Join key type mismatches, cross joins, non-equi joins |
| `unused_columns` | DAG-level | A020-A021 | Columns produced but never consumed downstream |

**Lowering pipeline:**
```
SQL AST (sqlparser Statement)
    → lower_statement(stmt, schema_catalog)
    → RelOp tree with TypedExpr nodes
    → PassManager runs all passes
    → Vec<Diagnostic>
```

### `ff-jinja` — Template Rendering

Thin Jinja layer for config extraction and macro expansion. Does NOT participate in dependency resolution.

**Key features:**
- `config()` — captures materialization, schema, and tags from the template
- `var()` — variable lookup with defaults from `featherflow.yml`
- `is_incremental()` — returns true only when: model is incremental AND table exists AND not full-refresh
- 17 built-in macros across 5 categories: date/time, string, math, cross-db, utility
- Custom test macro discovery from user-defined `.sql` files via regex

### `ff-db` — Database Backend

DuckDB-specific database operations.

**Key types:**
- `Database` trait — async interface with `execute`, `execute_batch`, `query_count`, `query_sample_rows`, `get_table_schema`, `relation_exists`
- `DuckDbBackend` — implementation using `duckdb-rs` with bundled DuckDB
- Supports both file-based (`target/dev.duckdb`) and in-memory (`:memory:`) databases
- Schema-qualified table creation: `CREATE SCHEMA IF NOT EXISTS` + `CREATE TABLE/VIEW`
- Snapshot support: SCD Type 2 with `execute_snapshot`, insert/update/invalidate operations
- CSV loading with type inference and options

### `ff-test` — Schema Test Execution

Generates and runs test SQL against materialized models.

**Test types:**
| Type | SQL Pattern | What it checks |
|------|-------------|----------------|
| `unique` | `SELECT col, COUNT(*) ... HAVING COUNT(*) > 1` | No duplicate values |
| `not_null` | `SELECT * WHERE col IS NULL` | No NULL values |
| `positive` | `SELECT * WHERE col <= 0` | All values positive |
| `non_negative` | `SELECT * WHERE col < 0` | No negative values |
| `accepted_values` | `SELECT * WHERE col NOT IN (...)` | Values in allowed set |
| `min_value` | `SELECT * WHERE col < min` | Floor constraint |
| `max_value` | `SELECT * WHERE col > max` | Ceiling constraint |
| `regex` | `SELECT * WHERE NOT regexp_matches(col, pattern)` | Pattern match |
| `relationship` | `SELECT ... WHERE NOT EXISTS (...)` | Foreign key integrity |

All test SQL returns **failing rows** — a count of 0 means the test passed.

### `ff-cli` — Command-Line Interface

16 subcommands built with `clap` derive API:

| Command | Description |
|---------|-------------|
| `init` | Scaffold a new project |
| `parse` | Parse SQL and show extracted dependencies |
| `compile` | Render Jinja + validate SQL (no execution) |
| `run` | Execute models in DAG order against DuckDB |
| `ls` | List models with optional filtering |
| `test` | Run schema tests from `.yml` definitions |
| `seed` | Load CSV seed files into DuckDB tables |
| `validate` | Full validation: parse, no-CTE/derived-table check, dependency resolution |
| `docs` | Generate documentation for all models |
| `clean` | Remove target directory and compiled artifacts |
| `snapshot` | Snapshot table state for SCD Type 2 tracking |
| `run-operation` | Execute a standalone macro that returns SQL |
| `freshness` | Check model and source data freshness |
| `diff` | Compare model output between two databases |
| `lineage` | Column-level lineage with table/JSON/DOT output |
| `analyze` | Run static analysis passes and report diagnostics |

## Data Flow: `ff run`

```
1. Load featherflow.yml → Config
2. Discover models via discover_models_flat()
3. For each model:
   a. Read .sql and .yml files
   b. Render Jinja template → compiled SQL
   c. Parse SQL → AST
   d. Extract dependencies from AST
4. Build ModelDag from dependencies
5. Detect circular dependencies
6. Topological sort → execution order
7. For each model in order:
   a. Validate: no CTEs, no derived tables, only SELECT
   b. Execute against DuckDB (CREATE TABLE/VIEW AS ...)
   c. Track in RunState for resume-on-failure
8. Run schema tests if requested
```

## Error Code Taxonomy

| Range | Source | Examples |
|-------|--------|---------|
| E0xx | ff-core | E011 (loose SQL file), E012 (name mismatch), MissingSchemaFile |
| S0xx | ff-sql | S001 (parse error), S002 (empty SQL), S005 (CTE), S006 (derived table) |
| J0xx | ff-jinja | J001 (render error), J002 (unknown variable), J003 (invalid config) |
| A0xx | ff-analysis | A001-A005 (types), A010-A012 (nullability), A020-A021 (unused cols), A030-A033 (joins) |
