# Featherflow Technical Specification

A dbt-inspired CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB (primary target), with dialect-agnostic architecture for future database support.

---

## Table of Contents

1. [Vision & Philosophy](#vision--philosophy)
2. [Key Differentiators from dbt](#key-differentiators-from-dbt)
3. [Architecture Decisions & Rationale](#architecture-decisions--rationale)
4. [Configuration](#configuration)
5. [Project Structure](#project-structure)
6. [Subcommand Specifications](#subcommand-specifications)
7. [Static Analysis Engine](#static-analysis-engine)
8. [Sources Specification](#sources-specification)
9. [Custom Macros System](#custom-macros-system)
10. [Selection Syntax](#selection-syntax)
11. [Schema Testing Framework](#schema-testing-framework)
12. [Incremental Models](#incremental-models)
13. [Snapshots](#snapshots)
14. [Hooks & Operations](#hooks--operations)
15. [Error Handling](#error-handling)
16. [Known Limitations & Edge Cases](#known-limitations--edge-cases)
17. [Testing Strategy](#testing-strategy)
18. [Roadmap & Implementation Status](#roadmap--implementation-status)
19. [Dialect Extensibility](#dialect-extensibility)
20. [CI/CD](#cicd)
21. [Development Workflow](#development-workflow)
22. [Appendix: dbt Comparison](#appendix-dbt-comparison)

---

## Vision & Philosophy

Featherflow aims to be a fast, lightweight alternative to dbt that prioritizes:

1. **Simplicity**: No special functions like `ref()` or `source()` cluttering your SQL
2. **Speed**: Rust-native performance for parsing, compilation, and execution
3. **Portability**: SQL files that work in any SQL editor without preprocessing
4. **DuckDB-First**: Optimized for local analytics and embedded databases
5. **Developer Experience**: Clear errors, fast feedback loops, minimal configuration

### What Featherflow Is

- A SQL transformation tool for analytics engineering
- A templating engine for parameterized SQL
- A test framework for data quality
- A documentation generator for data catalogs
- A dependency manager for SQL models

### What Featherflow Is Not

- A full ETL/ELT orchestration tool (use Airflow, Dagster, etc.)
- A data ingestion tool (use Airbyte, Fivetran, etc.)
- A BI tool (use Metabase, Superset, etc.)
- A replacement for all dbt features (intentionally simpler)

---

## Key Differentiators from dbt

### AST-Based Dependency Extraction

**dbt approach**: Requires `{{ ref('model_name') }}` and `{{ source('source', 'table') }}` functions in SQL.

**Featherflow approach**: Parses your SQL and automatically detects dependencies from table references.

**Example scenario**: You have a model that joins orders with customers.

In dbt, you write:
```
SELECT * FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
```

In Featherflow, you write plain SQL:
```
SELECT * FROM staging.stg_orders o
JOIN staging.stg_customers c ON o.customer_id = c.customer_id
```

Featherflow's parser automatically detects that this model depends on `stg_orders` and `stg_customers`.

**Benefits**:
- SQL files work directly in DBeaver, DataGrip, or any SQL editor
- No learning curve for ref/source syntax
- Easier migration from raw SQL projects
- Column-level lineage becomes possible through AST analysis

### 1:1 Schema File Convention

**dbt approach**: One `schema.yml` file can define multiple models, leading to large files and ambiguity.

**Featherflow approach**: Each model lives in its own directory with a matching schema file.

**Example scenario**: You have a `stg_orders` model.

In dbt, you add to a shared `schema.yml`:
```
models:
  - name: stg_orders
    columns: ...
  - name: stg_customers
    columns: ...
  - name: stg_products
    columns: ...
```

In Featherflow, you create a directory-per-model structure:
```
models/
  stg_orders/
    stg_orders.sql
    stg_orders.yml
```

The schema file `stg_orders.yml` contains:
```
name: stg_orders
columns: ...
```

**Benefits**:
- Clear ownership: one file, one model
- Easier code reviews: changes are isolated
- IDE support: autocomplete knows which schema applies
- No merge conflicts on shared schema files

### Jinja Simplification

**dbt approach**: Full Jinja2 with many built-in macros, adapters, and context variables.

**Featherflow approach**: Minimal Jinja with only `config()` and `var()` functions, plus user-defined macros.

**What's included**:
- `config()` - Set model configuration (materialization, schema, etc.)
- `var()` - Access project variables
- Custom macros - Your own reusable SQL patterns
- Standard Jinja control flow (if/for/set)

**What's intentionally excluded**:
- `ref()` and `source()` - Replaced by AST extraction
- Adapter macros - Use dialect-specific SQL directly
- Complex context objects - Keep it simple

---

## Architecture Decisions & Rationale

### 1. AST-Based Dependency Extraction

**Decision**: Use `sqlparser-rs` (v0.60) to parse SQL and extract table dependencies from the AST.

**How it works**:
1. Parse each SQL file using the configured dialect
2. Walk the AST to find all table references (FROM, JOIN)
3. Reject CTEs (S005) and FROM-clause derived tables (S006) as hard errors
4. Match remaining tables against known models, seeds, and sources
5. Build a dependency graph for execution ordering

**Edge cases handled**:
- CTEs produce hard error `S005 CteNotAllowed` — each transform must be its own model
- FROM-clause derived tables (subqueries in FROM) produce hard error `S006 DerivedTableNotAllowed`
- Scalar subqueries in SELECT, WHERE, and HAVING clauses remain allowed
- Schema-qualified names are resolved correctly
- Case-insensitive matching for table names

### 2. Layered Error Handling

**Decision**: Use `thiserror` in library crates for typed errors, `anyhow` in CLI for user-friendly messages.

**Example error flow**:
1. SQL parser encounters syntax error
2. `ff-sql` returns `ParseError { file, line, column, message }`
3. `ff-cli` wraps with context: "Failed to parse model 'stg_orders'"
4. User sees: formatted error with file location and suggestion

### 3. Trait-Based Database Abstraction

**Decision**: Define a `Database` trait that all backends implement.

**Required capabilities**:
- Execute arbitrary SQL statements
- Create tables and views from SELECT statements
- Check if relations exist
- Load CSV files into tables
- Introspect table schemas
- Report dialect-specific features

**Adding a new database**:
1. Implement the `Database` trait
2. Add dialect-specific SQL generation
3. Register in the dialect enum
4. Add integration tests

### 4. Virtual Workspace Structure

**Decision**: Organize as a Cargo workspace with focused crates.

**Crate responsibilities**:
| Crate | Single Responsibility |
|-------|----------------------|
| `ff-cli` | Argument parsing, user interaction, command dispatch |
| `ff-core` | Configuration, model discovery, DAG construction |
| `ff-jinja` | Template rendering, macro loading |
| `ff-sql` | SQL parsing, dependency extraction, validation, AST-based lineage |
| `ff-db` | Database connections, query execution |
| `ff-test` | Test SQL generation, result evaluation |
| `ff-analysis` | DataFusion-based static analysis, type inference, schema propagation, LogicalPlan lineage |

---

## Configuration

### featherflow.yml Structure

| Field | Type | Required | Default | Purpose |
|-------|------|----------|---------|---------|
| `name` | string | Yes | - | Project identifier |
| `version` | string | No | "1" | Config version |
| `model_paths` | list | Yes | - | Directories containing SQL models |
| `seed_paths` | list | No | `["seeds"]` | Directories containing CSV seed files |
| `source_paths` | list | No | `["sources"]` | Source definition YAML files |
| `macro_paths` | list | No | `["macros"]` | Directories containing Jinja macros |
| `test_paths` | list | No | `["tests"]` | Directories containing singular tests |
| `snapshot_paths` | list | No | `["snapshots"]` | Directories containing snapshot definitions |
| `target_path` | string | No | `"target"` | Output directory for artifacts |
| `clean_targets` | list | No | `["target"]` | Directories to clean |
| `materialization` | string | No | `"view"` | Default materialization |
| `schema` | string | No | - | Default schema for models |
| `database` | object | Yes | - | Connection settings |
| `vars` | object | No | `{}` | Variables for Jinja templates |
| `targets` | object | No | - | Environment-specific overrides |

### Database Connection Settings

**DuckDB Configuration**:
| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | Must be `"duckdb"` |
| `path` | Yes | File path or `:memory:` |
| `read_only` | No | Open in read-only mode |
| `extensions` | No | Extensions to load (spatial, httpfs, etc.) |
| `settings` | No | DuckDB configuration settings |

**Example scenarios**:

*Local development with persistent database*:
- Type: duckdb
- Path: `./dev.duckdb`
- Use case: Iterate on models, data persists between runs

*CI testing with in-memory database*:
- Type: duckdb
- Path: `:memory:`
- Use case: Fast, isolated tests that don't leave artifacts

*Read-only analytics on shared database*:
- Type: duckdb
- Path: `/shared/analytics.duckdb`
- Read-only: true
- Use case: Generate docs without modifying data

### Environment Targets

Define multiple environments with different configurations:

**Example scenario**: You need dev, staging, and prod environments.

```yaml
targets:
  dev:
    database:
      path: dev.duckdb
    schema: dev_{{ var('developer') }}

  staging:
    database:
      path: /data/staging.duckdb
    schema: staging

  prod:
    database:
      path: /data/prod.duckdb
    schema: analytics
```

Select target at runtime: `ff run --target staging`

### Variable Precedence

From highest to lowest priority:

1. **CLI arguments**: `ff run --vars '{"key": "value"}'`
2. **Environment variables**: `FF_VAR_key=value`
3. **Target-specific vars**: Defined in target configuration
4. **Project vars**: Defined in `featherflow.yml` vars section
5. **Default values**: Specified in `var('key', 'default')` calls

---

## Project Structure

### Recommended Layout

```
my_project/
├── featherflow.yml          # Project configuration
├── models/                  # Directory-per-model (flat layout)
│   ├── stg_customers/
│   │   ├── stg_customers.sql
│   │   └── stg_customers.yml
│   ├── stg_orders/
│   │   ├── stg_orders.sql
│   │   └── stg_orders.yml
│   ├── int_orders_enriched/
│   │   ├── int_orders_enriched.sql
│   │   └── int_orders_enriched.yml
│   ├── fct_orders/
│   │   ├── fct_orders.sql
│   │   └── fct_orders.yml
│   └── dim_customers/
│       ├── dim_customers.sql
│       └── dim_customers.yml
├── seeds/
│   ├── country_codes.csv
│   └── currency_rates.csv
├── sources/
│   └── raw_data.yml         # External source definitions
├── macros/
│   ├── date_utils.jinja
│   └── string_helpers.jinja
├── tests/
│   └── assert_positive_revenue.sql
├── snapshots/
│   └── customer_history.yml
└── target/                  # Generated artifacts (gitignored)
    ├── compiled/
    ├── run_results.json
    └── manifest.json
```

> **Note**: Featherflow uses a **directory-per-model** flat layout. Each model lives in its own subdirectory under `models/`, with the directory name matching the model name. No nesting of model directories is allowed. See [File Discovery Rules](#file-discovery-rules) for enforcement details.

### Model Naming Conventions

| Layer | Prefix | Purpose | Example |
|-------|--------|---------|---------|
| Staging | `stg_` | Clean and rename source data | `stg_orders` |
| Intermediate | `int_` | Complex transformations | `int_orders_enriched` |
| Marts - Facts | `fct_` | Event/transaction tables | `fct_orders` |
| Marts - Dimensions | `dim_` | Entity tables | `dim_customers` |
| Metrics | `mtc_` | Aggregated metrics | `mtc_daily_revenue` |

### File Discovery Rules

**Models** are discovered via the `discover_models_flat()` algorithm:

1. Each immediate subdirectory of `model_paths` is treated as a model directory
2. Each model directory must contain exactly one `.sql` file whose name matches the directory name (e.g., `stg_orders/stg_orders.sql`)
3. A matching `.yml` or `.yaml` schema file is required for every model (enforced by `Model::from_file()`); missing schema files produce `E010 MissingSchemaFile`
4. Loose `.sql` files at the `model_paths` root are rejected with error `E011 InvalidModelDirectory`
5. A `.sql` file whose stem does not match its parent directory name is rejected with error `E012 ModelDirectoryMismatch`

**Other resources**:

1. **Seeds**: Any `.csv` file in `seed_paths` directories
2. **Macros**: Any `.jinja` file in `macro_paths` directories
3. **Tests**: Any `.sql` file in `test_paths` directories
4. **Sources**: Any `.yml` file in `source_paths` with `kind: sources`

---

## Subcommand Specifications

### 1. `ff parse`

**Purpose**: Parse SQL files and output dependency information for debugging.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to parse (supports selection syntax) |
| `--output` | `-o` | Output format: `json`, `pretty`, `deps` |
| `--dialect` | `-d` | Override SQL dialect |

**Example scenarios**:

*Debug dependency detection*:
- Command: `ff parse --models stg_orders --output deps`
- Output: Lists all tables that stg_orders depends on
- Use case: Verify Featherflow correctly detected your JOINs

*Export AST for tooling*:
- Command: `ff parse --output json > ast.json`
- Output: Full AST in JSON format
- Use case: Build custom tooling on top of parsed SQL

*Check for parse errors*:
- Command: `ff parse`
- Output: Parse errors with file:line:column
- Use case: Validate SQL syntax before running

**Definition of Done**:
- [x] Parses all model files in project
- [x] Extracts table dependencies from AST
- [x] Rejects CTEs with error S005 (CteNotAllowed)
- [x] Categorizes dependencies as model, seed, source, or external
- [x] Reports parse errors with file path, line, column
- [x] JSON output includes full AST structure
- [x] Pretty output shows human-readable tree
- [x] Deps output shows just dependency list
- [x] Integration test: parse sample project, verify deps

### 2. `ff compile`

**Purpose**: Render Jinja templates to raw SQL and generate manifest.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to compile |
| `--output-dir` | | Override output directory |
| `--vars` | | Pass variables as JSON |
| `--parse-only` | | Compile but don't write files |
| `--skip-static-analysis` | | Skip DataFusion static analysis |
| `--explain` | | Print the DataFusion LogicalPlan for a model |
| `--output` | `-o` | Output format: `text`, `json` |

**Example scenarios**:

*Preview compiled SQL*:
- Command: `ff compile --models fct_orders`
- Output: Compiled SQL in `target/compiled/models/fct_orders/fct_orders.sql`
- Use case: See what SQL will actually execute

*Compile with different variables*:
- Command: `ff compile --vars '{"start_date": "2024-01-01"}'`
- Output: All models compiled with specified variables
- Use case: Test how variables affect output

*Validate without writing*:
- Command: `ff compile --parse-only`
- Output: Success/failure status
- Use case: CI check that templates render correctly

*Inspect logical plan*:
- Command: `ff compile --explain stg_orders`
- Output: DataFusion LogicalPlan with schema for the model
- Use case: Debug type inference and query planning issues

*Skip static analysis for speed*:
- Command: `ff compile --skip-static-analysis`
- Output: Compiled SQL without DataFusion analysis phase
- Use case: Quick iteration when you know the SQL is correct

**Manifest Structure**:

The manifest (`target/manifest.json`) contains:

| Section | Contents |
|---------|----------|
| `metadata` | Featherflow version, generated timestamp, project name |
| `models` | Map of model unique_id to model metadata |
| `sources` | Map of source unique_id to source metadata |
| `seeds` | Map of seed unique_id to seed metadata |
| `macros` | Map of macro name to macro metadata |
| `dependencies` | Adjacency list of model dependencies |
| `parent_map` | For each node, list of upstream nodes |
| `child_map` | For each node, list of downstream nodes |

**Definition of Done**:
- [x] Compiles all Jinja templates to pure SQL
- [x] Extracts config() values and stores in manifest
- [x] Extracts dependencies from compiled SQL AST
- [x] Detects circular dependencies with clear error message
- [x] Writes compiled SQL to target/compiled/
- [x] Generates manifest.json with all metadata
- [x] Respects --vars for variable overrides
- [x] --parse-only validates without writing
- [x] Runs DataFusion static analysis (schema propagation, type checking)
- [x] --skip-static-analysis bypasses analysis phase
- [x] --explain prints LogicalPlan for a model
- [x] Reports schema mismatches (extra/missing/type) as warnings/errors
- [x] Integration test: compile project, verify manifest structure

### 3. `ff run`

**Purpose**: Execute compiled SQL against the database in dependency order.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to run |
| `--select` | `-s` | Selection syntax (see Selection Syntax) |
| `--exclude` | `-e` | Exclude models matching pattern |
| `--full-refresh` | | Drop and recreate all selected models |
| `--fail-fast` | | Stop on first error |
| `--threads` | `-t` | Number of parallel threads |
| `--defer` | | Defer to another manifest for unselected models |
| `--state` | | Path to manifest for state comparison |
| `--skip-static-analysis` | | Skip DataFusion static analysis gate |
| `--smart` | | Skip unchanged models (smart build) |
| `--resume` | | Resume from a previous failed run |
| `--output` | `-o` | Output format: `text`, `json` |

**Example scenarios**:

*Run entire project*:
- Command: `ff run`
- Behavior: Seeds loaded, then models executed in dependency order
- Output: Progress bar, timing per model, summary

*Run single model with dependencies*:
- Command: `ff run --select +fct_orders`
- Behavior: Runs fct_orders and all its upstream dependencies
- Use case: Update a specific mart and everything it needs

*Run only changed models*:
- Command: `ff run --select state:modified --state path/to/prod/manifest.json`
- Behavior: Compare against production, run only changed models
- Use case: Slim CI builds

*Full refresh a model*:
- Command: `ff run --select dim_customers --full-refresh`
- Behavior: Drops and recreates dim_customers table
- Use case: Rebuild from scratch after schema change

*Parallel execution*:
- Command: `ff run --threads 4`
- Behavior: Runs up to 4 independent models concurrently
- Use case: Speed up large projects

**Execution Flow**:

1. Load configuration and discover models
2. Compile all Jinja templates
3. Extract dependencies and build DAG
4. Validate no circular dependencies
5. **Run DataFusion static analysis** (unless `--skip-static-analysis`):
   - Build LogicalPlan for each model via schema propagation
   - Cross-check inferred schemas against YAML declarations
   - Fail with exit code 1 if schema errors found (e.g., column in YAML missing from SQL)
6. Apply selection filters
7. Apply smart build filtering (if `--smart`)
8. Create required schemas
9. Topologically sort selected models
10. For each model (respecting parallelism):
    - Check if upstream dependencies completed
    - For incremental: check if model exists and is incremental
    - Execute CREATE TABLE/VIEW AS or incremental merge
    - Record timing and status
11. Write run_results.json
12. Report summary

**Definition of Done**:
- [x] Executes models in correct dependency order
- [x] Creates views for `materialized='view'`
- [x] Creates tables for `materialized='table'`
- [x] Handles incremental models correctly
- [x] `--full-refresh` drops before creating
- [x] `--select` supports basic selection syntax (+model, model+)
- [x] `--exclude` removes models from selection
- [x] `--fail-fast` stops on first error
- [x] `--threads` enables parallel execution
- [x] `--defer` uses other manifest for missing models (partial - logs intent)
- [x] `--state` enables state-based selection
- [x] Creates schemas before models that need them
- [x] Clear error messages on SQL execution failure
- [x] Writes run_results.json with timing and status
- [x] Static analysis gates execution (blocks on schema errors)
- [x] `--skip-static-analysis` bypasses the analysis gate
- [x] `--smart` skips unchanged models
- [x] `--resume` continues from failed run state
- [x] Integration test: run models, verify tables exist

### 4. `ff test`

**Purpose**: Run data quality tests defined in schema files and test directory.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Run tests for specific models |
| `--select` | `-s` | Selection syntax for tests |
| `--exclude` | `-e` | Exclude tests matching pattern |
| `--fail-fast` | | Stop on first failure |
| `--store-failures` | | Save failing rows to tables |
| `--threads` | `-t` | Number of parallel threads |
| `--warn-only` | | Treat errors as warnings |

**Example scenarios**:

*Run all tests*:
- Command: `ff test`
- Behavior: Runs all schema tests and singular tests
- Output: Pass/fail per test, sample failing rows

*Run tests for specific model*:
- Command: `ff test --models fct_orders`
- Behavior: Runs only tests defined for fct_orders
- Use case: Quick validation of single model

*Store failures for debugging*:
- Command: `ff test --store-failures`
- Behavior: Failing rows saved to `target/test_failures/`
- Use case: Debug why tests are failing

*CI with fast feedback*:
- Command: `ff test --fail-fast --threads 4`
- Behavior: Parallel tests, stop on first failure
- Use case: Quick CI feedback

**Test Types**:

| Category | Description |
|----------|-------------|
| Schema tests | Defined in model's .yml file, column-level |
| Singular tests | Standalone SQL files that should return 0 rows |
| Source tests | Freshness and row count tests on sources |

**Definition of Done**:
- [x] Reads tests from model's .yml schema file
- [x] Discovers singular tests in test_paths
- [x] Generates correct SQL for all built-in test types (8 types)
- [x] Handles parameterized tests correctly
- [x] Reports pass/fail with timing
- [x] Shows sample failing rows (limit 5)
- [x] Schema files required for all models (E010 if missing)
- [x] `--store-failures` saves failing rows to tables
- [x] `--fail-fast` stops on first failure
- [x] `--threads` enables parallel execution
- [x] `--warn-only` treats failures as warnings
- [x] Exit code 0 for pass, 2 for failures
- [x] Integration test with pass and fail cases

### 5. `ff seed`

**Purpose**: Load CSV seed files into database tables.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--seeds` | `-s` | Specific seeds to load |
| `--select` | | Selection syntax for seeds |
| `--full-refresh` | | Drop existing tables first |
| `--show-columns` | | Display inferred schema |

**Example scenarios**:

*Load all seeds*:
- Command: `ff seed`
- Behavior: All CSVs loaded as tables, types auto-inferred
- Output: Row count per seed

*Refresh specific seed*:
- Command: `ff seed --seeds country_codes --full-refresh`
- Behavior: Drops and recreates country_codes table
- Use case: Update reference data

*Preview schema*:
- Command: `ff seed --show-columns`
- Behavior: Shows inferred types without loading
- Use case: Verify type inference is correct

**Seed Configuration**:

Seeds can have optional `.yml` files for configuration:

| Option | Purpose |
|--------|---------|
| `schema` | Override target schema |
| `quote_columns` | Force column quoting |
| `column_types` | Override inferred types |
| `delimiter` | CSV delimiter (default: comma) |
| `enabled` | Enable/disable seed |

**Definition of Done**:
- [x] Discovers all .csv files in seed_paths
- [x] Creates tables named after file (without extension)
- [x] Uses DuckDB's read_csv_auto() for type inference
- [x] Respects seed configuration from .yml files
- [x] `--seeds` filters which seeds to load
- [x] `--full-refresh` drops existing tables first
- [x] `--show-columns` displays inferred schema
- [x] Reports row count per seed
- [x] Handles missing seed directory gracefully
- [x] Handles empty CSV files gracefully
- [x] Integration test: seeds load and are queryable

### 6. `ff docs`

**Purpose**: Generate documentation from schema files and SQL analysis.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to document |
| `--output` | `-o` | Output directory |
| `--format` | `-f` | Format: `markdown`, `html`, `json` |
| `--no-lineage` | | Skip lineage diagram generation |

**Example scenarios**:

*Generate full documentation*:
- Command: `ff docs`
- Behavior: Generates docs for all models with schemas
- Output: Markdown files in target/docs/

*Generate HTML site*:
- Command: `ff docs --format html`
- Behavior: Generates static HTML documentation site
- Output: HTML files with navigation and search

*Export for external tools*:
- Command: `ff docs --format json`
- Behavior: Generates machine-readable documentation
- Output: JSON file for integration with data catalogs

**Documentation Contents**:

For each model, documentation includes:

| Section | Source |
|---------|--------|
| Name | File name |
| Description | Schema YAML `description` field |
| Owner | Schema YAML `meta.owner` field |
| Tags | Schema YAML `tags` field |
| Columns | Schema YAML `columns` with descriptions |
| Tests | Schema YAML tests for each column |
| Dependencies | Extracted from SQL (upstream models) |
| Dependents | Reverse lookup (downstream models) |
| SQL | Raw and compiled SQL |
| Materialization | From config() or default |
| Freshness | Last run timestamp |

**Definition of Done**:
- [x] Generates documentation for all models
- [x] Includes column descriptions from schema
- [x] Shows dependencies as linked graph (lineage.dot)
- [x] Works without database connection
- [x] Schema files required for all models (E010 if missing)
- [x] Markdown format is readable and complete
- [x] HTML format includes navigation and search
- [x] JSON format includes all metadata
- [x] Lineage diagram shows model relationships
- [x] Integration test: docs match expected output

### 7. `ff validate`

**Purpose**: Validate project configuration and SQL without execution.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to validate |
| `--strict` | | Fail on warnings |
| `--contracts` | | Validate schema contracts against a reference manifest |
| `--state` | | Path to reference manifest (used with `--contracts`) |
| `--governance` | | Enable governance checks (data classification completeness) |

**Example scenarios**:

*Full validation*:
- Command: `ff validate`
- Behavior: Checks syntax, dependencies, configuration
- Output: Errors, warnings, and info messages

*Strict CI validation*:
- Command: `ff validate --strict`
- Behavior: Fails if any warnings exist
- Use case: Enforce clean project in CI

*Validate specific models*:
- Command: `ff validate --models stg_orders`
- Behavior: Only validates the specified model
- Use case: Quick check of changes

**Validation Checks**:

| Check | Level | Description |
|-------|-------|-------------|
| SQL syntax errors | Error | Parse failures with location |
| Circular dependencies | Error | Cycles in dependency graph |
| Duplicate model names | Error | Same name in different paths |
| Undefined variables | Error | var() references without default |
| Invalid schema YAML | Error | Malformed YAML structure |
| Invalid test type | Error | Unknown test type used |
| Missing schema file (E010) | Error | Model without matching `.yml`/`.yaml` schema file |
| Invalid model directory (E011) | Error | Loose `.sql` file at model_paths root |
| Model directory mismatch (E012) | Error | SQL file stem doesn't match directory name |
| CTE not allowed (S005) | Error | CTEs prohibited — each transform must be its own model |
| Derived table not allowed (S006) | Error | FROM-clause subqueries prohibited |
| Static analysis: missing column (SA01) | Error | Column in YAML not produced by SQL |
| Static analysis: mismatch (SA02) | Warning | Extra column, type mismatch, or nullability mismatch vs YAML |
| Orphaned schema files | Warning | Schema without corresponding model |
| Undeclared external tables | Warning | References to unknown tables |
| Unused macros | Warning | Macros defined but never used |
| Model without description | Info | Missing documentation |
| Column without description | Info | Missing column documentation |
| Type/test mismatch | Info | Test may not suit column type |

**Definition of Done**:
- [x] Catches SQL syntax errors with file:line:col
- [x] Detects circular dependencies with cycle path
- [x] Detects duplicate model names
- [x] Warns on undefined Jinja variables
- [x] Validates schema YAML structure
- [x] Warns on orphaned schema files
- [x] Warns on undeclared external tables
- [x] Runs DataFusion static analysis (schema propagation and cross-checking)
- [x] Reports schema mismatches as SA01 errors / SA02 warnings
- [x] `--strict` mode fails on warnings
- [x] `--contracts` validates schema contracts
- [x] `--governance` checks data classification completeness
- [x] No database connection required
- [x] Exit code 0 for valid, 1 for errors
- [x] Integration test: validate pass and fail cases

### 8. `ff ls`

**Purpose**: List project resources with filtering and formatting options.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--select` | `-s` | Selection syntax for filtering |
| `--exclude` | `-e` | Exclude matching resources |
| `--output` | `-o` | Format: `table`, `json`, `tree`, `path` |
| `--resource-type` | | Filter by type: model, seed, source, test |

**Example scenarios**:

*List all models*:
- Command: `ff ls`
- Behavior: Shows all models in table format
- Output: Name, materialization, schema, path

*Show dependency tree*:
- Command: `ff ls --output tree --select fct_orders`
- Behavior: Shows fct_orders and its dependency tree
- Output: ASCII tree of upstream models

*List models as paths*:
- Command: `ff ls --output path`
- Behavior: Just file paths, one per line
- Use case: Pipe to other tools

*Filter by tag*:
- Command: `ff ls --select tag:daily`
- Behavior: Shows only models tagged "daily"
- Use case: See what runs in daily job

**Definition of Done**:
- [x] Lists all models with name, materialization
- [x] Shows dependencies (model vs external)
- [x] `--resource-type` filters by type
- [x] `--select` supports basic selection syntax (+model, model+)
- [x] `--exclude` removes matching resources
- [x] Table output is aligned and readable
- [x] JSON output is valid and complete
- [x] Tree output shows hierarchy clearly
- [x] Path output is one path per line
- [x] Integration test: ls output matches expected

### 9. `ff clean`

**Purpose**: Remove generated artifacts.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--dry-run` | | Show what would be deleted |

**Example scenarios**:

*Clean all artifacts*:
- Command: `ff clean`
- Behavior: Removes target/ directory
- Use case: Start fresh

*Preview cleanup*:
- Command: `ff clean --dry-run`
- Behavior: Shows what would be deleted
- Use case: Verify before deleting

**Definition of Done**:
- [x] Removes all directories in clean_targets
- [x] `--dry-run` shows without deleting
- [x] Handles missing directories gracefully
- [x] Reports what was cleaned

### 10. `ff source`

**Purpose**: Manage and test external data sources.

**Subcommands**:
| Subcommand | Description |
|------------|-------------|
| `freshness` | Check source freshness |
| `snapshot-freshness` | Store freshness results |

**Example scenarios**:

*Check all source freshness*:
- Command: `ff source freshness`
- Behavior: Queries each source's loaded_at_field
- Output: Pass/warn/error per source table

*Check specific source*:
- Command: `ff source freshness --select source:raw_data`
- Behavior: Only checks raw_data source
- Use case: Debug specific source issues

**Definition of Done**:
- [x] Queries freshness based on loaded_at_field
- [x] Compares against warn_after and error_after thresholds
- [x] Reports freshness status per source table
- [x] `--select` filters which sources to check
- [x] Writes results to target/sources.json
- [x] Unit tests for freshness check

### 11. `ff lineage`

**Purpose**: Trace column-level lineage across models using SQL AST analysis.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--model` | `-m` | Model to trace lineage for (required) |
| `--column` | `-c` | Specific column to trace |
| `--direction` | `-d` | Direction: `upstream`, `downstream`, `both` (default: `both`) |
| `--output` | `-o` | Output format: `table`, `json`, `dot` (default: `table`) |

**Example scenarios**:

*Trace full lineage for a model*:
- Command: `ff lineage --model fct_orders`
- Behavior: Shows all upstream and downstream column-level lineage
- Output: Table showing column references across models

*Trace a specific column upstream*:
- Command: `ff lineage --model fct_orders --column total_amount --direction upstream`
- Behavior: Traces where the `total_amount` column originates
- Output: Chain of column transformations back to source

*Export lineage as Graphviz DOT*:
- Command: `ff lineage --model fct_orders --output dot > lineage.dot`
- Behavior: Generates DOT graph of column-level lineage
- Use case: Visualize data flow with `dot -Tpng lineage.dot -o lineage.png`

*Export lineage as JSON*:
- Command: `ff lineage --model fct_orders --output json`
- Behavior: Outputs structured lineage data
- Use case: Integration with data catalogs or custom tooling

**Lineage Types** (defined in `ff-sql`):

| Type | Description |
|------|-------------|
| `ProjectLineage` | Full project lineage graph with all models |
| `ModelLineage` | Lineage for a single model |
| `ColumnLineage` | Lineage for a single column |
| `ColumnRef` | Reference to a specific column in a model |
| `LineageEdge` | Connection between column references across models |

**DataFusion Lineage Types** (defined in `ff-analysis`):

| Type | Description |
|------|-------------|
| `ModelColumnLineage` | All lineage edges for one model extracted from its LogicalPlan |
| `ColumnLineageEdge` | Single edge: output column, source table, source column, kind |
| `LineageKind` | Copy (direct ref), Transform (computation), Inspect (WHERE/JOIN) |

**Behavior**:
- **AST-based lineage** (`ff-sql`): Extracted from the SQL AST using `sqlparser-rs`. `ProjectLineage::resolve_edges()` connects cross-model column references. Column aliases, expressions, and pass-through columns are tracked.
- **DataFusion lineage** (`ff-analysis`): Extracted from DataFusion LogicalPlans by walking Projection, Filter, Join, Aggregate, and other plan nodes. Provides richer classification (Copy vs Transform vs Inspect) with full type information.
- DOT output can be rendered with Graphviz for visualization

**Definition of Done**:
- [x] `--model` selects target model for lineage
- [x] `--column` narrows to specific column
- [x] `--direction` supports upstream, downstream, both
- [x] Table output is human-readable
- [x] JSON output includes full lineage structure
- [x] DOT output is valid Graphviz format
- [x] Cross-model column references resolved
- [x] Integration test: lineage matches expected output

### 12. `ff analyze`

**Purpose**: Run static analysis passes on SQL models to detect potential issues without executing against the database.

**Options**:
| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to analyze (comma-separated) |
| `--pass` | | Run only specific passes (comma-separated) |
| `--output` | `-o` | Output format: `table`, `json` |
| `--severity` | `-s` | Minimum severity: `info`, `warning`, `error` |

**Example scenarios**:

*Run full analysis*:
- Command: `ff analyze`
- Behavior: Runs all analysis passes on all models
- Output: Table of diagnostics with model, severity, code, pass, and message

*Analyze specific model*:
- Command: `ff analyze --models fct_orders`
- Behavior: Runs all passes on fct_orders only
- Output: Diagnostics for that model

*Run specific pass*:
- Command: `ff analyze --pass type_inference,nullability`
- Behavior: Runs only the specified passes
- Output: Filtered diagnostics

*Show only warnings and errors*:
- Command: `ff analyze --severity warning`
- Behavior: Filters out info-level diagnostics
- Use case: Focus on actionable issues

*JSON output for CI*:
- Command: `ff analyze --output json`
- Behavior: Machine-readable diagnostic output
- Use case: Integration with CI pipelines or code review tools

**Analysis Passes**:

The analyze command runs two tiers of passes:

*IR-based passes* (custom relational algebra IR from `sqlparser-rs`):
| Pass | Diagnostics | Description |
|------|-------------|-------------|
| `type_inference` | A001-A005 | Type checking: unknown types, UNION mismatches, numeric ops on strings, lossy casts |
| `nullability` | A010-A012 | Nullable columns from JOINs without null guards, YAML vs inferred nullability |
| `join_keys` | A030, A032-A033 | Join key type mismatches, cross joins, non-equi joins |
| `unused_columns` | A020-A021 | Unused columns in downstream models, SELECT * detection |

*LogicalPlan-based passes* (DataFusion LogicalPlan):
| Pass | Diagnostics | Description |
|------|-------------|-------------|
| `cross_model_consistency` | A040-A041 | Cross-model schema mismatches: extra/missing columns, type conflicts, nullability conflicts |

**Exit codes**:
- 0: No error-severity diagnostics
- 1: At least one error-severity diagnostic found

**Definition of Done**:
- [x] Runs all IR-based passes (type_inference, nullability, join_keys, unused_columns)
- [x] Runs DataFusion LogicalPlan-based passes (cross_model_consistency)
- [x] Schema propagation through DAG in topological order
- [x] `--models` filters to specific models
- [x] `--pass` filters to specific passes
- [x] `--severity` filters by minimum severity
- [x] Table output with aligned columns
- [x] JSON output with full diagnostic structure
- [x] Exit code 1 on error-severity diagnostics
- [x] No database connection required

---

## Static Analysis Engine

Featherflow includes a DataFusion-based static analysis engine that validates SQL models semantically before execution. This catches type mismatches, schema inconsistencies, and structural issues at compile time without requiring a database connection.

### Architecture

The analysis engine lives in the `ff-analysis` crate and uses two complementary approaches:

1. **Custom IR passes**: SQL is parsed with `sqlparser-rs` (v0.60), lowered to a relational algebra IR (`RelOp`/`TypedExpr`), and analyzed by composable passes (type inference, nullability propagation, join key analysis, unused column detection).

2. **DataFusion LogicalPlan passes**: SQL is re-parsed through DataFusion's SQL planner (using DataFusion 52.x with its bundled sqlparser v0.59), producing `LogicalPlan` nodes with full type information. This enables cross-model schema propagation and consistency checking.

### DataFusion Bridge

The `datafusion_bridge` module provides:

| Component | File | Purpose |
|-----------|------|---------|
| `provider.rs` | `ContextProvider` impl | Resolves model/source schemas as DataFusion table sources |
| `planner.rs` | `sql_to_plan()` | Converts SQL strings to DataFusion LogicalPlans |
| `functions.rs` | DuckDB function stubs | Registers DuckDB-specific UDFs for planning (not execution) |
| `types.rs` | Type bridge | Converts between `SqlType` and Arrow `DataType` |
| `propagation.rs` | Schema propagation | Walks DAG in topological order, building schemas for downstream models |
| `lineage.rs` | Column lineage | Extracts Copy/Transform/Inspect lineage from LogicalPlans |

### Schema Propagation

The `propagate_schemas()` function is the core of cross-model analysis:

1. Start with source/seed schemas from YAML definitions
2. For each model in topological order:
   - Build a `ContextProvider` with the current schema catalog
   - Convert SQL to `LogicalPlan` via `sql_to_plan()`
   - Extract the inferred output schema from `plan.schema()`
   - Cross-check against the model's YAML schema declaration
   - Register the inferred schema in the catalog for downstream models
3. Return all plans, inferred schemas, and detected mismatches

### Schema Mismatch Types

| Mismatch | Severity | Description |
|----------|----------|-------------|
| `MissingFromSql` | Error | Column declared in YAML but not produced by the SQL query |
| `ExtraInSql` | Warning | Column produced by SQL but not declared in YAML |
| `TypeMismatch` | Warning | YAML-declared type differs from DataFusion-inferred type |
| `NullabilityMismatch` | Warning | YAML nullability constraint conflicts with inferred nullability |

### Diagnostic Codes

**IR-based passes:**
| Code | Pass | Description |
|------|------|-------------|
| A001 | type_inference | Unknown type for column |
| A002 | type_inference | Type mismatch in UNION columns |
| A003 | type_inference | UNION column count mismatch |
| A004 | type_inference | SUM/AVG on string column |
| A005 | type_inference | Lossy cast detected |
| A010 | nullability | Nullable column from JOIN without null guard |
| A011 | nullability | YAML NOT NULL vs JOIN-inferred nullable |
| A012 | nullability | Redundant IS NULL check |
| A020 | unused_columns | Unused column in downstream models |
| A021 | unused_columns | SELECT * blocks unused column detection |
| A030 | join_keys | Join key type mismatch |
| A032 | join_keys | Cross join detected |
| A033 | join_keys | Non-equi join detected |

**LogicalPlan-based passes:**
| Code | Pass | Description |
|------|------|-------------|
| A040 | cross_model_consistency | Schema mismatch (extra/missing/type) between YAML and SQL output |
| A041 | cross_model_consistency | Nullability mismatch between YAML and SQL output |

### Column-Level Lineage (DataFusion)

In addition to the AST-based lineage in `ff-sql`, the analysis engine extracts column-level lineage from DataFusion LogicalPlans:

| Lineage Kind | Description | Example |
|--------------|-------------|---------|
| `Copy` | Direct column reference | `SELECT id FROM orders` — `id` is Copy of `orders.id` |
| `Transform` | Column used in computation | `SELECT a + b AS c` — `c` is Transform of `a` and `b` |
| `Inspect` | Column read but not in output | `WHERE status = 'active'` — `status` is Inspect |

The LogicalPlan walker handles: Projection, Filter, Join (keys as Inspect), Aggregate (GROUP BY keys, aggregate expressions as Transform), SubqueryAlias, Sort, Limit, Union, and TableScan nodes.

### Integration Points

Static analysis is integrated into three CLI commands:

| Command | Behavior | Skip flag |
|---------|----------|-----------|
| `ff compile` | Runs after compilation, reports mismatches | `--skip-static-analysis` |
| `ff validate` | Always runs as part of validation | N/A |
| `ff run` | Gates execution — blocks on errors | `--skip-static-analysis` |
| `ff analyze` | Dedicated analysis command with pass filtering | N/A |

---

## Sources Specification

Sources represent external tables not managed by Featherflow.

### Source Definition Structure

Sources are defined in YAML files within `source_paths`:

| Field | Required | Description |
|-------|----------|-------------|
| `version` | Yes | Schema version (currently "1") |
| `kind` | Yes | Must be "sources" |
| `sources` | Yes | List of source definitions |

### Source Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Source group name (e.g., "raw_data") |
| `description` | No | Human-readable description |
| `database` | No | Database name (for multi-database setups) |
| `schema` | No | Schema containing source tables |
| `loader` | No | Tool that loads this data (documentation) |
| `loaded_at_field` | No | Column containing load timestamp |
| `freshness` | No | Default freshness config for all tables |
| `tables` | Yes | List of table definitions |
| `tags` | No | Tags for selection |
| `meta` | No | Custom metadata |

### Source Table Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Table name in database |
| `description` | No | Human-readable description |
| `identifier` | No | Override actual table name |
| `loaded_at_field` | No | Override source-level setting |
| `freshness` | No | Override source-level freshness |
| `columns` | No | Column definitions |
| `external` | No | External table configuration |
| `tags` | No | Tags for selection |

### Freshness Configuration

| Field | Description |
|-------|-------------|
| `warn_after.count` | Number of time periods |
| `warn_after.period` | Time period: minute, hour, day |
| `error_after.count` | Number of time periods |
| `error_after.period` | Time period: minute, hour, day |
| `filter` | SQL WHERE clause for freshness query |

**Example scenario**: You have raw data loaded hourly and need to know if it's stale.

Configuration:
- loaded_at_field: `_loaded_at`
- warn_after: 2 hours
- error_after: 6 hours

Behavior:
- Freshness query: `SELECT MAX(_loaded_at) FROM source_table`
- If > 2 hours old: Warning
- If > 6 hours old: Error

### How Sources Work in Featherflow

1. **Discovery**: YAML files with `kind: sources` are loaded during project discovery
2. **Dependency resolution**: When AST extraction finds an unknown table, it checks sources
3. **Matching**: Table references are matched by schema.table_name
4. **Manifest**: Sources appear in manifest with full metadata
5. **Documentation**: Sources are included in generated docs
6. **Testing**: Source freshness can be tested via `ff source freshness`

### Source vs Model vs Seed

| Aspect | Model | Seed | Source |
|--------|-------|------|--------|
| Managed by | Featherflow | Featherflow | External |
| Created by | `ff run` | `ff seed` | External ETL |
| Definition | SQL file | CSV file | YAML definition |
| Schema file | Required .yml | Optional .yml | Included in source YAML |
| In dependency graph | Yes | Yes | Yes (as leaf nodes) |
| Can be tested | Yes | Yes | Freshness only |

---

## Custom Macros System

Macros extend Jinja templating with reusable SQL patterns.

### Macro File Structure

Macros are Jinja files in `macro_paths`. Each file can define multiple macros.

**Example scenario**: You need date utility functions across many models.

File: `macros/date_utils.jinja`

Defines:
- `date_spine(start_date, end_date)` - Generate date range
- `fiscal_quarter(date_column)` - Calculate fiscal quarter
- `date_trunc(date_part, date_column)` - Truncate to date part

### Using Macros in Models

After defining a macro, use it in any model:

**Example scenario**: Generate a date dimension table.

In your model, you call `{{ date_spine('2020-01-01', '2025-12-31') }}` to generate the date range, then add additional columns using other macros.

### Macro Parameters

Macros can accept:
- **Positional parameters**: `{{ my_macro(arg1, arg2) }}`
- **Keyword parameters**: `{{ my_macro(name='value') }}`
- **Default values**: Define defaults in macro definition
- **Variable arguments**: Not supported in Minijinja

### Built-in Macros (Planned)

| Category | Macros | Purpose |
|----------|--------|---------|
| Date/Time | `date_spine`, `date_trunc`, `date_add`, `date_diff` | Date manipulation |
| String | `slugify`, `clean_string`, `split_part` | String manipulation |
| Math | `safe_divide`, `round_money`, `percent_of` | Safe math operations |
| Cross-DB | `limit_zero`, `bool_or`, `hash` | Dialect-compatible SQL |
| Testing | `test_unique`, `test_not_null` | Custom test helpers |
| Schema | `generate_schema_name`, `generate_alias_name` | Naming customization |

### Macro Loading Process

1. On startup, scan all directories in `macro_paths`
2. Find all `.jinja` files recursively
3. Parse each file and extract macro definitions
4. Register macros with Minijinja environment
5. Macros are available globally in all models

### Macro Limitations

| Limitation | Reason | Workaround |
|------------|--------|------------|
| No macro imports | Minijinja doesn't support import | Define all macros in single namespace |
| No recursive macros | Stack overflow protection | Break logic into separate models |
| Compile-time only | Macros expand during compile | Use SQL functions for runtime |
| No adapter methods | Simplified architecture | Use dialect-specific SQL |

### Macro Best Practices

1. **Naming**: Use verb_noun format (e.g., `generate_date_spine`)
2. **Documentation**: Add comment at top of macro explaining purpose
3. **Parameters**: Use descriptive parameter names
4. **Defaults**: Provide sensible defaults where possible
5. **Testing**: Create a test model that exercises each macro

---

## Selection Syntax

Selection syntax controls which resources are included in operations.

### Basic Selection

| Syntax | Description | Example |
|--------|-------------|---------|
| `model_name` | Select specific model | `fct_orders` |
| `model1 model2` | Select multiple models | `fct_orders dim_customers` |
| `*` | Select all models | `*` |

### Graph Operators

| Syntax | Description | Example |
|--------|-------------|---------|
| `+model` | Model and all ancestors | `+fct_orders` selects fct_orders, stg_orders, stg_customers |
| `model+` | Model and all descendants | `stg_orders+` selects stg_orders, int_orders, fct_orders |
| `+model+` | Model and all connected | `+fct_orders+` selects full lineage |
| `N+model` | Model and N levels of ancestors | `2+fct_orders` selects 2 levels up |
| `model+N` | Model and N levels of descendants | `stg_orders+2` selects 2 levels down |
| `@model` | Model and all connected (same as +model+) | `@fct_orders` |

**Example scenario**: You changed `stg_orders` and want to run everything affected.

Command: `ff run --select stg_orders+`

This runs:
1. `stg_orders` (the changed model)
2. `int_orders_enriched` (depends on stg_orders)
3. `fct_orders` (depends on int_orders_enriched)

### Path Selection

| Syntax | Description | Example |
|--------|-------------|---------|
| `path:models/` | All models in directory | `path:models/` |
| `path:stg_orders/` | Specific model directory | `path:stg_orders/` |
| `models/stg_orders/stg_orders.sql` | Exact file path | `models/stg_orders/stg_orders.sql` |

**Example scenario**: You want to run all staging models (use tag selection with flat layout).

Command: `ff run --select tag:staging`

> **Note**: With directory-per-model flat layout, use tag selection (`tag:staging`) or name-based selection (`stg_*`) to select groups of models by layer, rather than path-based selection.

### Tag Selection

| Syntax | Description | Example |
|--------|-------------|---------|
| `tag:daily` | Models with tag "daily" | `tag:daily` |
| `tag:pii` | Models with tag "pii" | `tag:pii` |

Tags are defined in schema files:
```yaml
tags:
  - daily
  - pii
```

**Example scenario**: You have a daily job that should only run certain models.

Command: `ff run --select tag:daily`

### Resource Type Selection

| Syntax | Description | Example |
|--------|-------------|---------|
| `resource_type:model` | All models | `resource_type:model` |
| `resource_type:seed` | All seeds | `resource_type:seed` |
| `resource_type:source` | All sources | `resource_type:source` |
| `resource_type:test` | All tests | `resource_type:test` |

### State Selection

| Syntax | Description | Example |
|--------|-------------|---------|
| `state:modified` | Models with changes | `state:modified` |
| `state:new` | Newly added models | `state:new` |
| `state:modified+` | Modified and downstream | `state:modified+` |

Requires `--state` flag pointing to reference manifest.

**Example scenario**: CI build that only runs changed models.

Command: `ff run --select state:modified+ --state prod-manifest.json`

### Set Operations

| Syntax | Description | Example |
|--------|-------------|---------|
| `a b` | Union (space) | `tag:daily tag:weekly` |
| `a,b` | Union (comma) | `fct_orders,fct_revenue` |
| `intersection(a b)` | Both conditions | `intersection(tag:daily tag:marts)` |
| `a --exclude b` | Difference | `tag:daily --exclude fct_legacy` |

**Example scenario**: Run daily models except the slow legacy one.

Command: `ff run --select tag:daily --exclude fct_legacy_report`

### Selection Resolution Order

1. Parse selection string into tokens
2. Resolve each model name against project
3. Apply resource type filters
4. Apply path filters
5. Apply tag filters
6. Apply state filters (if --state provided)
7. Expand graph operators (+, @)
8. Apply set operations
9. Apply exclusions
10. Return final ordered list

---

## Schema Testing Framework

### Schema File Structure

Each model can have a corresponding `.yml` file with the same name:

| Field | Required | Description |
|-------|----------|-------------|
| `version` | Yes | Schema version (currently "1") |
| `name` | Yes | Model name (must match SQL file) |
| `description` | No | Human-readable description |
| `config` | No | Model configuration overrides |
| `columns` | No | Column definitions and tests |
| `tests` | No | Model-level tests |
| `tags` | No | Tags for selection |
| `meta` | No | Custom metadata |

### Column Definition

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Column name |
| `description` | No | Human-readable description |
| `data_type` | No | Expected data type |
| `tests` | No | List of tests for this column |
| `tags` | No | Tags for selection |
| `meta` | No | Custom metadata |

### Built-in Test Types

| Test | Parameters | What It Checks |
|------|------------|----------------|
| `unique` | none | No duplicate values in column |
| `not_null` | none | No NULL values in column |
| `positive` | none | All values > 0 |
| `non_negative` | none | All values >= 0 |
| `accepted_values` | `values: [...]` | Value is in allowed list |
| `min_value` | `value: n` | All values >= n |
| `max_value` | `value: n` | All values <= n |
| `min_length` | `length: n` | String length >= n |
| `max_length` | `length: n` | String length <= n |
| `regex` | `pattern: str` | Value matches regex pattern |
| `relationships` | `to: model, field: col` | Foreign key exists |
| `expression_is_true` | `expression: sql` | Custom SQL evaluates true |

### Test Configuration

Tests can have additional configuration:

| Option | Description |
|--------|-------------|
| `severity` | `error` (default) or `warn` |
| `error_if` | SQL condition for failure threshold |
| `warn_if` | SQL condition for warning threshold |
| `where` | SQL WHERE clause to filter test |
| `limit` | Max failing rows to return |

**Example scenario**: You want unique emails but allow some duplicates during migration.

Test configuration:
- Test: unique on email column
- Severity: warn
- error_if: >100 duplicates
- where: created_at > migration_date

### Model-Level Tests

Tests can also be defined at model level:

| Test | Parameters | What It Checks |
|------|------------|----------------|
| `row_count` | `min: n, max: n` | Row count in range |
| `expression_is_true` | `expression: sql` | Custom SQL condition |
| `equal_rowcount` | `compare_model: name` | Same rows as other model |

### Singular Tests

Standalone SQL test files in `test_paths`:

- File: `tests/assert_no_orphan_orders.sql`
- Content: SELECT query that should return 0 rows
- Passes if query returns no rows
- Fails if query returns any rows

**Example scenario**: Business rule that all orders must have valid customers.

Test file: `tests/assert_orders_have_customers.sql`
Query: SELECT orders without matching customer_id
Expected: 0 rows (all orders have customers)

### Test Execution Flow

1. Discover all tests (schema + singular)
2. Filter by selection
3. Generate SQL for each test
4. Execute in parallel (respecting --threads)
5. Evaluate results
6. Store failures (if --store-failures)
7. Report pass/fail/warn/error
8. Exit with appropriate code

### Custom Tests via Macros (Planned)

Define custom test types in macros:

**Example scenario**: You need to validate email format across many columns.

Define: `test_valid_email(model, column)` macro
Use in schema: `tests: [valid_email]`
Featherflow: Calls macro to generate test SQL

---

## Incremental Models

Incremental models process only new/changed data instead of full refresh.

### Incremental Configuration

| Option | Required | Description |
|--------|----------|-------------|
| `materialized` | Yes | Must be "incremental" |
| `unique_key` | No | Column(s) for merge key |
| `incremental_strategy` | No | append, merge, delete+insert |
| `on_schema_change` | No | ignore, fail, append_new_columns |

### Incremental Strategies

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `append` | INSERT new rows only | Event logs, append-only data |
| `merge` | UPSERT based on unique_key | Slowly changing data |
| `delete+insert` | DELETE matching + INSERT | Full reload of changed partitions |

### How Incremental Works

**First run**:
1. Model doesn't exist yet
2. Execute full query
3. Create table with all results

**Subsequent runs**:
1. Model exists
2. `is_incremental()` returns true
3. Query filters to new/changed data
4. Apply incremental strategy

**Full refresh**:
1. User specifies `--full-refresh`
2. `is_incremental()` returns false
3. Drop and recreate table

### is_incremental() Macro

Use in your SQL to conditionally filter:

**Example scenario**: Process only orders since last run.

In your model:
- If incremental: Filter WHERE order_date > (SELECT MAX(order_date) FROM this_table)
- If not incremental: Process all orders

The `is_incremental()` macro returns true when:
1. Model already exists in database
2. Model is configured as incremental
3. `--full-refresh` was NOT specified

### Unique Key for Merge

When using merge strategy:

| unique_key type | Behavior |
|-----------------|----------|
| Single column | Match on one column |
| List of columns | Match on composite key |
| None | Append only (no merge) |

**Example scenario**: Update customer dimension with latest data.

Configuration:
- unique_key: customer_id
- strategy: merge

Behavior:
- New customers: INSERT
- Existing customers: UPDATE

### State Tracking

Featherflow tracks model state in `target/state.json`:

| Field | Description |
|-------|-------------|
| `model_name` | Model identifier |
| `last_run` | Timestamp of last successful run |
| `row_count` | Rows in model after last run |
| `checksum` | Hash of compiled SQL |
| `config` | Model configuration snapshot |

### Definition of Done - Incremental

- [x] Recognizes `materialized: incremental` config
- [x] Implements `is_incremental()` macro
- [x] Supports append strategy
- [x] Supports merge strategy with single unique_key
- [x] Supports merge strategy with composite unique_key
- [x] Supports delete+insert strategy
- [x] `--full-refresh` overrides incremental
- [x] on_schema_change: ignore works
- [x] on_schema_change: fail works
- [x] on_schema_change: append_new_columns works
- [x] State tracking in target/state.json
- [x] Integration tests for each strategy

---

## Snapshots

Snapshots track historical changes to mutable source data (SCD Type 2).

### Snapshot Configuration

| Option | Required | Description |
|--------|----------|-------------|
| `name` | Yes | Snapshot name |
| `source` | Yes | Source table to snapshot |
| `unique_key` | Yes | Column(s) identifying a record |
| `strategy` | Yes | timestamp or check |
| `updated_at` | If timestamp | Column containing update timestamp |
| `check_cols` | If check | Columns to compare for changes |
| `invalidate_hard_deletes` | No | Handle deleted records |

### Snapshot Strategies

| Strategy | How Changes Detected | Use Case |
|----------|---------------------|----------|
| `timestamp` | Compare updated_at column | Source has reliable timestamp |
| `check` | Compare specified columns | No timestamp available |

### Snapshot Output Columns

| Column | Description |
|--------|-------------|
| (source columns) | All columns from source |
| `dbt_scd_id` | Unique ID for each version |
| `dbt_updated_at` | When Featherflow captured change |
| `dbt_valid_from` | When this version became active |
| `dbt_valid_to` | When this version was superseded (NULL if current) |

### How Snapshots Work

**Example scenario**: Track customer address changes over time.

Source table: customers (id, name, address, updated_at)
Snapshot config: unique_key=id, strategy=timestamp, updated_at=updated_at

Day 1: Customer 123 has address "123 Main St"
- Snapshot row: id=123, address="123 Main St", valid_from=Day1, valid_to=NULL

Day 5: Customer 123 changes address to "456 Oak Ave"
- Old row updated: valid_to=Day5
- New row inserted: id=123, address="456 Oak Ave", valid_from=Day5, valid_to=NULL

Query current address: WHERE valid_to IS NULL
Query address on Day 3: WHERE Day3 BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')

### Hard Deletes

When `invalidate_hard_deletes: true`:

- Featherflow detects records missing from source
- Sets valid_to on snapshot row
- Records appear as "deleted" in history

### Definition of Done - Snapshots

- [x] Snapshot YAML configuration parsing
- [x] Timestamp strategy implementation
- [x] Check strategy implementation
- [x] Correct SCD Type 2 output columns
- [x] Handle inserts (new records)
- [x] Handle updates (changed records)
- [x] Handle hard deletes (when configured)
- [x] Idempotent execution (rerunnable)
- [x] ff snapshot command

---

## Hooks & Operations

### Model Hooks (Planned)

Hooks execute SQL before/after model runs:

| Hook | When Executed |
|------|---------------|
| `pre-hook` | Before model SQL |
| `post-hook` | After model SQL |

**Example scenario**: Grant permissions after creating a model.

Post-hook: GRANT SELECT ON {{ this }} TO analytics_role

### Run Hooks (Planned)

Hooks execute at project level:

| Hook | When Executed |
|------|---------------|
| `on-run-start` | Before any model runs |
| `on-run-end` | After all models complete |

**Example scenario**: Log run metadata.

On-run-start: INSERT INTO run_log (started_at) VALUES (NOW())
On-run-end: UPDATE run_log SET completed_at = NOW()

### Operations (Planned)

Standalone SQL operations not tied to models:

- Command: `ff run-operation my_operation --args '{"key": "value"}'`
- Use case: One-time maintenance scripts, grants, etc.

### Definition of Done - Hooks

- [x] Pre-hook execution before model
- [x] Post-hook execution after model
- [x] Hook access to `this` (current model)
- [x] Hook access to config variables
- [x] on-run-start execution
- [x] on-run-end execution
- [x] run-operation command

---

## Error Handling

### Error Code System

| Code | Type | Description |
|------|------|-------------|
| E001 | ConfigNotFound | Configuration file not found |
| E002 | ConfigParseError | Invalid YAML in configuration |
| E003 | ConfigInvalid | Configuration validation failed |
| E004 | ProjectNotFound | Project directory not found |
| E005 | ModelNotFound | Referenced model doesn't exist |
| E006 | ModelParseError | SQL syntax error |
| E007 | CircularDependency | Cycle detected in DAG |
| E008 | DuplicateModel | Same model name in multiple paths |
| E009 | JinjaRenderError | Template rendering failed |
| E010 | SchemaParseError | Invalid YAML in schema file / missing schema file |
| E011 | InvalidModelDirectory | Loose SQL file or malformed model directory |
| E012 | ModelDirectoryMismatch | SQL file stem doesn't match directory name |
| E013 | MacroNotFound | Referenced macro doesn't exist |
| E014 | MacroError | Macro execution failed |
| E015 | SelectionError | Invalid selection syntax |
| E016 | TestError | Test execution failed |
| E017 | SeedError | Seed loading failed |
| E018 | SnapshotError | Snapshot execution failed |
| E019 | ValidationError | Validation check failed |
| E020 | StateError | State file corrupted or invalid |
| S005 | CteNotAllowed | CTEs prohibited — each transform must be its own model |
| S006 | DerivedTableNotAllowed | FROM-clause subqueries prohibited |
| SA01 | StaticAnalysisError | Column in YAML not produced by SQL (from schema propagation) |
| SA02 | StaticAnalysisWarning | Extra column, type mismatch, or nullability mismatch vs YAML |
| A001-A005 | AnalysisTypeInference | Type inference diagnostics (see Static Analysis Engine) |
| A010-A012 | AnalysisNullability | Nullability propagation diagnostics |
| A020-A021 | AnalysisUnusedColumns | Unused column diagnostics |
| A030-A033 | AnalysisJoinKeys | Join key analysis diagnostics |
| A040-A041 | AnalysisCrossModel | Cross-model consistency diagnostics (DataFusion) |
| AE001-AE008 | AnalysisInternalError | Internal analysis errors (lowering, planning, etc.) |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Test failures (ff test only) |
| 3 | Circular dependency |
| 4 | Database error |
| 5 | Validation errors |

### Error Message Format

Errors follow a consistent format:

```
Error [E006]: SQL parse error
  --> models/stg_orders/stg_orders.sql:15:23
   |
15 |     SELECT * FORM raw_orders
   |                   ^^^^^^^^^^
   |
   = expected: FROM keyword
   = found: FORM (identifier)

Hint: Did you mean 'FROM'?

For more information, run: ff docs error E006
```

### Error Context

Errors include relevant context:

| Context | Included When |
|---------|---------------|
| File path | Error relates to a file |
| Line/column | Syntax errors |
| Model name | Error relates to a model |
| Dependency chain | Circular dependency |
| SQL statement | Database errors |
| Suggestion | Common mistakes detected |

### Error Recovery

| Scenario | Behavior |
|----------|----------|
| Model fails | Log error, continue with independent models |
| `--fail-fast` | Stop immediately on first error |
| Partial run | Record state in run_results.json |
| Retry after fix | Use state to resume from failure point |

---

## Known Limitations & Edge Cases

### By Design

| Limitation | Rationale |
|------------|-----------|
| No ref() function | AST extraction is cleaner |
| No source() function | AST extraction handles this |
| No adapters | Keep dialect handling simple |
| No packages yet | Focus on core features first |
| Minijinja vs Jinja2 | Rust native, 95% compatible |

### Technical Limitations

| Limitation | Workaround |
|------------|------------|
| Macro imports not supported | Put all macros in global namespace |
| No recursive macros | Break logic into separate models |
| No Python in macros | Macros are pure Jinja |
| Single database connection | Use --threads for parallelism |

### Edge Cases

| Case | Behavior |
|------|----------|
| Model references itself | Error: circular dependency |
| CTE in model SQL | Hard error S005 (CteNotAllowed) |
| Empty SQL file | Error: no SQL statements |
| Schema file without model | Warning during validation |
| Model without schema | Error E010 (MissingSchemaFile) — schema file required |
| Duplicate table across schemas | Both tracked with qualified names |
| Table function reference | Not detected as dependency |

### Known Issues

| Issue | Status | Mitigation |
|-------|--------|------------|
| Diamond deps with schema mismatch | Open | Use consistent schema names |
| No transaction boundaries | By design | Partial state on failure |
| Tests pass on empty tables | Open | Add row_count test |
| View deps during full refresh | Open | Order drops carefully |

---

## Testing Strategy

### Test Levels

| Level | Scope | Runner |
|-------|-------|--------|
| Unit tests | Individual functions | `cargo test --lib` |
| Integration tests | Full commands | `cargo test --test integration` |
| End-to-end tests | Real projects | Manual / CI |

### Test Fixtures

- Location: `tests/fixtures/sample_project/`
- Seed data: `testdata/seeds/`
- Expected outputs: `testdata/expected/`

### Integration Test Pattern

1. Set up: Create in-memory DuckDB, load seeds
2. Execute: Run command being tested
3. Verify: Check outputs match expected
4. Cleanup: Drop database (automatic for in-memory)

### Key Test Cases

| Command | Critical Tests |
|---------|----------------|
| `ff parse` | Dependency extraction, CTE rejection (S005), parse errors |
| `ff compile` | Jinja rendering, manifest structure, circular deps, static analysis |
| `ff run` | Execution order, materializations, error handling, static analysis gate |
| `ff test` | All test types, pass/fail cases, store failures |
| `ff validate` | All validation rules, strict mode, static analysis |
| `ff seed` | Type inference, full refresh |
| `ff docs` | All formats, missing schemas |
| `ff ls` | Selection syntax, output formats |
| `ff analyze` | All passes, severity filtering, pass filtering, JSON/table output |
| `ff lineage` | AST lineage, DataFusion lineage, DOT output |

### Test Coverage Goals

| Area | Target |
|------|--------|
| Core logic | 90%+ |
| CLI parsing | 80%+ |
| Error paths | 80%+ |
| Happy paths | 100% |

---

## Roadmap & Implementation Status

### Phase 1: Core Infrastructure (v0.1.x) - COMPLETE

| Component | Status |
|-----------|--------|
| Project structure | Done |
| Configuration loading | Done |
| Model discovery | Done |
| SQL parsing | Done |
| Dependency extraction | Done |
| DAG construction | Done |
| Jinja templating | Done |
| DuckDB integration | Done |

### Phase 2: CLI Commands (v0.1.x) - COMPLETE

| Command | Status |
|---------|--------|
| ff parse | Done |
| ff compile | Done |
| ff run | Done |
| ff test | Done |
| ff seed | Done |
| ff docs | Done |
| ff validate | Done |
| ff ls | Done |

### Phase 3: Selection & Sources (v0.2.0) - COMPLETE

| Feature | Status |
|---------|--------|
| Path selection (`path:`) | Done |
| Tag selection (`tag:`) | Done |
| Exclusion (`--exclude`) | Done |
| State selection (`state:`) | Done |
| Source YAML parsing | Done |
| Source freshness command | Done |
| Source in lineage | Done |
| ff clean command | Done |

### Phase 4: Advanced Testing (v0.3.0) - COMPLETE

| Feature | Status |
|---------|--------|
| Custom test macros | Not started |
| Test severity (warn/error) | Done |
| Store failures | Done |
| Singular tests | Done |
| Relationship tests | Done |

### Phase 5: Incremental Models (v0.4.0) - COMPLETE

| Feature | Status |
|---------|--------|
| is_incremental() macro | Done |
| Append strategy | Done |
| Merge strategy | Done |
| Delete+insert strategy | Done |
| State tracking | Done |

### Phase 6: Parallel Execution (v0.5.0) - COMPLETE

| Feature | Status |
|---------|--------|
| --threads option | Done |
| DAG-aware scheduling | Done |
| Connection pooling | N/A (DuckDB uses single connection) |
| Progress tracking | Done |

### Phase 7: Advanced Macros (v0.6.0) - COMPLETE

| Feature | Status |
|---------|--------|
| Built-in date macros | Done (date_spine, date_diff, date_add, date_trunc) |
| Built-in string macros | Done (slugify, clean_string, split_part) |
| Built-in cross-db macros | Done (hash, safe_divide, coalesce_columns) |
| Macro documentation | Not started |

### Phase 8: AST-Powered Features (v0.7.0) - COMPLETE

| Feature | Status |
|---------|--------|
| Column-level lineage | Done |
| Auto-documentation | Done |
| Test suggestions | Done |
| Breaking change detection | Done |

### Phase 9: Snapshots (v0.8.0) - COMPLETE

| Feature | Status |
|---------|--------|
| Snapshot YAML config | Done |
| Timestamp strategy | Done |
| Check strategy | Done |
| Hard delete handling | Done |

### Phase 10: Hooks & Operations (v0.9.0) - COMPLETE

| Feature | Status |
|---------|--------|
| Pre/post hooks | Done |
| Run hooks | Done |
| Operations | Done (run-operation command) |

### Phase A: Production Readiness (v1.0.0) - COMPLETE

| Feature | Work Units | Status |
|---------|------------|--------|
| Full --defer Implementation | 3 | Done |
| Integration Test Expansion | 2 | Done |
| Structured JSON Output | 1 | Done |
| Data Contracts / Schema Enforcement | 3 | Done |
| Data Freshness SLAs | 3 | Done |
| Retry and Partial Run Recovery | 2 | Done |

**Key Files**: `run.rs`, `contract.rs`, `freshness.rs`, `run_state.rs`

### Phase B: Developer Experience (v1.1.0) - COMPLETE

| Feature | Work Units | Status |
|---------|------------|--------|
| Custom Test Macros | 3 | Done |
| Environment/Target Management | 3 | Done |
| Progress Indicators | 1 | Done |
| Verbose Mode | 1 | Done |
| Exposure Definitions | 2 | Done |
| Data Diff / Compare | 2 | Done |
| Model Ownership and Metadata | 3 | Done |

**Key Files**: `custom_tests.rs`, `config.rs`, `exposure.rs`, `diff.rs`, `model.rs`

### Phase C: Advanced Features (v1.2.0) - COMPLETE

| Feature | Work Units | Status |
|---------|------------|--------|
| Ephemeral Materialization | 3 | Done |
| Macro Documentation | 2 | Done |
| Model Versioning | 3 | Done |
| Metric Definitions | 4 | Done |
| Freshness in Run Results | 1 | Done |
| Exposure Docs & Impact Analysis | 2 | Done |

**Key Files**: `inline.rs`, `builtins.rs`, `model.rs`, `project.rs`, `metric.rs`

### Phase D: DataFusion Static Analysis Engine (v1.3.0) - COMPLETE

| Feature | Work Units | Status |
|---------|------------|--------|
| sqlparser 0.52 → 0.60 upgrade | 2 | Done |
| Type registry & required column types (SqlType extensions, Arrow bridge) | 3 | Done |
| DataFusion LogicalPlan generator (ContextProvider, planner, DuckDB function stubs) | 4 | Done |
| DAG-wide schema propagation (cross-model type checking) | 3 | Done |
| Static validation passes (PlanPass/DagPlanPass traits, CrossModelConsistency) | 3 | Done |
| Column-level lineage from DataFusion LogicalPlan | 2 | Done |
| CLI integration (compile, validate, run, analyze) | 3 | Done |
| Smart builds (skip unchanged models via `--smart`) | 3 | Done |

**Key Files**: `crates/ff-analysis/src/datafusion_bridge/` (provider, planner, functions, types, propagation, lineage), `crates/ff-analysis/src/pass/plan_pass.rs`, `crates/ff-analysis/src/pass/plan_cross_model.rs`

**New Dependencies**: `datafusion-common 52`, `datafusion-expr 52`, `datafusion-sql 52`, `arrow 54`

### Phase E: Advanced Analysis & IDE (v1.4.0) - PLANNED

| Feature | Work Units | Status |
|---------|------------|--------|
| PII/sensitive data classification with lineage propagation | 4 | Not started |
| Impact analysis for compliance | 3 | Not started |
| IDE integration (LSP) | 4 | Not started |
| Port remaining IR passes to DataFusion LogicalPlan | 3 | Not started |
| Remove legacy RelOp IR (after all passes migrated) | 2 | Not started |

**PII/Sensitive Data Classification**: Tag columns as PII or sensitive in schema YAML, then use column-level lineage to automatically propagate classification downstream. Any model that derives from a PII column inherits the classification, enabling compliance teams to track sensitive data flow across the entire project.

**Impact Analysis for Compliance**: Given a source column change or deprecation, use lineage to enumerate every downstream model, metric, exposure, and test affected. Generate compliance reports showing the full blast radius of schema changes.

**IDE Integration**: Language server protocol (LSP) support for Featherflow projects, providing autocomplete for model names, column references, and Jinja variables, inline validation errors, go-to-definition for model dependencies, and lineage visualization in the editor.

**Pass Migration**: The existing IR-based passes (type_inference, nullability, join_keys, unused_columns) currently operate on a custom `RelOp`/`TypedExpr` IR. These will be rewritten to operate on DataFusion `LogicalPlan` nodes, leveraging DataFusion's richer type information and `DFSchema` nullability tracking. Once all passes are ported, the legacy `lowering/` module and custom IR can be removed.

---

## Extended Features Reference

### Data Contracts (Feature 8)

Define enforced schema contracts in model YAML:

```yaml
version: "1"
name: fct_orders
contract:
  enforced: true
columns:
  - name: order_id
    data_type: INTEGER
    constraints: [not_null, primary_key]
  - name: total_amount
    data_type: DECIMAL(10,2)
```

Commands:
- `ff run` validates contracts after model creation
- `ff validate --contracts --state` checks contracts without running

### Data Freshness SLAs (Feature 10)

Define freshness SLAs in model YAML:

```yaml
name: fct_orders
freshness:
  loaded_at_field: updated_at
  warn_after:
    count: 4
    period: hour
  error_after:
    count: 8
    period: hour
```

Commands:
- `ff freshness` - Check model freshness against SLAs
- Results included in `ff run` output

### Exposure Definitions (Feature 11)

Document downstream consumers in `exposures/` directory:

```yaml
version: "1"
kind: exposure
name: revenue_dashboard
type: dashboard
owner:
  name: Analytics Team
  email: analytics@company.com
depends_on:
  - fct_orders
  - dim_customers
url: https://bi.company.com/dashboard/123
maturity: high
```

Commands:
- `ff ls --downstream-exposures` - Show affected exposures
- Exposures included in documentation output

### Metric Definitions (Feature 12)

Define semantic metrics in `metrics/` directory:

```yaml
version: "1"
kind: metric
name: total_revenue
label: Total Revenue
model: fct_orders
calculation: sum
expression: order_amount
timestamp: order_date
dimensions:
  - customer_segment
  - product_category
filters:
  - is_valid = true
```

Commands:
- `ff metric` - List all metrics
- Metrics generate SQL for BI tools

### Data Diff (Feature 13)

Compare model output between environments:

```bash
ff diff fct_orders --compare-to /path/to/prod.duckdb
ff diff fct_orders --columns order_id,total_amount --compare-to prod.duckdb
```

### Model Versioning (Feature 9)

Support versioned models with `_v{N}` suffix:

- `fct_orders_v1.sql` and `fct_orders_v2.sql` can coexist
- Unversioned references resolve to latest version
- Deprecation warnings via `deprecated: true` in schema

### Retry and Recovery (Feature 15)

Resume failed runs:

```bash
ff run --resume                    # Continue from failed state
ff run --resume --retry-failed     # Only retry failed models
ff run --resume --state-file ./custom/run_state.json
```

State tracked in `target/run_state.json`.

---

## Dialect Extensibility

### Design Principles

1. **DuckDB First**: All features work on DuckDB before other dialects
2. **Trait Abstraction**: Database operations through `Database` trait
3. **Dialect Enum**: SQL generation varies by dialect
4. **Feature Flags**: Capabilities checked at runtime

### Database Trait Methods

| Method | Purpose |
|--------|---------|
| `execute` | Run single SQL statement |
| `execute_batch` | Run multiple statements |
| `create_table_as` | CREATE TABLE AS SELECT |
| `create_view_as` | CREATE VIEW AS SELECT |
| `drop_relation` | DROP TABLE/VIEW |
| `relation_exists` | Check if relation exists |
| `query_count` | COUNT(*) on query |
| `query_rows` | Execute and return rows |
| `load_csv` | Load CSV into table |
| `get_columns` | Introspect table schema |
| `dialect` | Return dialect identifier |
| `supports` | Check feature support |

### Dialect-Specific SQL

| Operation | DuckDB | PostgreSQL | Snowflake |
|-----------|--------|------------|-----------|
| Create schema | `CREATE SCHEMA IF NOT EXISTS` | Same | Same |
| Create table | `CREATE OR REPLACE TABLE` | `CREATE TABLE` | Same |
| Merge | `INSERT OR REPLACE` | `INSERT ON CONFLICT` | `MERGE` |
| CSV load | `read_csv_auto()` | `\COPY` | `COPY INTO` |
| Regex | `regexp_matches()` | `~` | `REGEXP_LIKE()` |
| Date diff | `date_diff()` | `DATE_PART()` | `DATEDIFF()` |

### Adding a New Dialect

1. Add variant to `Dialect` enum
2. Implement `Database` trait for backend
3. Add dialect-specific SQL generation
4. Implement feature capability checks
5. Add integration tests
6. Document limitations

### Feature Capabilities

| Feature | DuckDB | PostgreSQL | Snowflake |
|---------|--------|------------|-----------|
| In-memory | Yes | No | No |
| CSV auto-infer | Yes | No | Limited |
| MERGE statement | Limited | Yes | Yes |
| Transactions | Auto-commit | Full | Full |
| Regex | Full | Full | Full |
| JSON | Full | Full | Full |

---

## CI/CD

### CI Pipeline (ci.yml)

| Job | Purpose |
|-----|---------|
| `check` | Fast compile check (`cargo check`) |
| `fmt` | Format check (`cargo fmt --check`) |
| `clippy` | Lint check (`cargo clippy -D warnings`) |
| `test` | Run tests (ubuntu + macos matrix) |
| `docs` | Build documentation (`cargo doc`) |

### Release Pipeline (release.yml)

Triggered on `v*.*.*` tags.

Build targets:
- x86_64-unknown-linux-gnu
- x86_64-unknown-linux-musl
- x86_64-apple-darwin
- aarch64-apple-darwin

### CI Best Practices

| Practice | Reason |
|----------|--------|
| No windows-latest | DuckDB issues on Windows CI |
| Matrix for OS | Test on Linux + macOS |
| Cache cargo | Speed up builds |
| Fail fast | Quick feedback |

### Release Process

1. Update version in all Cargo.toml files
2. Update CHANGELOG.md
3. Create PR for version bump
4. Merge to main
5. Create and push tag: `git tag v0.2.0 && git push origin v0.2.0`
6. CI builds and uploads release artifacts
7. Create GitHub release with changelog

---

## Development Workflow

### Setup

1. Install Rust stable toolchain
2. Clone repository
3. Run `make build`
4. Run `make test`
5. Use sample project in `tests/fixtures/sample_project/`

### Make Targets

| Target | Purpose |
|--------|---------|
| `make build` | Build all crates |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests only |
| `make lint` | Run fmt check + clippy |
| `make ci` | Full CI check locally |
| `make clean` | Remove build artifacts |

### CLI Testing Targets

| Target | Command |
|--------|---------|
| `make ff-parse` | Run parse command |
| `make ff-compile` | Run compile command |
| `make ff-run` | Run run command |
| `make ff-test` | Run test command |
| `make ff-seed` | Run seed command |
| `make ff-docs` | Run docs command |
| `make ff-validate` | Run validate command |
| `make ff-ls` | Run ls command |

### Workflow Targets

| Target | Commands |
|--------|----------|
| `make dev-cycle` | seed → run → test |
| `make dev-validate` | compile → validate |
| `make dev-fresh` | Full refresh pipeline |

### Contributing

| Area | Guidelines |
|------|------------|
| Commits | Conventional commits format |
| PRs | Single feature/fix per PR |
| Tests | Required for new features |
| Docs | Update spec for API changes |
| Format | Run `cargo fmt` before commit |
| Lint | Run `cargo clippy` before commit |

---

## Appendix: dbt Comparison

### Feature Comparison

| Feature | dbt | Featherflow | Notes |
|---------|-----|-------------|-------|
| SQL templating | Jinja2 | Minijinja | Minijinja is subset |
| Dependencies | ref(), source() | AST extraction | FF is simpler |
| Materializations | view, table, incremental, ephemeral | view, table, incremental, ephemeral | Done |
| Tests | Built-in + custom | Built-in + custom | Done |
| Documentation | Yes | Yes | Done |
| Sources | Yes | Yes | Done |
| Seeds | Yes | Yes | Done |
| Snapshots | Yes | Yes | Done |
| Packages | Yes | No | Not planned |
| Hooks | Yes | Yes | Done |
| Exposures | Yes | Yes | Done |
| Metrics | Yes | Yes | Done |
| Column lineage | Limited | Full AST-based + DataFusion LogicalPlan | Done |
| Static analysis | None | DataFusion-based type checking, schema propagation | Done |
| Python models | Yes | No | Not planned |
| Adapters | Many | DuckDB first | Others later |

### When to Use dbt vs Featherflow

**Use dbt when**:
- You need many database adapters
- You rely on community packages
- You need Python models
- Your team already knows dbt
- You need enterprise support

**Use Featherflow when**:
- You use DuckDB primarily
- You want faster CLI performance
- You prefer plain SQL without ref()
- You want compile-time static analysis (type checking, schema validation)
- You want a simpler tool
- You want single binary deployment

### Migration Guide

| dbt Pattern | Featherflow Equivalent |
|-------------|----------------------|
| `{{ ref('model') }}` | Just use `model` or `schema.model` |
| `{{ source('src', 'table') }}` | Just use `schema.table` |
| `schema.yml` (multi-model) | Separate `model.yml` per model |
| `dbt_project.yml` | `featherflow.yml` |
| `profiles.yml` | Connection in `featherflow.yml` |
| `packages.yml` | Not supported (copy macros) |
| `{{ config(...) }}` | Same syntax |
| `{{ var(...) }}` | Same syntax |
| Custom macros | Same concept, `.jinja` files |

### Directory-Per-Model Migration Steps

When migrating from dbt (or from an earlier flat-file layout), follow these steps to adopt the directory-per-model structure:

1. **Create a subdirectory for each model**: For every `.sql` model file, create a subdirectory under `models/` with the same name as the model (without the `.sql` extension). For example, `models/stg_orders.sql` becomes `models/stg_orders/`.

2. **Move each `.sql` and `.yml` file into its matching directory**: Move `stg_orders.sql` and `stg_orders.yml` into `models/stg_orders/`. If migrating from dbt's shared `schema.yml`, split it into individual `.yml` files first.

3. **Ensure file names match directory names**: The `.sql` file stem must match the directory name exactly. `models/stg_orders/stg_orders.sql` is valid; `models/stg_orders/orders.sql` will produce error `E012 ModelDirectoryMismatch`.

4. **Remove nested subdirectories**: If you had `models/staging/stg_orders.sql`, flatten to `models/stg_orders/stg_orders.sql`. Featherflow does not support nested model directories — use naming conventions (e.g., `stg_`, `int_`, `fct_`) to indicate layers instead.

5. **Ensure every model has a schema file**: Featherflow requires a matching `.yml` or `.yaml` file for every model. Missing schema files will produce error `E010 MissingSchemaFile`.

**Example migration**:
```
# Before (dbt layout)
models/
  staging/
    stg_orders.sql
    stg_customers.sql
    schema.yml          # shared

# After (Featherflow layout)
models/
  stg_orders/
    stg_orders.sql
    stg_orders.yml      # extracted from schema.yml
  stg_customers/
    stg_customers.sql
    stg_customers.yml   # extracted from schema.yml
```
