# Featherflow

A lightweight dbt-like CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB.

## Features

- **SQL Templating**: Jinja-style templating with `config()` and `var()` functions
- **Custom Macros**: Reusable SQL macros loaded from `macro_paths` directories
- **AST-based Dependencies**: Automatically extracts dependencies from SQL using `sqlparser-rs` - no need for `ref()` or `source()` functions
- **Dependency-aware Execution**: Builds a DAG and executes models in topological order
- **Schema Testing**: Built-in support for 8 test types (`unique`, `not_null`, `positive`, `non_negative`, `accepted_values`, `min_value`, `max_value`, `regex`) with sample failing rows
- **Source Definitions**: Document and test external data sources
- **Documentation Generation**: Generate markdown or JSON docs from schema files
- **DuckDB Backend**: In-memory or file-based DuckDB database execution
- **Multiple Output Formats**: JSON, table, and tree output formats

## Installation

### From Source

```bash
cargo install --path crates/ff-cli
```

### Building

```bash
cargo build --release
```

## Quickstart

1. Create a project directory with a `featherflow.yml` configuration:

```yaml
name: my_project
version: "1.0"

database:
  type: duckdb
  path: ":memory:"

model_paths:
  - models
seed_paths:
  - seeds
source_paths:
  - sources
macro_paths:
  - macros

vars:
  env: dev
  start_date: "2024-01-01"
```

2. Create SQL models in the `models/` directory:

```sql
-- models/staging/stg_orders.sql
{{ config(materialized='view', schema='staging') }}

SELECT
    id AS order_id,
    user_id AS customer_id,
    created_at AS order_date,
    amount,
    status
FROM raw_orders
WHERE created_at >= '{{ var("start_date") }}'
```

3. Create a schema file with the same name as your model (1:1 convention):

```yaml
# models/staging/stg_orders.yml
version: 1
description: "Staged orders from raw source"
owner: data-team
tags:
  - staging
  - orders

columns:
  - name: order_id
    tests:
      - unique
      - not_null
  - name: customer_id
    tests:
      - not_null
  - name: order_date
  - name: amount
  - name: status
```

4. Optionally define external sources in `sources/`:

```yaml
# sources/raw_ecommerce.yml
kind: sources
version: 1

name: raw_ecommerce
description: "Raw e-commerce data"
schema: main

tables:
  - name: raw_orders
    description: "Raw order data"
    columns:
      - name: id
        type: INTEGER
        tests:
          - unique
          - not_null
      - name: user_id
        type: INTEGER
      - name: amount
        type: DECIMAL(10,2)
```

5. Run Featherflow commands:

```bash
# Load seed data
ff seed

# Compile models (renders Jinja, extracts dependencies, builds manifest)
ff compile

# List models and sources
ff ls

# Execute models in dependency order
ff run

# Run schema tests
ff test

# Validate project without execution
ff validate

# Generate documentation
ff docs
```

## Commands

### `ff compile`

Compiles SQL models by rendering Jinja templates, extracting dependencies, and generating a manifest.

```bash
ff compile [--project-dir <DIR>] [--models <MODELS>]
```

### `ff run`

Executes compiled models in topological order.

```bash
ff run [--target <DB_PATH>] [--select <SELECTOR>] [--full-refresh]
```

Selectors:
- `model_name` - Run a specific model
- `+model_name` - Run model and all ancestors
- `model_name+` - Run model and all descendants
- `+model_name+` - Run model, ancestors, and descendants

### `ff ls`

Lists models and sources with their dependencies and materialization settings.

```bash
ff ls [--output <FORMAT>] [--select <SELECTOR>]
```

Output formats: `table` (default), `json`, `tree`

### `ff parse`

Parses models and outputs AST or dependency information.

```bash
ff parse [--models <MODELS>] [--output <FORMAT>]
```

Output formats: `pretty` (default), `json`, `deps`

### `ff test`

Runs schema tests defined in model and source schema files.

```bash
ff test [--target <DB_PATH>] [--models <MODELS>] [--fail-fast]
```

### `ff seed`

Loads CSV seed files into the database.

```bash
ff seed [--seeds <NAMES>] [--full-refresh]
```

### `ff validate`

Validates project configuration, SQL syntax, and schema files without executing.

```bash
ff validate [--models <MODELS>] [--strict]
```

### `ff docs`

Generates documentation from schema files.

```bash
ff docs [--output <PATH>] [--format <FORMAT>] [--models <MODELS>]
```

Output formats: `markdown` (default), `json`, `html`

Generates `lineage.dot` Graphviz diagram for visualization.

## Global Options

- `--project-dir, -p <DIR>`: Project directory (default: current directory)
- `--config, -c <FILE>`: Config file path
- `--target, -t <PATH>`: Database path (overrides config)
- `--verbose, -v`: Enable verbose output

## Configuration

### featherflow.yml

```yaml
name: project_name          # Project name
version: "1.0"              # Project version

database:
  type: duckdb              # Database type (duckdb, snowflake)
  path: ":memory:"          # Database path (":memory:" for in-memory)

dialect: duckdb             # SQL dialect (duckdb, snowflake)
materialization: view       # Default materialization (view, table)
schema: main                # Default schema

model_paths:                # Directories containing SQL models
  - models
seed_paths:                 # Directories containing CSV seed files
  - seeds
source_paths:               # Directories containing source definitions
  - sources
macro_paths:                # Directories containing macro files
  - macros
target_path: target         # Output directory for compiled files

vars:                       # Variables accessible via var()
  env: dev
  start_date: "2024-01-01"
```

### Model Configuration

Use the `config()` function in your SQL models:

```sql
{{ config(materialized='table', schema='staging') }}

SELECT * FROM raw_data
```

Supported config options:
- `materialized`: `'view'` or `'table'`
- `schema`: Target schema name

### Variables

Access variables with `var()`:

```sql
SELECT * FROM {{ var('schema') }}.users
WHERE env = '{{ var('env', 'prod') }}'
```

### Custom Macros

Create reusable macros in `macros/`:

```sql
-- macros/date_utils.sql
{% macro date_trunc(date_col, granularity) %}
DATE_TRUNC('{{ granularity }}', {{ date_col }})
{% endmacro %}
```

Use in models:

```sql
{% from "date_utils.sql" import date_trunc %}

SELECT
    {{ date_trunc('order_date', 'month') }} AS order_month,
    SUM(amount) AS total
FROM orders
GROUP BY 1
```

## Project Structure

```
my_project/
├── featherflow.yml
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_orders.yml      # 1:1 schema file
│   │   ├── stg_customers.sql
│   │   └── stg_customers.yml   # 1:1 schema file
│   └── marts/
│       ├── fct_orders.sql
│       └── fct_orders.yml      # 1:1 schema file
├── seeds/
│   ├── raw_orders.csv
│   └── raw_customers.csv
├── sources/
│   └── raw_ecommerce.yml       # kind: sources
├── macros/
│   └── date_utils.sql
└── target/
    ├── compiled/
    ├── manifest.json
    ├── run_results.json
    └── docs/
```

## Development

### Building

```bash
# Build all crates
make build

# Run tests
make test

# Run CI checks (format, clippy, test, doc)
make ci
```

### Make Targets

```bash
# CLI commands
make ff-seed          # Load seed data
make ff-compile       # Compile models
make ff-run           # Execute models
make ff-ls            # List models
make ff-test          # Run tests
make ff-validate      # Validate project
make ff-docs          # Generate documentation

# Development workflows
make dev-cycle        # seed -> run -> test
make dev-validate     # compile -> validate
```

### Crate Structure

- `ff-cli`: CLI binary and commands
- `ff-core`: Core library (config, project, DAG, sources)
- `ff-sql`: SQL parsing and dependency extraction
- `ff-jinja`: Jinja templating layer with macro support
- `ff-db`: Database abstraction and DuckDB backend
- `ff-test`: Test generation and execution

## License

MIT
