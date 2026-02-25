# Featherflow

A lightweight dbt-like CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB.

## Features

- **SQL Templating**: Jinja-style templating with `config()` and `var()` functions
- **Custom Macros**: Reusable SQL macros loaded from `macro_paths` directories
- **AST-based Dependencies**: Automatically extracts dependencies from SQL using `sqlparser-rs` - no need for `ref()` or `source()` functions
- **Dependency-aware Execution**: Builds a DAG and executes models in topological order
- **Schema Testing**: Built-in support for 10 test types (`unique`, `not_null`, `positive`, `non_negative`, `accepted_values`, `min_value`, `max_value`, `regex`, `relationship`, `custom`) with sample failing rows
- **Source Definitions**: Document and test external data sources
- **Documentation Generation**: Generate markdown, JSON, or HTML docs from schema files, with an interactive docs server
- **DuckDB Backend**: In-memory or file-based DuckDB database execution
- **Static Analysis**: DataFusion-based SQL analysis passes for type checking and best practices
- **SQL Formatting**: Built-in SQL formatter with Jinja support
- **Column Lineage**: Trace column-level lineage across models

## Installation

### Quick install (macOS and Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/datastx/Feather-Flow/main/install.sh | bash
```

This detects your OS and architecture, downloads the correct binary from the latest GitHub Release, verifies the SHA256 checksum, and installs to `~/.local/bin`.

**Options:**

```bash
# Install a specific version
curl -fsSL https://raw.githubusercontent.com/datastx/Feather-Flow/main/install.sh | FF_VERSION=0.1.0 bash

# Install to a custom directory
curl -fsSL https://raw.githubusercontent.com/datastx/Feather-Flow/main/install.sh | INSTALL_DIR=/usr/local/bin bash
```

### Manual download

Pre-built binaries are available on the [Releases](https://github.com/datastx/Feather-Flow/releases) page:

| Platform | Artifact |
|---|---|
| Linux x86_64 | `ff-x86_64-linux-gnu` |
| macOS x86_64 | `ff-x86_64-apple-darwin` |
| macOS ARM (Apple Silicon) | `ff-aarch64-apple-darwin` |

```bash
# Example: download latest for Linux x86_64
curl -fsSL https://github.com/datastx/Feather-Flow/releases/latest/download/ff-x86_64-linux-gnu -o ff
chmod +x ff
sudo mv ff /usr/local/bin/
```

### Docker

```bash
docker pull ghcr.io/datastx/feather-flow:latest

# Run any ff command
docker run --rm -v "$(pwd)":/workspace -w /workspace ghcr.io/datastx/feather-flow compile
docker run --rm -v "$(pwd)":/workspace -w /workspace ghcr.io/datastx/feather-flow run
docker run --rm -v "$(pwd)":/workspace -w /workspace ghcr.io/datastx/feather-flow run --mode test
```

### From source

```bash
git clone https://github.com/datastx/Feather-Flow.git
cd Feather-Flow
make build-release
# Binary is at target/release/ff

# Or install directly
cargo install --path crates/ff-cli
```

## Quickstart

1. Create a project directory with a `featherflow.yml` configuration:

```yaml
name: my_project
version: "1.0.0"

database:
  type: duckdb
  path: ":memory:"

node_paths:
  - nodes
macro_paths:
  - macros

vars:
  env: dev
  start_date: "2024-01-01"
```

2. Create SQL models in the `nodes/` directory. Each node lives in its own directory with a `.sql` file and a `.yml` schema file:

```sql
-- nodes/stg_orders/stg_orders.sql
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

3. Create a schema file alongside your model (1:1 convention):

```yaml
# nodes/stg_orders/stg_orders.yml
version: 1
kind: sql
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

4. Run Featherflow commands:

```bash
# Load seed data
ff deploy seeds

# Compile models (renders Jinja, extracts dependencies, builds manifest)
ff compile

# List models and sources
ff ls

# Execute models in dependency order
ff run

# Run schema tests
ff run --mode test

# Validate project without execution
ff compile --parse-only

# Generate documentation
ff docs
```

## Commands

### `ff compile`

Compiles SQL models by rendering Jinja templates, extracting dependencies, and generating a manifest.

```bash
ff compile [--nodes <NODES>] [--parse-only] [--strict] [--output <FORMAT>]
```

Use `--parse-only` to validate without writing output files. Use `--strict` to treat warnings as errors.

### `ff run`

Executes models, runs tests, or both depending on the `--mode` flag.

```bash
ff run [--mode <MODE>] [--nodes <NODES>] [--full-refresh] [--fail-fast]
```

Run modes:
- `build` (default) - Execute models then run their tests in DAG order
- `models` - Execute models only
- `test` - Run tests only against existing tables

Node selectors:
- `model_name` - Run a specific model
- `+model_name` - Run model and all ancestors
- `model_name+` - Run model and all descendants
- `+model_name+` - Run model, ancestors, and descendants
- `tag:X` - Run models with a specific tag
- `path:X` - Run models in a specific path

### `ff ls`

Lists models and sources with their dependencies and materialization settings.

```bash
ff ls [--output <FORMAT>] [--nodes <NODES>] [--resource-type <TYPE>]
```

Output formats: `table` (default), `json`, `tree`, `path`

Resource types: `model`, `source`, `seed`, `test`, `function`

### `ff deploy`

Deploy seeds or functions to the database.

```bash
# Load CSV seed files
ff deploy seeds [--seeds <NAMES>] [--full-refresh]

# Deploy user-defined functions
ff deploy functions deploy [--functions <NAMES>]

# List, validate, show, or drop functions
ff deploy functions list|validate|show|drop
```

### `ff docs`

Generates documentation from schema files.

```bash
ff docs [--output <PATH>] [--format <FORMAT>] [--nodes <NODES>]

# Launch interactive documentation server
ff docs serve [--port <PORT>] [--no-browser] [--static-export <DIR>]
```

Output formats: `markdown` (default), `json`, `html`

### `ff lineage`

Trace column-level lineage across models.

```bash
ff lineage --node <NODE> --column <COLUMN> [--direction <DIR>] [--output <FORMAT>]
```

### `ff analyze`

Run static analysis passes on SQL models.

```bash
ff analyze [--nodes <NODES>] [--output <FORMAT>] [--severity <LEVEL>]
```

### `ff fmt`

Format SQL source files.

```bash
ff fmt [<PATHS>] [--nodes <NODES>] [--check] [--diff]
```

### `ff init`

Initialize a new Featherflow project.

```bash
ff init [--name <NAME>] [--database_path <PATH>]
```

### Other Commands

- `ff clean [--dry-run]` — Remove generated artifacts
- `ff run-macro <MACRO_NAME> [--args <JSON>]` — Execute a standalone SQL macro
- `ff meta query <SQL>` / `ff meta export` / `ff meta tables` — Query the metadata database

## Global Options

- `--project-dir, -p <DIR>`: Project directory (default: current directory)
- `--config, -c <FILE>`: Config file path
- `--target, -t <TARGET>`: Override target (database connection)
- `--verbose, -v`: Enable verbose output

## Configuration

### featherflow.yml

```yaml
name: project_name          # Project name
version: "1.0.0"            # Project version

database:
  type: duckdb              # Database type (duckdb, snowflake)
  path: ":memory:"          # Database path (":memory:" for in-memory)
  name: main                # Logical database name

dialect: duckdb             # SQL dialect (duckdb, snowflake)
materialization: view       # Default materialization (view, table, incremental, ephemeral)
schema: main                # Default schema

node_paths:                 # Directories containing all node types (models, seeds, functions, sources)
  - nodes
macro_paths:                # Directories containing macro files
  - macros
target_path: target         # Output directory for compiled files

on_run_start:               # SQL executed before each run
  - "CREATE TABLE IF NOT EXISTS audit (id INTEGER)"
on_run_end:                 # SQL executed after each run
  - "INSERT INTO audit (id) VALUES (1)"

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
- `materialized`: `'view'`, `'table'`, `'incremental'`, or `'ephemeral'`
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
├── nodes/
│   ├── stg_orders/
│   │   ├── stg_orders.sql      # SQL model
│   │   └── stg_orders.yml      # Schema + tests
│   ├── stg_customers/
│   │   ├── stg_customers.sql
│   │   └── stg_customers.yml
│   ├── fct_orders/
│   │   ├── fct_orders.sql
│   │   └── fct_orders.yml
│   └── raw_orders/
│       ├── raw_orders.csv      # Seed data (kind: seed)
│       └── raw_orders.yml
├── macros/
│   └── date_utils.sql
└── target/
    ├── compiled/
    ├── manifest.json
    └── run_results.json
```

## Development

### Building

```bash
# Build all crates
make build

# Run tests
make test

# Run end-to-end tests
make ci-e2e

# Run CI checks (format, clippy, test, doc)
make ci
```

### Crate Structure

- `ff-cli`: CLI binary and commands
- `ff-core`: Core library (config, project, DAG, node types)
- `ff-sql`: SQL parsing and dependency extraction
- `ff-jinja`: Jinja templating layer with macro support
- `ff-db`: Database abstraction and DuckDB backend
- `ff-test`: Test generation and execution
- `ff-analysis`: DataFusion-based static SQL analysis
- `ff-meta`: Metadata database (DuckDB) for schema, population, and rules

## License

MIT
