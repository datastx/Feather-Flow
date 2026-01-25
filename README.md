# Featherflow

A lightweight dbt-like CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB.

## Features

- **SQL Templating**: Jinja-style templating with `config()` and `var()` functions
- **AST-based Dependencies**: Automatically extracts dependencies from SQL using `sqlparser-rs` - no need for `ref()` or `source()` functions
- **Dependency-aware Execution**: Builds a DAG and executes models in topological order
- **Schema Testing**: Built-in support for `unique` and `not_null` column tests
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

vars:
  env: dev

external_tables:
  - raw_users
  - raw_events
```

2. Create SQL models in the `models/` directory:

```sql
-- models/stg_users.sql
{{ config(materialized='view') }}

SELECT
    id AS user_id,
    name AS user_name,
    created_at
FROM raw_users
```

```sql
-- models/fct_user_events.sql
{{ config(materialized='table') }}

SELECT
    u.user_id,
    u.user_name,
    COUNT(e.id) AS event_count
FROM stg_users u
LEFT JOIN raw_events e ON u.user_id = e.user_id
GROUP BY u.user_id, u.user_name
```

3. Optionally define tests in `models/schema.yml`:

```yaml
version: 2

models:
  - name: stg_users
    columns:
      - name: user_id
        tests:
          - unique
          - not_null
```

4. Run Featherflow commands:

```bash
# Compile models (renders Jinja, extracts dependencies, builds manifest)
ff compile

# List models and their dependencies
ff ls

# Execute models in dependency order
ff run --target data.duckdb

# Run schema tests
ff test --target data.duckdb
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

Lists models with their dependencies and materialization settings.

```bash
ff ls [--output <FORMAT>] [--select <SELECTOR>]
```

Output formats: `table` (default), `json`, `tree`

### `ff parse`

Parses models and outputs AST or dependency information.

```bash
ff parse [--models <MODELS>] [--output <FORMAT>]
```

Output formats: `deps` (default), `json`, `pretty`

### `ff test`

Runs schema tests defined in `schema.yml` files.

```bash
ff test [--target <DB_PATH>] [--models <MODELS>]
```

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

vars:                       # Variables accessible via var()
  env: dev
  schema: analytics

external_tables:            # Tables not defined as models
  - raw_orders
  - raw_customers
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

## Project Structure

```
my_project/
├── featherflow.yml
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── schema.yml
│   └── marts/
│       ├── fct_orders.sql
│       └── schema.yml
└── seeds/
    ├── raw_orders.csv
    └── raw_customers.csv
```

## Development

### Building

```bash
# Build all crates
cargo build

# Run tests
cargo test --workspace

# Run CI checks (format, clippy, test, doc)
make ci
```

### Crate Structure

- `ff-cli`: CLI binary and commands
- `ff-core`: Core library (config, project, DAG)
- `ff-sql`: SQL parsing and dependency extraction
- `ff-jinja`: Jinja templating layer
- `ff-db`: Database abstraction and DuckDB backend
- `ff-test`: Test generation and execution

## License

MIT
