# Featherflow

A Rust CLI tool for SQL templating and execution, similar to dbt.

## Tech Stack
- Rust (stable toolchain)
- Clap v4 (CLI framework, derive API)
- Minijinja (templating)
- sqlparser-rs (SQL parsing)
- duckdb-rs (database, bundled feature)
- tokio (async runtime)

## Project Structure
- `crates/ff-cli`: Main binary, subcommands in `commands/` module
- `crates/ff-core`: Shared types, config, DAG logic
- `crates/ff-jinja`: Template rendering (config, var functions only)
- `crates/ff-sql`: SQL parsing, table extraction from AST
- `crates/ff-db`: Database trait + DuckDB implementation
- `crates/ff-test`: Schema test generation (unique, not_null)

## Key Commands
```bash
make build          # Build all crates
make test           # Run all tests
make lint           # Run clippy + fmt check
make ci             # Full CI check locally
cargo run -p ff-cli -- <subcommand>
```

## Architecture Notes
- Dependencies extracted from SQL AST via `visit_relations`, NOT Jinja functions
- No ref() or source() - just plain SQL with table names
- Tables in models/ become dependencies; external tables defined in config
- Error handling: thiserror in libs, anyhow in CLI

## Node Architecture
All resource types are unified under a `NodeKind` system. Each node lives in its
own directory with a required `.yml` configuration file. The `kind` field in the
YAML determines the resource type:

| kind       | companion file | description                     |
|------------|----------------|---------------------------------|
| `sql`      | `<name>.sql`   | SQL transformation model        |
| `seed`     | `<name>.csv`   | CSV seed data                   |
| `source`   | *(none)*       | External data source definition |
| `function` | `<name>.sql`   | User-defined SQL function       |
| `python`   | `<name>.py`    | Python transformation (planned) |

### Unified node_paths layout (preferred)
```yaml
# featherflow.yml
node_paths: ["nodes"]
```
```
nodes/
  stg_orders/
    stg_orders.sql
    stg_orders.yml       # kind: sql
  raw_orders/
    raw_orders.csv
    raw_orders.yml       # kind: seed
  raw_ecommerce/
    raw_ecommerce.yml    # kind: source
  cents_to_dollars/
    cents_to_dollars.sql
    cents_to_dollars.yml # kind: function
```

### Legacy per-type layout (still supported)
```yaml
# featherflow.yml
model_paths: ["models"]
source_paths: ["sources"]
function_paths: ["functions"]
```

Legacy kind values (`model`, `sources`, `functions`) are normalised to their
modern equivalents (`sql`, `source`, `function`) automatically.

## Testing
- All tests: `make test`
- Unit tests only: `make test-unit`
- Integration tests: `make test-integration`
- Verbose output: `make test-verbose`
- Test fixtures in `tests/fixtures/sample_project/`
- Seed data in `testdata/seeds/`

## Code Style
- Use `?` for error propagation, add `.context()` at boundaries
- Prefer `impl Trait` over `Box<dyn Trait>` where possible
- All public items need rustdoc comments
- No unwrap() except in tests
