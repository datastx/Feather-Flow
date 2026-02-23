# Featherflow

A Rust CLI tool for SQL templating and execution, similar to dbt.

## Tech Stack
- Rust (stable toolchain)
- Clap v4 (CLI framework, derive API)
- Minijinja (templating)
- sqlparser-rs (SQL parsing)
- duckdb-rs (database, bundled feature)
- tokio (async runtime)


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

Legacy kind values (`model`, `sources`, `functions`) are normalised to their
modern equivalents (`sql`, `source`, `function`) automatically.

## Testing
- All tests: `make test`
- End to end test harness: `make ci-e2e`