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

## Testing
- Unit tests: `cargo test -p <crate>`
- Integration tests: `cargo test --test integration_tests`
- Test fixtures in `tests/fixtures/sample_project/`
- Seed data in `testdata/seeds/`

## Code Style
- Use `?` for error propagation, add `.context()` at boundaries
- Prefer `impl Trait` over `Box<dyn Trait>` where possible
- All public items need rustdoc comments
- No unwrap() except in tests
