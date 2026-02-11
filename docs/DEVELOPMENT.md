# Development Guide

## Prerequisites

- Rust stable toolchain (install via [rustup](https://rustup.rs))
- Make

No Python. No Node. No package managers beyond Cargo.

## Building

```bash
make build          # Build all crates (debug)
make build-release  # Build release binary
```

## Testing

```bash
make test           # Run all tests
make test-unit      # Unit tests only
make test-integration  # Integration tests only
make test-verbose   # Tests with full output
```

## Linting

```bash
make lint           # clippy + fmt check
make ci             # Full CI check (lint + test)
```

## Project Layout

```
crates/
  ff-cli/           # Binary crate: CLI entry point and subcommands
    src/
      cli.rs         # Clap argument definitions
      main.rs        # Entry point
      commands/      # One file per subcommand
        run.rs
        compile.rs
        validate.rs
        test.rs
        seed.rs
        clean.rs
        docs.rs
        ls.rs
        lineage.rs
        init.rs
        snapshot.rs
        freshness.rs
        parse.rs
        analyze.rs
        diff.rs
        run_operation.rs
        mod.rs
    tests/
      fixtures/      # Integration test fixtures

  ff-core/           # Shared types: config, model, DAG, manifest
    src/
      config.rs      # Config deserialization
      model.rs       # Model, ModelSchema, SchemaColumnDef
      project.rs     # Project loading and model discovery
      dag.rs         # ModelDag (petgraph wrapper)
      manifest.rs    # Build manifest for CI/CD
      run_state.rs   # Partial execution tracking
      source.rs      # External source definitions
      breaking.rs    # Breaking change detection
      error.rs       # CoreError (thiserror)
      lib.rs

  ff-sql/            # SQL parsing, validation, lineage
    src/
      parser.rs      # SqlParser (sqlparser-rs wrapper)
      dialect.rs     # DuckDB/Snowflake dialect support
      extractor.rs   # Dependency extraction from AST
      validator.rs   # CTE/derived table rejection
      lineage.rs     # Column-level lineage
      inline.rs      # Ephemeral model CTE inlining
      suggestions.rs # Test suggestion engine
      error.rs       # SqlError with codes S001-S006
      lib.rs

  ff-analysis/       # IR and static analysis passes
    src/
      ir/
        relop.rs     # RelOp: relational algebra operators
        expr.rs      # TypedExpr: typed expression tree
        schema.rs    # RelSchema: output schema
        types.rs     # SqlType, Nullability, TypedColumn
      pass/
        mod.rs       # PassManager, AnalysisPass, DagPass traits
        type_inference.rs
        nullability.rs
        join_keys.rs
        unused_columns.rs
      lowering/      # SQL AST → IR lowering
        mod.rs       # Entry point (lower_statement)
        expr.rs      # Expression lowering
        join.rs      # JOIN lowering
        query.rs     # Query lowering
        select.rs    # SELECT lowering
      context.rs     # AnalysisContext
      lib.rs

  ff-jinja/          # Jinja template rendering
    src/
      environment.rs # JinjaEnvironment
      functions.rs   # config(), var(), is_incremental()
      builtins.rs    # 17 built-in macros
      custom_tests.rs # Custom test macro discovery
      error.rs       # JinjaError with codes J001-J003
      lib.rs

  ff-db/             # Database backend
    src/
      traits.rs      # Database trait definition
      duckdb.rs      # DuckDbBackend implementation
      snowflake.rs   # Snowflake backend (future)
      error.rs       # DbError (thiserror)
      lib.rs         # Re-exports

  ff-test/           # Schema test generation and execution
    src/
      generator.rs   # Test SQL generation (9 test types)
      runner.rs      # TestRunner, TestResult, TestSummary
      lib.rs

tests/
  fixtures/          # Shared test fixtures
    sample_project/  # Valid project for integration tests
    broken_project/  # Invalid project (circular deps, syntax errors)
```

## Adding a New CLI Command

1. Add the subcommand enum variant and args struct to `crates/ff-cli/src/cli.rs`
2. Create `crates/ff-cli/src/commands/<name>.rs` with `pub async fn execute()`
3. Add the module to `crates/ff-cli/src/commands/mod.rs`
4. Wire the dispatch in `crates/ff-cli/src/main.rs`

## Adding a New Analysis Pass

1. Create `crates/ff-analysis/src/pass/<name>.rs`
2. Implement the `AnalysisPass` trait (per-model) or `DagPass` trait (cross-model)
3. Register in `PassManager::with_defaults()` in `crates/ff-analysis/src/pass/mod.rs`
4. Assign diagnostic codes from the appropriate range

## Error Handling

- Library crates (`ff-core`, `ff-sql`, etc.) use `thiserror` for typed errors
- The CLI crate (`ff-cli`) uses `anyhow` for error propagation with `.context()`
- Error codes are namespaced: E0xx (core), S0xx (sql), J0xx (jinja), A0xx (analysis)
- Use `?` with `.context()` at crate boundaries
- No `unwrap()` outside of tests

## Test Fixtures

Two fixture directories serve different purposes:

- `tests/fixtures/` -- shared fixtures used by integration tests at the workspace root
- `crates/ff-cli/tests/fixtures/` -- CLI-specific integration test fixtures

Both follow the directory-per-model layout. The `broken_project` fixture intentionally contains errors (circular deps, syntax errors) for negative testing.

**When creating test fixtures:**
- Every `.sql` file must have a matching `.yml`
- Use the directory-per-model layout: `models/<name>/<name>.sql`
- When writing unit tests with `TempDir`, create the subdirectory before writing files

## Key Dependencies

| Crate | Purpose |
| --- | --- |
| `clap` v4 | CLI argument parsing (derive API) |
| `minijinja` | Jinja template rendering |
| `sqlparser` | SQL parsing into AST |
| `duckdb` | DuckDB database (bundled feature) |
| `tokio` | Async runtime |
| `petgraph` | Directed graph for DAG |
| `serde` / `serde_yaml` / `serde_json` | Serialization |
| `thiserror` | Typed error definitions |
| `anyhow` | Error propagation in CLI |
| `chrono` | Date/time for run state and freshness |
| `uuid` | Run ID generation |
