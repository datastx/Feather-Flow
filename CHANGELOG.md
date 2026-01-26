# Changelog

All notable changes to Featherflow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-25

### Added

- Initial release of Featherflow CLI
- **8 CLI Commands**:
  - `ff parse` - Parse SQL files and extract dependencies (AST-based)
  - `ff compile` - Render Jinja templates to SQL, generate manifest
  - `ff run` - Execute models in dependency order with manifest caching
  - `ff ls` - List models with dependencies (table, JSON, tree output)
  - `ff test` - Run schema tests with 8 test types
  - `ff seed` - Load CSV seed files into database
  - `ff validate` - Validate project without execution
  - `ff docs` - Generate documentation (Markdown, JSON, HTML + lineage diagram)
- **DuckDB backend** - Full support for in-memory and file-based databases
- **Jinja templating** - `config()` and `var()` functions with custom macro support
- **DAG-based dependency resolution** - Topological sort with cycle detection
- **Selector syntax** - `+model` (ancestors), `model+` (descendants), `+model+` (both)
- **Schema testing** - 8 test types: unique, not_null, positive, non_negative, accepted_values, min_value, max_value, regex
- **1:1 schema file convention** - Model metadata in matching .yml files
- **Source file support** - Define external data sources with `kind: sources`
- **Custom macros** - Load macros from `macro_paths` directories
- **Manifest caching** - Skip recompilation when files unchanged (`--no-cache` to force)
- **Lineage diagram** - Generate Graphviz DOT file for visualization
- **Exit codes** - 0=success, 1=general, 2=test failures, 3=circular deps, 4=database
- **Error codes** - Comprehensive error codes (E001-E010, S001-S004, J001-J003, D001-D005, SRC001-SRC007, M001-M002)

### Known Limitations

- Snowflake backend not implemented (stub only)
- No parallel model execution
- No incremental runs (full execution each time)
- No pre/post hooks

## [Unreleased]

### Planned for v0.2.0

- Snowflake backend implementation
- Parallel model execution
- Incremental runs with state tracking
- Pre/post hooks for models
