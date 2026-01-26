# Featherflow Technical Specification (Distilled)

A dbt-like CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB (with Snowflake swappability).

---

## What Featherflow Delivers

Featherflow (`ff`) implements core dbt functionality—SQL templating with Jinja, dependency-aware model execution, and schema testing—while maintaining a clean abstraction layer for database swappability.

**8 Subcommands**: `parse`, `compile`, `run`, `ls`, `test`, `seed`, `docs`, `validate`

**Key Architectural Decision**: Dependencies are extracted directly from the SQL AST using `sqlparser-rs`'s `visit_relations` function, eliminating the need for `ref()` and `source()` Jinja functions. This simplifies the templating layer and makes SQL more portable.

---

## Architecture Decisions & Rationale

### 1. AST-Based Dependency Extraction (No ref/source)

**Why**:
- Simplifies Jinja layer (only `config()` and `var()` needed)
- SQL files are valid SQL without preprocessing
- Leverages existing parser for validation
- Future-proofs for column-level lineage
- Reduces cognitive load for users

**Implementation**: `ff-sql` crate's `extractor.rs` uses `visit_relations` to walk the AST and collect all `ObjectName` references.

### 2. Derive-Based Clap with Flattened Global Options

**Why**: Derive API covers 95%+ of use cases with declarative, type-safe argument definitions. Global options (`--verbose`, `--config`, `--target`) use `#[arg(global = true)]`.

### 3. Layered Error Handling (thiserror + anyhow)

**Why**: Library errors need typed variants for downstream handling. CLI wraps these with `anyhow::Context` for user-friendly messages.

### 4. Trait-Based Database Abstraction

**Why**: Enables Snowflake swappability. The `Database` trait includes: `execute_batch`, `create_table_as`, `create_view_as`, `relation_exists`, `query_count`, `load_csv`.

### 5. Virtual Workspace with Flat Crate Structure

**Why**: Scales cleanly for future tooling. Shared `workspace.dependencies` eliminates version drift.

### 6. CSV Seed Data for Integration Testing

**Why**:
- DuckDB has excellent CSV support (`read_csv_auto`)
- Human-readable and version-controllable
- Easy to generate expected outputs
- Matches dbt's seed concept

### 7. 1:1 Schema File Naming Convention

**Why** (unlike dbt's multi-model schema.yml):
- Explicit linking—no ambiguity about which schema applies to which model
- No database introspection needed—column metadata comes from YAML
- Faster execution—tests validate against declared schema without DB queries
- Better tooling—IDEs can provide autocomplete from schema files
- Optional by default—models work without schema files

---

## Configuration

### featherflow.yml Structure

| Field | Purpose |
|-------|---------|
| `name` | Project identifier |
| `model_paths` | Directories containing SQL models |
| `seed_paths` | Directories containing CSV seed files |
| `source_paths` | Source definition files |
| `target_path` | Output directory for compiled artifacts |
| `materialization` | Default: `view` or `table` |
| `schema` | Target schema for models |
| `dialect` | SQL dialect (`duckdb`, `snowflake`) |
| `database` | Connection settings |
| `vars` | Variables available in Jinja templates |
| `macro_paths` | Directories containing macro files |

### Config Precedence (highest to lowest)

1. SQL `config()` function
2. Schema YAML `config:` section
3. Project `featherflow.yml` defaults

---

## Subcommand Specifications

### 1. `ff parse`

**Purpose**: Parse SQL files and output structured AST representation for debugging/validation.

**Inputs**: `--models`, `--output` (json/pretty/deps), `--dialect`

**Outputs**: JSON AST, human-readable tree, or dependency list; parse errors with line/column

**Definition of Done**:
- [x] Parses all model files in project
- [x] Extracts table dependencies from AST
- [x] Categorizes deps as model vs external
- [x] Reports parse errors with file path, line, column
- [x] Integration test: parse sample project, verify deps

### 2. `ff compile`

**Purpose**: Render Jinja templates to raw SQL, extract dependencies, output to target directory.

**Inputs**: `--models`, `--output-dir`, `--vars`

**Outputs**: Compiled SQL files in `target/compiled/`, manifest at `target/manifest.json`

**Definition of Done**:
- [x] Compiles Jinja to pure SQL
- [x] Extracts dependencies from AST (not Jinja)
- [x] `config()` values captured in manifest
- [x] Circular dependency detection with clear error
- [x] Manifest includes: models, dependencies, materialization
- [x] Integration test: compile project, verify manifest

### 3. `ff run`

**Purpose**: Execute compiled SQL against the database in dependency order.

**Inputs**: `--models`, `--select` (dbt-style: `+model`, `model+`), `--full-refresh`

**Outputs**: Progress with timing, exit summary, `target/run_results.json`

**Definition of Done**:
- [x] Executes models in correct dependency order
- [x] `materialized='view'` creates VIEW
- [x] `materialized='table'` creates TABLE
- [x] Clear error messages on SQL execution failure
- [x] `--select +model` runs model and all ancestors
- [x] Integration test: run models, verify tables exist

### 4. `ff ls`

**Purpose**: List models, dependencies, and materialization types.

**Inputs**: `--output` (table/json/tree), `--select`

**Outputs**: Table listing, JSON array, or dependency tree

**Definition of Done**:
- [x] Lists all models with name, materialization
- [x] Shows dependencies (model vs external)
- [x] JSON output is valid and complete
- [x] Tree output shows hierarchy
- [x] Integration test: ls output matches expected

### 5. `ff test`

**Purpose**: Run schema tests on specified models.

**Inputs**: `--models`, `--fail-fast`

**Outputs**: Test results (pass/fail with row counts), sample failing rows, exit code (0=pass, 2=fail)

**Built-in Test Types**:
| Test | Description |
|------|-------------|
| `unique` | No duplicate values |
| `not_null` | No NULL values |
| `positive` | Values > 0 |
| `non_negative` | Values >= 0 |
| `accepted_values` | Value in allowed list |
| `min_value` | Value >= threshold |
| `max_value` | Value <= threshold |
| `regex` | Value matches pattern |

**Definition of Done**:
- [x] Reads tests from model's .yml schema file (1:1 naming)
- [x] Generates correct SQL for all 8 built-in tests
- [x] Handles parameterized tests (accepted_values)
- [x] Reports pass/fail with timing
- [x] Shows sample failing rows (limit 5)
- [x] Skips models without schema files (with info message)
- [x] Exit code 2 on any failure
- [x] Integration test with pass and fail cases

### 6. `ff seed`

**Purpose**: Load CSV seed files into database.

**Inputs**: `--seeds`, `--full-refresh`

**Outputs**: Row count per seed

**Definition of Done**:
- [x] Discovers all .csv files in seed_paths
- [x] Creates tables named after file (without .csv extension)
- [x] Uses DuckDB's `read_csv_auto()` for type inference
- [x] `--seeds` flag filters which seeds to load
- [x] `--full-refresh` drops existing tables first
- [x] Reports row count per seed
- [x] Handles missing seed directory gracefully
- [x] Integration test: seeds load and are queryable

### 7. `ff docs`

**Purpose**: Generate documentation from schema files (no database access).

**Inputs**: `--models`, `--output`, `--format` (markdown/html/json)

**Outputs**: Per-model docs, index file, optional lineage diagram

**Definition of Done**:
- [x] Generates markdown docs for each model with schema
- [x] Index file lists all models with descriptions
- [x] Works without database connection
- [x] Skips models without schema files (with note in index)
- [x] JSON output includes all metadata
- [x] Integration test: docs match expected output

### 8. `ff validate`

**Purpose**: Validate project without execution.

**Inputs**: `--models`, `--strict`

**Outputs**: Errors, warnings, info; exit code (0=valid, 1=errors)

**Validations**:
| Check | Level |
|-------|-------|
| SQL syntax | Error |
| Circular dependencies | Error |
| Duplicate model names | Error |
| Jinja variables defined | Error |
| Schema file syntax | Error |
| Schema column tests | Warning |
| Orphaned schema files | Warning |
| External table declaration | Warning |
| Reference model exists | Warning |
| Type/test compatibility | Info |

**Definition of Done**:
- [x] Catches SQL syntax errors with file:line:col
- [x] Detects circular dependencies
- [x] Detects duplicate model names
- [x] Warns on undefined Jinja variables
- [x] Warns on orphaned schema files
- [x] `--strict` mode fails on warnings
- [x] No database connection required
- [x] Integration test: validate pass and fail cases

---

## Error Handling

### Error Codes

| Code | Type | Template |
|------|------|----------|
| E001 | ConfigNotFound | Config file not found: {path} |
| E002 | ConfigParseError | Failed to parse config: {details} |
| E003 | ConfigInvalid | Invalid config: {field} - {reason} |
| E004 | ProjectNotFound | Project directory not found: {path} |
| E005 | ModelNotFound | Model not found: {name} |
| E006 | ModelParseError | SQL parse error in {file}:{line}:{col}: {message} |
| E007 | CircularDependency | Circular dependency detected: {path} |
| E008 | DuplicateModel | Duplicate model name: {name} in {path1} and {path2} |
| E010 | SchemaParseError | Schema file parse error in {file}: {details} |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Test failures |
| 3 | Circular dependency |
| 4 | Database error |

---

## Known Limitations & Edge Cases

### High Priority (Fixed)

| Issue | Status |
|-------|--------|
| CTE names captured as dependencies | Fixed—filtered out in extractor |
| Duplicate model name detection | Fixed—checked during discovery |
| Schema not auto-created before model | Fixed—schemas created before execution |
| Case-sensitive model matching | Fixed—uses case-insensitive matching |

### Medium Priority

| Issue | Mitigation |
|-------|------------|
| Diamond dependencies with schema mismatch | Preserve full qualified names when possible |
| No transaction boundaries (partial state on failure) | Document as expected; consider `--retry-failed` |
| View dependencies during full refresh | Order drops in reverse dependency order |
| Schema-SQL column mismatch | `ff validate` warns on mismatches |
| Test on non-existent column | Validate column existence during test generation |
| Unknown test types silently ignored | Warn when unknown test types encountered |
| Identifier quoting not implemented | Implement proper quoting for special characters |

### Low Priority / By Design

| Issue | Notes |
|-------|-------|
| Table functions not detected | By design—explicit config preferred |
| No source() function | By design—AST extraction handles this |
| Tests pass on empty tables | Add optional `row_count` test or `--warn-empty` |
| Macro cannot import other macros | Minijinja limitation—document it |
| Single macro loader | Use single macro_paths directory |

---

## Testing Strategy

### Test Commands

```bash
make test               # All tests
make test-unit          # Unit tests only
make test-integration   # Integration tests only
make test-verbose       # With output
```

### Test Fixtures

- **Location**: `tests/fixtures/sample_project/`
- **Seed data**: `testdata/seeds/` (raw_orders.csv, raw_customers.csv, raw_products.csv)

### Integration Test Pattern

1. Load seeds into in-memory DuckDB
2. Load project configuration
3. Run compile/run/test commands
4. Verify expected outputs (tables exist, row counts, manifest contents)

### Key Test Cases

| Command | Test Case |
|---------|-----------|
| `ff parse` | Parse sample project, verify dependency extraction |
| `ff compile` | Compile project, verify manifest.json structure |
| `ff run` | Run models, verify tables/views exist |
| `ff test` | Both passing and failing test scenarios |
| `ff validate` | Valid project passes; invalid project with cycle/duplicate fails |

---

## Implementation Status

### Core Infrastructure: Complete

| Component | Status |
|-----------|--------|
| Workspace structure | Done |
| ff-core | Done |
| ff-sql | Done |
| ff-jinja | Done |
| ff-db | Partial (DuckDB done, Snowflake stub) |
| ff-test | Done |
| ff-cli | Done (all 8 commands) |

### All CLI Commands: Complete

All 8 commands implemented with full feature sets.

### Outstanding Items

| Item | Priority |
|------|----------|
| External table DB verification | Low |
| `target/state.json` for incrementality | Not implemented |
| Update README to show 1:1 schema convention | Documentation |

---

## CI/CD

**Important**: Never use `windows-latest` in GitHub Actions. Only use `ubuntu-latest` and `macos-latest`.

### ci.yml Jobs

- `check`: cargo check
- `fmt`: cargo fmt --check
- `clippy`: cargo clippy -D warnings
- `test`: cargo test (ubuntu + macos matrix)
- `docs`: cargo doc

### release.yml

Triggered on `v*.*.*` tags. Builds for:
- x86_64-unknown-linux-gnu
- x86_64-unknown-linux-musl
- x86_64-apple-darwin
- aarch64-apple-darwin

---

## Development Workflow

### Make Targets

```bash
# Development
make build              # Build all crates
make lint               # fmt-check + clippy
make ci                 # Full CI check locally

# CLI Testing
make ff-parse           # Parse SQL files
make ff-compile         # Compile Jinja to SQL
make ff-run             # Execute models
make ff-ls              # List models
make ff-test            # Run schema tests
make ff-seed            # Load seed data
make ff-docs            # Generate documentation
make ff-validate        # Validate project

# Workflows
make dev-cycle          # seed -> run -> test
make dev-validate       # compile -> validate
make dev-fresh          # Full refresh pipeline
```

### Override Project Directory

```bash
make ff-run PROJECT_DIR=path/to/project
```
