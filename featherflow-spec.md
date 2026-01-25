# Featherflow Technical Specification

A primitive dbt-like CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB (with Snowflake swappability).

---

## Bottom line: what featherflow delivers

Featherflow (`ff`) is a greenfield Rust CLI that implements core dbt functionality—SQL templating with Jinja, dependency-aware model execution, and schema testing—while maintaining a clean abstraction layer that allows swapping DuckDB for Snowflake. The specification covers **5 subcommands** (`parse`, `compile`, `run`, `ls`, `test`), a monorepo structure with shared library crates, and a complete CI/CD pipeline. 

**Key architectural decision**: Dependencies are extracted directly from the SQL AST using `sqlparser-rs`'s `visit_relations` function, eliminating the need for `ref()` and `source()` Jinja functions. This simplifies the templating layer and makes the SQL more portable.

**Development approach**: This project will be built entirely using AI-assisted development (Claude Code), with appropriate tooling, documentation, and project structure to maximize AI effectiveness.

---

## Repository structure

```
featherflow/
├── Cargo.toml                    # Virtual workspace manifest
├── Cargo.lock
├── Makefile
├── rust-toolchain.toml
├── README.md
├── CLAUDE.md                     # AI assistant context file
├── .cargo/
│   └── config.toml               # Cargo aliases
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── release.yml
├── .claude/
│   ├── settings.json             # Claude Code hooks & config
│   └── commands/
│       ├── test-model.md         # /test-model command
│       └── add-feature.md        # /add-feature command
├── crates/
│   ├── ff-cli/                   # Main CLI binary (command: ff)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs
│   │       ├── cli.rs            # Clap definitions
│   │       ├── commands/
│   │       │   ├── mod.rs
│   │       │   ├── parse.rs
│   │       │   ├── compile.rs
│   │       │   ├── run.rs
│   │       │   ├── ls.rs
│   │       │   └── test.rs
│   │       └── context.rs        # Runtime context
│   ├── ff-core/                  # Core library (shared logic)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── config.rs         # Config structs
│   │       ├── project.rs        # Project discovery
│   │       ├── model.rs          # Model representation
│   │       ├── dag.rs            # DAG building
│   │       ├── manifest.rs       # Manifest types
│   │       └── error.rs          # Error types
│   ├── ff-jinja/                 # Jinja templating layer (simplified)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── environment.rs    # Minijinja setup
│   │       └── functions.rs      # config(), var() only
│   ├── ff-sql/                   # SQL parsing layer
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── parser.rs         # sqlparser-rs wrapper
│   │       ├── dialect.rs        # Dialect abstraction trait
│   │       ├── extractor.rs      # Table/column extraction via AST
│   │       └── validator.rs      # SQL validation
│   ├── ff-db/                    # Database abstraction layer
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── traits.rs         # Database trait
│   │       ├── duckdb.rs         # DuckDB implementation
│   │       └── snowflake.rs      # Snowflake stub
│   └── ff-test/                  # Test generation
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── generator.rs      # Test SQL generation
│           └── runner.rs         # Test execution
├── tests/                        # Integration tests
│   ├── fixtures/
│   │   └── sample_project/       # Test project fixture
│   │       ├── featherflow.yml
│   │       ├── schema.yml
│   │       ├── models/
│   │       └── seeds/            # CSV seed data
│   └── integration_tests.rs
├── testdata/                     # Sample data for integration tests
│   ├── seeds/
│   │   ├── raw_orders.csv
│   │   ├── raw_customers.csv
│   │   └── raw_products.csv
│   └── expected/
│       ├── stg_orders.csv
│       └── fct_orders.csv
└── examples/
    └── quickstart/               # Example project
        ├── featherflow.yml
        ├── models/
        └── seeds/
```

---

## Architecture decisions and rationale

### Decision 1: AST-based dependency extraction (no ref/source)

**Choice**: Extract table dependencies directly from parsed SQL AST using `sqlparser-rs`'s `visit_relations` function instead of requiring `ref()` and `source()` Jinja functions.

**Rationale**: 
- Simplifies Jinja layer significantly (only `config()` and `var()` needed)
- Makes SQL files portable—they're valid SQL without any preprocessing
- Leverages the parser we're already using for validation
- Future-proofs for column-level lineage (AST already contains this info)
- Reduces cognitive load for users familiar with standard SQL

**Implementation**: The `ff-sql` crate's `extractor.rs` module uses `visit_relations` to walk the AST and collect all `ObjectName` references from FROM clauses, JOINs, and subqueries.

```rust
use sqlparser::ast::{visit_relations, Statement};

pub fn extract_table_references(stmt: &Statement) -> Vec<String> {
    let mut tables = Vec::new();
    visit_relations(stmt, |relation| {
        tables.push(relation.0.iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join("."));
        std::ops::ControlFlow::Continue(())
    });
    tables
}
```

### Decision 2: Derive-based Clap with flattened global options

**Choice**: Use clap v4 derive API with `#[command(flatten)]` for global options.

**Rationale**: The derive API covers 95%+ of use cases with declarative, type-safe argument definitions. Global options (`--verbose`, `--config`, `--target`) use `#[arg(global = true)]` so they can appear anywhere on the command line.

### Decision 3: Layered error handling (thiserror + anyhow)

**Choice**: Use `thiserror` for library crates (ff-core, ff-sql, ff-db), `anyhow` for the CLI binary.

**Rationale**: Library errors need typed variants for downstream handling. The CLI wraps these with `anyhow::Context` for user-friendly messages.

### Decision 4: Trait-based database abstraction

**Choice**: Define a `Database` trait with `DuckDbBackend` implementation and `SnowflakeBackend` stub.

**Rationale**: Enables Snowflake swappability. The trait includes: `execute_batch`, `create_table_as`, `create_view_as`, `table_exists`, and `query`.

### Decision 5: Virtual workspace with flat crate structure

**Choice**: Virtual manifest at root, all crates under `crates/` with folder name = crate name.

**Rationale**: Scales cleanly for future dev tooling binaries. Shared `workspace.dependencies` eliminates version drift.

### Decision 6: CSV seed data for integration testing

**Choice**: Use CSV files as seed data, loaded into DuckDB at test setup.

**Rationale**: 
- DuckDB has excellent CSV support (`read_csv_auto`)
- Human-readable and version-controllable
- Easy to generate expected outputs
- Matches dbt's seed concept

---

## AI-assisted development setup

This project is designed to be built entirely with Claude Code. The following structure optimizes AI effectiveness.

### CLAUDE.md (root level)

```markdown
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
```

### .claude/settings.json

```json
{
  "hooks": {
    "preToolExecution": [
      {
        "matcher": "Edit|Create",
        "command": "cargo fmt --all -- --check 2>/dev/null || true"
      }
    ],
    "postToolExecution": [
      {
        "matcher": "Edit.*\\.rs$",
        "command": "cargo check -p $(dirname $FILE | xargs basename) 2>&1 | head -20"
      }
    ]
  },
  "permissions": {
    "allow": [
      "cargo *",
      "make *",
      "cat *",
      "ls *"
    ]
  }
}
```

### .claude/commands/add-feature.md

```markdown
---
name: add-feature
description: Add a new feature following project conventions
---

When adding a new feature:

1. First, read the relevant existing code to understand patterns
2. Create a plan listing files to modify/create
3. Implement with proper error handling (thiserror for libs)
4. Add unit tests in the same file
5. Run `make lint` and fix any issues
6. Run `make test` to verify

Always check existing similar code first for patterns.
```

---

## Interface and trait designs

### Database trait (ff-db/src/traits.rs)

```rust
use async_trait::async_trait;
use std::error::Error;

pub type DbResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[async_trait]
pub trait Database: Send + Sync {
    /// Execute SQL that modifies data, returns affected rows
    async fn execute(&self, sql: &str) -> DbResult<usize>;
    
    /// Execute multiple SQL statements
    async fn execute_batch(&self, sql: &str) -> DbResult<()>;
    
    /// Create table from SELECT
    async fn create_table_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()>;
    
    /// Create view from SELECT
    async fn create_view_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()>;
    
    /// Check if table/view exists
    async fn relation_exists(&self, name: &str) -> DbResult<bool>;
    
    /// Execute query returning row count (for tests)
    async fn query_count(&self, sql: &str) -> DbResult<usize>;
    
    /// Load CSV file into table
    async fn load_csv(&self, table: &str, path: &str) -> DbResult<()>;
    
    /// Database identifier for logging
    fn db_type(&self) -> &'static str;
}
```

### SQL dialect trait (ff-sql/src/dialect.rs)

```rust
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;

pub trait SqlDialect: Send + Sync {
    /// Get underlying sqlparser dialect
    fn dialect(&self) -> &dyn Dialect;
    
    /// Parse SQL into AST
    fn parse(&self, sql: &str) -> Result<Vec<Statement>, String>;
    
    /// Quote identifier for this dialect
    fn quote_ident(&self, ident: &str) -> String;
    
    /// Dialect name
    fn name(&self) -> &'static str;
}

pub struct DuckDbDialect;
pub struct SnowflakeDialect;
```

### Table extractor (ff-sql/src/extractor.rs)

```rust
use sqlparser::ast::{visit_relations, Statement, ObjectName};
use std::collections::HashSet;

/// Extract all table references from a SQL statement
pub fn extract_dependencies(statements: &[Statement]) -> HashSet<String> {
    let mut deps = HashSet::new();
    
    for stmt in statements {
        visit_relations(stmt, |relation| {
            let table_name = relation.0.iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            deps.insert(table_name);
            std::ops::ControlFlow::Continue(())
        });
    }
    
    deps
}

/// Categorize dependencies into models vs external tables
pub fn categorize_dependencies(
    deps: HashSet<String>,
    known_models: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut model_deps = Vec::new();
    let mut external_deps = Vec::new();
    
    for dep in deps {
        if known_models.contains(&dep) {
            model_deps.push(dep);
        } else {
            external_deps.push(dep);
        }
    }
    
    (model_deps, external_deps)
}
```

---

## Configuration file format

### featherflow.yml

```yaml
# featherflow.yml - Project configuration
name: my_analytics_project
version: "1.0.0"

# Directory paths (relative to project root)
model_paths: ["models"]
seed_paths: ["seeds"]
target_path: "target"

# Default materialization
materialization: view  # or: table

# Target schema for models
schema: analytics

# SQL dialect for parsing
dialect: duckdb  # or: snowflake

# Database connection
database:
  type: duckdb
  path: "./warehouse.duckdb"  # For DuckDB file-based
  # OR for in-memory:
  # path: ":memory:"

# External tables (not managed by featherflow)
# Used to distinguish model deps from source tables
external_tables:
  - raw.orders
  - raw.customers
  - raw.products

# Variables available in Jinja templates
vars:
  start_date: "2024-01-01"
  environment: dev
```

### Model file conventions

Models are `.sql` files in `model_paths` directories. No special functions needed—just reference tables directly:

```sql
-- models/staging/stg_orders.sql
{{ config(materialized='view', schema='staging') }}

SELECT
    id AS order_id,
    user_id AS customer_id,
    created_at AS order_date,
    amount
FROM raw.orders
WHERE created_at >= '{{ var("start_date") }}'
```

```sql
-- models/marts/fct_orders.sql
{{ config(materialized='table') }}

SELECT
    o.order_id,
    c.customer_name,
    o.order_date,
    o.amount
FROM stg_orders o
LEFT JOIN stg_customers c
    ON o.customer_id = c.customer_id
```

The parser extracts `raw.orders` as an external dependency and `stg_orders`, `stg_customers` as model dependencies.

### schema.yml (model tests)

```yaml
version: 1

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          
  - name: stg_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
```

---

## Sample data for integration testing

### testdata/seeds/raw_orders.csv

```csv
id,user_id,created_at,amount,status
1,101,2024-01-15,99.99,completed
2,102,2024-01-16,149.50,completed
3,101,2024-01-17,75.00,pending
4,103,2024-01-18,200.00,completed
5,102,2024-01-19,50.00,cancelled
6,101,2024-01-20,125.00,completed
7,104,2024-01-21,89.99,completed
8,103,2024-01-22,175.00,pending
9,105,2024-01-23,300.00,completed
10,101,2024-01-24,45.00,completed
```

### testdata/seeds/raw_customers.csv

```csv
id,name,email,created_at,tier
101,Alice Johnson,alice@example.com,2023-06-15,gold
102,Bob Smith,bob@example.com,2023-08-20,silver
103,Carol Williams,carol@example.com,2023-09-10,gold
104,David Brown,david@example.com,2023-11-05,bronze
105,Eve Davis,eve@example.com,2024-01-02,silver
```

### testdata/seeds/raw_products.csv

```csv
id,name,category,price,active
1,Widget Pro,electronics,99.99,true
2,Gadget Plus,electronics,149.50,true
3,Basic Tool,tools,25.00,true
4,Premium Kit,tools,200.00,true
5,Starter Pack,accessories,50.00,false
```

### Integration test setup (tests/common/mod.rs)

```rust
use duckdb::Connection;
use std::path::Path;

pub struct TestDb {
    pub conn: Connection,
}

impl TestDb {
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        Self { conn }
    }
    
    pub fn load_seeds(&self, seed_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        for entry in std::fs::read_dir(seed_dir)? {
            let path = entry?.path();
            if path.extension().map_or(false, |e| e == "csv") {
                let table_name = path.file_stem().unwrap().to_str().unwrap();
                let sql = format!(
                    "CREATE TABLE {} AS SELECT * FROM read_csv_auto('{}')",
                    table_name,
                    path.display()
                );
                self.conn.execute(&sql, [])?;
            }
        }
        Ok(())
    }
    
    pub fn assert_table_exists(&self, name: &str) {
        let count: i64 = self.conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}'",
                    name
                ),
                [],
                |row| row.get(0)
            )
            .unwrap();
        assert_eq!(count, 1, "Table {} should exist", name);
    }
    
    pub fn row_count(&self, table: &str) -> i64 {
        self.conn
            .query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| row.get(0))
            .unwrap()
    }
}
```

### Sample integration test

```rust
#[test]
fn test_full_pipeline() {
    let db = TestDb::new();
    db.load_seeds(Path::new("testdata/seeds")).unwrap();
    
    // Verify seeds loaded
    assert_eq!(db.row_count("raw_orders"), 10);
    assert_eq!(db.row_count("raw_customers"), 5);
    
    // Run compile (loads project, extracts deps)
    let project = Project::load("tests/fixtures/sample_project").unwrap();
    let manifest = compile(&project).unwrap();
    
    // Verify dependency extraction
    let stg_orders = manifest.models.get("stg_orders").unwrap();
    assert!(stg_orders.depends_on.contains(&"raw_orders".to_string()));
    
    let fct_orders = manifest.models.get("fct_orders").unwrap();
    assert!(fct_orders.depends_on.contains(&"stg_orders".to_string()));
    
    // Run models
    run(&project, &db).unwrap();
    
    // Verify outputs
    db.assert_table_exists("stg_orders");
    db.assert_table_exists("fct_orders");
    assert!(db.row_count("stg_orders") > 0);
}

#[test]
fn test_unique_constraint_failure() {
    let db = TestDb::new();
    
    // Insert duplicate data
    db.conn.execute_batch("
        CREATE TABLE test_table (id INT, name VARCHAR);
        INSERT INTO test_table VALUES (1, 'a'), (1, 'b'), (2, 'c');
    ").unwrap();
    
    // Run unique test
    let test_sql = generate_unique_test("test_table", "id");
    let failures: i64 = db.conn
        .query_row(&format!("SELECT COUNT(*) FROM ({})", test_sql), [], |row| row.get(0))
        .unwrap();
    
    assert_eq!(failures, 1, "Should detect 1 duplicate id value");
}
```

---

## Subcommand specifications

### 1. `ff parse`

**Purpose**: Parse SQL files and output structured AST representation for debugging/validation.

**Inputs**:
- `--models <NAMES>`: Comma-separated model names (default: all)
- `--output <FORMAT>`: Output format (`json`, `pretty`, `deps`) (default: `pretty`)
- `--dialect <DIALECT>`: Override dialect from config

**Outputs**:
- JSON AST when `--output json`
- Human-readable tree when `--output pretty`
- Dependency list when `--output deps`
- Parse errors with line/column numbers

**Behavior**:
1. Load project configuration
2. Discover model files matching filter
3. For each model: render Jinja (for var/config), then parse SQL
4. Extract dependencies using `visit_relations`
5. Output AST or dependency graph

**Definition of Done**:
- [ ] Parses all model files in project
- [ ] Extracts table dependencies from AST
- [ ] Categorizes deps as model vs external
- [ ] Reports parse errors with file path, line, column
- [ ] Integration test: parse sample project, verify deps

---

### 2. `ff compile`

**Purpose**: Render Jinja templates to raw SQL, extract dependencies, output to target directory.

**Inputs**:
- `--models <NAMES>`: Comma-separated model names (default: all)
- `--output-dir <PATH>`: Override target directory
- `--vars <JSON>`: Override/add variables as JSON object

**Outputs**:
- Compiled SQL files in `target/compiled/<project>/models/`
- Manifest file at `target/manifest.json`

**Behavior**:
1. Load project configuration
2. Set up Minijinja environment with `config()`, `var()`
3. For each model:
   - Render template
   - Parse resulting SQL
   - Extract dependencies via AST
   - Write compiled SQL to target
4. Build DAG from dependencies
5. Validate DAG is acyclic
6. Write manifest.json

**Definition of Done**:
- [ ] Compiles Jinja to pure SQL
- [ ] Extracts dependencies from AST (not Jinja)
- [ ] `config()` values captured in manifest
- [ ] Circular dependency detection with clear error
- [ ] Manifest includes: models, dependencies, materialization
- [ ] Integration test: compile project, verify manifest

---

### 3. `ff run`

**Purpose**: Execute compiled SQL against the database, creating tables/views in dependency order.

**Inputs**:
- `--models <NAMES>`: Comma-separated model names (default: all)
- `--select <SELECTOR>`: dbt-style selector (`+model`, `model+`)
- `--full-refresh`: Drop and recreate all models

**Outputs**:
- Console output: model execution progress with timing
- Exit summary: models run, success/fail counts
- Updated `target/run_results.json`

**Behavior**:
1. Run `compile` internally (or use cached manifest)
2. Topological sort DAG for execution order
3. For each model in order:
   - Read `materialized` config (default: `view`)
   - Execute appropriate CREATE statement
   - Log duration and status
4. Write run results

**Definition of Done**:
- [ ] Executes models in correct dependency order
- [ ] `materialized='view'` creates VIEW
- [ ] `materialized='table'` creates TABLE
- [ ] Clear error messages on SQL execution failure
- [ ] `--select +model` runs model and all ancestors
- [ ] Integration test: run models, verify tables exist

---

### 4. `ff ls`

**Purpose**: List models, their dependencies, materialization types.

**Inputs**:
- `--output <FORMAT>`: `table`, `json`, `tree` (default: `table`)
- `--select <SELECTOR>`: Filter models

**Outputs**:
- Table listing all models with metadata
- JSON array when `--output json`
- Dependency tree when `--output tree`

**Example Output**:
```
$ ff ls
NAME            MATERIALIZED  DEPENDS_ON
stg_orders      view          raw.orders (external)
stg_customers   view          raw.customers (external)
fct_orders      table         stg_orders, stg_customers

3 models found
```

**Definition of Done**:
- [ ] Lists all models with name, materialization
- [ ] Shows dependencies (model vs external)
- [ ] JSON output is valid and complete
- [ ] Tree output shows hierarchy
- [ ] Integration test: ls output matches expected

---

### 5. `ff test`

**Purpose**: Run schema tests (unique, not_null) on specified models.

**Inputs**:
- `--models <NAMES>`: Comma-separated model names (default: all with tests)
- `--fail-fast`: Stop on first failure

**Outputs**:
- Test results: pass/fail with row counts
- Exit code: 0 if all pass, 1 if any fail
- Sample failing rows on failure

**Test SQL Generation**:
```sql
-- not_null test for stg_orders.order_id
SELECT * FROM stg_orders WHERE order_id IS NULL

-- unique test for stg_orders.order_id  
SELECT order_id, COUNT(*) as cnt 
FROM stg_orders 
GROUP BY order_id 
HAVING COUNT(*) > 1
```

**Definition of Done**:
- [ ] Generates correct SQL for `not_null` test
- [ ] Generates correct SQL for `unique` test
- [ ] Reports pass/fail with timing
- [ ] Shows sample failing rows (limit 5)
- [ ] Exit code 1 on any failure
- [ ] Integration test: pass and fail cases

---

## CI/CD pipeline stages

### ci.yml

```yaml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:

env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: -Dwarnings

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo check --workspace --all-targets

  fmt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt
      - run: cargo fmt --all -- --check

  clippy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --workspace --all-targets -- -D warnings

  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace --all-features

  docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo doc --workspace --no-deps
```

### release.yml

```yaml
name: Release

on:
  push:
    tags: ['v*.*.*']

jobs:
  build:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
          - os: ubuntu-latest
            target: x86_64-unknown-linux-musl
          - os: macos-latest
            target: x86_64-apple-darwin
          - os: macos-latest
            target: aarch64-apple-darwin
          - os: windows-latest
            target: x86_64-pc-windows-msvc
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      - run: cargo build --release --target ${{ matrix.target }} -p ff-cli
      - uses: actions/upload-artifact@v4
        with:
          name: ff-${{ matrix.target }}
          path: target/${{ matrix.target }}/release/ff*
```

---

## Makefile targets

```makefile
.PHONY: build build-release test lint fmt check doc clean ci

# Development
build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

run:
	cargo run -p ff-cli --

watch:
	cargo watch -x 'build --workspace'

# Testing
test:
	cargo test --workspace --all-features

test-verbose:
	cargo test --workspace -- --nocapture

test-integration:
	cargo test --test '*' -- --test-threads=1

# Code Quality
lint: fmt-check clippy

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

check:
	cargo check --workspace --all-targets

# Documentation
doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open

# Maintenance
clean:
	cargo clean

update:
	cargo update

# CI (local verification)
ci: fmt-check clippy test doc
	@echo "CI checks passed!"

ci-quick: check fmt-check clippy
	@echo "Quick CI checks passed!"

# Release
install:
	cargo install --path crates/ff-cli
```

---

## Task breakdown with acceptance criteria

### Phase 1: Foundation (2-3 weeks)

#### Task 1.1: Repository scaffolding
**Scope**: Set up monorepo structure, CI/CD, Makefile, CLAUDE.md
**Acceptance**:
- [ ] Workspace compiles with `cargo build`
- [ ] CI runs on push (fmt, clippy, test)
- [ ] `make ci` passes locally
- [ ] CLAUDE.md provides accurate project context

#### Task 1.2: Configuration loading
**Scope**: Parse featherflow.yml, implement Config structs
**Acceptance**:
- [ ] Loads and validates featherflow.yml
- [ ] Errors on missing required fields
- [ ] Supports `external_tables` list
- [ ] Unit tests for config parsing

#### Task 1.3: Project discovery
**Scope**: Find model files, parse schema.yml
**Acceptance**:
- [ ] Discovers all .sql files in model_paths
- [ ] Parses schema.yml into test definitions
- [ ] Unit tests for discovery

### Phase 2: SQL Parsing & Dependency Extraction (1-2 weeks)

#### Task 2.1: SQL parser wrapper with dialect support
**Scope**: Wrap sqlparser-rs, implement DuckDbDialect
**Acceptance**:
- [ ] Parses SQL with DuckDB dialect
- [ ] Returns meaningful parse errors
- [ ] Unit tests for parsing various SQL patterns

#### Task 2.2: AST-based dependency extraction
**Scope**: Implement `extract_dependencies` using `visit_relations`
**Acceptance**:
- [ ] Extracts FROM clause tables
- [ ] Extracts JOIN tables
- [ ] Extracts subquery tables
- [ ] Handles schema-qualified names (schema.table)
- [ ] Unit tests for complex queries (CTEs, unions)

#### Task 2.3: Dependency categorization
**Scope**: Categorize deps as model vs external
**Acceptance**:
- [ ] Uses `external_tables` config to categorize
- [ ] Unknown tables default to external with warning
- [ ] Unit tests for categorization

### Phase 3: Jinja & Compile (1 week)

#### Task 3.1: Simplified Jinja environment
**Scope**: Minijinja setup with config() and var() only
**Acceptance**:
- [ ] `config(materialized='table')` captured correctly
- [ ] `var('name')` substitutes from config
- [ ] Unknown variables error clearly
- [ ] Unit tests for template rendering

#### Task 3.2: Implement `ff compile`
**Scope**: Full compile command
**Acceptance**:
- [ ] Renders Jinja to SQL
- [ ] Parses and extracts dependencies
- [ ] Builds DAG, detects cycles
- [ ] Writes manifest.json
- [ ] Integration test passes

### Phase 4: Database Layer (1-2 weeks)

#### Task 4.1: Database trait and DuckDB implementation
**Scope**: Define trait, implement for DuckDB
**Acceptance**:
- [ ] Opens in-memory and file databases
- [ ] Implements all trait methods
- [ ] `load_csv` works with test data
- [ ] Unit tests with in-memory DuckDB

#### Task 4.2: Implement `ff run`
**Scope**: Full run command
**Acceptance**:
- [ ] Executes in topological order
- [ ] Creates tables/views per config
- [ ] Reports progress and timing
- [ ] Integration test: tables exist after run

### Phase 5: Additional Commands (1 week)

#### Task 5.1: Implement `ff parse`
**Scope**: Parse command with output formats
**Acceptance**:
- [ ] JSON AST output works
- [ ] Deps output shows dependencies
- [ ] Integration test passes

#### Task 5.2: Implement `ff ls`
**Scope**: List command with formats
**Acceptance**:
- [ ] Table format shows all info
- [ ] JSON output is valid
- [ ] Integration test passes

#### Task 5.3: Implement `ff test`
**Scope**: Test execution
**Acceptance**:
- [ ] unique test SQL correct
- [ ] not_null test SQL correct
- [ ] Reports pass/fail
- [ ] Shows failing rows
- [ ] Integration test with both outcomes

### Phase 6: Polish (1 week)

#### Task 6.1: Error messages and UX
**Scope**: Improve error formatting, help text
**Acceptance**:
- [ ] All errors include context
- [ ] Help text is complete
- [ ] `--verbose` flag works

#### Task 6.2: Documentation
**Scope**: README, rustdoc, examples
**Acceptance**:
- [ ] README with quickstart
- [ ] All public APIs documented
- [ ] Example project works

#### Task 6.3: Release pipeline
**Scope**: Finalize release workflow
**Acceptance**:
- [ ] Tag creates release
- [ ] Binaries for all targets
- [ ] SHA256 checksums included

---

## Dependencies (Cargo.toml workspace)

```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "MIT"
repository = "https://github.com/yourorg/featherflow"

[workspace.dependencies]
# CLI
clap = { version = "4.5", features = ["derive", "env"] }

# Error handling
anyhow = "1.0"
thiserror = "2.0"

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
serde_yaml = "0.9"

# Templating
minijinja = { version = "2.0", features = ["loader"] }

# SQL parsing
sqlparser = { version = "0.52", features = ["visitor"] }

# Database
duckdb = { version = "1.1", features = ["bundled"] }

# Async
tokio = { version = "1.0", features = ["rt-multi-thread", "macros"] }
async-trait = "0.1"

# Utilities
petgraph = "0.6"  # For DAG operations
```

---

## Key implementation notes

**AST dependency extraction pattern**: Use `sqlparser::ast::visit_relations` which visits all table-factor nodes in the AST. This catches FROM clauses, JOINs, and subqueries automatically.

**External vs model tables**: The `external_tables` config list explicitly declares which tables are not featherflow models. Any table reference not in this list AND not matching a model file is treated as external with a warning.

**DAG execution**: Use petgraph for topological sort. Kahn's algorithm handles cycle detection naturally.

**Test SQL convention**: All schema tests return rows that *fail* the test. Zero rows = pass. This matches dbt's semantics.

**DuckDB CSV loading**: Use `read_csv_auto()` which auto-detects types. For integration tests, load all CSV files from testdata/seeds/ into tables named after the file.

**AI development workflow**: Start each session by having Claude read CLAUDE.md. Use `/add-feature` command for new features. Keep sessions focused—one feature per conversation for best results.
