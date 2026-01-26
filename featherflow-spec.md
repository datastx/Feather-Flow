# Featherflow Technical Specification

A primitive dbt-like CLI tool built in Rust for SQL templating, compilation, and execution against DuckDB (with Snowflake swappability).

---

## Bottom line: what featherflow delivers

Featherflow (`ff`) is a greenfield Rust CLI that implements core dbt functionality—SQL templating with Jinja, dependency-aware model execution, and schema testing—while maintaining a clean abstraction layer that allows swapping DuckDB for Snowflake. The specification covers **8 subcommands** (`parse`, `compile`, `run`, `ls`, `test`, `seed`, `docs`, `validate`), a monorepo structure with shared library crates, and a complete CI/CD pipeline. 

**Key architectural decision**: Dependencies are extracted directly from the SQL AST using `sqlparser-rs`'s `visit_relations` function, eliminating the need for `ref()` and `source()` Jinja functions. This simplifies the templating layer and makes the SQL more portable.

**Development approach**: This project will be built entirely using AI-assisted development (Claude Code), with appropriate tooling, documentation, and project structure to maximize AI effectiveness.

---

## Quick Start for Developers

### Prerequisites
- Rust stable toolchain (1.75+)
- Git

### Setup
```bash
# Clone the repository
git clone https://github.com/datastx/featherflow.git
cd featherflow

# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run the CLI
cargo run -p ff-cli -- --help
```

### First Commands
```bash
# Parse a sample project
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project parse

# Compile models
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project compile

# List models
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project ls

# Run models (after loading seeds)
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project run
```

### Development Workflow
1. Make changes to crates in `crates/`
2. Run `cargo fmt --all` to format code
3. Run `cargo clippy --workspace` to check for issues
4. Run `cargo test --workspace` to verify tests pass
5. Run `make ci` for full CI check locally

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

### Model Schema Files (1:1 naming convention)

**Key Design Decision**: Unlike dbt's approach where a single `schema.yml` contains metadata for multiple models, Featherflow uses a **1:1 naming convention** where each model's schema file must have the exact same name as its SQL file.

```
models/
├── staging/
│   ├── stg_orders.sql        # Model SQL
│   ├── stg_orders.yml        # Schema for stg_orders (optional)
│   ├── stg_customers.sql
│   └── stg_customers.yml
└── marts/
    ├── fct_orders.sql
    └── fct_orders.yml
```

**Rationale**:
- **Explicit linking**: No ambiguity about which schema applies to which model
- **No database introspection**: Column metadata comes from YAML, not `information_schema`
- **Faster execution**: Tests validate against declared schema without DB queries
- **Better tooling**: IDEs can provide autocomplete from schema files
- **Optional by default**: Models work without schema files; add them when needed

#### Schema File Format

```yaml
# models/staging/stg_orders.yml
version: 1

# Model-level metadata
description: "Staged orders from raw source, filtered by start_date"
owner: data-team
tags:
  - staging
  - orders
  - daily

# Optional: Override config from SQL file (lower precedence than SQL config())
config:
  materialized: view
  schema: staging

# Column definitions - THE SOURCE OF TRUTH for column metadata
columns:
  - name: order_id
    type: INTEGER           # Expected SQL type (used for documentation/validation)
    description: "Unique identifier for the order"
    primary_key: true       # Informational, used for docs/lineage
    tests:
      - unique
      - not_null

  - name: customer_id
    type: INTEGER
    description: "Foreign key to stg_customers"
    tests:
      - not_null
    references:             # Informational, for lineage/docs
      model: stg_customers
      column: customer_id

  - name: order_date
    type: DATE
    description: "Date the order was placed"
    tests:
      - not_null

  - name: amount
    type: DECIMAL(10,2)
    description: "Order total in USD"
    tests:
      - not_null
      - positive            # Custom test (if implemented)

  - name: status
    type: VARCHAR
    description: "Order status: pending, completed, cancelled"
    tests:
      - not_null
      - accepted_values:
          values: [pending, completed, cancelled]
```

#### Discovery and Loading

Schema files are discovered during project load alongside SQL files:

```rust
/// Load a model and its optional schema file
pub fn load_model(sql_path: &Path) -> CoreResult<Model> {
    let raw_sql = fs::read_to_string(sql_path)?;
    let name = sql_path.file_stem().unwrap().to_string_lossy().to_string();

    // Look for matching .yml or .yaml file
    let yml_path = sql_path.with_extension("yml");
    let yaml_path = sql_path.with_extension("yaml");

    let schema = if yml_path.exists() {
        Some(ModelSchema::load(&yml_path)?)
    } else if yaml_path.exists() {
        Some(ModelSchema::load(&yaml_path)?)
    } else {
        None
    };

    Ok(Model {
        name,
        path: sql_path.to_path_buf(),
        raw_sql,
        schema,
        // ... other fields
    })
}
```

#### Schema Structs

```rust
/// Schema metadata for a single model (from .yml file)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSchema {
    pub version: u32,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub owner: Option<String>,

    #[serde(default)]
    pub tags: Vec<String>,

    #[serde(default)]
    pub config: Option<SchemaConfig>,

    #[serde(default)]
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub materialized: Option<Materialization>,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,

    #[serde(rename = "type")]
    pub data_type: Option<String>,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub primary_key: bool,

    #[serde(default)]
    pub tests: Vec<ColumnTest>,

    #[serde(default)]
    pub references: Option<ColumnReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnTest {
    /// Simple test: "unique", "not_null"
    Simple(String),

    /// Parameterized test: accepted_values, relationships, etc.
    Parameterized(HashMap<String, serde_yaml::Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnReference {
    pub model: String,
    pub column: String,
}
```

#### Config Precedence

When config values are specified in multiple places, precedence is (highest to lowest):

1. **SQL `config()` function** - Always wins
2. **Schema YAML `config:` section** - Fallback if not in SQL
3. **Project `featherflow.yml` defaults** - Global fallback

```sql
-- models/staging/stg_orders.sql
{{ config(materialized='table') }}  -- This wins over schema.yml
SELECT * FROM raw_orders
```

```yaml
# models/staging/stg_orders.yml
config:
  materialized: view  # Ignored because SQL specifies 'table'
  schema: staging     # Used because SQL doesn't specify schema
```

#### Test Generation Without Database

Tests are generated from schema file metadata, not database introspection:

```rust
/// Generate tests for a model using its schema file
pub fn generate_tests_from_schema(model: &Model) -> Vec<GeneratedTest> {
    let Some(schema) = &model.schema else {
        return Vec::new();
    };

    let mut tests = Vec::new();

    for column in &schema.columns {
        for test in &column.tests {
            match test {
                ColumnTest::Simple(test_type) => {
                    tests.push(generate_simple_test(
                        &model.name,
                        &column.name,
                        test_type,
                    ));
                }
                ColumnTest::Parameterized(params) => {
                    tests.push(generate_parameterized_test(
                        &model.name,
                        &column.name,
                        params,
                    ));
                }
            }
        }
    }

    tests
}
```

#### Built-in Test Types

| Test | Description | Generated SQL |
|------|-------------|---------------|
| `unique` | No duplicate values | `SELECT col, COUNT(*) FROM model GROUP BY col HAVING COUNT(*) > 1` |
| `not_null` | No NULL values | `SELECT * FROM model WHERE col IS NULL` |
| `positive` | Values > 0 | `SELECT * FROM model WHERE col <= 0` |
| `non_negative` | Values >= 0 | `SELECT * FROM model WHERE col < 0` |
| `accepted_values` | Value in allowed list | `SELECT * FROM model WHERE col NOT IN (...)` |
| `min_value` | Value >= threshold | `SELECT * FROM model WHERE col < {min}` |
| `max_value` | Value <= threshold | `SELECT * FROM model WHERE col > {max}` |
| `regex` | Value matches pattern | `SELECT * FROM model WHERE NOT regexp_matches(col, '{pattern}')` |

#### Example: accepted_values Test

```yaml
# In schema file
columns:
  - name: status
    tests:
      - accepted_values:
          values: [pending, completed, cancelled]
          quote: true  # Optional: quote string values
```

Generated SQL:
```sql
SELECT *
FROM stg_orders
WHERE status NOT IN ('pending', 'completed', 'cancelled')
   OR status IS NULL
```

#### Validation Without Database

The schema file enables validation without touching the database:

1. **Schema-SQL consistency check** (future): Parse SQL to extract output columns, compare against schema
2. **Test validity**: Ensure referenced columns exist in schema
3. **Reference integrity**: Warn if `references.model` doesn't exist in project
4. **Type compatibility**: Warn if test doesn't make sense for declared type (e.g., `positive` on VARCHAR)

```rust
/// Validate schema file without database access
pub fn validate_schema(model: &Model, project: &Project) -> Vec<ValidationWarning> {
    let Some(schema) = &model.schema else {
        return Vec::new();
    };

    let mut warnings = Vec::new();

    for column in &schema.columns {
        // Check references point to valid models
        if let Some(ref refs) = column.references {
            if !project.has_model(&refs.model) {
                warnings.push(ValidationWarning::UnknownReferenceModel {
                    model: model.name.clone(),
                    column: column.name.clone(),
                    referenced_model: refs.model.clone(),
                });
            }
        }

        // Check test/type compatibility
        for test in &column.tests {
            if let Some(warning) = check_test_type_compatibility(test, &column.data_type) {
                warnings.push(warning);
            }
        }
    }

    warnings
}
```

#### Documentation Generation

Schema files power documentation generation without database access:

```rust
/// Generate markdown documentation for a model
pub fn generate_docs(model: &Model) -> String {
    let Some(schema) = &model.schema else {
        return format!("# {}\n\nNo schema file found.", model.name);
    };

    let mut doc = String::new();

    writeln!(doc, "# {}", model.name).unwrap();

    if let Some(desc) = &schema.description {
        writeln!(doc, "\n{}", desc).unwrap();
    }

    if let Some(owner) = &schema.owner {
        writeln!(doc, "\n**Owner**: {}", owner).unwrap();
    }

    if !schema.tags.is_empty() {
        writeln!(doc, "\n**Tags**: {}", schema.tags.join(", ")).unwrap();
    }

    writeln!(doc, "\n## Columns\n").unwrap();
    writeln!(doc, "| Column | Type | Description |").unwrap();
    writeln!(doc, "|--------|------|-------------|").unwrap();

    for col in &schema.columns {
        let type_str = col.data_type.as_deref().unwrap_or("-");
        let desc_str = col.description.as_deref().unwrap_or("-");
        writeln!(doc, "| {} | {} | {} |", col.name, type_str, desc_str).unwrap();
    }

    doc
}
```

#### Migration from dbt-style schema.yml

For users migrating from dbt, a conversion utility can split multi-model schema files:

```bash
# Future: ff migrate-schema models/schema.yml
# Splits into individual model.yml files
```

Input (`models/schema.yml`):
```yaml
version: 2
models:
  - name: stg_orders
    columns: [...]
  - name: stg_customers
    columns: [...]
```

Output:
- `models/stg_orders.yml`
- `models/stg_customers.yml`

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

### 6. `ff docs`

**Purpose**: Generate documentation from model schema files without database access.

**Inputs**:
- `--models <NAMES>`: Comma-separated model names (default: all with schema files)
- `--output <PATH>`: Output directory (default: `target/docs`)
- `--format <FORMAT>`: Output format (`markdown`, `html`, `json`) (default: `markdown`)

**Outputs**:
- Per-model documentation files
- Index file with model listing
- Lineage diagram (optional, if graphviz available)

**Example Output** (`target/docs/stg_orders.md`):
```markdown
# stg_orders

Staged orders from raw source, filtered by start_date

**Owner**: data-team
**Tags**: staging, orders, daily
**Materialized**: view
**Schema**: staging

## Dependencies

- `raw.orders` (external)

## Columns

| Column | Type | Description | Tests |
|--------|------|-------------|-------|
| order_id | INTEGER | Unique identifier for the order | unique, not_null |
| customer_id | INTEGER | Foreign key to stg_customers | not_null |
| order_date | DATE | Date the order was placed | not_null |
| amount | DECIMAL(10,2) | Order total in USD | not_null, positive |

## Relationships

- `customer_id` references `stg_customers.customer_id`
```

**Behavior**:
1. Load project and all schema files
2. For each model with schema:
   - Extract metadata (description, owner, tags)
   - Extract column definitions and tests
   - Extract relationships/references
   - Generate documentation in requested format
3. Generate index file listing all models
4. Optionally generate dependency graph

**Definition of Done**:
- [ ] Generates markdown docs for each model with schema
- [ ] Index file lists all models with descriptions
- [ ] Works without database connection
- [ ] Skips models without schema files (with note in index)
- [ ] JSON output includes all metadata
- [ ] Integration test: docs match expected output

---

### 7. `ff validate`

**Purpose**: Validate project configuration, SQL syntax, and schema files without executing anything.

**Inputs**:
- `--models <NAMES>`: Comma-separated model names (default: all)
- `--strict`: Enable strict mode (warnings become errors)

**Outputs**:
- Validation results: errors, warnings, info
- Exit code: 0 if valid (no errors), 1 if errors exist

**Validations Performed**:

| Check | Level | Description |
|-------|-------|-------------|
| SQL syntax | Error | All model SQL parses successfully |
| Circular dependencies | Error | No cycles in model DAG |
| Duplicate model names | Error | No two models share a name |
| Jinja variables defined | Error | All `var()` calls have values or defaults |
| Schema file syntax | Error | All .yml files parse as valid YAML |
| Schema column tests | Warning | Test references column defined in same schema |
| Orphaned schema files | Warning | Schema file without matching .sql |
| External table declaration | Warning | Tables not in models or external_tables list |
| Reference model exists | Warning | `references.model` in schema points to real model |
| Type/test compatibility | Info | Test makes sense for declared type |

**Example Output**:
```
$ ff validate

Validating project: my_analytics_project

[ERROR] models/staging/stg_orders.sql:15:1
  SQL parse error: unexpected token 'FORM' (did you mean 'FROM'?)

[ERROR] Circular dependency detected
  fct_orders -> dim_customers -> fct_orders

[WARNING] models/staging/old_model.yml
  Orphaned schema file: no matching .sql file found

[WARNING] models/marts/fct_orders.yml:12
  Column 'customer_name' has test 'unique' but is not declared in columns

[INFO] models/staging/stg_orders.yml:8
  Column 'status' has type VARCHAR but test 'positive' is typically for numeric types

Validation complete: 2 errors, 2 warnings, 1 info
```

**Behavior**:
1. Load project configuration
2. Discover all models and schema files
3. For each model:
   - Parse SQL and validate syntax
   - Render Jinja and check all variables
   - Check schema file if present
4. Build DAG and check for cycles
5. Check for duplicate names
6. Report all findings grouped by severity
7. Exit with code based on error presence

**Definition of Done**:
- [ ] Catches SQL syntax errors with file:line:col
- [ ] Detects circular dependencies
- [ ] Detects duplicate model names
- [ ] Warns on undefined Jinja variables
- [ ] Warns on orphaned schema files
- [ ] `--strict` mode fails on warnings
- [ ] No database connection required
- [ ] Integration test: validate pass and fail cases

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

#### Task 1.3: Project discovery with 1:1 schema files
**Scope**: Find model files, discover matching schema files (1:1 naming)
**Acceptance**:
- [ ] Discovers all .sql files in model_paths
- [ ] For each .sql file, looks for matching .yml/.yaml file (same name)
- [ ] Parses ModelSchema from schema files
- [ ] Handles missing schema files gracefully (optional)
- [ ] Warns on orphaned schema files (no matching .sql)
- [ ] Unit tests for discovery with and without schema files

#### Task 1.4: Schema file structs and parsing
**Scope**: Implement ModelSchema, ColumnSchema, ColumnTest types
**Acceptance**:
- [ ] ModelSchema struct with version, description, owner, tags, config, columns
- [ ] ColumnSchema struct with name, type, description, primary_key, tests, references
- [ ] ColumnTest enum handles simple ("unique") and parameterized (accepted_values) tests
- [ ] Serde deserialization from YAML
- [ ] Config precedence: SQL config() > schema config > project defaults
- [ ] Unit tests for various schema file formats

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
- [ ] **CRITICAL**: Filters out CTE names from dependencies (CTEs defined in WITH clause should not appear as deps)
- [ ] Filters out self-references (model referencing its own name)
- [ ] Unit tests for complex queries (CTEs, unions)
- [ ] Unit test: `WITH orders AS (...) SELECT * FROM orders` should NOT include "orders" as dependency

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

#### Task 5.3: Implement `ff test` with schema-based test generation
**Scope**: Generate and execute tests from model schema files (no DB introspection)
**Acceptance**:
- [ ] Reads tests from model's .yml schema file (1:1 naming)
- [ ] Generates correct SQL for built-in tests (unique, not_null, positive, accepted_values)
- [ ] Handles parameterized tests (accepted_values with values list)
- [ ] Reports pass/fail with timing
- [ ] Shows sample failing rows (limit 5)
- [ ] Skips models without schema files (with info message)
- [ ] Validates test columns exist in schema before execution
- [ ] Exit code 1 on any failure
- [ ] Integration test with pass and fail cases

#### Task 5.4: Implement `ff docs`
**Scope**: Generate documentation from schema files without database access
**Acceptance**:
- [ ] Generates markdown documentation for each model with schema
- [ ] Includes description, owner, tags from schema
- [ ] Includes column table with types, descriptions, tests
- [ ] Includes dependency information
- [ ] Generates index file listing all models
- [ ] JSON output format for programmatic use
- [ ] Works entirely offline (no DB connection)
- [ ] Integration test: docs match expected output

#### Task 5.5: Implement `ff validate`
**Scope**: Validate project without execution
**Acceptance**:
- [ ] Validates SQL syntax for all models
- [ ] Validates Jinja variables are defined
- [ ] Validates schema file YAML syntax
- [ ] Detects circular dependencies
- [ ] Detects duplicate model names
- [ ] Warns on orphaned schema files
- [ ] Warns on test/column mismatches in schema
- [ ] `--strict` mode fails on warnings
- [ ] Reports errors with file:line:col where applicable
- [ ] No database connection required
- [ ] Integration test: validate pass and fail cases

#### Task 5.6: Implement `ff seed`
**Scope**: Load CSV seed files into database
**Acceptance**:
- [ ] Discovers all .csv files in seed_paths
- [ ] Creates tables named after file (without .csv extension)
- [ ] Uses DuckDB's `read_csv_auto()` for type inference
- [ ] `--seeds` flag filters which seeds to load
- [ ] `--full-refresh` drops existing tables first
- [ ] Reports row count per seed
- [ ] Handles missing seed directory gracefully
- [ ] Integration test: seeds load and are queryable

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
repository = "https://github.com/datastx/featherflow"

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

---

## Error Handling Specification

### Error Codes and Messages

All errors should be user-friendly and actionable. Each error type should include:
1. **What went wrong** - Clear description
2. **Where it happened** - File path, line number if applicable
3. **How to fix it** - Suggested action

### CoreError Types

| Error | Code | Message Template | Example |
|-------|------|------------------|---------|
| ConfigNotFound | E001 | Config file not found: {path} | `E001: Config file not found: ./featherflow.yml` |
| ConfigParseError | E002 | Failed to parse config: {details} | `E002: Failed to parse config: invalid YAML at line 15` |
| ConfigInvalid | E003 | Invalid config: {field} - {reason} | `E003: Invalid config: name - cannot be empty` |
| ProjectNotFound | E004 | Project directory not found: {path} | `E004: Project directory not found: ./my_project` |
| ModelNotFound | E005 | Model not found: {name} | `E005: Model not found: stg_orders` |
| ModelParseError | E006 | SQL parse error in {file}:{line}:{col}: {message} | `E006: SQL parse error in models/stg_orders.sql:15:1: unexpected token` |
| CircularDependency | E007 | Circular dependency detected: {path} | `E007: Circular dependency: a → b → c → a` |
| DuplicateModel | E008 | Duplicate model name: {name} found in {path1} and {path2} | `E008: Duplicate model: orders in staging/orders.sql and marts/orders.sql` |
| IoError | E009 | IO error: {details} | `E009: IO error: permission denied reading models/` |
| SchemaParseError | E010 | Schema file parse error in {file}: {details} | `E010: Schema parse error in stg_orders.yml: invalid YAML` |

### SqlError Types

| Error | Code | Message Template |
|-------|------|------------------|
| ParseError | S001 | SQL parse error at line {line}, column {col}: {message} |
| EmptySql | S002 | Empty SQL file: {path} |
| UnsupportedStatement | S003 | Unsupported SQL statement type: {type} |
| ValidationError | S004 | SQL validation failed: {details} |

### JinjaError Types

| Error | Code | Message Template |
|-------|------|------------------|
| RenderError | J001 | Jinja render error in {file}: {message} |
| UnknownVariable | J002 | Undefined variable '{name}' in {file}. Define it in vars: section of featherflow.yml |
| InvalidConfigKey | J003 | Invalid config key '{key}'. Valid keys: materialized, schema, tags |

### DbError Types

| Error | Code | Message Template |
|-------|------|------------------|
| ConnectionError | D001 | Database connection failed: {details} |
| ExecutionError | D002 | SQL execution failed: {message} |
| TableNotFound | D003 | Table or view not found: {name} |
| CsvError | D004 | CSV load failed for {file}: {details} |
| NotImplemented | D005 | Feature not implemented for {backend}: {feature} |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (parse, config, etc.) |
| 2 | Test failures |
| 3 | Circular dependency |
| 4 | Database error |

---

## Known Limitations & Corner Cases

This section documents edge cases, known limitations, and behaviors that developers should be aware of.

### SQL Parsing & Dependency Extraction

#### CTE Names Captured as Dependencies
**Issue**: `visit_relations` captures CTE (Common Table Expression) names as table references. A CTE named the same as an external table or model would be incorrectly categorized as a dependency.

```sql
-- "staged" appears as a dependency even though it's a CTE
WITH staged AS (
    SELECT * FROM raw_orders
)
SELECT * FROM staged
```

**Mitigation**: Post-process extracted dependencies to filter out CTE names defined in the same statement. Requires tracking CTE definitions during AST traversal.

**Priority**: Medium

#### Table Functions Not Detected
**Issue**: DuckDB table functions like `read_csv()`, `read_parquet()`, `read_json()` are not captured as dependencies since they're function calls, not table relations.

```sql
-- No dependencies detected for this model
SELECT * FROM read_csv('data/raw_orders.csv')
```

**Mitigation**: Either require external table declarations in config, or extend extraction to identify table-valued function calls.

**Priority**: Low (explicit config preferred)

#### Quoted Identifiers with Special Characters
**Issue**: Schema-qualified names with special characters (e.g., `"my-schema"."my-table"`) may not normalize correctly since `normalize_table_name` uses simple `.split('.')` which doesn't account for quoted identifiers.

```sql
-- May not correctly extract as my-schema.my-table
SELECT * FROM "my-schema"."my-table"
```

**Mitigation**: Parse identifiers respecting quote boundaries.

**Priority**: Low

#### Self-Referential Models
**Issue**: A model that references itself (e.g., for recursive CTEs using the model's output name) isn't explicitly handled. This would create a self-loop in the DAG.

```sql
-- models/dim_employees.sql
-- References itself for hierarchical data
WITH RECURSIVE emp_tree AS (
    SELECT * FROM dim_employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.* FROM dim_employees e JOIN emp_tree t ON e.manager_id = t.id
)
SELECT * FROM emp_tree
```

**Mitigation**: Detect and filter self-references during dependency categorization.

**Priority**: Medium

#### Case Sensitivity Mismatch
**Issue**: DuckDB is case-insensitive by default, but model name matching is case-sensitive. `stg_Orders` vs `stg_orders` could cause dependency resolution failures.

**Mitigation**: Normalize all model names and dependencies to lowercase before comparison.

**Priority**: Medium

### Jinja Templating

#### Undefined Variable Error Timing
**Issue**: `var()` with no default throws at render time. There's no validation phase to catch all undefined variables before execution starts, meaning failures can occur mid-run.

**Mitigation**: Add a `--validate` or `--dry-run` flag that renders all templates without executing, catching variable errors early.

**Priority**: Medium

#### Non-String Config Values
**Issue**: `config()` with non-string values like `config(tags=['a', 'b'])` or `config(enabled=true)` may not serialize correctly. Code assumes strings in several places.

**Mitigation**: Properly handle all JSON-compatible types in config capture and manifest serialization.

**Priority**: Low

#### Jinja in SQL String Literals
**Issue**: Jinja expressions inside SQL string literals could produce SQL-breaking output if the rendered value contains quotes or special characters.

```sql
SELECT '{{ var("message") }}' as msg  -- If message contains ', SQL breaks
```

**Mitigation**: Document that users should escape values, or provide a `quote()` filter.

**Priority**: Low

### DAG & Execution

#### Diamond Dependencies with Schema Mismatch
**Issue**: If model `A` creates `staging.orders` but model `B` references just `orders`, the schema stripping in `normalize_table_name` could create false dependency matches or misses.

**Mitigation**: Preserve full qualified names when possible; only strip schema for model matching, not for execution.

**Priority**: Medium

#### Model Name Conflicts Across Directories
**Issue**: Models in different directories with the same filename (`models/staging/orders.sql` and `models/marts/orders.sql`) both resolve to name `orders`, causing conflicts.

**Current behavior**: Last discovered model wins (undefined order).

**Mitigation**: Either namespace by path (`staging.orders`, `marts.orders`) or error on duplicate names.

**Priority**: High

#### Schema Creation Not Automatic
**Issue**: When a model specifies `config(schema='staging')`, the `run` command attempts to create `staging.model_name` but doesn't ensure the schema exists first. DuckDB will error if the schema doesn't exist.

**Mitigation**: Auto-create schemas before model execution, or validate schema existence during compile.

**Priority**: High

#### No Transaction Boundaries
**Issue**: If model 5 of 10 fails, models 1-4 are already committed. The database is left in a partial state with no rollback option.

**Mitigation**:
- Option 1: Wrap entire run in transaction (may hit limits on large runs)
- Option 2: Track run state and support `--retry-failed`
- Option 3: Document as expected behavior

**Priority**: Medium

#### View Dependencies During Full Refresh
**Issue**: `--full-refresh` drops and recreates tables. If a view depends on a table being recreated, the view may fail during the drop window or reference stale schema.

**Mitigation**: Order drops in reverse dependency order, or use `CREATE OR REPLACE` consistently.

**Priority**: Medium

### Schema Files (1:1 Model Metadata)

#### Schema-SQL Column Mismatch
**Issue**: Schema file declares columns that don't exist in SQL output, or SQL produces columns not in schema.

**Example**:
```yaml
# stg_orders.yml declares 'customer_name'
columns:
  - name: customer_name
```
```sql
-- stg_orders.sql doesn't select customer_name
SELECT order_id, amount FROM raw_orders
```

**Mitigation**: Add `ff validate` command that parses SQL and compares output columns against schema. Warn on mismatches.

**Priority**: Medium

#### Type Declaration vs Actual Type
**Issue**: Schema declares `type: INTEGER` but SQL produces `BIGINT` or `DECIMAL`. No runtime validation occurs.

**Mitigation**: Types in schema are documentation-only. Consider optional strict mode that validates via `DESCRIBE` after model runs.

**Priority**: Low

#### Orphaned Schema Files
**Issue**: Schema file exists without corresponding SQL file (`stg_orders.yml` exists but `stg_orders.sql` was deleted).

**Mitigation**: Warn during project load about schema files without matching SQL files.

**Priority**: Low

#### Schema File Format Errors
**Issue**: Invalid YAML in schema file should not prevent model execution, only test execution.

**Behavior**: Log warning, skip schema, continue with model. Tests for that model will be skipped.

**Priority**: Medium

#### Test References Non-Existent Column
**Issue**: Test defined for column not declared in same schema file.

```yaml
columns:
  - name: order_id
    tests:
      - unique
  # No 'customer_id' column defined, but test might reference it
```

**Mitigation**: Validate that all tested columns are declared in schema.

**Priority**: Medium

### Schema Testing

#### Tests Pass on Empty Tables
**Issue**: Both `unique` and `not_null` tests pass on empty tables (0 rows = 0 failures). This could mask data pipeline issues where expected data is missing.

**Mitigation**: Add optional `row_count` test or `--warn-empty` flag.

**Priority**: Low

#### Test on Non-Existent Column
**Issue**: Testing `unique` on column `foo` when the model doesn't have that column produces a SQL error at runtime rather than a clear validation error.

**Mitigation**: Validate column existence during test generation by querying `information_schema`.

**Priority**: Medium

#### Test on Non-Existent Model
**Issue**: `schema.yml` referencing a model that doesn't exist isn't caught until test execution.

**Mitigation**: Validate model references during project load.

**Priority**: Medium

#### Unknown Test Types Silently Ignored
**Issue**: Test types like `accepted_values`, `positive`, `relationships` defined in schema.yml are silently skipped during test extraction. No warning is generated.

**Mitigation**: Either implement the test types or warn when unknown test types are encountered.

**Priority**: Medium

#### Identifier Quoting Not Implemented
**Issue**: Table and schema names containing special characters (hyphens, spaces) are not quoted in generated SQL, causing execution failures.

```sql
-- Current: DROP VIEW IF EXISTS my-schema.orders (fails)
-- Needed:  DROP VIEW IF EXISTS "my-schema"."orders"
```

**Mitigation**: Implement proper identifier quoting for all generated SQL.

**Priority**: Medium

#### CSV Path Not Sanitized
**Issue**: CSV file paths in `load_csv()` are directly interpolated into SQL without escaping. Paths with single quotes will cause SQL syntax errors.

**Mitigation**: Escape single quotes in paths or use parameterized queries.

**Priority**: Low

---

## Implementation Status Tracker

This section tracks the implementation status of all features. Update checkboxes as features are completed.

### Core Infrastructure

| Component | Status | Verification |
|-----------|--------|--------------|
| Workspace structure | ✅ Done | `cargo build --workspace` succeeds |
| ff-core crate | ✅ Done | All types compile, tests pass |
| ff-sql crate | ✅ Done | SQL parsing works |
| ff-jinja crate | ✅ Done | config() and var() work |
| ff-db crate | ⚠️ Partial | DuckDB works, Snowflake stub only |
| ff-test crate | ✅ Done | unique/not_null tests work |
| ff-cli crate | ⚠️ Partial | 5/7 commands implemented |

### CLI Commands

| Command | Status | Missing Features |
|---------|--------|------------------|
| `ff parse` | ✅ Done | - |
| `ff compile` | ✅ Done | `--vars` and `--output-dir` ARE implemented |
| `ff run` | ⚠️ Partial | `run_results.json` not written, no manifest caching |
| `ff ls` | ✅ Done | - |
| `ff test` | ⚠️ Partial | Only unique/not_null, no sample failing rows |
| `ff docs` | ❌ Not started | Entirely missing |
| `ff validate` | ❌ Not started | Entirely missing |
| `ff seed` | ❌ Not started | Specified in spec, needs implementation |

### Critical Bugs/Gaps to Fix

| Bug/Gap | Severity | Status | Notes |
|---------|----------|--------|-------|
| CTE names included in dependencies | High | ❌ Not fixed | `visit_relations` includes CTE references |
| No duplicate model name detection | High | ❌ Not fixed | Silent override, undefined behavior |
| Schema not auto-created before model | High | ❌ Not fixed | DuckDB errors if schema doesn't exist |
| 1:1 schema file convention not implemented | High | ❌ Not implemented | Currently uses dbt-style multi-model schema.yml |
| Case-insensitive model matching | Medium | ❌ Not fixed | DuckDB is case-insensitive but matching isn't |
| Line/column numbers always 0 in parse errors | Medium | ❌ Not fixed | Hardcoded in error types |
| ModelSchema struct not implemented | Medium | ❌ Not implemented | Spec defines it but code doesn't have it |

---

## Implementation Gaps

Items specified but not yet implemented or incomplete.

### Missing CLI Features

| Feature | Spec Reference | Status | Blocking? |
|---------|---------------|--------|-----------|
| `--vars <JSON>` for compile | Section: ff compile inputs | ✅ Implemented | No |
| `--output-dir <PATH>` for compile | Section: ff compile inputs | ✅ Implemented | No |
| `target/run_results.json` output | Section: ff run outputs | ❌ Not implemented | No |
| Manifest caching in run | Section: ff run behavior | ❌ Not implemented | No |
| `ff seed` command | Section: ff seed | ❌ Not implemented | Yes |
| `ff docs` command | Section: ff docs | ❌ Not implemented | No |
| `ff validate` command | Section: ff validate | ❌ Not implemented | Yes |
| Sample failing rows in test output | Section: ff test outputs | ❌ Partial | No |

### Missing Validation

| Validation | Description | Priority | Status |
|------------|-------------|----------|--------|
| Pre-run variable check | Validate all vars defined before execution | Medium | ❌ |
| Schema existence check | Ensure target schemas exist | High | ❌ |
| Duplicate model name detection | Error on conflicting model names | High | ❌ |
| External table verification | Warn if external table doesn't exist in DB | Low | ❌ |
| CTE filtering | Remove CTE names from dependency list | High | ❌ |

### Missing Output Files

| File | Purpose | Status |
|------|---------|--------|
| `target/run_results.json` | Execution history, timing, status | Not implemented |
| `target/state.json` | Track file hashes for incrementality | Not implemented |

### Test Fixture Updates Required

| Fixture | Issue | Required Change |
|---------|-------|-----------------|
| tests/fixtures/sample_project/models/schema.yml | Uses dbt-style multi-model format | Split into stg_orders.yml, stg_customers.yml, fct_orders.yml |
| tests/fixtures/sample_project/seeds/ | Seeds exist but no ff seed command | Add ff seed integration test |

### Documentation Alignment Required

| Document | Issue | Required Change |
|----------|-------|-----------------|
| README.md | Shows dbt-style schema.yml with `version: 2` and `models:` list | Update to show 1:1 schema file naming convention |
| README.md | Missing ff seed, ff docs, ff validate commands | Add documentation for all 8 commands when implemented |
| tests/fixtures/sample_project/seeds/ | Exists but not used by CLI | Add ff seed integration test |

---

## Prioritized Action Items for MVP

This section provides a prioritized list of action items to complete the MVP. Items are ordered by blocking priority.

### P0 - Critical/Blocking (Must fix before release)

| # | Item | Type | Impact | Effort |
|---|------|------|--------|--------|
| 1 | Filter CTE names from dependencies | Bug Fix | Incorrect dependencies break DAG | Low |
| 2 | Detect duplicate model names | Bug Fix | Silent override causes confusion | Low |
| 3 | Auto-create schema before model execution | Bug Fix | Run failures on first use | Low |
| 4 | Implement `ff seed` command | Feature | Cannot load test data | Medium |
| 5 | Implement `ff validate` command | Feature | No pre-run validation | Medium |

### P1 - High Priority (Should fix before release)

| # | Item | Type | Impact | Effort |
|---|------|------|--------|--------|
| 6 | Implement 1:1 schema file convention | Feature | Spec/impl mismatch | Medium |
| 7 | Add ModelSchema struct | Feature | Required for 1:1 schema | Low |
| 8 | Update sample_project fixture | Test | Tests don't match spec | Low |
| 9 | Fix line/column numbers in parse errors | Bug Fix | Poor debugging experience | Low |
| 10 | Case-insensitive model matching | Bug Fix | DuckDB compatibility | Low |

### P2 - Medium Priority (Nice to have for release)

| # | Item | Type | Impact | Effort |
|---|------|------|--------|--------|
| 11 | Implement `ff docs` command | Feature | No documentation generation | Medium |
| 12 | Write `run_results.json` | Feature | No execution history | Low |
| 13 | Add sample failing rows to test output | Enhancement | Better debugging | Low |
| 14 | Pre-run variable validation | Enhancement | Fail early on missing vars | Low |

### P3 - Low Priority (Post-MVP)

| # | Item | Type | Impact | Effort |
|---|------|------|--------|--------|
| 15 | Manifest caching | Enhancement | Performance | Medium |
| 16 | Incremental runs | Feature | Performance | High |
| 17 | Parallel execution | Feature | Performance | Medium |
| 18 | Snowflake backend | Feature | Extended support | High |

---

## ff seed Command Specification

### 8. `ff seed`

**Purpose**: Load CSV seed files into the database as tables.

**Inputs**:
- `--seeds <NAMES>`: Comma-separated seed names (default: all)
- `--full-refresh`: Drop and recreate seed tables

**Outputs**:
- Console output: seed loading progress with row counts
- Tables created in database matching seed file names

**Behavior**:
1. Load project configuration
2. Discover CSV files in `seed_paths` directories
3. For each seed file:
   - Drop table if `--full-refresh`
   - Create table using `read_csv_auto()`
   - Report row count
4. Report summary

**Definition of Done**:
- [ ] Discovers all .csv files in seed_paths
- [ ] Creates tables named after CSV file (without extension)
- [ ] Uses DuckDB's `read_csv_auto()` for type inference
- [ ] `--full-refresh` drops existing tables first
- [ ] Reports row counts per seed
- [ ] Integration test: seeds load correctly

---

## AST-Powered Features Roadmap

This section documents advanced features enabled by the SQL AST (Abstract Syntax Tree) that differentiate Featherflow from other tools.

### Currently Implemented

| Feature | Description | Implementation |
|---------|-------------|----------------|
| Dependency extraction | Extract table references from SQL | `visit_relations()` on AST |
| SQL validation | Ensure SQL parses correctly | `sqlparser::parse()` |
| Dialect support | Parse DuckDB/Snowflake syntax | `Dialect` trait |

### Planned AST Features (v0.2+)

#### Column-Level Lineage
Track which input columns flow to which output columns.

```sql
-- Input: SELECT customer_id, SUM(amount) as total FROM orders GROUP BY 1
-- Output lineage:
--   total <- orders.amount (aggregation: SUM)
--   customer_id <- orders.customer_id (passthrough)
```

**Implementation approach**:
- Walk SELECT expressions
- Track column aliases
- Map through JOINs and subqueries
- Store in manifest for visualization

#### Automatic Documentation
Extract column names, types, and expressions from SQL AST.

```sql
SELECT
    o.id AS order_id,           -- Extracted: order_id (alias of o.id)
    c.name AS customer_name,    -- Extracted: customer_name (alias of c.name)
    o.amount * 1.1 AS with_tax  -- Extracted: with_tax (computed)
FROM orders o
JOIN customers c ON o.customer_id = c.id
```

**Output**: Auto-generated schema file from SQL analysis

#### Smart Test Suggestions
Analyze SQL to suggest appropriate tests.

| SQL Pattern | Suggested Test |
|-------------|----------------|
| `column AS xxx_id` | `unique`, `not_null` |
| `SUM(...)`, `COUNT(...)` | `non_negative` |
| `COALESCE(x, default)` | `not_null` |
| `WHERE status IN (...)` | `accepted_values` |

#### Query Optimization Hints
Analyze SQL for performance issues.

| Pattern | Warning |
|---------|---------|
| `SELECT *` | Suggest explicit columns |
| Missing `WHERE` on large table | Suggest filter |
| `DISTINCT` on entire row | Suggest specific columns |
| Cross join detected | Confirm intentional |

#### Breaking Change Detection
Compare AST before/after to detect schema changes.

```
Model: stg_orders
  [REMOVED] Column: legacy_field
  [RENAMED] old_id -> order_id
  [TYPE CHANGE] amount: INTEGER -> DECIMAL
  [ADDED] Column: updated_at
```

### Adding a New Dialect

To add support for a new SQL dialect:

1. **Implement the dialect trait** (`crates/ff-sql/src/dialect.rs`):

```rust
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn parser_dialect(&self) -> Box<dyn Dialect> {
        Box::new(sqlparser::dialect::PostgreSqlDialect {})
    }

    fn name(&self) -> &'static str {
        "postgres"
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}
```

2. **Add to dialect factory** (`from_dialect_name`):

```rust
pub fn from_dialect_name(name: &str) -> Option<Box<dyn SqlDialect>> {
    match name.to_lowercase().as_str() {
        "duckdb" => Some(Box::new(DuckDbDialect)),
        "snowflake" => Some(Box::new(SnowflakeDialect)),
        "postgres" => Some(Box::new(PostgresDialect)),  // NEW
        _ => None,
    }
}
```

3. **Add database backend** (`crates/ff-db/src/`):

```rust
pub struct PostgresBackend {
    pool: sqlx::PgPool,
}

#[async_trait]
impl Database for PostgresBackend {
    // Implement all trait methods
}
```

4. **Update config parsing** to accept new dialect

5. **Add integration tests** with new dialect

### Dialect-Specific SQL Generation

Some SQL constructs differ by dialect:

| Feature | DuckDB | Snowflake | Postgres |
|---------|--------|-----------|----------|
| String concat | `\|\|` | `\|\|` | `\|\|` |
| Current timestamp | `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP()` | `NOW()` |
| Array literal | `[1, 2, 3]` | `ARRAY_CONSTRUCT(1,2,3)` | `ARRAY[1,2,3]` |
| Upsert | `INSERT OR REPLACE` | `MERGE` | `ON CONFLICT` |
| Table sample | `USING SAMPLE 10%` | `SAMPLE (10)` | `TABLESAMPLE (10)` |

The `SqlDialect` trait can be extended with methods for dialect-specific SQL generation.

---

## Future Improvements (Backlog)

Enhancements beyond the MVP specification, prioritized by impact.

### High Priority

#### Parallel Model Execution
Execute independent models (same DAG level) concurrently using `tokio::spawn`.

**Benefit**: Significant speedup for wide DAGs with many independent models.

**Implementation**:
```rust
// Group models by DAG level, execute each level in parallel
for level in dag.levels() {
    let handles: Vec<_> = level.iter()
        .map(|model| tokio::spawn(execute_model(model)))
        .collect();
    futures::future::join_all(handles).await;
}
```

#### Incremental Runs
Track file hashes and timestamps; only recompile/run models that changed or have changed ancestors.

**Benefit**: Dramatically faster iteration during development.

**Implementation**:
- Store `target/state.json` with `{ model: { hash, last_run, deps_hash } }`
- On run, compare hashes; skip unchanged models
- `--full-refresh` ignores state

#### Model Selection Enhancements
Support richer selection syntax:
- Multiple selectors: `--select "+modelA,+modelB"`
- Tag-based: `--select "tag:staging"`
- Path-based: `--select "path:models/staging/*"`
- Exclusion: `--exclude "model_name"`

### Medium Priority

#### Dry-Run Mode
`ff run --dry-run` shows what would execute without touching the database.

**Benefit**: Safe validation before production runs.

#### Structured Error Output
`--output json` flag for machine-readable errors across all commands.

**Benefit**: CI/CD integration, programmatic error handling.

#### Pre/Post Hooks
Model-level hooks for tasks like running grants after materialization:

```yaml
# schema.yml
models:
  - name: fct_orders
    post_hook:
      - "GRANT SELECT ON {{ this }} TO analyst_role"
```

#### Lineage Visualization
`ff lineage [model] --output dot` for Graphviz output.

**Benefit**: Documentation, debugging dependency chains.

#### Custom Tests
Support arbitrary SQL tests in schema.yml:

```yaml
columns:
  - name: amount
    tests:
      - positive:  # Custom test
          sql: "SELECT * FROM {{ model }} WHERE {{ column }} < 0"
```

### Low Priority

#### `ff init` Command
Bootstrap a new project with template files.

#### `ff clean` Command
Remove target directory and cached state.

#### `ff debug` Command
Debug Jinja rendering for a single model without execution.

#### Documentation Generation
Auto-generate markdown docs from models and schema.yml.

#### Snowflake Backend
Complete the `SnowflakeBackend` implementation (currently stub).

---

## Test Coverage Requirements

### Unit Tests Required

| Component | Test Cases |
|-----------|------------|
| `extractor.rs` | CTEs not treated as deps, LATERAL joins, UNION, nested subqueries |
| `dag.rs` | Complex cycles, self-loops, diamond dependencies |
| `config.rs` | Invalid YAML, missing required fields, type coercion |
| `functions.rs` | var() with/without default, config() type handling |

### Integration Tests Required

| Scenario | Validation |
|----------|------------|
| Model name collision | Should error on duplicate names |
| Missing schema | Should create schema or error clearly |
| Circular dependency | Clear error with cycle path |
| Empty model directory | Graceful handling, no crash |
| Invalid SQL in model | Parse error with file:line:col |
| Undefined variable | Clear error before execution starts |
| Full refresh with views | Views recreated correctly |

### Edge Case Tests

```rust
#[test]
fn test_cte_not_treated_as_dependency() {
    let sql = r#"
        WITH orders AS (SELECT * FROM raw_orders)
        SELECT * FROM orders
    "#;
    let deps = extract_dependencies(&parse(sql));
    assert!(deps.contains("raw_orders"));
    assert!(!deps.contains("orders")); // CTE should not appear
}

#[test]
fn test_model_name_collision_errors() {
    // Setup: two models with same name in different dirs
    let result = Project::load("fixtures/duplicate_models");
    assert!(matches!(result, Err(CoreError::DuplicateModel { .. })));
}

#[test]
fn test_schema_creation_before_model() {
    // Model with schema='staging' should work even if schema doesn't exist
    let result = run_model("fixtures/new_schema_project");
    assert!(result.is_ok());
    assert!(db.schema_exists("staging"));
}
```

---

## Expected CLI Output Examples

These examples document the expected behavior of each command for verification.

### ff parse

```bash
$ ff parse --project-dir tests/fixtures/sample_project --output deps
stg_orders: raw_orders (external)
stg_customers: raw_customers (external)
fct_orders: stg_customers, stg_orders
```

```bash
$ ff parse --project-dir tests/fixtures/sample_project --output json --models stg_orders
{
  "models": [
    {
      "name": "stg_orders",
      "dependencies": {
        "models": [],
        "external": ["raw_orders"]
      },
      "statements": 1
    }
  ]
}
```

### ff compile

```bash
$ ff compile --project-dir tests/fixtures/sample_project
Compiling 3 models...
  ✓ stg_orders (view)
  ✓ stg_customers (view)
  ✓ fct_orders (table)

Compiled 3 models to target/compiled/sample_project/models/
Manifest written to target/manifest.json
```

Expected manifest structure:
```json
{
  "project_name": "sample_project",
  "compiled_at": "2024-01-25T12:00:00Z",
  "model_count": 3,
  "models": {
    "stg_orders": {
      "name": "stg_orders",
      "source_path": "models/staging/stg_orders.sql",
      "compiled_path": "target/compiled/sample_project/models/staging/stg_orders.sql",
      "materialized": "view",
      "schema": "staging",
      "depends_on": [],
      "external_deps": ["raw_orders"],
      "referenced_tables": ["raw_orders"]
    }
  }
}
```

### ff run

```bash
$ ff run --project-dir tests/fixtures/sample_project
Running 3 models...
  ✓ stg_orders (view) [45ms]
  ✓ stg_customers (view) [32ms]
  ✓ fct_orders (table) [128ms]

Completed: 3 succeeded, 0 failed
Total time: 205ms
```

With selector:
```bash
$ ff run --project-dir tests/fixtures/sample_project --select +fct_orders
Running 3 models (selected with ancestors)...
  ✓ stg_orders (view) [45ms]
  ✓ stg_customers (view) [32ms]
  ✓ fct_orders (table) [128ms]
```

### ff ls

```bash
$ ff ls --project-dir tests/fixtures/sample_project
NAME            MATERIALIZED  SCHEMA    DEPENDS_ON
stg_orders      view          staging   raw_orders (external)
stg_customers   view          staging   raw_customers (external)
fct_orders      table         -         stg_orders, stg_customers

3 models found
```

Tree output:
```bash
$ ff ls --project-dir tests/fixtures/sample_project --output tree
fct_orders (table)
├── stg_orders (view)
│   └── raw_orders (external)
└── stg_customers (view)
    └── raw_customers (external)
```

### ff test

```bash
$ ff test --project-dir tests/fixtures/sample_project
Running 4 tests...
  ✓ unique_stg_orders_order_id [12ms]
  ✓ not_null_stg_orders_order_id [8ms]
  ✓ not_null_stg_orders_customer_id [9ms]
  ✓ unique_stg_customers_customer_id [11ms]

Passed: 4, Failed: 0
```

With failure:
```bash
$ ff test --project-dir tests/fixtures/sample_project
Running 4 tests...
  ✓ unique_stg_orders_order_id [12ms]
  ✗ not_null_stg_orders_customer_id [8ms]
    Found 3 NULL values

Passed: 1, Failed: 1
Exit code: 1
```

### ff seed (to be implemented)

```bash
$ ff seed --project-dir tests/fixtures/sample_project
Loading 3 seeds...
  ✓ raw_orders (10 rows)
  ✓ raw_customers (5 rows)
  ✓ raw_products (4 rows)

Loaded 3 seeds (19 total rows)
```

### ff validate (to be implemented)

```bash
$ ff validate --project-dir tests/fixtures/sample_project
Validating project: sample_project

Checking SQL syntax... ✓
Checking Jinja variables... ✓
Checking dependencies... ✓
Checking for cycles... ✓
Checking schema files... ✓

Validation passed: 0 errors, 0 warnings
```

With errors:
```bash
$ ff validate --project-dir tests/fixtures/broken_project
Validating project: broken_project

[ERROR] E006: SQL parse error in models/bad_model.sql:5:1
  unexpected token 'FORM' (did you mean 'FROM'?)

[ERROR] E007: Circular dependency detected
  model_a → model_b → model_c → model_a

[WARNING] W001: Undefined variable 'missing_var' in models/stg_orders.sql
  Consider adding it to vars: in featherflow.yml

Validation failed: 2 errors, 1 warning
Exit code: 1
```

### ff docs (to be implemented)

```bash
$ ff docs --project-dir tests/fixtures/sample_project
Generating documentation...
  ✓ stg_orders.md
  ✓ stg_customers.md
  ✓ fct_orders.md
  ✓ index.md

Generated 4 files in target/docs/
```

---

## Priority Matrix

| Issue | Impact | Effort | Priority |
|-------|--------|--------|----------|
| Model name collisions | High | Medium | P0 |
| Schema doesn't exist error | High | Low | P0 |
| CTE false positives in deps | Medium | Low | P1 |
| Case sensitivity mismatches | Medium | Low | P1 |
| Unknown test types silently ignored | Medium | Low | P1 |
| Identifier quoting not implemented | Medium | Medium | P1 |
| Missing run_results.json | Low | Low | P2 |
| Parallel execution | Medium | Medium | P2 |
| --vars CLI override | Low | Low | P2 |
| Incremental runs | High | High | P2 |
| CSV path sanitization | Low | Low | P3 |
| Custom tests (accepted_values, etc.) | Medium | Medium | P3 |

---

## Quick Reference Card

### Commands at a Glance

```
ff parse    [--models X] [--output json|pretty|deps]   # Parse SQL, show AST/deps
ff compile  [--models X] [--vars '{}']                 # Render Jinja, write SQL
ff run      [--select +model|model+] [--full-refresh]  # Execute models in order
ff ls       [--output table|json|tree] [--select X]    # List models
ff test     [--models X] [--fail-fast]                 # Run schema tests
ff seed     [--seeds X] [--full-refresh]               # Load CSV seeds
ff docs     [--models X] [--format md|html|json]       # Generate documentation
ff validate [--models X] [--strict]                    # Validate without running
```

### Global Options

```
-p, --project-dir <PATH>   Project directory (default: .)
-c, --config <FILE>        Config file override
-t, --target <PATH>        Database path override
-v, --verbose              Enable verbose output
```

### Config File (featherflow.yml)

```yaml
name: project_name          # Required
model_paths: ["models"]     # Default: ["models"]
seed_paths: ["seeds"]       # Default: ["seeds"]
target_path: "target"       # Default: "target"
materialization: view       # Default: view (or: table)
dialect: duckdb             # Default: duckdb (or: snowflake)
database:
  type: duckdb
  path: ":memory:"          # or: ./file.duckdb
external_tables:            # Tables not managed by ff
  - raw_orders
  - raw_customers
vars:                       # Variables for Jinja
  start_date: "2024-01-01"
```

### Model SQL Template

```sql
{{ config(materialized='table', schema='marts') }}

SELECT
    o.order_id,
    c.customer_name
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '{{ var("start_date") }}'
```

### Schema File (1:1 with model)

```yaml
# models/stg_orders.yml (matches stg_orders.sql)
version: 1
description: "Staged orders"
owner: data-team
tags: [staging]
columns:
  - name: order_id
    type: INTEGER
    tests: [unique, not_null]
  - name: amount
    tests:
      - not_null
      - accepted_values:
          values: [pending, completed]
```

### Selector Syntax

```
+model      # Model and all its ancestors (dependencies)
model+      # Model and all its descendants (dependents)
+model+     # Both ancestors and descendants
model       # Just the model
```

### Exit Codes

```
0 = Success
1 = General error
2 = Test failures
3 = Circular dependency
4 = Database error
```

---

## MVP Definition of Done Checklist

This checklist defines when the MVP is complete and ready for release.

### Core Functionality (All Required)

**Project Loading**
- [ ] `featherflow.yml` parses correctly
- [ ] Model discovery finds all .sql files recursively
- [ ] Schema files (1:1 naming) are discovered and parsed
- [ ] External tables are recognized from config
- [ ] Variables are loaded and accessible

**SQL Parsing**
- [ ] DuckDB dialect parses all standard SQL
- [ ] Dependencies extracted via AST (not regex)
- [ ] CTE names are NOT included in dependencies
- [ ] Self-references filtered out
- [ ] Parse errors include file:line:column

**Jinja Templating**
- [ ] `config()` function captures materialization, schema, tags
- [ ] `var()` function substitutes variables with optional defaults
- [ ] Undefined variables produce clear error messages
- [ ] Rendered SQL is syntactically valid

**DAG Building**
- [ ] Circular dependencies detected with clear error message showing cycle path
- [ ] Topological sort produces correct execution order
- [ ] `+model` selector includes ancestors
- [ ] `model+` selector includes descendants
- [ ] Duplicate model names produce error (not silent override)

**Database Operations (DuckDB)**
- [ ] In-memory databases work
- [ ] File-based databases work
- [ ] `CREATE TABLE AS` works
- [ ] `CREATE VIEW AS` works
- [ ] `CREATE OR REPLACE` works
- [ ] CSV loading works via `read_csv_auto()`
- [ ] Schema creation (if model specifies schema)

**Schema Testing**
- [ ] `unique` test generates correct SQL
- [ ] `not_null` test generates correct SQL
- [ ] Tests read from 1:1 schema files
- [ ] Test results include pass/fail and timing
- [ ] Failed tests show failure count

### CLI Commands (All Required)

**ff parse**
- [ ] Parses all models in project
- [ ] `--output json` produces valid JSON AST
- [ ] `--output deps` shows dependency list
- [ ] `--models` filter works
- [ ] Parse errors clearly reported

**ff compile**
- [ ] Renders Jinja templates
- [ ] Writes compiled SQL to target/compiled/
- [ ] Generates manifest.json with model metadata
- [ ] Detects and reports circular dependencies

**ff run**
- [ ] Compiles before running
- [ ] Executes models in dependency order
- [ ] Creates views for `materialized='view'`
- [ ] Creates tables for `materialized='table'`
- [ ] `--select` filters models
- [ ] `--full-refresh` drops before recreating
- [ ] Reports timing per model

**ff ls**
- [ ] Lists all models
- [ ] Shows materialization type
- [ ] Shows dependencies
- [ ] `--output json` produces valid JSON
- [ ] `--output tree` shows dependency tree

**ff test**
- [ ] Runs tests defined in schema files
- [ ] Reports pass/fail per test
- [ ] `--fail-fast` stops on first failure
- [ ] Exit code 1 on any failure

**ff seed** (NEW)
- [ ] Loads CSV files from seed_paths
- [ ] Creates tables with inferred types
- [ ] `--full-refresh` recreates tables

**ff validate** (NEW)
- [ ] Validates SQL syntax
- [ ] Validates Jinja variables defined
- [ ] Detects circular dependencies
- [ ] Detects duplicate model names
- [ ] Reports all issues with severity levels

**ff docs** (NEW)
- [ ] Generates markdown per model with schema
- [ ] Includes column definitions and tests
- [ ] Generates index file
- [ ] Works without database connection

### Integration Tests (All Required)

- [ ] Full pipeline: seed → compile → run → test
- [ ] Circular dependency detection
- [ ] Model with dependencies executes after deps
- [ ] Schema tests pass with valid data
- [ ] Schema tests fail with invalid data
- [ ] Parse errors reported correctly
- [ ] Missing variable errors reported

### Documentation (All Required)

- [ ] README with installation and quickstart
- [ ] All CLI commands documented with examples
- [ ] CLAUDE.md accurate for AI assistance
- [ ] Example project in examples/quickstart/

### CI/CD (All Required)

- [ ] `cargo build` succeeds
- [ ] `cargo test` all pass
- [ ] `cargo clippy` no warnings
- [ ] `cargo fmt --check` passes
- [ ] GitHub Actions CI configured

---

## Verification Commands

Run these commands to verify the MVP is complete:

```bash
# Build
cargo build --workspace
cargo build --release --workspace

# Lint
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# Test
cargo test --workspace --all-features

# Integration (requires sample project)
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project compile
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project run
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project test
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project ls --output json
cargo run -p ff-cli -- --project-dir tests/fixtures/sample_project validate

# Full CI
make ci
```

---

## Release Criteria

### v0.1.0 - Minimum Viable Product

The first release requires these features to be complete and tested:

**Must Have (Blocking Release)**
- [x] `ff parse` - Parse SQL, extract dependencies
- [x] `ff compile` - Render Jinja, write compiled SQL, generate manifest
- [x] `ff run` - Execute models in dependency order
- [x] `ff ls` - List models with dependencies
- [x] `ff test` - Run unique/not_null tests
- [ ] `ff seed` - Load CSV seed files
- [ ] `ff validate` - Validate without execution
- [x] DuckDB backend fully functional
- [x] DAG building with cycle detection
- [x] Selector syntax (+model, model+)
- [x] config() and var() Jinja functions
- [ ] CTE names filtered from dependencies (BUG FIX)
- [ ] Duplicate model name detection (BUG FIX)

**Nice to Have (Not Blocking)**
- [ ] `ff docs` - Generate documentation
- [ ] `run_results.json` output
- [ ] Manifest caching
- [ ] Sample failing rows in test output
- [ ] Schema auto-creation

**Deferred to v0.2.0**
- Snowflake backend
- Parallel execution
- Incremental runs
- Custom tests (accepted_values, etc.)
- Pre/post hooks

### Release Verification Script

Create this script as `scripts/verify-release.sh`:

```bash
#!/bin/bash
set -e

echo "=== Featherflow Release Verification ==="
echo ""

# 1. Build
echo "1. Building..."
cargo build --workspace --release
echo "   ✓ Build successful"

# 2. Lint
echo "2. Linting..."
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
echo "   ✓ Lint passed"

# 3. Test
echo "3. Running tests..."
cargo test --workspace --all-features
echo "   ✓ Tests passed"

# 4. Integration tests
echo "4. Running integration tests..."
PROJECT="tests/fixtures/sample_project"

# Parse
./target/release/ff --project-dir $PROJECT parse --output deps > /dev/null
echo "   ✓ Parse works"

# Compile
./target/release/ff --project-dir $PROJECT compile > /dev/null
echo "   ✓ Compile works"

# Check manifest exists
if [ -f "$PROJECT/target/manifest.json" ]; then
    echo "   ✓ Manifest generated"
else
    echo "   ✗ Manifest not found"
    exit 1
fi

# List
./target/release/ff --project-dir $PROJECT ls --output json > /dev/null
echo "   ✓ List works"

# Run (requires seeds to be loaded)
# ./target/release/ff --project-dir $PROJECT seed
# ./target/release/ff --project-dir $PROJECT run
# echo "   ✓ Run works"

# Validate
# ./target/release/ff --project-dir $PROJECT validate
# echo "   ✓ Validate works"

echo ""
echo "=== All checks passed! Ready for release. ==="
```

### Version Numbering

Follow semantic versioning:
- **0.1.x**: Initial development, API may change
- **0.2.x**: Feature additions, API stable
- **1.0.0**: Production ready, API frozen

### Changelog Entry Template

```markdown
## [0.1.0] - YYYY-MM-DD

### Added
- Initial release of Featherflow CLI
- Commands: parse, compile, run, ls, test, seed, validate
- DuckDB backend support
- Jinja templating with config() and var()
- DAG-based dependency resolution
- Schema testing (unique, not_null)
- 1:1 schema file convention

### Known Limitations
- Snowflake backend not implemented
- No incremental execution
- No parallel model execution
```

---

## Detailed Behavioral Specifications

This section documents the detailed behavioral specifications discovered through codebase analysis. These behaviors are implicit in the code but should be considered the source of truth for expected behavior.

### Project Discovery Behavior

#### Configuration File Discovery
- Searches for `featherflow.yml` first, then `featherflow.yaml`
- Configuration file is relative to the project root directory
- If neither file exists, returns `ConfigNotFound` error

```rust
// Discovery order:
// 1. {project_dir}/featherflow.yml
// 2. {project_dir}/featherflow.yaml
```

#### Model Discovery
- Models are discovered recursively in all `model_paths` directories
- Only `.sql` files are considered models
- Model name is derived from filename (without `.sql` extension)
- Subdirectory structure is preserved in paths but NOT in model names
- **Critical**: Model names must be globally unique across all paths

```
models/
├── staging/
│   ├── stg_orders.sql     → Model name: "stg_orders"
│   └── stg_customers.sql  → Model name: "stg_customers"
└── marts/
    └── fct_orders.sql     → Model name: "fct_orders"
```

#### Schema File Discovery (1:1 Convention)
- For each model file, checks for matching `.yml` then `.yaml`
- Match is by filename only (not path)
- Schema files are optional - models work without them

```
models/staging/stg_orders.sql     → looks for stg_orders.yml or stg_orders.yaml
```

### DAG Construction & Execution Order

#### Dependency Graph Building
1. Parse all models to extract dependencies via AST
2. Categorize dependencies as model vs external
3. Build directed graph: model → dependencies
4. External tables are NOT added as nodes
5. Validate graph is acyclic (no circular dependencies)

#### Topological Sort
- Models executed in topological order (dependencies first)
- Models at same level can theoretically run in parallel (future feature)
- Sort is deterministic for same input

#### Selector Behavior
| Selector | Meaning | Example |
|----------|---------|---------|
| `model` | Just this model | `--select stg_orders` |
| `+model` | Model and all ancestors | `--select +fct_orders` (runs stg_orders, stg_customers, fct_orders) |
| `model+` | Model and all descendants | `--select stg_orders+` (runs stg_orders and models that depend on it) |
| `+model+` | Ancestors, model, and descendants | Full connected subgraph |

### SQL Validation Rules

#### Allowed Statement Types
Models must contain only SELECT-based statements. The following are validated:

| Statement | Allowed | Notes |
|-----------|---------|-------|
| SELECT | ✅ Yes | Primary model content |
| WITH (CTE) | ✅ Yes | Common table expressions |
| CREATE VIEW AS | ✅ Yes | Implicit in materialization |
| CREATE TABLE AS | ✅ Yes | Implicit in materialization |
| INSERT | ❌ No | Rejected during validation |
| UPDATE | ❌ No | Rejected during validation |
| DELETE | ❌ No | Rejected during validation |
| DROP | ❌ No | Rejected during validation |
| TRUNCATE | ❌ No | Rejected during validation |

**Error handling**: `UnsupportedStatement` error with statement type

#### Multiple Statements
- SQL files may contain multiple semicolon-separated statements
- All statements are parsed and validated
- Dependencies extracted from ALL statements combined

### Jinja Template Behavior

#### Variable Resolution Order
1. CLI `--vars` JSON (highest priority)
2. `vars:` section in `featherflow.yml`
3. Default value in `var('name', 'default')` call
4. Error if no value found and no default

#### Variable Types Supported
- Strings: `var('name')` → `"value"`
- Numbers: `var('count')` → `42`
- Booleans: `var('enabled')` → `true`
- Arrays: `var('list')` → `["a", "b"]`
- Objects: `var('config')` → `{"key": "value"}`

#### Config Function Behavior
- `config()` captures ALL keyword arguments as key-value pairs
- Known keys: `materialized`, `schema`, `tags`
- Unknown keys are preserved in manifest for extensibility
- Config values stored as minijinja `Value` type

```sql
{{ config(materialized='table', schema='marts', custom_key='custom_value') }}
```

#### Error Messages
| Condition | Error Type | Example Message |
|-----------|------------|-----------------|
| Undefined variable, no default | `UndefinedError` | `Variable 'missing' is not defined` |
| Jinja syntax error | `RenderError` | `Unexpected token at line 5` |
| Invalid config key type | `InvalidConfigKey` | `Config key 'materialized' must be string` |

### Database Backend Behavior

#### DuckDB-Specific Behavior
- Supports in-memory (`:memory:`) and file-based paths
- CSV loading uses `read_csv_auto()` for schema inference
- Schema creation: `CREATE SCHEMA IF NOT EXISTS {schema}`
- Views: `CREATE OR REPLACE VIEW`
- Tables: `CREATE OR REPLACE TABLE`

#### Materialization SQL Generation
```sql
-- For materialized='view'
CREATE OR REPLACE VIEW {schema}.{model_name} AS
{compiled_sql}

-- For materialized='table'
CREATE OR REPLACE TABLE {schema}.{model_name} AS
{compiled_sql}
```

#### Full Refresh Behavior
When `--full-refresh` is specified:
1. Attempt to drop as VIEW first
2. If not a view, drop as TABLE
3. Then create new relation
4. **Note**: Drop order is reverse topological to handle dependencies

#### Relation Existence Check
```sql
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = '{schema}' AND table_name = '{name}'
```

### Test Execution Behavior

#### Test Result Categories
| Result | Condition | Exit Code Contribution |
|--------|-----------|------------------------|
| PASSED | Query returns 0 rows | None |
| FAILED | Query returns > 0 rows | Exit code 1 |
| ERROR | SQL execution error | Exit code 1 |

#### Test Timing
- Each test reports execution duration in milliseconds
- Total time includes all test execution
- Connection setup time NOT included in per-test timing

#### Fail-Fast Mode
When `--fail-fast` enabled:
- Stop execution on first FAILED or ERROR test
- Report partial results (tests run so far)
- Exit code 1 immediately

### Output Formats Specification

#### JSON Output Requirements
All `--output json` modes must produce:
- Valid JSON parseable by standard JSON parsers
- UTF-8 encoding
- No trailing commas
- Arrays and objects properly nested

#### ls Command Output Modes

**Table mode** (default):
```
NAME            MATERIALIZED  SCHEMA    DEPENDS_ON
stg_orders      view          staging   raw_orders (external)
fct_orders      table         marts     stg_orders, stg_customers
```

**JSON mode**:
```json
{
  "models": [
    {
      "name": "stg_orders",
      "materialized": "view",
      "schema": "staging",
      "depends_on": ["raw_orders"],
      "external_deps": ["raw_orders"]
    }
  ]
}
```

**Tree mode**:
```
fct_orders (table)
├── stg_orders (view)
│   └── raw_orders (external)
└── stg_customers (view)
    └── raw_customers (external)
```

### Manifest File Specification

#### Location
`{project_dir}/{target_path}/manifest.json`

Default: `./target/manifest.json`

#### Schema
```json
{
  "project_name": "string",
  "compiled_at": "ISO 8601 timestamp",
  "dialect": "duckdb|snowflake",
  "models": {
    "model_name": {
      "name": "string",
      "path": "relative path to sql file",
      "compiled_sql": "rendered SQL string",
      "materialized": "view|table",
      "schema": "string|null",
      "depends_on": ["model_name", ...],
      "external_deps": ["table_name", ...],
      "referenced_tables": ["all tables referenced"],
      "tags": ["string", ...]
    }
  }
}
```

#### Timestamp Format
- ISO 8601 format: `YYYY-MM-DDTHH:MM:SSZ`
- Always UTC timezone
- Generated without external date libraries (internal calculation)

### Exit Code Specification

| Exit Code | Meaning | When Used |
|-----------|---------|-----------|
| 0 | Success | All operations completed without error |
| 1 | General error | Parse failure, execution error, test failure |
| 2 | Reserved | Future: Test failures specifically |
| 3 | Reserved | Future: Circular dependency |
| 4 | Reserved | Future: Database connection error |

**Current behavior**: All errors return 1 (not yet differentiated)

### Logging & Verbose Output

#### Standard Output vs Error
- Normal command output → stdout
- Verbose logging → stderr
- Error messages → stderr
- JSON output → stdout

#### Verbose Mode (`--verbose`)
When enabled, outputs to stderr:
- Model discovery count
- Compilation progress per model
- Execution order
- Timing information
- Dependency resolution details

```
[DEBUG] Discovered 5 models in models/
[DEBUG] Compiling stg_orders...
[DEBUG] Extracted dependencies: [raw_orders]
[DEBUG] Execution order: [stg_orders, stg_customers, fct_orders]
```

### Additional Implementation Details

This section documents subtle implementation behaviors discovered through code analysis.

#### Full Refresh Behavior (`--full-refresh`)
When `--full-refresh` is specified:
1. First attempts: `DROP VIEW IF EXISTS {name}`
2. Then attempts: `DROP TABLE IF EXISTS {name}`
3. Finally creates the new relation

This dual-drop strategy handles cases where materialization type changed.

#### Selector Edge Cases
- `+model+` syntax (both prefix and suffix) selects ancestors, model, and descendants
- If model doesn't exist, `select()` returns `ModelNotFound` error
- Results are always returned in topological order
- Empty selector returns all models

#### Schema-Qualified Name Normalization
When matching dependencies against known models:
- `raw.orders` is normalized to `orders` by taking the last component
- This allows `SELECT * FROM raw.orders` to match a model named `orders`
- Full qualified names are preserved for external table matching

#### Materialization Resolution Order
Configuration values are resolved in this priority (highest to lowest):
1. `config()` function call in SQL template
2. Project-level default from `featherflow.yml`
3. Hard-coded default: `View`

#### Unknown Test Types Handling
Test types not recognized by the parser are silently skipped:
- Supported: `unique`, `not_null`
- Silently skipped: `accepted_values`, `positive`, `relationships`, custom tests
- No warning is generated for unknown test types

#### Default Schema Assumption
- DuckDB defaults to the `main` schema
- Relation existence checks query `information_schema` with schema defaulting to `main`
- Schema-qualified model names like `staging.orders` require the schema to exist

#### Variable Type Conversion
When converting YAML variables to Jinja values:
- `null` YAML values become empty tuple `()`
- Float parsing failures result in `Null`
- Tagged YAML values are unwrapped before conversion
- Complex types (arrays, objects) are converted via JSON intermediary

#### Fail-Fast Behavior
With `--fail-fast`:
- Stops execution immediately on first FAILED or ERROR test
- Already-running tests complete before stopping
- Summary includes only tests run before stopping

#### Identifier Quoting Limitation
**Current limitation**: Table and schema names are NOT quoted in generated SQL:
```sql
-- Generated SQL (current behavior)
DROP VIEW IF EXISTS my-schema.my-table  -- WILL FAIL for special chars

-- Correct behavior (not yet implemented)
DROP VIEW IF EXISTS "my-schema"."my-table"
```

Special characters in names (hyphens, spaces) will cause SQL execution failures.

#### Mutex and Concurrency
- Internal mutexes use `.lock().unwrap()` which will panic on poison
- Application is single-threaded; mutex poisoning indicates serious bug
- Future parallel execution will require proper error handling

---

## Error Catalog

This section catalogs all error types, their codes, and suggested resolutions.

### Configuration Errors (E0xx)

| Code | Error | Cause | Resolution |
|------|-------|-------|------------|
| E001 | ConfigNotFound | Neither featherflow.yml nor featherflow.yaml found | Create featherflow.yml in project root |
| E002 | ConfigParseError | Invalid YAML syntax in config | Fix YAML syntax (validate with yamllint) |
| E003 | InvalidConfigValue | Config value wrong type or out of range | Check expected type in documentation |
| E004 | MissingRequiredField | Required config field missing | Add required field to featherflow.yml |

### SQL Errors (E1xx)

| Code | Error | Cause | Resolution |
|------|-------|-------|------------|
| E101 | ParseError | SQL syntax error | Check SQL syntax at reported line:column |
| E102 | EmptySql | Model file is empty or only whitespace | Add SELECT statement to model |
| E103 | UnsupportedStatement | INSERT/UPDATE/DELETE in model | Models must be SELECT-based only |
| E104 | ValidationError | SQL semantically invalid | Review SQL logic |

### DAG Errors (E2xx)

| Code | Error | Cause | Resolution |
|------|-------|-------|------------|
| E201 | CircularDependency | Cycle in model dependencies | Break cycle: A → B → C → A |
| E202 | ModelNotFound | Selector references non-existent model | Check model name spelling |
| E203 | DuplicateModel | Two models have same name | Rename one of the conflicting models |

### Jinja Errors (E3xx)

| Code | Error | Cause | Resolution |
|------|-------|-------|------------|
| E301 | UndefinedVariable | var() called with undefined variable | Add variable to vars: or provide default |
| E302 | RenderError | Jinja template syntax error | Fix Jinja syntax |
| E303 | InvalidConfigKey | config() called with invalid key type | Ensure config keys are valid identifiers |

### Database Errors (E4xx)

| Code | Error | Cause | Resolution |
|------|-------|-------|------------|
| E401 | ConnectionError | Cannot connect to database | Check database path/credentials |
| E402 | ExecutionError | SQL execution failed | Check generated SQL and database logs |
| E403 | TableNotFound | Referenced table doesn't exist | Create table or add to external_tables |
| E404 | CsvError | Error loading CSV file | Check CSV file format and path |
| E405 | NotImplemented | Feature not available for backend | Use supported backend or wait for implementation |

### Schema Errors (E5xx)

| Code | Error | Cause | Resolution |
|------|-------|-------|------------|
| E501 | SchemaParseError | Invalid YAML in schema file | Fix schema file YAML syntax |
| E502 | InvalidTestType | Unknown test type in schema | Use supported test types |
| E503 | ColumnNotDefined | Test references undefined column | Add column to schema or remove test |

---

## Acceptance Test Fixtures

This section defines the test fixtures and expected outcomes for acceptance testing.

### Fixture: sample_project

Location: `tests/fixtures/sample_project/`

```
sample_project/
├── featherflow.yml
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_orders.yml       # 1:1 schema file
│   │   ├── stg_customers.sql
│   │   └── stg_customers.yml
│   └── marts/
│       ├── fct_orders.sql
│       └── fct_orders.yml
└── seeds/
    ├── raw_orders.csv
    └── raw_customers.csv
```

#### featherflow.yml
```yaml
name: sample_project
model_paths: ["models"]
seed_paths: ["seeds"]
target_path: "target"
materialization: view
dialect: duckdb
database:
  type: duckdb
  path: ":memory:"
external_tables:
  - raw_orders
  - raw_customers
vars:
  start_date: "2024-01-01"
```

### Expected Outcomes

#### ff parse
```bash
$ ff parse --project-dir tests/fixtures/sample_project --output deps
```
Expected:
- Exit code: 0
- Output includes:
  - stg_orders → depends on: raw_orders (external)
  - stg_customers → depends on: raw_customers (external)
  - fct_orders → depends on: stg_orders, stg_customers

#### ff compile
```bash
$ ff compile --project-dir tests/fixtures/sample_project
```
Expected:
- Exit code: 0
- Creates target/manifest.json
- Creates target/compiled/ with rendered SQL files
- manifest.json contains all 3 models

#### ff ls
```bash
$ ff ls --project-dir tests/fixtures/sample_project --output json
```
Expected:
- Exit code: 0
- Valid JSON output
- Contains 3 models
- Each model has name, materialized, depends_on fields

#### ff validate
```bash
$ ff validate --project-dir tests/fixtures/sample_project
```
Expected:
- Exit code: 0
- "Validation passed" message
- No errors or warnings

### Fixture: broken_project

Location: `tests/fixtures/broken_project/`

For testing error conditions.

#### models/syntax_error.sql
```sql
SELECTT * FORM orders  -- Multiple typos
```

#### models/circular_a.sql
```sql
SELECT * FROM circular_b
```

#### models/circular_b.sql
```sql
SELECT * FROM circular_a
```

### Expected Error Outcomes

#### SQL Syntax Error
```bash
$ ff parse --project-dir tests/fixtures/broken_project
```
Expected:
- Exit code: 1
- Error message includes "syntax_error.sql"
- Error mentions unexpected token

#### Circular Dependency
```bash
$ ff compile --project-dir tests/fixtures/broken_project
```
Expected:
- Exit code: 1
- Error message: "Circular dependency detected"
- Shows cycle path: circular_a → circular_b → circular_a

---

## Glossary

| Term | Definition |
|------|------------|
| **Model** | A SQL file in model_paths that produces a table or view |
| **Materialization** | How a model is persisted: 'view' or 'table' |
| **Schema file** | A .yml file with same name as model containing metadata |
| **External table** | A table not managed by Featherflow (data sources) |
| **DAG** | Directed Acyclic Graph of model dependencies |
| **Selector** | Syntax for selecting models: +model, model+, +model+ |
| **Manifest** | JSON file describing compiled project state |
| **Seed** | CSV file loaded into database as a table |
| **Test** | Validation query that passes if it returns 0 rows |
| **Compile** | Process of rendering Jinja templates to pure SQL |
| **Run** | Process of executing compiled SQL against database |

---

## Appendix: SQL Parser Considerations

### DuckDB Dialect Specifics

Features supported by DuckDB dialect parser:

| Feature | Supported | Notes |
|---------|-----------|-------|
| CTEs (WITH clause) | ✅ Yes | Including recursive |
| Window functions | ✅ Yes | OVER, PARTITION BY |
| Array types | ✅ Yes | `[1, 2, 3]` syntax |
| Struct types | ✅ Yes | `{'key': value}` |
| QUALIFY clause | ✅ Yes | DuckDB-specific |
| SAMPLE clause | ✅ Yes | `USING SAMPLE n%` |
| PIVOT/UNPIVOT | ✅ Yes | |
| LATERAL joins | ✅ Yes | |
| UNION/INTERSECT/EXCEPT | ✅ Yes | All set operations |
| JSON functions | ✅ Yes | `->`, `->>` operators |

### Parser Limitations

| Limitation | Impact | Workaround |
|------------|--------|------------|
| Macros not expanded | Jinja must render first | Always compile before parse |
| Comments preserved | May affect AST | No action needed |
| Non-standard syntax | May fail parse | Use dialect-specific syntax |

---

## Project Management Guide

This section provides guidance for managing the Featherflow project.

### Sprint Planning Template

Each sprint should focus on completing items from the Prioritized Action Items section.

**Sprint Goal Template**:
```
Sprint [N]: [Theme, e.g., "Core Bug Fixes" or "Schema File Implementation"]

Committed Items:
- [ ] P[X] Item #[N]: [Description]
- [ ] P[X] Item #[N]: [Description]

Stretch Goals:
- [ ] P[X] Item #[N]: [Description]

Definition of Done:
- All committed items completed
- All tests pass (cargo test --workspace)
- No clippy warnings (cargo clippy -- -D warnings)
- Code reviewed and merged
```

### Suggested Sprint Breakdown

**Sprint 1: Critical Bug Fixes** (P0 items 1-3)
- Filter CTE names from dependencies
- Detect duplicate model names
- Auto-create schema before model execution
- Update affected tests

**Sprint 2: Core Commands** (P0 items 4-5)
- Implement `ff seed` command
- Implement `ff validate` command
- Integration tests for both

**Sprint 3: Schema Convention** (P1 items 6-10)
- Implement 1:1 schema file convention
- Add ModelSchema struct
- Update sample_project fixture
- Fix line/column numbers in errors
- Case-insensitive matching

**Sprint 4: Documentation & Polish** (P2 items)
- Implement `ff docs` command
- Write run_results.json
- Sample failing rows in test output
- Pre-run variable validation
- README and user documentation

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| CTE filtering breaks existing behavior | Medium | High | Comprehensive test coverage |
| Schema convention migration breaks projects | Low | High | Provide migration tool |
| DuckDB API changes | Low | Medium | Pin version, monitor releases |
| Performance issues with large projects | Medium | Medium | Add benchmarks, profile early |

### Progress Tracking

Track progress by updating checkboxes in:
1. **MVP Definition of Done Checklist** (Section: MVP Definition of Done Checklist)
2. **Implementation Status Tracker** (Section: Implementation Status Tracker)
3. **Prioritized Action Items** (Section: Prioritized Action Items for MVP)

**Progress Formula**:
```
MVP Progress = (Completed P0 + P1 checkboxes) / (Total P0 + P1 checkboxes) × 100%
```

### Decision Log Template

Record key decisions using this format:

```markdown
### Decision: [Title]
**Date**: YYYY-MM-DD
**Status**: Proposed | Accepted | Rejected | Superseded
**Context**: [Why was this decision needed?]
**Options Considered**:
1. [Option A] - Pros: ... Cons: ...
2. [Option B] - Pros: ... Cons: ...
**Decision**: [What was decided]
**Consequences**: [What this means for the project]
```

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 0.1.0 | Initial | Initial specification |
| 0.2.0 | Updated | Added 1:1 schema file convention |
| 0.3.0 | Updated | Added ff seed, validate, docs commands |
| 0.4.0 | Updated | Added Implementation Status Tracker |
| 0.5.0 | Updated | Added AST-Powered Features Roadmap |
| 0.6.0 | Updated | Added Expected CLI Output Examples |
| 0.7.0 | Updated | Added Release Criteria |
| 0.8.0 | Updated | Added Detailed Behavioral Specifications |
| 0.9.0 | Updated | Added Error Catalog and Acceptance Test Fixtures |
| 0.10.0 | Updated | Added Prioritized Action Items and Project Management Guide |
| 1.0.0 | 2026-01-25 | **SPEC COMPLETE** - Exhaustive specification ready for implementation |

---

## Specification Status

**Status: COMPLETE**

This specification is exhaustive and ready for use as a PM-level tracking document. It contains:

- **3855 lines** of comprehensive documentation
- **46 top-level sections** covering all project aspects
- **246 acceptance criteria checkboxes** for tracking completion
- **318 table rows** of structured reference data
- **218 code examples** for implementation guidance

### Next Steps

The specification phase is complete. Development should proceed with:

1. **Sprint 1**: P0 Bug Fixes (CTE filtering, duplicate detection, schema creation)
2. **Sprint 2**: P0 Commands (ff seed, ff validate)
3. **Sprint 3**: P1 Schema Convention (1:1 naming implementation)
4. **Sprint 4**: P2 Polish (ff docs, run_results.json)

Update checkboxes in the "MVP Definition of Done Checklist" section as features are completed.
