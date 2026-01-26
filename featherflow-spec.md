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
make build

# Run tests
make test

# Show available commands
make help
```

### First Commands

All CLI commands have corresponding make targets for easy execution:

```bash
# Parse a sample project
make ff-parse

# Compile models
make ff-compile

# List models
make ff-ls

# Load seed data, then run models
make ff-seed
make ff-run

# Run schema tests
make ff-test

# Validate project without execution
make ff-validate

# Generate documentation
make ff-docs
```

To run against a different project:
```bash
make ff-run PROJECT_DIR=path/to/your/project
```

### Development Workflow
1. Make changes to crates in `crates/`
2. Run `make fmt` to format code
3. Run `make clippy` to check for issues
4. Run `make test` to verify tests pass
5. Run `make ci` for full CI check locally

### Common Workflows
```bash
# Full development cycle: seed -> run -> test
make dev-cycle

# Quick validation: compile -> validate
make dev-validate

# Fresh pipeline with full refresh
make dev-fresh
```

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
│   │       ├── sources/          # Source definitions
│   │       │   └── raw_ecommerce.yml
│   │       ├── macros/           # User-defined macros
│   │       │   └── utils.sql
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

### Build & Test
```bash
make build          # Build all crates
make test           # Run all tests
make lint           # Run clippy + fmt check
make ci             # Full CI check locally
```

### CLI Commands (use make targets, not raw cargo)
```bash
make ff-parse       # Parse SQL files
make ff-compile     # Compile Jinja to SQL
make ff-run         # Execute models
make ff-ls          # List models
make ff-test        # Run schema tests
make ff-seed        # Load seed data
make ff-docs        # Generate documentation
make ff-validate    # Validate project

# Override project directory:
make ff-run PROJECT_DIR=path/to/project
```

### Common Workflows
```bash
make dev-cycle      # seed -> run -> test
make dev-validate   # compile -> validate
make help           # Show all available targets
```

## Architecture Notes
- Dependencies extracted from SQL AST via `visit_relations`, NOT Jinja functions
- No ref() or source() - just plain SQL with table names
- Tables in models/ become dependencies; external tables defined in config
- Error handling: thiserror in libs, anyhow in CLI

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
source_paths: ["sources"]         # Source definitions (kind: sources)
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

# DEPRECATED: Use source_paths and source files (kind: sources) instead
# External tables (not managed by featherflow)
# external_tables:
#   - raw.orders
#   - raw.customers
#   - raw.products

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
- [x] Parses all model files in project
- [x] Extracts table dependencies from AST
- [x] Categorizes deps as model vs external
- [x] Reports parse errors with file path, line, column
- [x] Integration test: parse sample project, verify deps

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
- [x] Compiles Jinja to pure SQL
- [x] Extracts dependencies from AST (not Jinja)
- [x] `config()` values captured in manifest
- [x] Circular dependency detection with clear error
- [x] Manifest includes: models, dependencies, materialization
- [x] Integration test: compile project, verify manifest

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
- [x] Executes models in correct dependency order
- [x] `materialized='view'` creates VIEW
- [x] `materialized='table'` creates TABLE
- [x] Clear error messages on SQL execution failure
- [x] `--select +model` runs model and all ancestors
- [x] Integration test: run models, verify tables exist

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
```bash
$ make ff-ls
NAME            MATERIALIZED  DEPENDS_ON
stg_orders      view          raw.orders (external)
stg_customers   view          raw.customers (external)
fct_orders      table         stg_orders, stg_customers

3 models found
```

**Definition of Done**:
- [x] Lists all models with name, materialization
- [x] Shows dependencies (model vs external)
- [x] JSON output is valid and complete
- [x] Tree output shows hierarchy
- [x] Integration test: ls output matches expected

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
- [x] Generates correct SQL for `not_null` test
- [x] Generates correct SQL for `unique` test
- [x] Reports pass/fail with timing
- [x] Shows sample failing rows (limit 5)
- [x] Exit code 2 on any failure (per spec exit codes)
- [x] Integration test: pass and fail cases

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
- [x] Generates markdown docs for each model with schema
- [x] Index file lists all models with descriptions
- [x] Works without database connection
- [x] Skips models without schema files (with note in index)
- [x] JSON output includes all metadata
- [x] Integration test: docs match expected output

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
```bash
$ make ff-validate PROJECT_DIR=path/to/project

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
- [x] Catches SQL syntax errors with file:line:col
- [x] Detects circular dependencies
- [x] Detects duplicate model names
- [x] Warns on undefined Jinja variables
- [x] Warns on orphaned schema files
- [x] `--strict` mode fails on warnings
- [x] No database connection required
- [x] Integration test: validate pass and fail cases

---

## CI/CD pipeline stages

**Important**: Never use `windows-latest` in GitHub Actions workflows. Windows CI is slow, expensive, and not a target platform for Featherflow. Only use `ubuntu-latest` and `macos-latest`.

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
        os: [ubuntu-latest, macos-latest]
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

**Design Principle**: Every CLI command must have a corresponding make target for consistency and reusability. This enables:
- Quick testing during development without remembering cargo syntax
- Consistent project directory defaults for sample project testing
- Easy CI integration and scripting
- Discoverable commands via `make help`

### Configuration Variables

```makefile
# Default project for testing CLI commands
PROJECT_DIR ?= tests/fixtures/sample_project

# Output format for commands that support it
OUTPUT_FORMAT ?= table
```

### Complete Makefile

```makefile
.PHONY: build build-release test lint fmt check doc clean ci help \
        ff-parse ff-compile ff-run ff-ls ff-test ff-seed ff-docs ff-validate

# Configuration
PROJECT_DIR ?= tests/fixtures/sample_project
OUTPUT_FORMAT ?= table

# =============================================================================
# Development
# =============================================================================

build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

watch:
	cargo watch -x 'build --workspace'

# =============================================================================
# Rust Testing
# =============================================================================

test:
	cargo test --workspace --all-features

test-verbose:
	cargo test --workspace -- --nocapture

test-integration:
	cargo test --test '*' -- --test-threads=1

test-unit:
	cargo test --workspace --lib

# =============================================================================
# Code Quality
# =============================================================================

lint: fmt-check clippy

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

check:
	cargo check --workspace --all-targets

# =============================================================================
# Documentation
# =============================================================================

doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open

# =============================================================================
# CLI Commands - Featherflow (ff)
# Each CLI subcommand has a dedicated make target for easy testing
# =============================================================================

## ff parse - Parse SQL files and output AST/dependencies
ff-parse:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse

ff-parse-json:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse --output json

ff-parse-deps:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse --output deps

## ff compile - Render Jinja templates to SQL, extract dependencies
ff-compile:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile

ff-compile-verbose:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile --verbose

## ff run - Execute compiled SQL against database
ff-run:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run

ff-run-full-refresh:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --full-refresh

ff-run-select:
	@echo "Usage: make ff-run-select MODELS='+model_name'"
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --select $(MODELS)

## ff ls - List models with dependencies and materialization
ff-ls:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls

ff-ls-json:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json

ff-ls-tree:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output tree

## ff test - Run schema tests (unique, not_null, etc.)
ff-test:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test

ff-test-verbose:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test --verbose

ff-test-fail-fast:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test --fail-fast

## ff seed - Load CSV seed files into database
ff-seed:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) seed

ff-seed-full-refresh:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) seed --full-refresh

## ff docs - Generate documentation from schema files
ff-docs:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs

ff-docs-json:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs --format json

## ff validate - Validate project without execution
ff-validate:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) validate

ff-validate-strict:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) validate --strict

## ff sources - List sources (future: ff sources command)
ff-sources:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --type sources

## ff help - Show CLI help
ff-help:
	cargo run -p ff-cli -- --help

# =============================================================================
# Workflows - Common development workflows
# =============================================================================

## Full development cycle: seed -> run -> test
dev-cycle: ff-seed ff-run ff-test
	@echo "Development cycle complete!"

## Quick validation: compile -> validate
dev-validate: ff-compile ff-validate
	@echo "Validation complete!"

## Full pipeline with fresh data
dev-fresh: ff-seed-full-refresh ff-run-full-refresh ff-test
	@echo "Fresh pipeline complete!"

# =============================================================================
# Maintenance
# =============================================================================

clean:
	cargo clean
	rm -rf $(PROJECT_DIR)/target

update:
	cargo update

# =============================================================================
# CI (local verification)
# =============================================================================

ci: fmt-check clippy test doc
	@echo "CI checks passed!"

ci-quick: check fmt-check clippy
	@echo "Quick CI checks passed!"

ci-full: ci ff-compile ff-validate
	@echo "Full CI checks passed!"

# =============================================================================
# Release
# =============================================================================

install:
	cargo install --path crates/ff-cli

# =============================================================================
# Help
# =============================================================================

help:
	@echo "Featherflow Development Makefile"
	@echo ""
	@echo "Development:"
	@echo "  make build              Build all crates"
	@echo "  make build-release      Build release binaries"
	@echo "  make watch              Watch and rebuild on changes"
	@echo ""
	@echo "Testing:"
	@echo "  make test               Run all tests"
	@echo "  make test-verbose       Run tests with output"
	@echo "  make test-integration   Run integration tests only"
	@echo "  make test-unit          Run unit tests only"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint               Run fmt-check and clippy"
	@echo "  make fmt                Format code"
	@echo "  make clippy             Run clippy linter"
	@echo ""
	@echo "CLI Commands (use PROJECT_DIR=path to override):"
	@echo "  make ff-parse           Parse SQL files"
	@echo "  make ff-compile         Compile Jinja to SQL"
	@echo "  make ff-run             Execute models"
	@echo "  make ff-ls              List models"
	@echo "  make ff-test            Run schema tests"
	@echo "  make ff-seed            Load seed data"
	@echo "  make ff-docs            Generate documentation"
	@echo "  make ff-validate        Validate project"
	@echo ""
	@echo "Workflows:"
	@echo "  make dev-cycle          seed -> run -> test"
	@echo "  make dev-validate       compile -> validate"
	@echo "  make dev-fresh          Full refresh pipeline"
	@echo ""
	@echo "CI:"
	@echo "  make ci                 Full CI check"
	@echo "  make ci-quick           Quick CI check"
	@echo "  make ci-full            CI + compile + validate"
```

---

## Task breakdown with acceptance criteria

### Phase 1: Foundation (2-3 weeks)

#### Task 1.1: Repository scaffolding
**Scope**: Set up monorepo structure, CI/CD, Makefile, CLAUDE.md
**Acceptance**:
- [x] Workspace compiles with `cargo build`
- [x] CI runs on push (fmt, clippy, test)
- [x] `make ci` passes locally
- [x] CLAUDE.md provides accurate project context

#### Task 1.2: Configuration loading
**Scope**: Parse featherflow.yml, implement Config structs
**Acceptance**:
- [x] Loads and validates featherflow.yml
- [x] Errors on missing required fields
- [x] Supports `external_tables` list
- [x] Unit tests for config parsing

#### Task 1.3: Project discovery with 1:1 schema files
**Scope**: Find model files, discover matching schema files (1:1 naming)
**Acceptance**:
- [x] Discovers all .sql files in model_paths
- [x] For each .sql file, looks for matching .yml/.yaml file (same name)
- [x] Parses ModelSchema from schema files
- [x] Handles missing schema files gracefully (optional)
- [x] Warns on orphaned schema files (no matching .sql)
- [x] Unit tests for discovery with and without schema files

#### Task 1.4: Schema file structs and parsing
**Scope**: Implement ModelSchema, ColumnSchema, ColumnTest types
**Acceptance**:
- [x] ModelSchema struct with version, description, owner, tags, config, columns
- [x] ColumnSchema struct with name, type, description, primary_key, tests, references
- [x] ColumnTest enum handles simple ("unique") and parameterized (accepted_values) tests
- [x] Serde deserialization from YAML
- [x] Config precedence: SQL config() > schema config > project defaults
- [x] Unit tests for various schema file formats

### Phase 2: SQL Parsing & Dependency Extraction (1-2 weeks)

#### Task 2.1: SQL parser wrapper with dialect support
**Scope**: Wrap sqlparser-rs, implement DuckDbDialect
**Acceptance**:
- [x] Parses SQL with DuckDB dialect
- [x] Returns meaningful parse errors
- [x] Unit tests for parsing various SQL patterns

#### Task 2.2: AST-based dependency extraction
**Scope**: Implement `extract_dependencies` using `visit_relations`
**Acceptance**:
- [x] Extracts FROM clause tables
- [x] Extracts JOIN tables
- [x] Extracts subquery tables
- [x] Handles schema-qualified names (schema.table)
- [x] **CRITICAL**: Filters out CTE names from dependencies (CTEs defined in WITH clause should not appear as deps)
- [x] Filters out self-references (model referencing its own name)
- [x] Unit tests for complex queries (CTEs, unions)
- [x] Unit test: `WITH orders AS (...) SELECT * FROM orders` should NOT include "orders" as dependency

#### Task 2.3: Dependency categorization
**Scope**: Categorize deps as model vs external
**Acceptance**:
- [x] Uses `external_tables` config to categorize
- [x] Unknown tables default to external with warning
- [x] Unit tests for categorization

### Phase 3: Jinja & Compile (1 week)

#### Task 3.1: Simplified Jinja environment
**Scope**: Minijinja setup with config() and var() only
**Acceptance**:
- [x] `config(materialized='table')` captured correctly
- [x] `var('name')` substitutes from config
- [x] Unknown variables error clearly
- [x] Unit tests for template rendering

#### Task 3.2: Custom macros support
**Scope**: Load user-defined macros from macro_paths directories
**Acceptance**:
- [x] `macro_paths` config field parsed
- [x] Macro files discovered from directories
- [x] Minijinja `path_loader` enables imports
- [x] `{% from "file.sql" import macro %}` works
- [x] Compiled SQL has expanded macros (no Jinja syntax)
- [x] Missing macro file errors clearly
- [x] Unit tests for macro loading and expansion

#### Task 3.3: Implement `ff compile`
**Scope**: Full compile command
**Acceptance**:
- [x] Renders Jinja to SQL (including macros)
- [x] Parses and extracts dependencies
- [x] Builds DAG, detects cycles
- [x] Writes manifest.json
- [x] Integration test passes

### Phase 4: Database Layer (1-2 weeks)

#### Task 4.1: Database trait and DuckDB implementation
**Scope**: Define trait, implement for DuckDB
**Acceptance**:
- [x] Opens in-memory and file databases
- [x] Implements all trait methods
- [x] `load_csv` works with test data
- [x] Unit tests with in-memory DuckDB

#### Task 4.2: Implement `ff run`
**Scope**: Full run command
**Acceptance**:
- [x] Executes in topological order
- [x] Creates tables/views per config
- [x] Reports progress and timing
- [x] Integration test: tables exist after run

### Phase 5: Additional Commands (1 week)

#### Task 5.1: Implement `ff parse`
**Scope**: Parse command with output formats
**Acceptance**:
- [x] JSON AST output works
- [x] Deps output shows dependencies
- [x] Integration test passes

#### Task 5.2: Implement `ff ls`
**Scope**: List command with formats
**Acceptance**:
- [x] Table format shows all info
- [x] JSON output is valid
- [x] Integration test passes

#### Task 5.3: Implement `ff test` with schema-based test generation
**Scope**: Generate and execute tests from model schema files (no DB introspection)
**Acceptance**:
- [x] Reads tests from model's .yml schema file (1:1 naming)
- [x] Generates correct SQL for built-in tests (unique, not_null, positive, accepted_values)
- [x] Handles parameterized tests (accepted_values with values list)
- [x] Reports pass/fail with timing
- [x] Shows sample failing rows (limit 5)
- [x] Skips models without schema files (with info message)
- [x] Validates test columns exist in schema before execution
- [x] Exit code 1 on any failure
- [x] Integration test with pass and fail cases

#### Task 5.4: Implement `ff docs`
**Scope**: Generate documentation from schema files without database access
**Acceptance**:
- [x] Generates markdown documentation for each model with schema
- [x] Includes description, owner, tags from schema
- [x] Includes column table with types, descriptions, tests
- [x] Includes dependency information
- [x] Generates index file listing all models
- [x] JSON output format for programmatic use
- [x] Works entirely offline (no DB connection)
- [x] Integration test: docs match expected output

#### Task 5.5: Implement `ff validate`
**Scope**: Validate project without execution
**Acceptance**:
- [x] Validates SQL syntax for all models
- [x] Validates Jinja variables are defined
- [x] Validates schema file YAML syntax
- [x] Detects circular dependencies
- [x] Detects duplicate model names
- [x] Warns on orphaned schema files
- [x] Warns on test/column mismatches in schema
- [x] Warns on test/type compatibility (e.g., positive on VARCHAR)
- [x] Warns on unknown reference models in column references
- [x] `--strict` mode fails on warnings
- [x] Reports errors with file:line:col where applicable
- [x] No database connection required
- [x] Integration test: validate pass and fail cases

#### Task 5.6: Implement `ff seed`
**Scope**: Load CSV seed files into database
**Acceptance**:
- [x] Discovers all .csv files in seed_paths
- [x] Creates tables named after file (without .csv extension)
- [x] Uses DuckDB's `read_csv_auto()` for type inference
- [x] `--seeds` flag filters which seeds to load
- [x] `--full-refresh` drops existing tables first
- [x] Reports row count per seed
- [x] Handles missing seed directory gracefully
- [x] Integration test: seeds load and are queryable

### Phase 6: Polish (1 week)

#### Task 6.1: Error messages and UX
**Scope**: Improve error formatting, help text
**Acceptance**:
- [x] All errors include context
- [x] Help text is complete
- [x] `--verbose` flag works

#### Task 6.2: Documentation
**Scope**: README, rustdoc, examples
**Acceptance**:
- [x] README with quickstart
- [x] All public APIs documented
- [x] Example project works

#### Task 6.3: Release pipeline
**Scope**: Finalize release workflow
**Acceptance**:
- [x] Tag creates release
- [x] Binaries for all targets
- [x] SHA256 checksums included

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

# Templating (loader feature enables path_loader for macro files)
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

### Sources

#### No Freshness Checks (MVP)
**Issue**: The `freshness` configuration is parsed from source files but not executed. No `ff source freshness` command exists.

**Mitigation**: Document as future feature. Freshness config is stored in manifest for future use.

**Priority**: Low (post-MVP feature)

#### No source() Function
**Issue**: Unlike dbt, models reference source tables by name (e.g., `raw.orders`) rather than `{{ source('raw', 'orders') }}`.

**Mitigation**: This is by design. AST-based extraction handles dependencies without special Jinja functions.

**Priority**: N/A (design decision)

#### No Database Validation
**Issue**: Source table definitions are not validated against the actual database. A source table that doesn't exist won't be caught until model execution.

**Mitigation**: Future: add `ff validate --check-sources` flag to verify sources exist in database.

**Priority**: Low

### Custom Macros

#### Single Loader Limitation
**Issue**: Minijinja's `set_loader` replaces the previous loader. If `macro_paths` contains multiple directories, only the last one will be active.

**Workaround for MVP**: Recommend using a single macro_paths directory. Post-MVP, implement a custom composite loader.

**Priority**: Low (documented limitation)

#### Macro Cannot Import Other Macros
**Issue**: Due to Minijinja's execution model, macros cannot have `{% from ... import %}` statements inside them. All imports must be at the model level.

**Mitigation**: Document this limitation. Users can define all related macros in the same file if they need to share logic.

**Priority**: Low (documented limitation)

#### Macro Names Not Validated Against SQL Keywords
**Issue**: A macro named `SELECT` or `FROM` could cause confusing errors when used.

**Mitigation**: Future: add warning for macros named after SQL keywords.

**Priority**: Low

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
| ff-jinja crate | ✅ Done | config(), var(), and custom macros work |
| ff-db crate | ⚠️ Partial | DuckDB works, Snowflake stub only |
| ff-test crate | ✅ Done | unique/not_null tests work |
| ff-cli crate | ✅ Done | All 8 commands implemented |

### CLI Commands

| Command | Status | Missing Features |
|---------|--------|------------------|
| `ff parse` | ✅ Done | - |
| `ff compile` | ✅ Done | `--vars` and `--output-dir` ARE implemented |
| `ff run` | ✅ Done | `run_results.json` ✅, manifest caching ✅, `--no-cache` flag ✅ |
| `ff ls` | ✅ Done | - |
| `ff test` | ✅ Done | All 8 test types (unique, not_null, positive, non_negative, accepted_values, min_value, max_value, regex) with sample failing rows |
| `ff seed` | ✅ Done | Full implementation with `--seeds` and `--full-refresh` |
| `ff validate` | ✅ Done | SQL syntax, Jinja vars, cycles, duplicates, schema files |
| `ff docs` | ✅ Done | Markdown, JSON, HTML output + lineage.dot diagram |

### Critical Bugs/Gaps to Fix

| Bug/Gap | Severity | Status | Notes |
|---------|----------|--------|-------|
| CTE names included in dependencies | High | ✅ Fixed | `extractor.rs` filters out CTE names |
| No duplicate model name detection | High | ✅ Fixed | `discover_models_recursive()` checks for duplicates |
| Schema not auto-created before model | High | ✅ Fixed | `run.rs` collects unique schemas and creates them before execution |
| 1:1 schema file convention not implemented | High | ✅ Implemented | `Model::from_file()` loads matching .yml file |
| Case-insensitive model matching | Medium | ✅ Fixed | `categorize_dependencies()` uses case-insensitive matching |
| Line/column numbers always 0 in parse errors | Medium | ✅ Fixed | `parse_location_from_error()` extracts line/column from sqlparser errors |
| ModelSchema struct not implemented | Medium | ✅ Implemented | `model.rs` has ModelSchema struct |
| Custom macros not implemented | Medium | ✅ Implemented | macro_paths config and path_loader set up |
| Sources not implemented | Medium | ✅ Implemented | source_paths config and SourceFile/SourceTable structs in ff-core |

---

## Implementation Gaps

Items specified but not yet implemented or incomplete.

### Missing CLI Features

| Feature | Spec Reference | Status | Blocking? |
|---------|---------------|--------|-----------|
| `--vars <JSON>` for compile | Section: ff compile inputs | ✅ Implemented | No |
| `--output-dir <PATH>` for compile | Section: ff compile inputs | ✅ Implemented | No |
| `target/run_results.json` output | Section: ff run outputs | ✅ Implemented | No |
| Manifest caching in run | Section: ff run behavior | ✅ Implemented | No |
| `ff seed` command | Section: ff seed | ✅ Implemented | No |
| `ff docs` command | Section: ff docs | ✅ Implemented | No |
| `ff validate` command | Section: ff validate | ✅ Implemented | No |
| Sample failing rows in test output | Section: ff test outputs | ✅ Implemented | No |
| Custom macros support | Section: Custom Macros | ✅ Implemented | No |
| Sources support | Section: Sources | ✅ Implemented | No |

### Missing Validation

| Validation | Description | Priority | Status |
|------------|-------------|----------|--------|
| Pre-run variable check | Validate all vars defined before execution | Medium | ✅ (via ff validate) |
| Schema existence check | Ensure target schemas exist | High | ✅ Fixed |
| Duplicate model name detection | Error on conflicting model names | High | ✅ Fixed |
| External table verification | Warn if external table doesn't exist in DB | Low | ❌ |
| CTE filtering | Remove CTE names from dependency list | High | ✅ Fixed |

### Missing Output Files

| File | Purpose | Status |
|------|---------|--------|
| `target/run_results.json` | Execution history, timing, status | ✅ Implemented |
| `target/state.json` | Track file hashes for incrementality | Not implemented |

### Test Fixture Updates Required

| Fixture | Issue | Required Change |
|---------|-------|-----------------|
| tests/fixtures/sample_project/models/schema.yml | Legacy multi-model format kept for backward compat | ✅ 1:1 schema files added alongside |
| tests/fixtures/sample_project/seeds/ | Seeds exist, ff seed implemented | ✅ Add ff seed integration test |

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
| 11 | Implement sources | Feature | External table documentation/testing | Medium |
| 12 | Implement custom macros | Feature | Reusable SQL snippets | Medium |
| 13 | Implement `ff docs` command | Feature | No documentation generation | Medium |
| 14 | Write `run_results.json` | Feature | No execution history | Low |
| 15 | Add sample failing rows to test output | Enhancement | Better debugging | Low |
| 16 | Pre-run variable validation | Enhancement | Fail early on missing vars | Low |

### P3 - Low Priority (Post-MVP)

| # | Item | Type | Impact | Effort |
|---|------|------|--------|--------|
| 17 | Manifest caching | Enhancement | Performance | Medium |
| 18 | Incremental runs | Feature | Performance | High |
| 19 | Parallel execution | Feature | Performance | Medium |
| 20 | Snowflake backend | Feature | Extended support | High |

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
- [x] Discovers all .csv files in seed_paths
- [x] Creates tables named after CSV file (without extension)
- [x] Uses DuckDB's `read_csv_auto()` for type inference
- [x] `--full-refresh` drops existing tables first
- [x] Reports row counts per seed
- [x] Integration test: seeds load correctly

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

## Sources

This section specifies how to define external data sources in Featherflow. Sources represent raw data tables that exist in the database but are not managed by Featherflow (e.g., tables loaded by ETL pipelines, operational databases, or third-party data).

### Design Decisions

**Choice**: Use dedicated source definition files with a required `kind: sources` field to explicitly identify and document external data sources.

**Rationale**:
- **Explicit identification**: The `kind: sources` field makes files self-documenting and prevents accidental misuse
- **Replaces external_tables**: Provides richer metadata than a simple list in `featherflow.yml`
- **Enables testing**: Sources can have column-level tests just like models
- **Supports freshness** (future): Schema designed to support data freshness monitoring
- **Documentation**: Source tables appear in generated docs with descriptions
- **Strict validation**: Required `kind` field catches configuration errors early

### Directory Structure

Sources are defined in YAML files within the `sources/` directory:

```
project/
├── sources/                    # Source definitions
│   ├── raw_ecommerce.yml       # E-commerce raw data sources
│   ├── raw_crm.yml             # CRM system sources
│   └── external_apis.yml       # Third-party API data
├── models/
├── seeds/
├── macros/
└── featherflow.yml
```

### Configuration

Add `source_paths` to `featherflow.yml`:

```yaml
# featherflow.yml
name: my_analytics_project
version: "1.0.0"

model_paths: ["models"]
seed_paths: ["seeds"]
macro_paths: ["macros"]
source_paths: ["sources"]        # NEW: directories containing source definitions
target_path: "target"

# DEPRECATED: external_tables list (use source files instead)
# external_tables:
#   - raw.orders
```

**Default**: If `source_paths` is not specified, defaults to `["sources"]` if the directory exists.

**Migration**: The `external_tables` list in `featherflow.yml` is deprecated but still supported for backwards compatibility. When both exist, source files take precedence.

### Source File Format

Source files are YAML files with a **required** `kind: sources` field:

```yaml
# sources/raw_ecommerce.yml
kind: sources                    # REQUIRED: Must be "sources"
version: 1

# Source group metadata
name: raw_ecommerce
description: "Raw e-commerce data from the production PostgreSQL database"
database: raw_db                 # Optional: override default database
schema: ecommerce                # Required: schema containing the tables

# Optional metadata
owner: data-engineering
tags:
  - raw
  - production
  - pii

# Table definitions
tables:
  - name: orders
    identifier: api_orders       # Optional: actual table name if different
    description: "One record per order, including cancelled and deleted"
    columns:
      - name: id
        type: INTEGER
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: user_id
        type: INTEGER
        description: "Foreign key to users table"
        tests:
          - not_null
      - name: status
        type: VARCHAR
        description: "Order status: pending, completed, cancelled"
        tests:
          - accepted_values:
              values: [pending, completed, cancelled]
      - name: amount
        type: DECIMAL(10,2)
        description: "Order total in USD"
      - name: created_at
        type: TIMESTAMP
        description: "When the order was placed"

    # Optional: freshness configuration (future implementation)
    freshness:
      loaded_at_field: created_at
      warn_after:
        count: 12
        period: hour
      error_after:
        count: 24
        period: hour

  - name: customers
    description: "Customer master data"
    columns:
      - name: id
        type: INTEGER
        tests:
          - unique
          - not_null
      - name: email
        type: VARCHAR
        description: "Customer email address"
        tests:
          - unique
          - not_null
      - name: name
        type: VARCHAR
      - name: tier
        type: VARCHAR
        tests:
          - accepted_values:
              values: [bronze, silver, gold, platinum]

  - name: products
    description: "Product catalog"
    # Minimal definition - columns optional
```

### Strict Validation Rules

The `kind: sources` field enables strict validation:

| Rule | Severity | Description |
|------|----------|-------------|
| `kind` must equal `"sources"` | Error | Files with wrong/missing `kind` are rejected |
| `name` is required | Error | Source group must have a name |
| `schema` is required | Error | Must specify the database schema |
| `tables` must not be empty | Error | At least one table required |
| Table `name` is required | Error | Each table must have a name |
| Valid YAML syntax | Error | File must be valid YAML |
| Column names unique per table | Warning | Duplicate column names in same table |
| Unknown fields | Warning | Warn on unrecognized fields (typo detection) |

### Source Structs

```rust
/// A source definition file (from .yml with kind: sources)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFile {
    /// Must be "sources" - enforced during parsing
    pub kind: SourceKind,

    #[serde(default = "default_version")]
    pub version: u32,

    /// Logical name for this source group
    pub name: String,

    #[serde(default)]
    pub description: Option<String>,

    /// Database name (optional, uses default if not specified)
    #[serde(default)]
    pub database: Option<String>,

    /// Schema name (required)
    pub schema: String,

    #[serde(default)]
    pub owner: Option<String>,

    #[serde(default)]
    pub tags: Vec<String>,

    /// Tables in this source
    pub tables: Vec<SourceTable>,
}

/// Enforces kind: sources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Sources,
}

impl SourceKind {
    pub fn validate(&self) -> Result<(), SourceError> {
        match self {
            SourceKind::Sources => Ok(()),
        }
    }
}

/// A single table within a source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTable {
    /// Logical name used in models
    pub name: String,

    /// Actual table name in database (if different from name)
    #[serde(default)]
    pub identifier: Option<String>,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub columns: Vec<SourceColumn>,

    /// Freshness configuration (future)
    #[serde(default)]
    pub freshness: Option<FreshnessConfig>,
}

/// Column definition within a source table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceColumn {
    pub name: String,

    #[serde(rename = "type")]
    pub data_type: Option<String>,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub tests: Vec<ColumnTest>,
}

/// Freshness monitoring configuration (future implementation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessConfig {
    /// Column containing the loaded_at timestamp
    pub loaded_at_field: String,

    #[serde(default)]
    pub warn_after: Option<FreshnessPeriod>,

    #[serde(default)]
    pub error_after: Option<FreshnessPeriod>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessPeriod {
    pub count: u32,
    pub period: FreshnessPeriodUnit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessPeriodUnit {
    Minute,
    Hour,
    Day,
}
```

### Source Discovery and Loading

```rust
/// Discover and load all source files
pub fn discover_sources(
    project_root: &Path,
    source_paths: &[PathBuf],
) -> Result<Vec<SourceFile>, SourceError> {
    let mut sources = Vec::new();

    for source_path in source_paths {
        let full_path = project_root.join(source_path);
        if !full_path.exists() {
            continue;
        }

        for entry in walkdir::WalkDir::new(&full_path)
            .max_depth(2)  // Allow one level of subdirectories
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "yml" || e == "yaml") {
                let source_file = load_source_file(path)?;
                sources.push(source_file);
            }
        }
    }

    Ok(sources)
}

/// Load and validate a single source file
pub fn load_source_file(path: &Path) -> Result<SourceFile, SourceError> {
    let content = fs::read_to_string(path)
        .map_err(|e| SourceError::IoError { path: path.to_path_buf(), source: e })?;

    let source: SourceFile = serde_yaml::from_str(&content)
        .map_err(|e| SourceError::ParseError {
            path: path.to_path_buf(),
            details: e.to_string(),
        })?;

    // Validate kind field
    if source.kind != SourceKind::Sources {
        return Err(SourceError::InvalidKind {
            path: path.to_path_buf(),
            expected: "sources".to_string(),
            found: format!("{:?}", source.kind),
        });
    }

    // Validate required fields
    if source.tables.is_empty() {
        return Err(SourceError::EmptyTables {
            path: path.to_path_buf(),
        });
    }

    Ok(source)
}
```

### Integration with Dependency Extraction

Sources replace the `external_tables` list for dependency categorization:

```rust
/// Build lookup of known source tables
pub fn build_source_lookup(sources: &[SourceFile]) -> HashSet<String> {
    let mut lookup = HashSet::new();

    for source in sources {
        for table in &source.tables {
            // Build fully qualified name: schema.table
            let fqn = format!("{}.{}", source.schema, table.name);
            lookup.insert(fqn);

            // Also add just the table name for unqualified references
            lookup.insert(table.name.clone());

            // If identifier differs, add that too
            if let Some(ref ident) = table.identifier {
                let fqn_ident = format!("{}.{}", source.schema, ident);
                lookup.insert(fqn_ident);
                lookup.insert(ident.clone());
            }
        }
    }

    lookup
}

/// Categorize dependencies using source definitions
pub fn categorize_dependencies_with_sources(
    deps: HashSet<String>,
    known_models: &HashSet<String>,
    known_sources: &HashSet<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut model_deps = Vec::new();
    let mut source_deps = Vec::new();
    let mut unknown_deps = Vec::new();

    for dep in deps {
        if known_models.contains(&dep) {
            model_deps.push(dep);
        } else if known_sources.contains(&dep) {
            source_deps.push(dep);
        } else {
            unknown_deps.push(dep);
        }
    }

    (model_deps, source_deps, unknown_deps)
}
```

### Source Tests

Sources can have column-level tests, executed via `ff test`:

```rust
/// Generate tests for source tables
pub fn generate_source_tests(sources: &[SourceFile]) -> Vec<GeneratedTest> {
    let mut tests = Vec::new();

    for source in sources {
        for table in &source.tables {
            let table_ref = format!("{}.{}", source.schema, table.name);

            for column in &table.columns {
                for test in &column.tests {
                    tests.push(GeneratedTest {
                        name: format!("source_{}_{}_{}",
                            source.name, table.name, test.name()),
                        test_type: TestType::Source,
                        table: table_ref.clone(),
                        column: column.name.clone(),
                        test_config: test.clone(),
                    });
                }
            }
        }
    }

    tests
}
```

### CLI Integration

#### ff ls

Show sources alongside models:

```bash
$ make ff-ls
NAME                TYPE      MATERIALIZED  DEPENDS_ON
raw_ecommerce.orders   source    -             -
raw_ecommerce.customers source   -             -
stg_orders          model     view          raw_ecommerce.orders
stg_customers       model     view          raw_ecommerce.customers
fct_orders          model     table         stg_orders, stg_customers

3 models, 2 sources
```

#### ff test

Test sources alongside models:

```bash
$ make ff-test
Running 8 tests...
  ✓ source_raw_ecommerce_orders_id_unique [15ms]
  ✓ source_raw_ecommerce_orders_id_not_null [12ms]
  ✓ unique_stg_orders_order_id [12ms]
  ...

Passed: 8, Failed: 0
```

#### ff docs

Include sources in documentation:

```bash
$ make ff-docs
Generating documentation...
  ✓ sources/raw_ecommerce.md
  ✓ models/stg_orders.md
  ...
```

#### ff validate

Validate source files:

```bash
$ make ff-validate
Validating project: sample_project

Checking source files...
  ✓ sources/raw_ecommerce.yml (kind: sources, 3 tables)
  ✓ sources/raw_crm.yml (kind: sources, 2 tables)

[WARNING] Unknown dependency 'legacy_table' in model stg_legacy
  Not defined as a model or source. Add to sources/ or verify table exists.

Validation passed: 0 errors, 1 warning
```

### Error Messages

| Error | Code | Message Template |
|-------|------|------------------|
| MissingKind | SRC001 | Source file missing required 'kind' field: {path}. Add `kind: sources` |
| InvalidKind | SRC002 | Invalid 'kind' in {path}: expected 'sources', found '{found}' |
| MissingSchema | SRC003 | Source '{name}' missing required 'schema' field in {path} |
| EmptyTables | SRC004 | Source '{name}' has no tables defined in {path} |
| ParseError | SRC005 | Failed to parse source file {path}: {details} |
| DuplicateSource | SRC006 | Duplicate source name '{name}' in {path1} and {path2} |
| DuplicateTable | SRC007 | Duplicate table '{table}' in source '{source}' |

### Manifest Integration

Sources appear in `target/manifest.json`:

```json
{
  "sources": {
    "raw_ecommerce.orders": {
      "name": "orders",
      "source_name": "raw_ecommerce",
      "schema": "ecommerce",
      "database": "raw_db",
      "identifier": "api_orders",
      "description": "One record per order...",
      "columns": [...],
      "freshness": {...}
    }
  },
  "models": {...}
}
```

### Limitations (MVP)

1. **No freshness checks**: `freshness` config is parsed but not executed (future feature)
2. **No source() function**: Models reference sources by table name, not `{{ source() }}`
3. **No database verification**: Sources are not validated against the actual database
4. **Single schema per file**: Each source file defines tables in one schema

### Definition of Done

- [x] `source_paths` config field parsed from featherflow.yml
- [x] Source files discovered from configured directories
- [x] `kind: sources` validation enforced (error if missing or wrong)
- [x] SourceFile, SourceTable, SourceColumn structs implemented
- [x] Source tables included in dependency categorization
- [x] `ff ls` shows sources alongside models
- [x] `ff test` runs tests defined on source columns
- [x] `ff docs` generates documentation for sources
- [x] `ff validate` validates source file syntax and kind
- [x] Unknown dependencies warned if not in models or sources
- [x] Deprecation warning for `external_tables` in featherflow.yml
- [x] Integration test: model depending on source compiles correctly
- [x] Integration test: source with tests passes ff test
- [x] Integration test: missing kind field errors clearly
- [x] Unit tests for source parsing and validation

### Task Breakdown

#### Task: Implement Sources (MVP)

**Phase 1: Config & Structs**
- [x] Add `source_paths` to `ProjectConfig` struct
- [x] Implement `SourceKind` enum with validation
- [x] Implement `SourceFile`, `SourceTable`, `SourceColumn` structs
- [x] Implement `FreshnessConfig` struct (parsed but not used)
- [x] Unit tests for serde deserialization

**Phase 2: Discovery & Validation**
- [x] Implement `discover_sources()` function
- [x] Implement `load_source_file()` with kind validation
- [x] Add source error types (SRC001-SRC007)
- [x] Integration with project loading
- [x] Unit tests for discovery and validation

**Phase 3: Dependency Integration**
- [x] Implement `build_source_lookup()`
- [x] Update `categorize_dependencies()` to use sources
- [x] Deprecation warning for `external_tables`
- [x] Integration tests for dependency categorization

**Phase 4: CLI Integration**
- [x] Update `ff ls` to show sources
- [x] Update `ff test` to run source tests
- [x] Update `ff docs` to document sources
- [x] Update `ff validate` to validate sources
- [x] Add make targets for source-related operations

**Estimated effort**: Medium (2-3 days)

---

## Custom Macros

This section specifies user-defined macros for Featherflow, enabling reusable SQL/Jinja snippets across models.

### Design Decisions

**Choice**: Use Minijinja's native macro system with explicit imports, loading macros from a `macros/` directory.

**Rationale**:
- Leverages Minijinja's built-in `{% macro %}` and `{% from ... import %}` syntax
- Explicit imports make dependencies clear and avoid namespace pollution
- File-based organization aligns with model organization patterns
- Uses Minijinja's `path_loader` feature for filesystem loading
- Minimal implementation effort for MVP

### Directory Structure

```
project/
├── macros/                     # User-defined macros
│   ├── utils.sql               # General utility macros
│   ├── dates.sql               # Date-related macros
│   └── testing.sql             # Test helper macros
├── models/
│   └── ...
└── featherflow.yml
```

### Configuration

Add `macro_paths` to `featherflow.yml`:

```yaml
# featherflow.yml
name: my_analytics_project
version: "1.0.0"

model_paths: ["models"]
seed_paths: ["seeds"]
macro_paths: ["macros"]          # NEW: directories containing macro files
target_path: "target"

# ... rest of config
```

**Default**: If `macro_paths` is not specified, defaults to `["macros"]` if the directory exists, otherwise empty.

### Macro File Format

Macro files are `.sql` files containing one or more macro definitions using Jinja syntax:

```sql
-- macros/utils.sql

{% macro cents_to_dollars(column_name) %}
({{ column_name }} / 100.0)
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
CASE
  WHEN {{ denominator }} = 0 THEN {{ default }}
  ELSE {{ numerator }} / {{ denominator }}
END
{% endmacro %}

{% macro generate_surrogate_key(field_list) %}
MD5(CONCAT_WS('|', {% for field in field_list %}{{ field }}{% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}
```

```sql
-- macros/dates.sql

{% macro date_trunc_to_week(date_column) %}
DATE_TRUNC('week', {{ date_column }})
{% endmacro %}

{% macro fiscal_quarter(date_column) %}
CASE
  WHEN MONTH({{ date_column }}) IN (1, 2, 3) THEN 'Q4'
  WHEN MONTH({{ date_column }}) IN (4, 5, 6) THEN 'Q1'
  WHEN MONTH({{ date_column }}) IN (7, 8, 9) THEN 'Q2'
  ELSE 'Q3'
END
{% endmacro %}
```

### Using Macros in Models

Models import macros explicitly using Jinja's `{% from ... import %}` syntax:

```sql
-- models/staging/stg_orders.sql
{% from "utils.sql" import cents_to_dollars, safe_divide %}
{% from "dates.sql" import date_trunc_to_week %}

{{ config(materialized='view') }}

SELECT
  order_id,
  {{ cents_to_dollars("amount_cents") }} AS amount_dollars,
  {{ safe_divide("revenue", "order_count") }} AS avg_revenue,
  {{ date_trunc_to_week("created_at") }} AS order_week
FROM raw_orders
WHERE created_at >= '{{ var("start_date") }}'
```

Alternative import syntax (import entire module):

```sql
{% import "utils.sql" as utils %}

SELECT
  {{ utils.cents_to_dollars("amount") }} AS amount_dollars
FROM orders
```

### Implementation

#### Config Struct Update

```rust
/// Project configuration from featherflow.yml
#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,

    #[serde(default = "default_model_paths")]
    pub model_paths: Vec<PathBuf>,

    #[serde(default = "default_seed_paths")]
    pub seed_paths: Vec<PathBuf>,

    #[serde(default = "default_macro_paths")]
    pub macro_paths: Vec<PathBuf>,  // NEW

    #[serde(default = "default_target_path")]
    pub target_path: PathBuf,

    // ... other fields
}

fn default_macro_paths() -> Vec<PathBuf> {
    vec![PathBuf::from("macros")]
}
```

#### Jinja Environment Setup

Update `ff-jinja` to load macros using Minijinja's `path_loader`:

```rust
use minijinja::{Environment, path_loader};
use std::path::Path;

/// Create Jinja environment with macros loaded
pub fn create_environment(
    project_root: &Path,
    macro_paths: &[PathBuf],
    vars: &HashMap<String, Value>,
) -> Result<Environment<'static>, JinjaError> {
    let mut env = Environment::new();

    // Add built-in functions
    env.add_function("config", config_function);
    env.add_function("var", make_var_function(vars.clone()));

    // Load macros from each macro path
    for macro_path in macro_paths {
        let full_path = project_root.join(macro_path);
        if full_path.exists() && full_path.is_dir() {
            // Use path_loader to enable {% from "file.sql" import macro %}
            env.set_loader(path_loader(full_path));
        }
    }

    Ok(env)
}
```

#### Macro Discovery

```rust
/// Discover macro files in configured directories
pub fn discover_macros(
    project_root: &Path,
    macro_paths: &[PathBuf],
) -> Vec<PathBuf> {
    let mut macros = Vec::new();

    for macro_path in macro_paths {
        let full_path = project_root.join(macro_path);
        if full_path.exists() && full_path.is_dir() {
            for entry in walkdir::WalkDir::new(&full_path)
                .max_depth(1)  // Flat structure for MVP
                .into_iter()
                .filter_map(|e| e.ok())
            {
                let path = entry.path();
                if path.extension().map_or(false, |e| e == "sql") {
                    macros.push(path.to_path_buf());
                }
            }
        }
    }

    macros
}
```

### Error Handling

| Error | Code | Message Template |
|-------|------|------------------|
| MacroNotFound | M001 | Macro file not found: '{file}'. Check macro_paths in featherflow.yml |
| MacroParseError | M002 | Failed to parse macro file '{file}': {details} |
| MacroImportError | M003 | Cannot import '{macro}' from '{file}': macro not defined |
| CircularMacroImport | M004 | Circular macro import detected: {path} |

### Validation

The `ff validate` command should check macros:

1. **Macro file syntax**: All `.sql` files in macro_paths parse as valid Jinja
2. **Macro naming**: No duplicate macro names across files (warn, not error)
3. **Import resolution**: All `{% from ... import %}` statements resolve
4. **Unused macros**: Warn about macros never imported (optional, low priority)

### Compile Output

Macros are inlined during compilation. The compiled SQL contains no macro references:

```sql
-- target/compiled/my_project/models/staging/stg_orders.sql
-- No {% from %} or {{ macro() }} - all expanded

SELECT
  order_id,
  (amount_cents / 100.0) AS amount_dollars,
  CASE WHEN order_count = 0 THEN 0 ELSE revenue / order_count END AS avg_revenue,
  DATE_TRUNC('week', created_at) AS order_week
FROM raw_orders
WHERE created_at >= '2024-01-01'
```

### Limitations (MVP)

1. **Flat directory structure**: Macros must be in the root of macro_paths directories (no subdirectories)
2. **No macro chaining**: Macros cannot import other macros (Minijinja limitation)
3. **No runtime macro generation**: Macros are static, defined in files
4. **Single loader**: Only the last macro_path is active if multiple are specified (Minijinja limitation - can be worked around post-MVP)

### Definition of Done

- [x] `macro_paths` config field parsed from featherflow.yml
- [x] Macro files discovered from configured directories
- [x] Minijinja environment loads macros via path_loader
- [x] `{% from "file.sql" import macro %}` works in models
- [x] `{% import "file.sql" as alias %}` works in models
- [x] Compiled SQL contains expanded macro output (no macro syntax)
- [x] Parse errors in macro files report file:line:column
- [x] `ff validate` checks macro file syntax
- [x] Integration test: model using macros compiles correctly
- [x] Integration test: undefined macro import errors clearly
- [x] Documentation in README for macro usage

### Task Breakdown

#### Task: Implement Custom Macros (MVP)

**Phase 1: Config & Discovery**
- [x] Add `macro_paths` to `ProjectConfig` struct
- [x] Add `default_macro_paths()` function
- [x] Update config parsing tests
- [x] Implement `discover_macros()` function
- [x] Unit tests for macro discovery

**Phase 2: Jinja Integration**
- [x] Enable `loader` feature in minijinja Cargo.toml
- [x] Update `create_environment()` to use `path_loader`
- [x] Test macro imports work in isolation
- [x] Handle missing macro file errors

**Phase 3: Compile Integration**
- [x] Update `ff compile` to load macros
- [x] Verify compiled output has no macro syntax
- [x] Add compile integration test with macros

**Phase 4: Validation**
- [x] Add macro validation to `ff validate`
- [x] Check macro file syntax
- [x] Check import resolution

**Estimated effort**: Medium (1-2 days)

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
| `config.rs` | Invalid YAML, missing required fields, type coercion, macro_paths |
| `functions.rs` | var() with/without default, config() type handling |
| `environment.rs` | Macro loading, macro import resolution, macro expansion |
| `source.rs` | Source file parsing, kind validation, source lookup building |

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
| Model with macro imports | Compiles with expanded macros |
| Missing macro file import | Clear error with file name |
| Undefined macro import | Clear error with macro name |
| Model depends on source | Dependency categorized as source |
| Source missing kind field | Clear error about required kind |
| Source with column tests | Tests execute on source table |

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

#[test]
fn test_macro_expansion_in_compiled_sql() {
    // Macros should be expanded, not present in compiled output
    let project = Project::load("fixtures/macro_project").unwrap();
    let compiled = compile_model(&project, "model_with_macro").unwrap();

    // Should NOT contain macro import syntax
    assert!(!compiled.contains("{% from"));
    assert!(!compiled.contains("{% import"));

    // Should contain expanded macro content
    assert!(compiled.contains("/ 100.0"));  // cents_to_dollars expansion
}

#[test]
fn test_undefined_macro_import_errors() {
    let project = Project::load("fixtures/bad_macro_import").unwrap();
    let result = compile_model(&project, "model_with_bad_import");

    assert!(matches!(result, Err(JinjaError::MacroImportError { .. })));
}

#[test]
fn test_source_kind_required() {
    // Source file without kind: sources should error
    let content = r#"
        name: raw_data
        schema: raw
        tables:
          - name: orders
    "#;
    let result = parse_source_file(content);
    assert!(matches!(result, Err(SourceError::MissingKind { .. })));
}

#[test]
fn test_source_kind_must_be_sources() {
    // Source file with wrong kind should error
    let content = r#"
        kind: models
        name: raw_data
        schema: raw
        tables:
          - name: orders
    "#;
    let result = parse_source_file(content);
    assert!(matches!(result, Err(SourceError::InvalidKind { .. })));
}

#[test]
fn test_model_depends_on_source() {
    let project = Project::load("fixtures/project_with_sources").unwrap();
    let model = project.get_model("stg_orders").unwrap();

    // raw.orders should be categorized as a source, not unknown
    assert!(model.source_deps.contains(&"raw.orders".to_string()));
    assert!(model.unknown_deps.is_empty());
}
```

---

## Expected CLI Output Examples

These examples document the expected behavior of each command for verification. Use the make targets for consistent execution.

### ff parse

```bash
$ make ff-parse-deps
# Equivalent: ff parse --project-dir tests/fixtures/sample_project --output deps
stg_orders: raw_orders (external)
stg_customers: raw_customers (external)
fct_orders: stg_customers, stg_orders
```

```bash
$ make ff-parse-json
# Equivalent: ff parse --project-dir tests/fixtures/sample_project --output json
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
$ make ff-compile
# Equivalent: ff compile --project-dir tests/fixtures/sample_project
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
$ make ff-run
# Equivalent: ff run --project-dir tests/fixtures/sample_project
Running 3 models...
  ✓ stg_orders (view) [45ms]
  ✓ stg_customers (view) [32ms]
  ✓ fct_orders (table) [128ms]

Completed: 3 succeeded, 0 failed
Total time: 205ms
```

With selector:
```bash
$ make ff-run-select MODELS="+fct_orders"
# Equivalent: ff run --select +fct_orders
Running 3 models (selected with ancestors)...
  ✓ stg_orders (view) [45ms]
  ✓ stg_customers (view) [32ms]
  ✓ fct_orders (table) [128ms]
```

### ff ls

```bash
$ make ff-ls
# Equivalent: ff ls --project-dir tests/fixtures/sample_project
NAME            MATERIALIZED  SCHEMA    DEPENDS_ON
stg_orders      view          staging   raw_orders (external)
stg_customers   view          staging   raw_customers (external)
fct_orders      table         -         stg_orders, stg_customers

3 models found
```

Tree output:
```bash
$ make ff-ls-tree
# Equivalent: ff ls --output tree
fct_orders (table)
├── stg_orders (view)
│   └── raw_orders (external)
└── stg_customers (view)
    └── raw_customers (external)
```

### ff test

```bash
$ make ff-test
# Equivalent: ff test --project-dir tests/fixtures/sample_project
Running 4 tests...
  ✓ unique_stg_orders_order_id [12ms]
  ✓ not_null_stg_orders_order_id [8ms]
  ✓ not_null_stg_orders_customer_id [9ms]
  ✓ unique_stg_customers_customer_id [11ms]

Passed: 4, Failed: 0
```

With failure:
```bash
$ make ff-test
Running 4 tests...
  ✓ unique_stg_orders_order_id [12ms]
  ✗ not_null_stg_orders_customer_id [8ms]
    Found 3 NULL values

Passed: 1, Failed: 1
Exit code: 1
```

### ff seed (to be implemented)

```bash
$ make ff-seed
# Equivalent: ff seed --project-dir tests/fixtures/sample_project
Loading 3 seeds...
  ✓ raw_orders (10 rows)
  ✓ raw_customers (5 rows)
  ✓ raw_products (4 rows)

Loaded 3 seeds (19 total rows)
```

### ff validate (to be implemented)

```bash
$ make ff-validate
# Equivalent: ff validate --project-dir tests/fixtures/sample_project
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
$ make ff-validate PROJECT_DIR=tests/fixtures/broken_project
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
$ make ff-docs
# Equivalent: ff docs --project-dir tests/fixtures/sample_project
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
- [x] `featherflow.yml` parses correctly
- [x] Model discovery finds all .sql files recursively
- [x] Schema files (1:1 naming) are discovered and parsed
- [x] External tables are recognized from config
- [x] Variables are loaded and accessible

**SQL Parsing**
- [x] DuckDB dialect parses all standard SQL
- [x] Dependencies extracted via AST (not regex)
- [x] CTE names are NOT included in dependencies
- [x] Self-references filtered out
- [x] Parse errors include file:line:column

**Jinja Templating**
- [x] `config()` function captures materialization, schema, tags
- [x] `var()` function substitutes variables with optional defaults
- [x] Undefined variables produce clear error messages
- [x] Rendered SQL is syntactically valid

**DAG Building**
- [x] Circular dependencies detected with clear error message showing cycle path
- [x] Topological sort produces correct execution order
- [x] `+model` selector includes ancestors
- [x] `model+` selector includes descendants
- [x] Duplicate model names produce error (not silent override)

**Database Operations (DuckDB)**
- [x] In-memory databases work
- [x] File-based databases work
- [x] `CREATE TABLE AS` works
- [x] `CREATE VIEW AS` works
- [x] `CREATE OR REPLACE` works
- [x] CSV loading works via `read_csv_auto()`
- [x] Schema creation (if model specifies schema)

**Schema Testing**
- [x] `unique` test generates correct SQL
- [x] `not_null` test generates correct SQL
- [x] Tests read from 1:1 schema files
- [x] Test results include pass/fail and timing
- [x] Failed tests show failure count

### CLI Commands (All Required)

**ff parse**
- [x] Parses all models in project
- [x] `--output json` produces valid JSON AST
- [x] `--output deps` shows dependency list
- [x] `--models` filter works
- [x] Parse errors clearly reported

**ff compile**
- [x] Renders Jinja templates
- [x] Writes compiled SQL to target/compiled/
- [x] Generates manifest.json with model metadata
- [x] Detects and reports circular dependencies

**ff run**
- [x] Compiles before running
- [x] Executes models in dependency order
- [x] Creates views for `materialized='view'`
- [x] Creates tables for `materialized='table'`
- [x] `--select` filters models
- [x] `--full-refresh` drops before recreating
- [x] Reports timing per model

**ff ls**
- [x] Lists all models
- [x] Shows materialization type
- [x] Shows dependencies
- [x] `--output json` produces valid JSON
- [x] `--output tree` shows dependency tree

**ff test**
- [x] Runs tests defined in schema files
- [x] Reports pass/fail per test
- [x] `--fail-fast` stops on first failure
- [x] Exit code 1 on any failure

**ff seed**
- [x] Loads CSV files from seed_paths
- [x] Creates tables with inferred types
- [x] `--full-refresh` recreates tables

**ff validate**
- [x] Validates SQL syntax
- [x] Validates Jinja variables defined
- [x] Detects circular dependencies
- [x] Detects duplicate model names
- [x] Reports all issues with severity levels

**ff docs**
- [x] Generates markdown per model with schema
- [x] Includes column definitions and tests
- [x] Generates index file
- [x] Works without database connection

### Integration Tests (All Required)

- [x] Full pipeline: seed → compile → run → test
- [x] Circular dependency detection
- [x] Model with dependencies executes after deps
- [x] Schema tests pass with valid data
- [x] Schema tests fail with invalid data
- [x] Parse errors reported correctly
- [x] Missing variable errors reported

### Documentation (All Required)

- [x] README with installation and quickstart
- [x] All CLI commands documented with examples
- [x] CLAUDE.md accurate for AI assistance
- [x] Example project in examples/quickstart/

### CI/CD (All Required)

- [x] `cargo build` succeeds
- [x] `cargo test` all pass
- [x] `cargo clippy` no warnings
- [x] `cargo fmt --check` passes
- [x] GitHub Actions CI configured

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
- [x] `ff seed` - Load CSV seed files
- [x] `ff validate` - Validate without execution
- [x] DuckDB backend fully functional
- [x] DAG building with cycle detection
- [x] Selector syntax (+model, model+)
- [x] config() and var() Jinja functions
- [x] CTE names filtered from dependencies (BUG FIX)
- [x] Duplicate model name detection (BUG FIX)

**Nice to Have (Not Blocking)**
- [x] `ff docs` - Generate documentation
- [x] `run_results.json` output
- [x] Manifest caching (with `--no-cache` flag)
- [x] Sample failing rows in test output
- [x] Schema auto-creation
- [x] Lineage diagram generation (lineage.dot)

**Deferred to v0.2.0**
- Snowflake backend
- Parallel execution
- Incremental runs
- Pre/post hooks

**Note**: Custom tests (accepted_values, positive, non_negative, min_value, max_value, regex) were originally deferred but are now fully implemented in v0.1.0.

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
$ make ff-ls-json
```
Expected:
- Exit code: 0
- Valid JSON output
- Contains 3 models
- Each model has name, materialized, depends_on fields

#### ff validate
```bash
$ make ff-validate
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
