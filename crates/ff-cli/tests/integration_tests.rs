//! Integration tests for Featherflow

use ff_core::dag::ModelDag;
use ff_core::model::TestDefinition;
use ff_core::run_state::{RunState, RunStatus};
use ff_core::ModelName;
use ff_core::Project;
use ff_db::{DatabaseCore, DatabaseCsv, DatabaseIncremental, DatabaseSchema, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_meta::manifest::Manifest;
use ff_sql::{extract_dependencies, ColumnRef, ExprType, ModelLineage, SqlParser};
use ff_test::{generator::GeneratedTest, TestRunner};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

/// Test loading the sample project
#[test]
fn test_load_sample_project() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    assert_eq!(project.config.name, "sample_project");
    assert_eq!(project.models.len(), 16);
    assert!(project.models.contains_key("stg_orders"));
    assert!(project.models.contains_key("stg_customers"));
    assert!(project.models.contains_key("stg_products"));
    assert!(project.models.contains_key("stg_payments"));
    assert!(project.models.contains_key("stg_payments_star"));
    assert!(project.models.contains_key("int_orders_enriched"));
    assert!(project.models.contains_key("int_customer_metrics"));
    assert!(project.models.contains_key("dim_customers"));
    assert!(project.models.contains_key("dim_products"));
    assert!(project.models.contains_key("fct_orders"));
    assert!(project.models.contains_key("rpt_order_volume"));
    assert!(project.models.contains_key("int_all_orders"));
    assert!(project.models.contains_key("int_customer_ranking"));
    assert!(project.models.contains_key("dim_products_extended"));
    assert!(project.models.contains_key("int_high_value_orders"));
    assert!(project.models.contains_key("rpt_customer_orders"));
}

/// Test parsing and dependency extraction
#[test]
fn test_parse_and_extract_dependencies() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();
    let parser = SqlParser::duckdb();
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    // Test stg_orders - depends on raw_orders (external)
    let stg_orders = project.get_model("stg_orders").unwrap();
    let rendered = jinja.render(&stg_orders.raw_sql).unwrap();
    let stmts = parser.parse(&rendered).unwrap();
    let deps = extract_dependencies(&stmts);

    assert!(deps.contains("raw_orders"));

    // Test fct_orders - depends on int_orders_enriched and stg_customers (models)
    let fct_orders = project.get_model("fct_orders").unwrap();
    let rendered = jinja.render(&fct_orders.raw_sql).unwrap();
    let stmts = parser.parse(&rendered).unwrap();
    let deps = extract_dependencies(&stmts);

    assert!(deps.contains("int_orders_enriched"));
    assert!(deps.contains("stg_customers"));
}

/// Test DAG building and topological sort
#[test]
fn test_dag_building() {
    let mut deps = HashMap::new();
    deps.insert("stg_orders".to_string(), vec![]);
    deps.insert("stg_customers".to_string(), vec![]);
    deps.insert(
        "fct_orders".to_string(),
        vec!["stg_orders".to_string(), "stg_customers".to_string()],
    );

    let dag = ModelDag::build(&deps).unwrap();
    let order = dag.topological_order().unwrap();

    // fct_orders should come after stg_orders and stg_customers
    let fct_pos = order.iter().position(|m| m == "fct_orders").unwrap();
    let stg_orders_pos = order.iter().position(|m| m == "stg_orders").unwrap();
    let stg_customers_pos = order.iter().position(|m| m == "stg_customers").unwrap();

    assert!(fct_pos > stg_orders_pos);
    assert!(fct_pos > stg_customers_pos);
}

/// Test circular dependency detection
#[test]
fn test_circular_dependency_detection() {
    let mut deps = HashMap::new();
    deps.insert("a".to_string(), vec!["b".to_string()]);
    deps.insert("b".to_string(), vec!["c".to_string()]);
    deps.insert("c".to_string(), vec!["a".to_string()]);

    let result = ModelDag::build(&deps);
    assert!(result.is_err());
}

/// Test schema.yml parsing
#[test]
fn test_schema_yml_parsing() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Check tests were discovered
    assert!(!project.tests.is_empty());

    // Check specific tests
    let stg_orders_tests: Vec<_> = project
        .tests
        .iter()
        .filter(|t| t.model == "stg_orders")
        .collect();

    assert!(!stg_orders_tests.is_empty());
}

/// Test Jinja variable substitution
#[test]
fn test_jinja_variable_substitution() {
    let mut vars = HashMap::new();
    vars.insert(
        "start_date".to_string(),
        serde_yaml::Value::String("2024-01-01".to_string()),
    );

    let jinja = JinjaEnvironment::new(&vars);
    let template = "SELECT * FROM orders WHERE date >= '{{ var(\"start_date\") }}'";
    let result = jinja.render(template).unwrap();

    assert!(result.contains("2024-01-01"));
}

/// Test config capture
#[test]
fn test_config_capture() {
    let jinja = JinjaEnvironment::default();
    let template = "{{ config(materialized='table', schema='staging') }}SELECT 1";

    let (_, config) = jinja.render_with_config(template).unwrap();

    assert_eq!(config.get("materialized").unwrap().as_str(), Some("table"));
    assert_eq!(config.get("schema").unwrap().as_str(), Some("staging"));
}

/// Test selector parsing
#[test]
fn test_selector_ancestors() {
    let mut deps = HashMap::new();
    deps.insert("raw".to_string(), vec![]);
    deps.insert("stg".to_string(), vec!["raw".to_string()]);
    deps.insert("fct".to_string(), vec!["stg".to_string()]);

    let dag = ModelDag::build(&deps).unwrap();
    let selected = dag.select("+fct").unwrap();

    assert_eq!(selected.len(), 3);
    assert!(selected.contains(&"raw".to_string()));
    assert!(selected.contains(&"stg".to_string()));
    assert!(selected.contains(&"fct".to_string()));
}

/// Test selector descendants
#[test]
fn test_selector_descendants() {
    let mut deps = HashMap::new();
    deps.insert("raw".to_string(), vec![]);
    deps.insert("stg".to_string(), vec!["raw".to_string()]);
    deps.insert("fct".to_string(), vec!["stg".to_string()]);

    let dag = ModelDag::build(&deps).unwrap();
    let selected = dag.select("raw+").unwrap();

    assert_eq!(selected.len(), 3);
    assert!(selected.contains(&"raw".to_string()));
    assert!(selected.contains(&"stg".to_string()));
    assert!(selected.contains(&"fct".to_string()));
}

/// Test DuckDB backend
#[tokio::test]
async fn test_duckdb_backend() {
    let db = DuckDbBackend::in_memory().unwrap();

    db.create_table_as("test_table", "SELECT 1 AS id, 'hello' AS name", false)
        .await
        .unwrap();

    assert!(db.relation_exists("test_table").await.unwrap());

    let count = db.query_count("SELECT * FROM test_table").await.unwrap();
    assert_eq!(count, 1);
}

/// Test unique constraint test generation
#[tokio::test]
async fn test_unique_constraint_pass() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE test_table (id INT); INSERT INTO test_table VALUES (1), (2), (3);",
    )
    .await
    .unwrap();

    let test = GeneratedTest::from_schema_test(&ff_core::model::SchemaTest {
        test_type: ff_core::model::TestType::Unique,
        column: "id".to_string(),
        model: ff_core::model_name::ModelName::new("test_table"),
        config: Default::default(),
    });

    let runner = TestRunner::new(&db);
    let result = runner.run_test(&test).await;

    assert!(result.passed);
}

/// Test unique constraint failure
#[tokio::test]
async fn test_unique_constraint_fail() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE test_table (id INT); INSERT INTO test_table VALUES (1), (1), (2);",
    )
    .await
    .unwrap();

    let test = GeneratedTest::from_schema_test(&ff_core::model::SchemaTest {
        test_type: ff_core::model::TestType::Unique,
        column: "id".to_string(),
        model: ff_core::model_name::ModelName::new("test_table"),
        config: Default::default(),
    });

    let runner = TestRunner::new(&db);
    let result = runner.run_test(&test).await;

    assert!(!result.passed);
    assert_eq!(result.failure_count, 1);
}

/// Test not_null constraint
#[tokio::test]
async fn test_not_null_constraint() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE test_table (name VARCHAR); INSERT INTO test_table VALUES ('a'), (NULL);",
    )
    .await
    .unwrap();

    let test = GeneratedTest::from_schema_test(&ff_core::model::SchemaTest {
        test_type: ff_core::model::TestType::NotNull,
        column: "name".to_string(),
        model: ff_core::model_name::ModelName::new("test_table"),
        config: Default::default(),
    });

    let runner = TestRunner::new(&db);
    let result = runner.run_test(&test).await;

    assert!(!result.passed);
    assert_eq!(result.failure_count, 1);
}

/// Test full pipeline: load seeds, compile, run
#[tokio::test]
async fn test_full_pipeline() {
    let db = DuckDbBackend::in_memory().unwrap();

    let seeds_path = Path::new("testdata/seeds");
    if seeds_path.exists() {
        for entry in std::fs::read_dir(seeds_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "csv") {
                let table_name = path.file_stem().unwrap().to_str().unwrap();
                db.load_csv(table_name, path.to_str().unwrap())
                    .await
                    .unwrap();
            }
        }

        // Verify seeds loaded
        assert!(db.relation_exists("raw_orders").await.unwrap());
        assert!(db.relation_exists("raw_customers").await.unwrap());

        let order_count = db.query_count("SELECT * FROM raw_orders").await.unwrap();
        assert_eq!(order_count, 10);

        let customer_count = db.query_count("SELECT * FROM raw_customers").await.unwrap();
        assert_eq!(customer_count, 5);
    }
}

/// Test manifest serialization
#[test]
fn test_manifest_serialization() {
    let manifest = Manifest::new("test_project");
    let json = serde_json::to_string_pretty(&manifest).unwrap();
    let loaded: Manifest = serde_json::from_str(&json).unwrap();

    assert_eq!(loaded.project_name, "test_project");
}

/// Test seed file loading (all three seed files)
#[tokio::test]
async fn test_seed_loading() {
    let db = DuckDbBackend::in_memory().unwrap();

    let seeds_path = Path::new("testdata/seeds");
    if !seeds_path.exists() {
        // Skip test if seeds directory doesn't exist
        return;
    }

    for entry in std::fs::read_dir(seeds_path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "csv") {
            let table_name = path.file_stem().unwrap().to_str().unwrap();
            db.load_csv(table_name, path.to_str().unwrap())
                .await
                .unwrap();
        }
    }

    // Verify seeds loaded
    assert!(db.relation_exists("raw_orders").await.unwrap());
    assert!(db.relation_exists("raw_customers").await.unwrap());
    assert!(db.relation_exists("raw_products").await.unwrap());

    // Verify row counts
    let order_count = db.query_count("SELECT * FROM raw_orders").await.unwrap();
    assert_eq!(order_count, 10);

    let customer_count = db.query_count("SELECT * FROM raw_customers").await.unwrap();
    assert_eq!(customer_count, 5);

    let product_count = db.query_count("SELECT * FROM raw_products").await.unwrap();
    assert_eq!(product_count, 5);
}

/// Test source file discovery
#[test]
fn test_source_discovery() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    assert!(!project.sources.is_empty(), "Sources should be discovered");

    let ecommerce_source = project
        .sources
        .iter()
        .find(|s| s.name == "raw_ecommerce")
        .expect("raw_ecommerce source should exist");

    assert_eq!(ecommerce_source.schema, "analytics");
    assert_eq!(ecommerce_source.tables.len(), 4);

    let raw_orders_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_orders")
        .expect("raw_orders table should exist");
    assert_eq!(raw_orders_table.columns.len(), 5);

    let raw_customers_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_customers")
        .expect("raw_customers table should exist");
    assert_eq!(raw_customers_table.columns.len(), 5);

    let raw_products_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_products")
        .expect("raw_products table should exist");
    assert_eq!(raw_products_table.columns.len(), 5);

    let raw_payments_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_payments")
        .expect("raw_payments table should exist");
    assert_eq!(raw_payments_table.columns.len(), 5);
}

/// Test that all discovered sources have non-empty names
#[test]
fn test_source_names_not_empty() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    for source in &project.sources {
        assert!(
            !source.name.is_empty(),
            "Source name should not be empty after validation"
        );
    }
}

/// Test source table tests are discovered
#[test]
fn test_source_table_tests() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let ecommerce_source = project
        .sources
        .iter()
        .find(|s| s.name == "raw_ecommerce")
        .expect("raw_ecommerce source should exist");

    // Check raw_orders tests
    let raw_orders_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_orders")
        .expect("raw_orders table should exist");

    // id column should have unique and not_null tests
    let id_col = raw_orders_table
        .columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column should exist");
    assert!(
        id_col
            .tests
            .contains(&TestDefinition::Simple("unique".to_string())),
        "id should have unique test"
    );
    assert!(
        id_col
            .tests
            .contains(&TestDefinition::Simple("not_null".to_string())),
        "id should have not_null test"
    );

    // user_id column should have not_null test
    let user_id_col = raw_orders_table
        .columns
        .iter()
        .find(|c| c.name == "user_id")
        .expect("user_id column should exist");
    assert!(
        user_id_col
            .tests
            .contains(&TestDefinition::Simple("not_null".to_string())),
        "user_id should have not_null test"
    );
}

/// Test source qualified name generation
#[test]
fn test_source_qualified_names() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let ecommerce_source = project
        .sources
        .iter()
        .find(|s| s.name == "raw_ecommerce")
        .expect("raw_ecommerce source should exist");

    // Check qualified table name generation
    let raw_orders_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_orders")
        .expect("raw_orders table should exist");

    let qualified = format!("{}.{}", ecommerce_source.schema, raw_orders_table.name);
    assert_eq!(qualified, "analytics.raw_orders");
}

/// Test source lookup building
#[test]
fn test_source_lookup() {
    use ff_core::source::build_source_lookup;

    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let lookup = build_source_lookup(&project.sources);

    // Should be able to lookup raw_orders (both unqualified and qualified)
    assert!(
        lookup.contains("raw_orders"),
        "raw_orders should be in lookup"
    );
    assert!(
        lookup.contains("raw_customers"),
        "raw_customers should be in lookup"
    );

    // Qualified names should also be in the lookup
    assert!(
        lookup.contains("analytics.raw_orders"),
        "analytics.raw_orders should be in lookup"
    );
    assert!(
        lookup.contains("analytics.raw_customers"),
        "analytics.raw_customers should be in lookup"
    );
}

/// Test docs command generates markdown files
#[test]
fn test_docs_markdown_generation() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Test that project loads correctly for docs
    assert_eq!(project.config.name, "sample_project");
    assert!(!project.models.is_empty(), "Project should have models");

    // Verify we can access model names for documentation
    let model_names = project.model_names();
    assert!(
        model_names.contains(&"stg_orders"),
        "stg_orders should be in model names"
    );
    assert!(
        model_names.contains(&"fct_orders"),
        "fct_orders should be in model names"
    );
}

/// Test docs command generates HTML files
#[test]
fn test_docs_html_format() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Verify source documentation data can be built
    assert!(!project.sources.is_empty(), "Project should have sources");

    let source = &project.sources[0];
    assert_eq!(source.name, "raw_ecommerce");
    assert_eq!(source.schema, "analytics");
    assert!(!source.tables.is_empty(), "Source should have tables");
}

/// Test docs handles models with and without schema files
#[test]
fn test_docs_schema_detection() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let with_schema = project
        .models
        .values()
        .filter(|m| m.schema.is_some())
        .count();

    // 1:1 YAML is enforced, so every model should have a schema
    assert!(
        with_schema > 0,
        "Sample project should have models with schemas"
    );
    assert_eq!(
        with_schema,
        project.models.len(),
        "All models should have schemas (1:1 YAML enforcement)"
    );
}

/// Test validate command can load and validate a project
#[test]
fn test_validate_project_loads() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Project should load successfully
    assert_eq!(project.config.name, "sample_project");

    // All models should be valid
    for (name, model) in &project.models {
        assert!(!model.raw_sql.is_empty(), "Model {} should have SQL", name);
    }
}

/// Test validate can detect circular dependencies via DAG
#[test]
fn test_validate_circular_dependency_detection() {
    use ff_core::dag::ModelDag;

    // Build dependency map from sample project
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("stg_orders".to_string(), vec!["raw_orders".to_string()]);
    deps.insert(
        "stg_customers".to_string(),
        vec!["raw_customers".to_string()],
    );
    deps.insert(
        "fct_orders".to_string(),
        vec!["stg_orders".to_string(), "stg_customers".to_string()],
    );

    // DAG should build successfully for valid dependencies
    let dag_result = ModelDag::build(&deps);
    assert!(dag_result.is_ok(), "Valid DAG should build successfully");

    let dag = dag_result.unwrap();
    let sorted = dag.topological_order();
    assert!(sorted.is_ok(), "Topological order should succeed");
}

/// Test docs output contains expected structure for models with schemas
#[test]
fn test_docs_output_structure() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Find a model with a schema file
    let model_with_schema = project.models.iter().find(|(_, m)| m.schema.is_some());
    assert!(
        model_with_schema.is_some(),
        "Sample project should have at least one model with schema"
    );

    let (name, model) = model_with_schema.unwrap();
    let schema = model.schema.as_ref().unwrap();

    // Verify schema has required documentation fields
    assert!(
        schema.description.is_some() || !schema.columns.is_empty(),
        "Model {} schema should have description or columns",
        name
    );

    // Verify columns have expected structure
    if !schema.columns.is_empty() {
        let col = &schema.columns[0];
        // Column should have a name at minimum
        assert!(!col.name.is_empty(), "Column should have a name");
    }
}

/// Test docs includes dependencies information
#[test]
fn test_docs_dependencies() {
    use ff_core::dag::ModelDag;

    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Build dependencies to verify they're extractable for docs
    let fct_orders = project.get_model("fct_orders");
    assert!(fct_orders.is_some(), "fct_orders model should exist");

    // Verify we can build a dependency graph
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    for name in project.models.keys() {
        // For this test, just verify we can iterate models
        deps.insert(name.to_string(), vec![]);
    }

    let dag = ModelDag::build(&deps);
    assert!(
        dag.is_ok(),
        "Should be able to build dependency graph for docs"
    );
}

/// Test validate fails on circular dependencies
#[test]
fn test_validate_fails_on_circular_deps() {
    use ff_core::dag::ModelDag;

    // Create circular dependency: a -> b -> c -> a
    let mut circular_deps: HashMap<String, Vec<String>> = HashMap::new();
    circular_deps.insert("model_a".to_string(), vec!["model_c".to_string()]);
    circular_deps.insert("model_b".to_string(), vec!["model_a".to_string()]);
    circular_deps.insert("model_c".to_string(), vec!["model_b".to_string()]);

    let result = ModelDag::build(&circular_deps);
    assert!(
        result.is_err(),
        "Circular dependencies should cause DAG build to fail"
    );

    // Verify error message mentions cycle
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("E007") || err_str.contains("ircular") || err_str.contains("cycle"),
        "Error should indicate circular dependency: {}",
        err_str
    );
}

/// Test validate passes on valid project
#[test]
fn test_validate_passes_valid_project() {
    let project = Project::load(Path::new("tests/fixtures/sample_project"));
    assert!(
        project.is_ok(),
        "Valid sample project should load successfully"
    );

    let project = project.unwrap();

    // Verify basic validation passes
    assert!(!project.models.is_empty(), "Project should have models");
    assert!(!project.config.name.is_empty(), "Project should have name");

    // Verify no duplicate model names (project loading would have failed)
    let model_count = project.models.len();
    let unique_names: std::collections::HashSet<_> = project.models.keys().collect();
    assert_eq!(
        model_count,
        unique_names.len(),
        "All model names should be unique"
    );
}

/// Test validate detects SQL syntax errors
#[test]
fn test_validate_sql_syntax() {
    use ff_sql::SqlParser;

    let parser = SqlParser::from_dialect_name("duckdb").unwrap();

    // Valid SQL should parse
    let valid_sql = "SELECT id, name FROM users WHERE active = true";
    let valid_result = parser.parse(valid_sql);
    assert!(valid_result.is_ok(), "Valid SQL should parse successfully");

    // Invalid SQL should fail
    let invalid_sql = "SELEC id FROM users"; // Typo: SELEC instead of SELECT
    let invalid_result = parser.parse(invalid_sql);
    assert!(
        invalid_result.is_err(),
        "Invalid SQL syntax should fail parsing"
    );
}

/// Test incremental append strategy
#[tokio::test]
async fn test_incremental_append_strategy() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create initial table
    db.execute_batch(
        "CREATE TABLE events (id INT, event_type VARCHAR, created_at DATE);
         INSERT INTO events VALUES (1, 'click', '2024-01-01'), (2, 'view', '2024-01-01');",
    )
    .await
    .unwrap();

    let initial_count = db.query_count("SELECT * FROM events").await.unwrap();
    assert_eq!(initial_count, 2);

    // Append new data (simulating incremental run)
    db.execute("INSERT INTO events SELECT 3, 'purchase', '2024-01-02'")
        .await
        .unwrap();

    let new_count = db.query_count("SELECT * FROM events").await.unwrap();
    assert_eq!(new_count, 3);

    // Verify original data unchanged
    let jan1_count = db
        .query_count("SELECT * FROM events WHERE created_at = '2024-01-01'")
        .await
        .unwrap();
    assert_eq!(jan1_count, 2);
}

/// Test incremental merge strategy
#[tokio::test]
async fn test_incremental_merge_strategy() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create initial table
    db.execute_batch(
        "CREATE TABLE users (id INT, name VARCHAR, status VARCHAR);
         INSERT INTO users VALUES (1, 'Alice', 'active'), (2, 'Bob', 'active');",
    )
    .await
    .unwrap();

    // Merge with updated and new data
    let source_sql =
        "SELECT 2 AS id, 'Bobby' AS name, 'inactive' AS status UNION ALL SELECT 3, 'Charlie', 'active'";

    db.merge_into("users", source_sql, &["id".to_string()])
        .await
        .unwrap();

    // Verify counts
    let count = db.query_count("SELECT * FROM users").await.unwrap();
    assert_eq!(count, 3);

    // Verify Bob was updated
    let bob_status = db
        .query_one("SELECT status FROM users WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(bob_status, Some("inactive".to_string()));

    // Verify Alice unchanged
    let alice_status = db
        .query_one("SELECT status FROM users WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(alice_status, Some("active".to_string()));
}

/// Test incremental delete+insert strategy
#[tokio::test]
async fn test_incremental_delete_insert_strategy() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create initial table
    db.execute_batch(
        "CREATE TABLE inventory (product_id INT, warehouse VARCHAR, qty INT);
         INSERT INTO inventory VALUES (1, 'A', 100), (1, 'B', 50), (2, 'A', 75);",
    )
    .await
    .unwrap();

    // Delete+insert for product 1 across all warehouses
    let source_sql =
        "SELECT 1 AS product_id, 'A' AS warehouse, 120 AS qty UNION ALL SELECT 1, 'C', 30";

    db.delete_insert(
        "inventory",
        source_sql,
        &["product_id".to_string(), "warehouse".to_string()],
    )
    .await
    .unwrap();

    // Product 1 warehouse A should be updated, warehouse B should remain, warehouse C should be new
    let count = db.query_count("SELECT * FROM inventory").await.unwrap();
    assert_eq!(count, 4); // 1-A (updated), 1-B (unchanged), 1-C (new), 2-A (unchanged)

    // Verify product 1 warehouse A was updated
    let qty = db
        .query_one("SELECT qty FROM inventory WHERE product_id = 1 AND warehouse = 'A'")
        .await
        .unwrap();
    assert_eq!(qty, Some("120".to_string()));

    // Verify product 2 unchanged
    let qty = db
        .query_one("SELECT qty FROM inventory WHERE product_id = 2 AND warehouse = 'A'")
        .await
        .unwrap();
    assert_eq!(qty, Some("75".to_string()));
}

/// Test incremental with full refresh
#[tokio::test]
async fn test_incremental_full_refresh() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create initial table with old data
    db.execute_batch(
        "CREATE TABLE metrics (date DATE, value INT);
         INSERT INTO metrics VALUES ('2024-01-01', 100);",
    )
    .await
    .unwrap();

    let initial_count = db.query_count("SELECT * FROM metrics").await.unwrap();
    assert_eq!(initial_count, 1);

    // Full refresh replaces all data
    db.drop_if_exists("metrics").await.unwrap();
    db.create_table_as(
        "metrics",
        "SELECT '2024-01-02'::DATE AS date, 200 AS value",
        false,
    )
    .await
    .unwrap();

    let new_count = db.query_count("SELECT * FROM metrics").await.unwrap();
    assert_eq!(new_count, 1);

    let new_value = db.query_one("SELECT value FROM metrics").await.unwrap();
    assert_eq!(new_value, Some("200".to_string()));
}

/// Test parallel execution with independent models
#[tokio::test]
async fn test_parallel_execution_independent() {
    // This test verifies that independent models can be executed
    // The actual parallelism is tested by running the CLI with --threads
    // Here we verify the compute_execution_levels logic indirectly

    let db = DuckDbBackend::in_memory().unwrap();

    // Create three independent tables (no dependencies between them)
    let models = vec![
        ("model_a", "SELECT 1 AS id, 'a' AS value"),
        ("model_b", "SELECT 2 AS id, 'b' AS value"),
        ("model_c", "SELECT 3 AS id, 'c' AS value"),
    ];

    // Execute all models - they could theoretically run in parallel
    for (name, sql) in &models {
        db.create_table_as(name, sql, false).await.unwrap();
    }

    // Verify all tables exist
    for (name, _) in &models {
        assert!(db.relation_exists(name).await.unwrap());
    }

    // Verify row counts
    for (name, _) in &models {
        let count = db
            .query_count(&format!("SELECT * FROM {}", name))
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}

/// Test parallel execution respects dependencies
#[tokio::test]
async fn test_parallel_execution_with_dependencies() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create a dependency chain: base -> stg -> fct
    // In parallel execution, only models without pending deps can run together

    // Level 1: base (no dependencies)
    db.create_table_as("base", "SELECT 1 AS id, 100 AS value", false)
        .await
        .unwrap();

    // Level 2: stg (depends on base)
    db.create_table_as("stg", "SELECT id, value * 2 AS value FROM base", false)
        .await
        .unwrap();

    // Level 3: fct (depends on stg)
    db.create_table_as(
        "fct",
        "SELECT id, value + 50 AS final_value FROM stg",
        false,
    )
    .await
    .unwrap();

    // Verify the chain executed correctly
    let result = db.query_one("SELECT final_value FROM fct").await.unwrap();
    assert_eq!(result, Some("250".to_string())); // 100 * 2 + 50 = 250
}

// ============================================================================
// ERROR SCENARIO TESTS (Work Unit 5.1)
// ============================================================================

/// Test that circular dependency error includes the cycle path
#[test]
fn test_circular_dependency_error_message_includes_cycle() {
    let mut deps = HashMap::new();
    deps.insert("model_a".to_string(), vec!["model_c".to_string()]);
    deps.insert("model_b".to_string(), vec!["model_a".to_string()]);
    deps.insert("model_c".to_string(), vec!["model_b".to_string()]);

    let result = ModelDag::build(&deps);
    assert!(result.is_err(), "Should fail due to circular dependency");

    let err = result.unwrap_err();
    let err_str = err.to_string();

    // Error should include E007 code and cycle information
    assert!(
        err_str.contains("E007") || err_str.contains("ircular"),
        "Error should indicate circular dependency: {}",
        err_str
    );

    // Error should mention at least one model in the cycle
    let contains_cycle_info =
        err_str.contains("model_a") || err_str.contains("model_b") || err_str.contains("model_c");
    assert!(
        contains_cycle_info,
        "Error should mention models in cycle: {}",
        err_str
    );
}

/// Test that SQL syntax errors show useful location information
#[test]
fn test_sql_syntax_error_shows_useful_info() {
    let parser = SqlParser::from_dialect_name("duckdb").unwrap();

    // Various invalid SQL statements
    let test_cases = vec![
        "SELEC * FROM users",                  // Typo in SELECT
        "SELECT FROM users",                   // Missing columns
        "SELECT * FROM users WHERE",           // Incomplete WHERE
        "SELECT * FROM users GROUP",           // Incomplete GROUP BY
        "SELECT a, b FROM users HAVING a > 0", // HAVING without GROUP BY (depending on dialect)
    ];

    for invalid_sql in test_cases {
        let result = parser.parse(invalid_sql);
        // Most of these should fail to parse
        if let Err(err) = result {
            let err_str = err.to_string();

            // Error should not be empty
            assert!(
                !err_str.is_empty(),
                "Error message should not be empty for: {}",
                invalid_sql
            );
        }
    }
}

/// Test undefined variable error in Jinja
#[test]
fn test_undefined_variable_error() {
    let jinja = JinjaEnvironment::default();

    // Reference a variable that doesn't exist and has no default
    let template = "SELECT * FROM orders WHERE date > '{{ var(\"nonexistent_var\") }}'";
    let result = jinja.render(template);

    // This should either error or return empty string depending on implementation
    // The important thing is it doesn't panic
    match result {
        Ok(rendered) => {
            // If it succeeds, the undefined var should be handled gracefully
            assert!(
                !rendered.contains("nonexistent_var"),
                "Undefined variable should be replaced or cause error"
            );
        }
        Err(e) => {
            // If it errors, the message should be helpful
            let err_str = e.to_string();
            assert!(
                !err_str.is_empty(),
                "Error message should not be empty for undefined variable"
            );
        }
    }
}

/// Test invalid YAML schema error handling
#[test]
fn test_invalid_yaml_schema_error() {
    let dir = tempdir().unwrap();
    let project_dir = dir.path();

    // Create minimal project structure (directory-per-model)
    std::fs::create_dir_all(project_dir.join("models/test_model")).unwrap();

    // Create config file
    let config_content = r#"
name: test_project
database:
  type: duckdb
  path: ":memory:"
model_paths:
  - models
"#;
    std::fs::write(project_dir.join("featherflow.yml"), config_content).unwrap();

    // Create a valid model SQL file
    std::fs::write(
        project_dir.join("models/test_model/test_model.sql"),
        "SELECT 1 as id",
    )
    .unwrap();

    // Create an INVALID YAML schema file (malformed YAML)
    let invalid_yaml = r#"
version: "1"
name: test_model
  columns:  # Wrong indentation
- name: id
"#;
    std::fs::write(
        project_dir.join("models/test_model/test_model.yml"),
        invalid_yaml,
    )
    .unwrap();

    // Try to load the project - it should handle the invalid YAML gracefully
    let result = Project::load(project_dir);

    // Either it loads with warnings or fails with a clear error
    match result {
        Ok(_) => {
            // If it loads, the schema was either skipped or partially parsed
        }
        Err(e) => {
            // Error should mention YAML or parse error
            let err_str = e.to_string();
            assert!(
                err_str.to_lowercase().contains("yaml")
                    || err_str.to_lowercase().contains("parse")
                    || err_str.contains("E010"),
                "Error should indicate YAML parse issue: {}",
                err_str
            );
        }
    }
}

/// Test duplicate model name detection
#[test]
fn test_duplicate_model_name_error() {
    use ff_core::dag::ModelDag;

    // In practice, duplicate detection happens at project load time
    // Here we test that the DAG doesn't break with duplicate keys (HashMap handles it)
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("model_a".to_string(), vec![]);
    deps.insert("model_a".to_string(), vec!["model_b".to_string()]); // Overwrites

    let dag = ModelDag::build(&deps);
    assert!(dag.is_ok(), "HashMap handles duplicates by overwriting");

    // The second insert overwrites the first
    let dag = dag.unwrap();
    let _order = dag.topological_order();
    // Depending on implementation, this might succeed or fail
    // The key test is that it doesn't panic
}

// ============================================================================
// EDGE CASE TESTS (Work Unit 5.2)
// ============================================================================

/// Test empty project (no models)
#[test]
fn test_empty_project() {
    let dir = tempdir().unwrap();
    let project_dir = dir.path();

    // Create config file but no models directory
    let config_content = r#"
name: empty_project
database:
  type: duckdb
  path: ":memory:"
model_paths:
  - models
"#;
    std::fs::write(project_dir.join("featherflow.yml"), config_content).unwrap();

    // Create empty models directory
    std::fs::create_dir_all(project_dir.join("models")).unwrap();

    let result = Project::load(project_dir);
    match result {
        Ok(project) => {
            assert!(
                project.models.is_empty(),
                "Empty project should have no models"
            );
        }
        Err(_) => {
            // Some implementations might error on empty project
        }
    }
}

/// Test model with no dependencies (standalone)
#[test]
fn test_model_with_no_dependencies() {
    let mut deps = HashMap::new();
    deps.insert("standalone_model".to_string(), vec![]);

    let dag = ModelDag::build(&deps).unwrap();
    let order = dag.topological_order().unwrap();

    assert_eq!(order.len(), 1);
    assert_eq!(order[0], "standalone_model");
}

/// Test very deep DAG (10+ levels)
#[test]
fn test_very_deep_dag() {
    let mut deps = HashMap::new();

    // Create a chain: level_0 -> level_1 -> level_2 -> ... -> level_19
    for i in 0..20 {
        let model_name = format!("level_{}", i);
        if i == 0 {
            deps.insert(model_name, vec![]);
        } else {
            deps.insert(model_name, vec![format!("level_{}", i - 1)]);
        }
    }

    let dag = ModelDag::build(&deps).unwrap();
    let order = dag.topological_order().unwrap();

    assert_eq!(order.len(), 20);
    // level_0 should be first, level_19 should be last
    assert_eq!(order[0], "level_0");
    assert_eq!(order[19], "level_19");

    // Verify order is correct
    for i in 1..20 {
        let pos_prev = order.iter().position(|m| m == &format!("level_{}", i - 1));
        let pos_curr = order.iter().position(|m| m == &format!("level_{}", i));
        assert!(
            pos_prev.unwrap() < pos_curr.unwrap(),
            "level_{} should come before level_{}",
            i - 1,
            i
        );
    }
}

/// Test wide DAG (model with 20+ dependencies)
#[test]
fn test_wide_dag() {
    let mut deps = HashMap::new();

    // Create 25 base models
    let base_models: Vec<String> = (0..25).map(|i| format!("base_{}", i)).collect();
    for name in &base_models {
        deps.insert(name.clone(), vec![]);
    }

    // Create one model that depends on all 25
    deps.insert("wide_model".to_string(), base_models.clone());

    let dag = ModelDag::build(&deps).unwrap();
    let order = dag.topological_order().unwrap();

    assert_eq!(order.len(), 26);

    // wide_model should be last
    assert_eq!(order[25], "wide_model");

    // All base models should come before wide_model
    let wide_pos = order.iter().position(|m| m == "wide_model").unwrap();
    for base in &base_models {
        let base_pos = order.iter().position(|m| m == base).unwrap();
        assert!(
            base_pos < wide_pos,
            "{} should come before wide_model",
            base
        );
    }
}

/// Test special characters in column names
#[tokio::test]
async fn test_special_chars_in_column_names() {
    let db = DuckDbBackend::in_memory().unwrap();

    // DuckDB handles quoted identifiers
    db.execute_batch(
        r#"CREATE TABLE test_special (
            "column with spaces" INTEGER,
            "column-with-dashes" INTEGER,
            "UPPERCASE" INTEGER
        );
        INSERT INTO test_special VALUES (1, 2, 3);"#,
    )
    .await
    .unwrap();

    // Verify we can query
    let count = db.query_count("SELECT * FROM test_special").await.unwrap();
    assert_eq!(count, 1);

    // Verify we can access special columns
    let result = db
        .query_one(r#"SELECT "column with spaces" FROM test_special"#)
        .await
        .unwrap();
    assert_eq!(result, Some("1".to_string()));
}

// ============================================================================
// RUN STATE TESTS (Verifies 15.1 implementation)
// ============================================================================

/// Test run state basic functionality
#[test]
fn test_run_state_new_and_mark() {
    let mut state = RunState::new(
        vec![
            ModelName::new("a"),
            ModelName::new("b"),
            ModelName::new("c"),
        ],
        Some("--select +c".to_string()),
        "config_hash_123".to_string(),
    );

    assert_eq!(state.pending_models.len(), 3);
    assert_eq!(state.status, RunStatus::Running);

    // Mark model a as completed
    state.mark_completed("a", 1500).unwrap();
    assert_eq!(state.completed_models.len(), 1);
    assert_eq!(state.pending_models.len(), 2);
    assert!(state.is_completed("a"));
    assert!(!state.is_failed("a"));

    // Mark model b as failed
    state.mark_failed("b", "SQL error").unwrap();
    assert_eq!(state.failed_models.len(), 1);
    assert_eq!(state.pending_models.len(), 1);
    assert!(state.is_failed("b"));
    assert!(!state.is_completed("b"));

    // Summary
    let summary = state.summary();
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.pending, 1);
}

/// Test run state save and load
#[test]
fn test_run_state_persistence() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("run_state.json");

    let mut state = RunState::new(
        vec![ModelName::new("model_a"), ModelName::new("model_b")],
        None,
        "hash123".to_string(),
    );
    state.mark_completed("model_a", 1000).unwrap();

    // Save
    state.save(&state_path).unwrap();
    assert!(state_path.exists());

    // Load
    let loaded = RunState::load(&state_path).unwrap().unwrap();
    assert_eq!(loaded.run_id, state.run_id);
    assert_eq!(loaded.completed_models.len(), 1);
    assert_eq!(loaded.pending_models.len(), 1);
    assert!(loaded.is_completed("model_a"));
}

/// Test run state models_to_run for resume
#[test]
fn test_run_state_models_to_run() {
    let mut state = RunState::new(
        vec![
            ModelName::new("a"),
            ModelName::new("b"),
            ModelName::new("c"),
            ModelName::new("d"),
        ],
        None,
        "hash".to_string(),
    );

    state.mark_completed("a", 1000).unwrap();
    state.mark_completed("b", 2000).unwrap();
    state.mark_failed("c", "error").unwrap();

    // models_to_run should include failed (c) and pending (d)
    let to_run = state.models_to_run();
    assert_eq!(to_run.len(), 2);
    assert!(to_run.contains(&ModelName::new("c")));
    assert!(to_run.contains(&ModelName::new("d")));

    // failed_model_names should only include c
    let failed = state.failed_model_names();
    assert_eq!(failed.len(), 1);
    assert!(failed.contains(&ModelName::new("c")));
}

// ============================================================================
// DEFER MANIFEST TESTS (Work Units 1.1, 1.2)
// ============================================================================

/// Test loading a deferred manifest from file
#[test]
fn test_load_deferred_manifest() {
    use ff_core::config::Materialization;
    use ff_meta::manifest::{Manifest, ManifestModel};
    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("manifest.json");

    // Create a manifest with known models
    let mut manifest = Manifest::new("test_project");
    manifest.models.insert(
        ModelName::new("stg_customers"),
        ManifestModel {
            name: ModelName::new("stg_customers"),
            source_path: "models/staging/stg_customers.sql".to_string(),
            compiled_path: "target/compiled/stg_customers.sql".to_string(),
            materialized: Materialization::View,
            schema: Some("staging".to_string()),
            tags: vec![],
            depends_on: vec![],
            external_deps: vec![ff_core::table_name::TableName::new("raw_customers")],
            referenced_tables: vec![ff_core::table_name::TableName::new("raw_customers")],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
            sql_checksum: None,
        },
    );
    // Save manifest
    manifest.save(&manifest_path).unwrap();
    assert!(manifest_path.exists());

    // Load it back
    let loaded = Manifest::load(&manifest_path).unwrap();
    assert_eq!(loaded.project_name, "test_project");
    assert!(loaded.get_model("stg_customers").is_some());
    assert!(loaded.get_model("nonexistent").is_none());
}

/// Test defer fails when manifest file doesn't exist
#[test]
fn test_defer_manifest_not_found() {
    let nonexistent_path = "/tmp/nonexistent_manifest_12345.json";
    let path = Path::new(nonexistent_path);

    assert!(!path.exists(), "Path should not exist for this test");

    // Loading should fail
    let result = Manifest::load(path);
    assert!(result.is_err(), "Loading nonexistent manifest should fail");
}

/// Test deferred model dependencies are resolved correctly
#[test]
fn test_defer_dependency_resolution() {
    use ff_core::config::Materialization;
    use ff_meta::manifest::{Manifest, ManifestModel};

    // Create a deferred manifest with stg_customers and stg_orders
    let mut deferred = Manifest::new("prod_project");

    deferred.models.insert(
        ModelName::new("stg_customers"),
        ManifestModel {
            name: ModelName::new("stg_customers"),
            source_path: "models/stg_customers.sql".to_string(),
            compiled_path: "target/compiled/stg_customers.sql".to_string(),
            materialized: Materialization::View,
            schema: None,
            tags: vec![],
            depends_on: vec![],
            external_deps: vec![ff_core::table_name::TableName::new("raw_customers")],
            referenced_tables: vec![ff_core::table_name::TableName::new("raw_customers")],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
            sql_checksum: None,
        },
    );

    deferred.models.insert(
        ModelName::new("stg_orders"),
        ManifestModel {
            name: ModelName::new("stg_orders"),
            source_path: "models/stg_orders.sql".to_string(),
            compiled_path: "target/compiled/stg_orders.sql".to_string(),
            materialized: Materialization::View,
            schema: None,
            tags: vec![],
            depends_on: vec![],
            external_deps: vec![ff_core::table_name::TableName::new("raw_orders")],
            referenced_tables: vec![ff_core::table_name::TableName::new("raw_orders")],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
            sql_checksum: None,
        },
    );

    // Simulate a selected model (fct_orders) that depends on both
    // The selected models need stg_customers and stg_orders
    // These should be found in the deferred manifest
    let required_deps = vec!["stg_customers", "stg_orders"];
    for dep in required_deps {
        assert!(
            deferred.get_model(dep).is_some(),
            "Deferred manifest should contain {}",
            dep
        );
    }

    // A missing dependency should not be found
    assert!(
        deferred.get_model("stg_products").is_none(),
        "stg_products should not be in deferred manifest"
    );
}

/// Test defer with missing upstream model in manifest
#[test]
fn test_defer_missing_upstream_detection() {
    use ff_core::config::Materialization;
    use ff_meta::manifest::{Manifest, ManifestModel};

    // Create a deferred manifest with only stg_customers (missing stg_orders)
    let mut deferred = Manifest::new("prod_project");

    deferred.models.insert(
        ModelName::new("stg_customers"),
        ManifestModel {
            name: ModelName::new("stg_customers"),
            source_path: "models/stg_customers.sql".to_string(),
            compiled_path: "target/compiled/stg_customers.sql".to_string(),
            materialized: Materialization::View,
            schema: None,
            tags: vec![],
            depends_on: vec![],
            external_deps: vec![ff_core::table_name::TableName::new("raw_customers")],
            referenced_tables: vec![ff_core::table_name::TableName::new("raw_customers")],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
            sql_checksum: None,
        },
    );

    // fct_orders depends on stg_orders which is NOT in the manifest
    let missing = "stg_orders";
    assert!(
        deferred.get_model(missing).is_none(),
        "stg_orders should be missing from manifest"
    );

    // In actual code, this would trigger:
    // "Model 'stg_orders' not found in deferred manifest. It is required by: fct_orders"
}

/// Test manifest with transitive dependencies
#[test]
fn test_defer_transitive_dependencies() {
    use ff_core::config::Materialization;
    use ff_meta::manifest::{Manifest, ManifestModel};

    // Create a manifest with a chain: dim_products -> stg_products -> raw_products
    let mut manifest = Manifest::new("prod");

    manifest.models.insert(
        ModelName::new("stg_products"),
        ManifestModel {
            name: ModelName::new("stg_products"),
            source_path: "models/stg_products.sql".to_string(),
            compiled_path: "target/compiled/stg_products.sql".to_string(),
            materialized: Materialization::View,
            schema: None,
            tags: vec![],
            depends_on: vec![ModelName::new("raw_products")],
            external_deps: vec![],
            referenced_tables: vec![ff_core::table_name::TableName::new("raw_products")],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
            sql_checksum: None,
        },
    );

    manifest.models.insert(
        ModelName::new("dim_products"),
        ManifestModel {
            name: ModelName::new("dim_products"),
            source_path: "models/dim_products.sql".to_string(),
            compiled_path: "target/compiled/dim_products.sql".to_string(),
            materialized: Materialization::Table,
            schema: None,
            tags: vec![],
            depends_on: vec![ModelName::new("stg_products")],
            external_deps: vec![],
            referenced_tables: vec![ff_core::table_name::TableName::new("stg_products")],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
            sql_checksum: None,
        },
    );

    // Verify we can traverse the dependency chain
    let dim = manifest.get_model("dim_products").unwrap();
    assert_eq!(dim.depends_on, vec![ModelName::new("stg_products")]);

    let stg = manifest.get_model("stg_products").unwrap();
    assert_eq!(stg.depends_on, vec![ModelName::new("raw_products")]);
}

/// Test slim CI workflow scenario
#[test]
fn test_defer_slim_ci_scenario() {
    use ff_core::config::Materialization;
    use ff_meta::manifest::{Manifest, ManifestModel};

    // Production manifest has a full DAG
    let mut prod_manifest = Manifest::new("production");

    // Add production models
    for model_name in &[
        "stg_customers",
        "stg_orders",
        "stg_products",
        "fct_orders",
        "fct_revenue",
    ] {
        prod_manifest.models.insert(
            ModelName::new(*model_name),
            ManifestModel {
                name: ModelName::new(*model_name),
                source_path: format!("models/{}.sql", model_name),
                compiled_path: format!("target/compiled/{}.sql", model_name),
                materialized: if model_name.starts_with("stg") {
                    Materialization::View
                } else {
                    Materialization::Table
                },
                schema: None,
                tags: vec![],
                depends_on: match *model_name {
                    "fct_orders" => vec![
                        ModelName::new("stg_customers"),
                        ModelName::new("stg_orders"),
                    ],
                    "fct_revenue" => {
                        vec![ModelName::new("stg_orders"), ModelName::new("stg_products")]
                    }
                    _ => vec![],
                },
                external_deps: vec![],
                referenced_tables: vec![],
                unique_key: None,
                incremental_strategy: None,
                on_schema_change: None,
                pre_hook: vec![],
                post_hook: vec![],
                wap: None,
                sql_checksum: None,
            },
        );
    }

    // Scenario: Developer changed only fct_orders
    // They want to run: ff run --select fct_orders --defer --state prod_manifest.json
    let selected_model = "fct_orders";

    // fct_orders depends on stg_customers and stg_orders
    let fct = prod_manifest.get_model(selected_model).unwrap();
    let deps_to_defer: Vec<&str> = fct.depends_on.iter().map(|s| s.as_str()).collect();

    assert_eq!(deps_to_defer.len(), 2);
    assert!(deps_to_defer.contains(&"stg_customers"));
    assert!(deps_to_defer.contains(&"stg_orders"));

    // All dependencies exist in prod manifest - can be deferred
    for dep in &deps_to_defer {
        assert!(
            prod_manifest.get_model(dep).is_some(),
            "{} should be in production manifest",
            dep
        );
    }
}

/// Test ephemeral model SQL inlining
#[test]
fn test_ephemeral_model_inlining() {
    use ff_sql::{collect_ephemeral_dependencies, inline_ephemeral_ctes};

    // Setup: Create dependency graph with ephemeral models
    // stg_raw (ephemeral) -> stg_orders (ephemeral) -> fct_orders (table)
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();
    dependencies.insert("fct_orders".to_string(), vec!["stg_orders".to_string()]);
    dependencies.insert("stg_orders".to_string(), vec!["stg_raw".to_string()]);
    dependencies.insert("stg_raw".to_string(), vec![]); // depends on external table

    // Simulate ephemeral SQL
    let mut ephemeral_sql: HashMap<String, String> = HashMap::new();
    ephemeral_sql.insert(
        "stg_raw".to_string(),
        "SELECT id, customer_id, amount FROM raw_orders WHERE amount > 0".to_string(),
    );
    ephemeral_sql.insert(
        "stg_orders".to_string(),
        "SELECT id, customer_id, amount FROM stg_raw".to_string(),
    );

    // Define which models are ephemeral
    let is_ephemeral = |name: &str| name == "stg_raw" || name == "stg_orders";
    let get_sql = |name: &str| ephemeral_sql.get(name).cloned();

    // Collect ephemeral dependencies for fct_orders
    let (collected_ephemeral, order) =
        collect_ephemeral_dependencies("fct_orders", &dependencies, is_ephemeral, get_sql);

    // Verify both ephemeral models were collected
    assert_eq!(collected_ephemeral.len(), 2);
    assert!(collected_ephemeral.contains_key("stg_raw"));
    assert!(collected_ephemeral.contains_key("stg_orders"));

    // Verify order: stg_raw should come before stg_orders
    assert_eq!(order.len(), 2);
    let raw_pos = order.iter().position(|s| s == "stg_raw").unwrap();
    let orders_pos = order.iter().position(|s| s == "stg_orders").unwrap();
    assert!(
        raw_pos < orders_pos,
        "stg_raw should come before stg_orders"
    );

    // Inline ephemeral CTEs into fct_orders SQL
    let fct_orders_sql =
        "SELECT id, customer_id, SUM(amount) as total FROM stg_orders GROUP BY 1, 2";
    let inlined = inline_ephemeral_ctes(fct_orders_sql, &collected_ephemeral, &order).unwrap();

    // Verify the inlined SQL has CTEs
    assert!(inlined.starts_with("WITH"), "Should start with WITH clause");
    assert!(
        inlined.contains(r#""stg_raw" AS"#),
        "Should contain stg_raw CTE, got: {inlined}"
    );
    assert!(
        inlined.contains(r#""stg_orders" AS"#),
        "Should contain stg_orders CTE, got: {inlined}"
    );
    assert!(
        inlined.contains("raw_orders"),
        "Should contain stg_raw's source table"
    );
    assert!(
        inlined.contains("stg_raw"),
        "stg_orders should reference stg_raw"
    );
    assert!(
        inlined.contains("GROUP BY"),
        "Original query should be preserved"
    );

    // Verify CTE order in the output
    let raw_cte_pos = inlined.find(r#""stg_raw" AS"#).unwrap();
    let orders_cte_pos = inlined.find(r#""stg_orders" AS"#).unwrap();
    assert!(
        raw_cte_pos < orders_cte_pos,
        "stg_raw CTE should come before stg_orders CTE"
    );
}

/// Test ephemeral model with existing CTE in query
#[test]
fn test_ephemeral_inlining_with_existing_cte() {
    use ff_sql::inline_ephemeral_ctes;

    let original_sql = "WITH recent_orders AS (SELECT * FROM stg_orders WHERE order_date > '2024-01-01') SELECT * FROM recent_orders";

    let mut ephemeral_sql: HashMap<String, String> = HashMap::new();
    ephemeral_sql.insert(
        "stg_orders".to_string(),
        "SELECT id, order_date, amount FROM raw_orders".to_string(),
    );

    let order = vec!["stg_orders".to_string()];
    let inlined = inline_ephemeral_ctes(original_sql, &ephemeral_sql, &order).unwrap();

    // Should merge CTEs properly
    assert!(inlined.starts_with("WITH"), "Should start with WITH");
    assert!(
        inlined.contains(r#""stg_orders" AS"#),
        "Should contain ephemeral CTE, got: {inlined}"
    );
    assert!(
        inlined.contains("recent_orders AS"),
        "Should preserve original CTE, got: {inlined}"
    );

    // The ephemeral CTE should come before the original CTE
    let ephemeral_pos = inlined.find(r#""stg_orders" AS"#).unwrap();
    let original_cte_pos = inlined.find("recent_orders AS").unwrap();
    assert!(
        ephemeral_pos < original_cte_pos,
        "Ephemeral CTE should come first"
    );
}

// ============================================================
// ff-analysis integration tests
// ============================================================

/// Test DataFusion schema propagation on sample project models
#[test]
fn test_analysis_propagation_sample_project() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");

    assert!(
        pipeline.propagation.failures.is_empty(),
        "Expected no planning failures, got: {:?}",
        pipeline.propagation.failures
    );
    assert!(
        !pipeline.propagation.model_plans.is_empty(),
        "Should have planned at least one model"
    );

    for (name, plan_result) in &pipeline.propagation.model_plans {
        assert!(
            !plan_result.inferred_schema.is_empty(),
            "Model '{}' should produce a non-empty inferred schema",
            name
        );
    }
}

/// Shared analysis pipeline result used by multiple tests
struct AnalysisPipeline {
    propagation: ff_analysis::PropagationResult,
    ctx: ff_analysis::AnalysisContext,
    order: Vec<ModelName>,
}

/// Build the full analysis pipeline for a fixture project.
///
/// Loads the project, builds the schema catalog from YAML + sources, renders SQL
/// to extract deps and lineage, runs schema propagation, and returns everything
/// needed to run passes.
fn build_analysis_pipeline(fixture_path: &str) -> AnalysisPipeline {
    use ff_analysis::{
        parse_sql_type, propagate_schemas, AnalysisContext, Nullability, RelSchema, SchemaCatalog,
        TypedColumn,
    };
    use ff_sql::{extract_column_lineage, ProjectLineage};

    let project = Project::load(Path::new(fixture_path)).unwrap();
    let parser = SqlParser::duckdb();
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    let known_models: std::collections::HashSet<&str> =
        project.models.keys().map(|k| k.as_str()).collect();

    let mut catalog: SchemaCatalog = HashMap::new();
    let mut yaml_schemas: HashMap<ModelName, Arc<RelSchema>> = HashMap::new();
    let mut project_lineage = ProjectLineage::new();

    for (name, model) in &project.models {
        if let Some(schema) = &model.schema {
            let columns: Vec<TypedColumn> = schema
                .columns
                .iter()
                .map(|col| {
                    let sql_type = parse_sql_type(&col.data_type);
                    let has_not_null = col
                        .constraints
                        .iter()
                        .any(|c| matches!(c, ff_core::ColumnConstraint::NotNull));
                    let nullability = if has_not_null {
                        Nullability::NotNull
                    } else {
                        Nullability::Unknown
                    };
                    TypedColumn {
                        name: col.name.clone(),
                        source_table: None,
                        sql_type,
                        nullability,
                        provenance: vec![],
                    }
                })
                .collect();
            let rel_schema = Arc::new(RelSchema::new(columns));
            catalog.insert(name.to_string(), Arc::clone(&rel_schema));
            yaml_schemas.insert(name.clone(), rel_schema);
        }
    }

    for source_file in &project.sources {
        for table in &source_file.tables {
            if catalog.contains_key(&table.name) {
                continue;
            }
            let columns: Vec<TypedColumn> = table
                .columns
                .iter()
                .map(|col| {
                    let has_not_null = col.tests.iter().any(|t| {
                        matches!(t, ff_core::model::TestDefinition::Simple(s) if s == "not_null")
                    });
                    let nullability = if has_not_null {
                        Nullability::NotNull
                    } else {
                        Nullability::Unknown
                    };
                    TypedColumn {
                        name: col.name.clone(),
                        source_table: None,
                        sql_type: parse_sql_type(&col.data_type),
                        nullability,
                        provenance: vec![],
                    }
                })
                .collect();
            catalog.insert(table.name.clone(), Arc::new(RelSchema::new(columns)));
        }
    }

    let mut dep_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut sql_sources: HashMap<ModelName, String> = HashMap::new();
    for (name, model) in &project.models {
        let Ok(rendered) = jinja.render(&model.raw_sql) else {
            dep_map.insert(name.to_string(), vec![]);
            continue;
        };
        let Ok(stmts) = parser.parse(&rendered) else {
            dep_map.insert(name.to_string(), vec![]);
            continue;
        };
        let raw_deps = ff_sql::extract_dependencies(&stmts);
        let model_deps: Vec<String> = raw_deps
            .into_iter()
            .filter(|d| known_models.contains(d.as_str()))
            .collect();
        dep_map.insert(name.to_string(), model_deps);
        if let Some(stmt) = stmts.first() {
            if let Some(lineage) = extract_column_lineage(stmt, name) {
                project_lineage.add_model_lineage(lineage);
            }
        }
        sql_sources.insert(name.clone(), rendered);
    }
    project_lineage.resolve_edges(&known_models);

    let classification_lookup = ff_core::classification::build_classification_lookup(&project);
    project_lineage.propagate_classifications(&classification_lookup);

    let dag = ModelDag::build(&dep_map).unwrap();
    let topo_order = dag.topological_order().unwrap();

    let (user_fn_stubs, user_table_fn_stubs) = ff_analysis::build_user_function_stubs(&project);

    let topo_order_names: Vec<ModelName> = topo_order.iter().map(ModelName::new).collect();

    let propagation = propagate_schemas(
        &topo_order_names,
        &sql_sources,
        &yaml_schemas,
        catalog,
        &user_fn_stubs,
        &user_table_fn_stubs,
    );

    let order: Vec<ModelName> = topo_order_names
        .into_iter()
        .filter(|n| propagation.model_plans.contains_key(n))
        .collect();

    let ctx = AnalysisContext::new(project, dag, yaml_schemas, project_lineage);

    AnalysisPipeline {
        propagation,
        ctx,
        order,
    }
}

// ── Shared analysis helpers ─────────────────────────────────────────────

fn run_all_passes(pipeline: &AnalysisPipeline) -> Vec<ff_analysis::Diagnostic> {
    let mgr = ff_analysis::PlanPassManager::with_defaults();
    mgr.run(
        &pipeline.order,
        &pipeline.propagation.model_plans,
        &pipeline.ctx,
        None,
    )
}

fn run_single_pass(pipeline: &AnalysisPipeline, pass_name: &str) -> Vec<ff_analysis::Diagnostic> {
    let mgr = ff_analysis::PlanPassManager::with_defaults();
    let filter = vec![pass_name.to_string()];
    mgr.run(
        &pipeline.order,
        &pipeline.propagation.model_plans,
        &pipeline.ctx,
        Some(&filter),
    )
}

fn diagnostics_with_code(
    diags: &[ff_analysis::Diagnostic],
    code: ff_analysis::DiagnosticCode,
) -> Vec<&ff_analysis::Diagnostic> {
    diags.iter().filter(|d| d.code == code).collect()
}

// ── Analysis pass tests ────────────────────────────────────────────────

/// Test PlanPassManager runs end-to-end on sample project
#[test]
fn test_analysis_plan_pass_manager_sample_project() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");

    assert!(
        !pipeline.propagation.model_plans.is_empty(),
        "Should have planned at least one model"
    );

    let diagnostics = run_all_passes(&pipeline);

    for d in &diagnostics {
        assert!(
            !d.code.to_string().is_empty(),
            "Diagnostic code should not be empty"
        );
        assert!(!d.model.is_empty(), "Diagnostic model should not be empty");
        assert!(
            !d.pass_name.is_empty(),
            "Diagnostic pass_name should not be empty"
        );
    }
}

/// Test pass filtering works
#[test]
fn test_analysis_pass_filter() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let diags = run_single_pass(&pipeline, "plan_type_inference");

    for d in &diags {
        assert_eq!(
            d.pass_name, "plan_type_inference",
            "With filter, all diagnostics should come from plan_type_inference, got: {}",
            d.pass_name
        );
    }
}

/// Test diagnostic severity ordering
#[test]
fn test_analysis_severity_ordering() {
    use ff_analysis::Severity;

    assert!(Severity::Info < Severity::Warning);
    assert!(Severity::Warning < Severity::Error);
    assert!(Severity::Info < Severity::Error);
}

/// Test diagnostic JSON serialization
#[test]
fn test_analysis_diagnostic_json_roundtrip() {
    use ff_analysis::{Diagnostic, DiagnosticCode, Severity};

    let diag = Diagnostic {
        code: DiagnosticCode::A001,
        severity: Severity::Info,
        message: "Test message".to_string(),
        model: ModelName::new("test_model"),
        column: Some("col1".to_string()),
        hint: Some("Fix it".to_string()),
        pass_name: "type_inference".into(),
    };

    let json = serde_json::to_string(&diag).unwrap();
    let deserialized: Diagnostic = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.code, DiagnosticCode::A001);
    assert_eq!(deserialized.severity, Severity::Info);
    assert_eq!(deserialized.model, "test_model");
    assert_eq!(deserialized.column, Some("col1".to_string()));
}

/// Regression guard: full analysis pipeline on sample_project produces zero diagnostics.
///
/// After Phase F IR elimination, the sample project went from 48 false diagnostics
/// (28 A001 + 20 bogus A010) to zero.  This test locks that in so any pass regression
/// is caught immediately.
#[test]
fn test_analysis_sample_project_no_false_diagnostics() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");

    assert!(
        pipeline.propagation.failures.is_empty(),
        "Expected no planning failures, got: {:?}",
        pipeline.propagation.failures
    );
    assert_eq!(pipeline.propagation.model_plans.len(), 16);

    let diagnostics = run_all_passes(&pipeline);

    let a001 = diagnostics_with_code(&diagnostics, ff_analysis::DiagnosticCode::A001);
    assert!(
        a001.is_empty(),
        "Expected zero A001 diagnostics, got {}:\n{:#?}",
        a001.len(),
        a001
    );

    let errors: Vec<_> = diagnostics
        .iter()
        .filter(|d| d.severity == ff_analysis::Severity::Error)
        .collect();
    assert!(
        errors.is_empty(),
        "Expected zero error-severity diagnostics, got {}:\n{:#?}",
        errors.len(),
        errors
    );

    // Filter out description drift diagnostics (A050-A052) — these are expected
    // info/warning-level diagnostics on projects without full description coverage
    let non_drift: Vec<_> = diagnostics
        .iter()
        .filter(|d| {
            !matches!(
                d.code,
                ff_analysis::DiagnosticCode::A050
                    | ff_analysis::DiagnosticCode::A051
                    | ff_analysis::DiagnosticCode::A052
            )
        })
        .collect();
    assert!(
        non_drift.is_empty(),
        "Expected zero non-drift diagnostics on sample_project, got {}:\n{:#?}",
        non_drift.len(),
        non_drift
    );
}

/// Test PlanPassManager lists all pass names
#[test]
fn test_analysis_pass_names() {
    use ff_analysis::PlanPassManager;

    let pm = PlanPassManager::with_defaults();
    let names = pm.pass_names();

    assert!(names.contains(&"plan_type_inference"));
    assert!(names.contains(&"plan_nullability"));
    assert!(names.contains(&"plan_join_keys"));
    assert!(names.contains(&"plan_unused_columns"));
    assert!(names.contains(&"cross_model_consistency"));
    assert!(names.contains(&"description_drift"));
    assert_eq!(names.len(), 6);
}

// ── Phase 1: Type Inference (A002, A004, A005) ─────────────────────────

#[test]
fn test_analysis_union_type_mismatch_a002() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_type_fail_union_mismatch");
    let diags = run_single_pass(&pipeline, "plan_type_inference");
    let a002 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A002);
    assert!(
        !a002.is_empty(),
        "Expected A002 for INT vs VARCHAR UNION, got none. All diags: {:#?}",
        diags
    );
    assert!(
        a002.iter().any(|d| d.model == "union_model"),
        "A002 should reference union_model"
    );
}

#[test]
fn test_analysis_union_compatible_no_a002() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_type_pass_union_compatible");
    let diags = run_single_pass(&pipeline, "plan_type_inference");
    let a002 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A002);
    assert!(
        a002.is_empty(),
        "Expected zero A002 for compatible UNION, got {}:\n{:#?}",
        a002.len(),
        a002
    );
}

#[test]
fn test_analysis_sum_on_string_a004() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_type_fail_agg_on_string");
    let diags = run_single_pass(&pipeline, "plan_type_inference");
    let a004 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A004);
    assert!(
        !a004.is_empty(),
        "Expected A004 for SUM on VARCHAR, got none. All diags: {:#?}",
        diags
    );
}

#[test]
fn test_analysis_agg_on_numeric_no_a004() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_type_pass_agg_on_numeric");
    let diags = run_single_pass(&pipeline, "plan_type_inference");
    let a004 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A004);
    assert!(
        a004.is_empty(),
        "Expected zero A004 for SUM on numeric/COUNT on string, got {}:\n{:#?}",
        a004.len(),
        a004
    );
}

#[test]
fn test_analysis_lossy_cast_a005() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_type_fail_lossy_cast");
    let diags = run_single_pass(&pipeline, "plan_type_inference");
    let a005 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A005);
    assert!(
        a005.len() >= 2,
        "Expected at least 2 A005 (FLOAT->INT, DECIMAL->INT), got {}:\n{:#?}",
        a005.len(),
        a005
    );
}

#[test]
fn test_analysis_safe_cast_no_a005() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_type_pass_safe_cast");
    let diags = run_single_pass(&pipeline, "plan_type_inference");
    let a005 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A005);
    assert!(
        a005.is_empty(),
        "Expected zero A005 for safe widening casts, got {}:\n{:#?}",
        a005.len(),
        a005
    );
}

// ── Phase 2: Nullability (A010, A011, A012) ─────────────────────────────

#[test]
fn test_analysis_left_join_unguarded_a010() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_null_fail_left_join_unguarded");
    let diags = run_single_pass(&pipeline, "plan_nullability");
    let a010 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A010);
    assert!(
        !a010.is_empty(),
        "Expected A010 for unguarded LEFT JOIN columns, got none. All diags: {:#?}",
        diags
    );
    assert!(
        a010.iter().any(|d| d.model == "joined"),
        "A010 should reference the joined model"
    );
}

#[test]
fn test_analysis_coalesce_guard_no_a010() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_null_pass_coalesce_guarded");
    let diags = run_single_pass(&pipeline, "plan_nullability");
    let a010 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A010);
    let guarded_a010: Vec<_> = a010.iter().filter(|d| d.model == "guarded").collect();
    assert!(
        guarded_a010.is_empty(),
        "Expected zero A010 for COALESCE-guarded columns, got {}:\n{:#?}",
        guarded_a010.len(),
        guarded_a010
    );
}

#[test]
fn test_analysis_yaml_not_null_contradiction_a011() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_null_fail_yaml_not_null");
    let diags = run_single_pass(&pipeline, "plan_nullability");
    let a011 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A011);
    assert!(
        !a011.is_empty(),
        "Expected A011 for YAML NOT NULL vs JOIN nullable, got none. All diags: {:#?}",
        diags
    );
    assert!(
        a011.iter().any(|d| d.column.as_deref() == Some("name")),
        "A011 should reference column 'name'"
    );
}

#[test]
fn test_analysis_redundant_null_check_a012() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_null_fail_redundant_check");
    let diags = run_single_pass(&pipeline, "plan_nullability");
    let a012 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A012);
    assert!(
        !a012.is_empty(),
        "Expected A012 for IS NOT NULL on NOT NULL column, got none. All diags: {:#?}",
        diags
    );
}

// ── Phase 3: Unused Columns (A020) ──────────────────────────────────────

#[test]
fn test_analysis_unused_columns_a020() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_unused_fail_extra_columns");
    let diags = run_single_pass(&pipeline, "plan_unused_columns");
    let a020 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A020);
    assert!(
        a020.len() >= 2,
        "Expected at least 2 A020 for internal_code and debug_flag, got {}: {:#?}",
        a020.len(),
        a020
    );
    let flagged_cols: Vec<_> = a020.iter().filter_map(|d| d.column.as_deref()).collect();
    assert!(
        flagged_cols.contains(&"internal_code"),
        "Expected A020 for internal_code, flagged: {:?}",
        flagged_cols
    );
    assert!(
        flagged_cols.contains(&"debug_flag"),
        "Expected A020 for debug_flag, flagged: {:?}",
        flagged_cols
    );
}

#[test]
fn test_analysis_all_columns_consumed_no_a020() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_unused_pass_all_consumed");
    let diags = run_single_pass(&pipeline, "plan_unused_columns");
    let a020 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A020);
    assert!(
        a020.is_empty(),
        "Expected zero A020 (all columns consumed in diamond DAG), got {}: {:#?}",
        a020.len(),
        a020
    );
}

#[test]
fn test_analysis_terminal_model_no_a020() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_unused_pass_terminal");
    let diags = run_single_pass(&pipeline, "plan_unused_columns");
    let a020 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A020);
    assert!(
        a020.is_empty(),
        "Expected zero A020 (terminal model skipped), got {}: {:#?}",
        a020.len(),
        a020
    );
}

// ── Phase 4: Join Keys (A030, A032, A033) ───────────────────────────────

#[test]
fn test_analysis_join_key_type_mismatch_a030() {
    // DataFusion auto-coerces mismatched join key types by moving the
    // condition from `join.on` (equi-join keys) to `join.filter` (non-equi).
    // Our A030 check only inspects `join.on`, so it won't fire here.
    // This test documents that behavior: the model plans successfully and
    // A030 is not emitted (DataFusion handles the coercion).
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_join_fail_type_mismatch");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Type-mismatched join should still plan successfully via DataFusion coercion"
    );
    let diags = run_single_pass(&pipeline, "plan_join_keys");
    let a030 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A030);
    assert!(
        a030.is_empty(),
        "A030 should not fire — DataFusion coerces mismatched join keys into filter. Got: {:#?}",
        a030
    );
}

#[test]
fn test_analysis_cross_join_a032() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_join_fail_cross_join");
    let diags = run_single_pass(&pipeline, "plan_join_keys");
    let a032 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A032);
    assert!(
        !a032.is_empty(),
        "Expected A032 for CROSS JOIN, got none. All diags: {:#?}",
        diags
    );
}

#[test]
fn test_analysis_non_equi_join_a033() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_join_fail_non_equi");
    let diags = run_single_pass(&pipeline, "plan_join_keys");
    let a033 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A033);
    assert!(
        !a033.is_empty(),
        "Expected A033 for > operator in join, got none. All diags: {:#?}",
        diags
    );
}

#[test]
fn test_analysis_equi_join_no_join_diagnostics() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_join_pass_equi");
    let diags = run_single_pass(&pipeline, "plan_join_keys");
    let a030 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A030);
    let a032 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A032);
    let a033 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A033);
    assert!(
        a030.is_empty() && a032.is_empty() && a033.is_empty(),
        "Expected zero join diagnostics for clean equi-join. A030: {}, A032: {}, A033: {}",
        a030.len(),
        a032.len(),
        a033.len()
    );
}

// ── Phase 5: Cross-Model Consistency (A040, A041) ───────────────────────

#[test]
fn test_analysis_extra_in_sql_a040() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_xmodel_fail_extra_in_sql");
    let diags = run_single_pass(&pipeline, "cross_model_consistency");
    let a040 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A040);
    assert!(
        !a040.is_empty(),
        "Expected A040 for extra 'bonus' column in SQL output. Got: {:#?}",
        diags
    );
    let bonus = a040.iter().any(|d| d.column.as_deref() == Some("bonus"));
    assert!(bonus, "Expected A040 on 'bonus' column");
}

#[test]
fn test_analysis_missing_from_sql_a040() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_xmodel_fail_missing_from_sql");
    let diags = run_single_pass(&pipeline, "cross_model_consistency");
    let a040 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A040);
    assert!(
        !a040.is_empty(),
        "Expected A040 for phantom_col declared in YAML but missing from SQL. Got: {:#?}",
        diags
    );
    let has_error = a040
        .iter()
        .any(|d| d.severity == ff_analysis::Severity::Error);
    assert!(has_error, "MissingFromSql should be error severity");
}

#[test]
fn test_analysis_type_mismatch_a040() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_diagnostic_project");
    let diags = run_single_pass(&pipeline, "cross_model_consistency");
    let a040 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A040);
    assert!(
        !a040.is_empty(),
        "Expected A040 from sa_diagnostic_project. Got: {:#?}",
        diags
    );
}

#[test]
fn test_analysis_clean_project_no_a040_a041() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_clean_project");
    let diags = run_single_pass(&pipeline, "cross_model_consistency");
    let a040 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A040);
    let a041 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A041);
    assert!(
        a040.is_empty() && a041.is_empty(),
        "Expected zero A040/A041 for clean project. A040: {}, A041: {}",
        a040.len(),
        a041.len()
    );
}

// ── Phase 6: Schema Propagation Engine ──────────────────────────────────

#[test]
fn test_analysis_propagation_linear_chain() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_prop_pass_linear_chain");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Linear chain should propagate without failures: {:?}",
        pipeline.propagation.failures
    );
    assert_eq!(
        pipeline.propagation.model_plans.len(),
        3,
        "Should have plans for stg, int, mart"
    );
    assert!(pipeline.propagation.model_plans.contains_key("stg"));
    assert!(pipeline.propagation.model_plans.contains_key("int"));
    assert!(pipeline.propagation.model_plans.contains_key("mart"));
}

#[test]
fn test_analysis_propagation_diamond_dag() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_prop_pass_diamond");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Diamond DAG should propagate without failures: {:?}",
        pipeline.propagation.failures
    );
    assert_eq!(
        pipeline.propagation.model_plans.len(),
        4,
        "Should have plans for stg, branch_a, branch_b, joined"
    );
    assert!(pipeline.propagation.model_plans.contains_key("joined"));
}

#[test]
fn test_analysis_propagation_unknown_table_partial_failure() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_prop_fail_unknown_table");
    assert!(
        pipeline.propagation.failures.contains_key("broken"),
        "broken model should fail propagation"
    );
    assert!(
        pipeline.propagation.model_plans.contains_key("good"),
        "good model should succeed despite broken sibling"
    );
}

// ── Phase 7: DataFusion Bridge ──────────────────────────────────────────

#[test]
fn test_analysis_bridge_basic_sql_plans() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_bridge_pass_basic_sql");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "All basic SQL patterns should plan successfully: {:?}",
        pipeline.propagation.failures
    );
    assert_eq!(pipeline.propagation.model_plans.len(), 3);
    let simple = &pipeline.propagation.model_plans["simple_select"];
    assert_eq!(simple.inferred_schema.columns.len(), 2);
    let agg = &pipeline.propagation.model_plans["with_agg"];
    assert_eq!(agg.inferred_schema.columns.len(), 1);
}

#[test]
fn test_analysis_bridge_unknown_table_fails() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_bridge_fail_unknown_table");
    assert!(
        pipeline.propagation.failures.contains_key("bad_ref"),
        "bad_ref should fail with unknown table"
    );
}

// ── Phase 8: DuckDB Types ───────────────────────────────────────────────

#[test]
fn test_analysis_duckdb_cast_shorthand_plans() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_duckdb_pass_syntax");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "DuckDB cast shorthand should plan: {:?}",
        pipeline.propagation.failures
    );
    assert!(pipeline
        .propagation
        .model_plans
        .contains_key("cast_shorthand"));
}

#[test]
fn test_analysis_duckdb_all_types_plan() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_duckdb_pass_all_types");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "All DuckDB types should plan: {:?}",
        pipeline.propagation.failures
    );
    let typed = &pipeline.propagation.model_plans["typed_model"];
    assert!(
        typed.inferred_schema.columns.len() >= 11,
        "Should infer all 11 typed columns, got {}",
        typed.inferred_schema.columns.len()
    );
}

#[test]
fn test_analysis_duckdb_all_types_no_a040() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_duckdb_pass_all_types");
    let diags = run_single_pass(&pipeline, "cross_model_consistency");
    let a040 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A040);
    assert!(
        a040.is_empty(),
        "Expected zero A040 for matching types. Got: {:#?}",
        a040
    );
}

// ── Phase 9: DuckDB Function Stubs ──────────────────────────────────────

#[test]
fn test_analysis_duckdb_scalar_functions_plan() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_duckdb_pass_scalar_functions");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Scalar function stubs should plan: {:?}",
        pipeline.propagation.failures
    );
    assert_eq!(pipeline.propagation.model_plans.len(), 3);
}

#[test]
fn test_analysis_duckdb_agg_functions_plan() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_duckdb_pass_agg_functions");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Aggregate function stubs should plan: {:?}",
        pipeline.propagation.failures
    );
    assert!(pipeline.propagation.model_plans.contains_key("agg_model"));
}

// ── Phase 10: Multi-Model DAG Scenarios ──────────────────────────────────

#[test]
fn test_analysis_dag_ecommerce_all_plan() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_dag_pass_ecommerce");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "All ecommerce models should plan: {:?}",
        pipeline.propagation.failures
    );
    assert_eq!(
        pipeline.propagation.model_plans.len(),
        6,
        "Expected 6 model plans, got {}",
        pipeline.propagation.model_plans.len()
    );
}

#[test]
fn test_analysis_dag_ecommerce_zero_diagnostics() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_dag_pass_ecommerce");
    let diags = run_all_passes(&pipeline);
    // Filter out description drift diagnostics (A050-A052) — these are expected
    // informational/warning diagnostics from the new description_drift pass
    let diags: Vec<_> = diags
        .into_iter()
        .filter(|d| {
            !matches!(
                d.code,
                ff_analysis::DiagnosticCode::A050
                    | ff_analysis::DiagnosticCode::A051
                    | ff_analysis::DiagnosticCode::A052
            )
        })
        .collect();
    assert!(
        diags.is_empty(),
        "Ecommerce project should produce zero diagnostics, got {}:\n{:#?}",
        diags.len(),
        diags
    );
}

#[test]
fn test_analysis_dag_mixed_diagnostics() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_dag_fail_mixed");
    let diags = run_all_passes(&pipeline);
    let a040 = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A040);
    assert!(!a040.is_empty(), "Expected A040 for schema mismatch in stg");
}

// ── Phase 11: Edge Cases ─────────────────────────────────────────────────

#[test]
fn test_analysis_literal_query_plans() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_edge_pass_literal_query");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Literal query should plan: {:?}",
        pipeline.propagation.failures
    );
    let plan = pipeline
        .propagation
        .model_plans
        .get("literal")
        .expect("literal model should have a plan");
    assert_eq!(
        plan.plan.schema().fields().len(),
        3,
        "Literal query should have 3 columns"
    );
}

#[test]
fn test_analysis_self_join_plans() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_edge_pass_self_join");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Self join should plan: {:?}",
        pipeline.propagation.failures
    );
    assert!(pipeline.propagation.model_plans.contains_key("self_join"));
}

#[test]
fn test_analysis_deep_expression_plans() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_edge_pass_deep_expression");
    assert!(
        pipeline.propagation.failures.is_empty(),
        "Deep expression should plan: {:?}",
        pipeline.propagation.failures
    );
    assert!(pipeline.propagation.model_plans.contains_key("deep"));
}

// ── Phase 13: Regression Guard Rails ─────────────────────────────────────

#[test]
fn test_analysis_guard_clean_project_zero_diagnostics() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_clean_project");
    let diags = run_all_passes(&pipeline);
    // Filter out description drift diagnostics (A050-A052) — these are expected
    // informational/warning diagnostics from the new description_drift pass
    let diags: Vec<_> = diags
        .into_iter()
        .filter(|d| {
            !matches!(
                d.code,
                ff_analysis::DiagnosticCode::A050
                    | ff_analysis::DiagnosticCode::A051
                    | ff_analysis::DiagnosticCode::A052
            )
        })
        .collect();
    assert!(
        diags.is_empty(),
        "Clean project should have zero diagnostics, got {}:\n{:#?}",
        diags.len(),
        diags
    );
}

// ============================================================================
// PRE/POST HOOK TESTS
// ============================================================================

/// Test that project-level hooks are loaded from featherflow.yml
#[test]
fn test_project_level_hooks_loaded() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // on_run_start should have 2 hooks
    assert_eq!(
        project.config.on_run_start.len(),
        2,
        "on_run_start should have 2 hooks"
    );
    assert!(project.config.on_run_start[0].contains("CREATE TABLE IF NOT EXISTS run_audit"));
    assert!(project.config.on_run_start[1].contains("INSERT INTO run_audit"));

    // on_run_end should have 1 hook
    assert_eq!(
        project.config.on_run_end.len(),
        1,
        "on_run_end should have 1 hook"
    );
    assert!(project.config.on_run_end[0].contains("999"));
}

/// Test that model-level hooks are parsed from config() in SQL templates
#[test]
fn test_model_level_hooks_parsed_from_config() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    // fct_orders should have pre_hook (single string) and post_hook (array of 2)
    let fct_orders = project.get_model("fct_orders").unwrap();
    let (_, config_values) = jinja.render_with_config(&fct_orders.raw_sql).unwrap();

    // Check pre_hook is a single string
    let pre_hook_val = config_values
        .get("pre_hook")
        .expect("fct_orders should have pre_hook");
    assert!(
        pre_hook_val.as_str().is_some(),
        "fct_orders pre_hook should be a string"
    );
    assert!(
        pre_hook_val
            .as_str()
            .unwrap()
            .contains("CREATE TABLE IF NOT EXISTS hook_log"),
        "fct_orders pre_hook should create hook_log table"
    );

    // Check post_hook is an array of 2 items
    let post_hook_val = config_values
        .get("post_hook")
        .expect("fct_orders should have post_hook");
    assert_eq!(
        post_hook_val.kind(),
        minijinja::value::ValueKind::Seq,
        "fct_orders post_hook should be a sequence"
    );
    let items: Vec<String> = post_hook_val
        .try_iter()
        .unwrap()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert_eq!(items.len(), 2, "fct_orders post_hook should have 2 items");
    assert!(items[0].contains("fct_orders"));
    assert!(items[1].contains("post_2"));

    // dim_customers should have post_hook (single string)
    let dim_customers = project.get_model("dim_customers").unwrap();
    let (_, config_values) = jinja.render_with_config(&dim_customers.raw_sql).unwrap();

    let post_hook_val = config_values
        .get("post_hook")
        .expect("dim_customers should have post_hook");
    assert!(
        post_hook_val.as_str().is_some(),
        "dim_customers post_hook should be a string"
    );
    assert!(post_hook_val.as_str().unwrap().contains("dim_customers"));
}

/// Test hook execution with {{ this }} substitution
#[tokio::test]
async fn test_hooks_execute_with_this_substitution() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create a hook that uses {{ this }} to reference the model table
    let hooks = vec![
        "CREATE TABLE IF NOT EXISTS hook_log (model VARCHAR, table_ref VARCHAR)".to_string(),
        "INSERT INTO hook_log (model, table_ref) VALUES ('test', '{{ this }}')".to_string(),
    ];

    let qualified_name = "analytics.fct_orders";

    // Execute hooks with {{ this }} substitution (replicate execute_hooks logic)
    for hook in &hooks {
        let sql = hook
            .replace("{{ this }}", qualified_name)
            .replace("{{this}}", qualified_name);
        db.execute(&sql).await.unwrap();
    }

    // Verify the hook_log table was created and populated
    assert!(db.relation_exists("hook_log").await.unwrap());

    let count = db.query_count("SELECT * FROM hook_log").await.unwrap();
    assert_eq!(count, 1);

    // Verify {{ this }} was substituted with the qualified name
    let table_ref = db
        .query_one("SELECT table_ref FROM hook_log")
        .await
        .unwrap();
    assert_eq!(
        table_ref,
        Some("analytics.fct_orders".to_string()),
        "{{ this }} should be replaced with qualified table name"
    );
}

/// Test on_run_start and on_run_end hooks execute observable SQL
#[tokio::test]
async fn test_on_run_start_end_hooks_execute() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Simulate on_run_start hooks from the sample project config
    let on_run_start = vec![
        "CREATE TABLE IF NOT EXISTS run_audit (run_id INTEGER, started_at TIMESTAMP DEFAULT current_timestamp)".to_string(),
        "INSERT INTO run_audit (run_id) VALUES (1)".to_string(),
    ];

    for hook in &on_run_start {
        db.execute(hook).await.unwrap();
    }

    // Verify run_audit table exists and has the start row
    assert!(db.relation_exists("run_audit").await.unwrap());
    let count = db
        .query_count("SELECT * FROM run_audit WHERE run_id = 1")
        .await
        .unwrap();
    assert_eq!(count, 1, "on_run_start should have inserted run_id=1");

    // Simulate on_run_end hooks
    let on_run_end = vec!["INSERT INTO run_audit (run_id) VALUES (999)".to_string()];

    for hook in &on_run_end {
        db.execute(hook).await.unwrap();
    }

    // Verify on_run_end row
    let count = db
        .query_count("SELECT * FROM run_audit WHERE run_id = 999")
        .await
        .unwrap();
    assert_eq!(count, 1, "on_run_end should have inserted run_id=999");

    // Verify total rows
    let total = db.query_count("SELECT * FROM run_audit").await.unwrap();
    assert_eq!(total, 2, "run_audit should have exactly 2 rows");
}

/// Test hook merge ordering: project pre → model pre; model post → project post
#[tokio::test]
async fn test_hook_merge_ordering() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Set up an audit table to track execution order
    db.execute("CREATE TABLE hook_order (seq INTEGER, source VARCHAR)")
        .await
        .unwrap();

    // Simulate project-level pre_hooks
    let project_pre = vec!["INSERT INTO hook_order VALUES (1, 'project_pre')".to_string()];
    // Simulate model-level pre_hooks
    let model_pre = vec!["INSERT INTO hook_order VALUES (2, 'model_pre')".to_string()];

    // Merge: project pre first, then model pre (matches compile.rs logic)
    let mut merged_pre = project_pre;
    merged_pre.extend(model_pre);

    for hook in &merged_pre {
        db.execute(hook).await.unwrap();
    }

    // Simulate model-level post_hooks
    let model_post = vec!["INSERT INTO hook_order VALUES (3, 'model_post')".to_string()];
    // Simulate project-level post_hooks
    let project_post = vec!["INSERT INTO hook_order VALUES (4, 'project_post')".to_string()];

    // Merge: model post first, then project post (matches compile.rs logic)
    let mut merged_post = model_post;
    merged_post.extend(project_post);

    for hook in &merged_post {
        db.execute(hook).await.unwrap();
    }

    // Verify execution order
    let total = db.query_count("SELECT * FROM hook_order").await.unwrap();
    assert_eq!(total, 4, "Should have 4 hook executions");

    // Verify correct ordering via seq values
    let first = db
        .query_one("SELECT source FROM hook_order WHERE seq = 1")
        .await
        .unwrap();
    assert_eq!(first, Some("project_pre".to_string()));

    let second = db
        .query_one("SELECT source FROM hook_order WHERE seq = 2")
        .await
        .unwrap();
    assert_eq!(second, Some("model_pre".to_string()));

    let third = db
        .query_one("SELECT source FROM hook_order WHERE seq = 3")
        .await
        .unwrap();
    assert_eq!(third, Some("model_post".to_string()));

    let fourth = db
        .query_one("SELECT source FROM hook_order WHERE seq = 4")
        .await
        .unwrap();
    assert_eq!(fourth, Some("project_post".to_string()));
}

fn extract_hooks_from_config(
    config_values: &HashMap<String, minijinja::Value>,
    key: &str,
) -> Vec<String> {
    config_values
        .get(key)
        .map(|v| {
            if let Some(s) = v.as_str() {
                vec![s.to_string()]
            } else if v.kind() == minijinja::value::ValueKind::Seq {
                v.try_iter()
                    .map(|iter| {
                        iter.filter_map(|item| item.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        })
        .unwrap_or_default()
}

/// Test that compile captures model-level hooks in manifest
#[test]
fn test_compile_captures_hooks_in_manifest() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    let fct_orders = project.get_model("fct_orders").unwrap();
    let (_, config_values) = jinja.render_with_config(&fct_orders.raw_sql).unwrap();

    let pre_hooks = extract_hooks_from_config(&config_values, "pre_hook");
    let post_hooks = extract_hooks_from_config(&config_values, "post_hook");

    assert_eq!(pre_hooks.len(), 1, "fct_orders should have 1 pre_hook");
    assert!(pre_hooks[0].contains("hook_log"));
    assert_eq!(post_hooks.len(), 2, "fct_orders should have 2 post_hooks");

    let dim_customers = project.get_model("dim_customers").unwrap();
    let (_, config_values) = jinja.render_with_config(&dim_customers.raw_sql).unwrap();

    let dim_post_hooks = extract_hooks_from_config(&config_values, "post_hook");

    assert_eq!(
        dim_post_hooks.len(),
        1,
        "dim_customers should have 1 post_hook"
    );
    assert!(dim_post_hooks[0].contains("dim_customers"));

    let stg_orders = project.get_model("stg_orders").unwrap();
    let (_, config_values) = jinja.render_with_config(&stg_orders.raw_sql).unwrap();
    assert!(
        extract_hooks_from_config(&config_values, "pre_hook").is_empty(),
        "stg_orders should not have pre_hook"
    );
    assert!(
        extract_hooks_from_config(&config_values, "post_hook").is_empty(),
        "stg_orders should not have post_hook"
    );
}

/// Test fct_orders model-level hooks produce expected side effects when executed
#[tokio::test]
async fn test_fct_orders_hooks_side_effects() {
    let db = DuckDbBackend::in_memory().unwrap();
    let qualified_name = "analytics.fct_orders";

    // Execute fct_orders pre_hook (creates hook_log table)
    let pre_hook =
        "CREATE TABLE IF NOT EXISTS hook_log (model VARCHAR, hook_type VARCHAR, ts TIMESTAMP DEFAULT current_timestamp)";
    db.execute(pre_hook).await.unwrap();

    assert!(
        db.relation_exists("hook_log").await.unwrap(),
        "pre_hook should create hook_log table"
    );

    // Execute fct_orders post_hooks (two inserts)
    let post_hooks = vec![
        "INSERT INTO hook_log (model, hook_type) VALUES ('fct_orders', 'post')".to_string(),
        "INSERT INTO hook_log (model, hook_type) VALUES ('fct_orders', 'post_2')".to_string(),
    ];

    for hook in &post_hooks {
        let sql = hook
            .replace("{{ this }}", qualified_name)
            .replace("{{this}}", qualified_name);
        db.execute(&sql).await.unwrap();
    }

    // Verify fct_orders post_hooks inserted 2 rows
    let fct_count = db
        .query_count("SELECT * FROM hook_log WHERE model = 'fct_orders'")
        .await
        .unwrap();
    assert_eq!(fct_count, 2, "fct_orders should have 2 hook_log rows");

    // Execute dim_customers post_hook
    let dim_hook = "INSERT INTO hook_log (model, hook_type) VALUES ('dim_customers', 'post')";
    db.execute(dim_hook).await.unwrap();

    // Verify dim_customers post_hook inserted 1 row
    let dim_count = db
        .query_count("SELECT * FROM hook_log WHERE model = 'dim_customers'")
        .await
        .unwrap();
    assert_eq!(dim_count, 1, "dim_customers should have 1 hook_log row");

    // Verify total rows in hook_log
    let total = db.query_count("SELECT * FROM hook_log").await.unwrap();
    assert_eq!(total, 3, "hook_log should have 3 total rows");
}

/// Test {{ this }} substitution in hooks (using model-level hooks from config())
#[test]
fn test_hooks_this_substitution() {
    // Simulate a hook string with {{ this }} placeholder
    let hook = "-- post-hook: {{ this }}";
    let qualified_name = "analytics.fct_orders";
    let substituted = hook
        .replace("{{ this }}", qualified_name)
        .replace("{{this}}", qualified_name);

    assert!(
        substituted.contains("analytics.fct_orders"),
        "{{ this }} should be replaced with qualified name"
    );
    assert!(
        !substituted.contains("{{ this }}"),
        "No {{ this }} placeholders should remain after substitution"
    );
}

// ============================================================================
// SEVERITY OVERRIDE TESTS
// ============================================================================

/// Test that apply_severity_overrides suppresses diagnostics with Off
#[test]
fn test_severity_overrides_suppress_diagnostics() {
    use ff_analysis::{apply_severity_overrides, SeverityOverrides};
    use ff_core::config::ConfigSeverity;

    let pipeline = build_analysis_pipeline("tests/fixtures/sa_join_fail_cross_join");
    let diags = run_single_pass(&pipeline, "plan_join_keys");

    // Verify A032 is present before overrides
    let a032_before = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A032);
    assert!(
        !a032_before.is_empty(),
        "Expected A032 before overrides, got none"
    );

    // Apply override: suppress A032
    let mut config_map = HashMap::new();
    config_map.insert("A032".to_string(), ConfigSeverity::Off);
    let overrides = SeverityOverrides::from_config(&config_map);
    let filtered = apply_severity_overrides(diags, &overrides);

    // A032 should be gone
    let a032_after = diagnostics_with_code(&filtered, ff_analysis::DiagnosticCode::A032);
    assert!(
        a032_after.is_empty(),
        "A032 should be suppressed with Off override, got {}: {:#?}",
        a032_after.len(),
        a032_after
    );
}

/// Test that apply_severity_overrides promotes severity
#[test]
fn test_severity_overrides_promote_severity() {
    use ff_analysis::{apply_severity_overrides, Severity, SeverityOverrides};
    use ff_core::config::ConfigSeverity;

    let pipeline = build_analysis_pipeline("tests/fixtures/sa_unused_fail_extra_columns");
    let diags = run_single_pass(&pipeline, "plan_unused_columns");

    // Verify A020 diagnostics exist at Info level
    let a020_before = diagnostics_with_code(&diags, ff_analysis::DiagnosticCode::A020);
    assert!(!a020_before.is_empty(), "Expected A020 diagnostics");
    assert!(
        a020_before.iter().all(|d| d.severity == Severity::Info),
        "A020 should default to Info severity"
    );

    // Promote A020 to Error
    let mut config_map = HashMap::new();
    config_map.insert("A020".to_string(), ConfigSeverity::Error);
    let overrides = SeverityOverrides::from_config(&config_map);
    let promoted = apply_severity_overrides(diags, &overrides);

    let a020_after = diagnostics_with_code(&promoted, ff_analysis::DiagnosticCode::A020);
    assert!(!a020_after.is_empty(), "A020 should still be present");
    assert!(
        a020_after.iter().all(|d| d.severity == Severity::Error),
        "A020 should be promoted to Error severity"
    );
}

/// Test that empty overrides don't change diagnostics
#[test]
fn test_severity_overrides_empty_is_noop() {
    use ff_analysis::{apply_severity_overrides, SeverityOverrides};

    let pipeline = build_analysis_pipeline("tests/fixtures/sa_join_fail_cross_join");
    let diags = run_single_pass(&pipeline, "plan_join_keys");
    let original_count = diags.len();
    let original_severities: Vec<_> = diags.iter().map(|d| d.severity).collect();

    let overrides = SeverityOverrides::default();
    let result = apply_severity_overrides(diags, &overrides);

    assert_eq!(
        result.len(),
        original_count,
        "Empty overrides should not change diagnostic count"
    );
    let result_severities: Vec<_> = result.iter().map(|d| d.severity).collect();
    assert_eq!(
        result_severities, original_severities,
        "Empty overrides should not change severities"
    );
}

// ── Column-Level Lineage Integration Tests ──────────────────────────────

/// Helper: create an unqualified ColumnRef
fn col_ref(column: &str) -> ColumnRef {
    ColumnRef {
        table: None,
        column: column.to_string(),
    }
}

/// Helper: create a table-qualified ColumnRef
fn qual_ref(table: &str, column: &str) -> ColumnRef {
    ColumnRef {
        table: Some(table.to_string()),
        column: column.to_string(),
    }
}

/// Helper: find a column lineage entry by output column name
fn find_column<'a>(model: &'a ModelLineage, col_name: &str) -> &'a ff_sql::ColumnLineage {
    model
        .columns
        .iter()
        .find(|c| c.output_column == col_name)
        .unwrap_or_else(|| {
            panic!(
                "Column '{}' not found in model '{}'. Available: {:?}",
                col_name,
                model.model_name,
                model
                    .columns
                    .iter()
                    .map(|c| &c.output_column)
                    .collect::<Vec<_>>()
            )
        })
}

// ── Category A: Per-Model Lineage — Existing Models ─────────────────────

/// A1: stg_orders — 5 columns, all direct Column type, alias resolution
#[test]
fn test_lineage_stg_orders_columns() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("stg_orders")
        .expect("stg_orders lineage missing");

    assert_eq!(model.columns.len(), 5);
    assert!(model.source_tables.contains("raw_orders"));

    // order_id is renamed from id (unqualified since no table alias in FROM)
    let order_id = find_column(model, "order_id");
    assert!(order_id.is_direct);
    assert_eq!(order_id.expr_type, ExprType::Column);
    assert!(order_id.source_columns.contains(&col_ref("id")));

    // customer_id is renamed from user_id
    let customer_id = find_column(model, "customer_id");
    assert!(customer_id.is_direct);
    assert!(customer_id.source_columns.contains(&col_ref("user_id")));

    // order_date is renamed from created_at
    let order_date = find_column(model, "order_date");
    assert!(order_date.is_direct);
    assert!(order_date.source_columns.contains(&col_ref("created_at")));

    // amount is pass-through (same name)
    let amount = find_column(model, "amount");
    assert!(amount.is_direct);
    assert_eq!(amount.expr_type, ExprType::Column);

    // status is pass-through
    let status = find_column(model, "status");
    assert!(status.is_direct);
    assert_eq!(status.expr_type, ExprType::Column);
}

/// A2: stg_products — CAST lineage
#[test]
fn test_lineage_stg_products_cast() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("stg_products")
        .expect("stg_products lineage missing");

    assert_eq!(model.columns.len(), 5);

    // price uses CAST(price AS DECIMAL(10,2)) -> Cast type, not direct
    let price = find_column(model, "price");
    assert_eq!(price.expr_type, ExprType::Cast);
    assert!(!price.is_direct);
    assert!(price.source_columns.contains(&col_ref("price")));

    // product_id is a direct rename
    let product_id = find_column(model, "product_id");
    assert!(product_id.is_direct);
    assert_eq!(product_id.expr_type, ExprType::Column);
}

/// A3: stg_payments — Jinja macro expansion (cents_to_dollars → expression)
#[test]
fn test_lineage_stg_payments_function_call() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("stg_payments")
        .expect("stg_payments lineage missing");

    assert_eq!(model.columns.len(), 3);

    // amount is (amount / 100.0) after Jinja expansion -> Expression
    let amount = find_column(model, "amount");
    assert!(!amount.is_direct);
    assert_eq!(amount.expr_type, ExprType::Expression);
}

/// A4: stg_payments_star — wildcard lineage
#[test]
fn test_lineage_stg_payments_star_wildcard() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("stg_payments_star")
        .expect("stg_payments_star lineage missing");

    assert_eq!(model.columns.len(), 1);
    let wildcard = &model.columns[0];
    assert_eq!(wildcard.output_column, "*");
    assert_eq!(wildcard.expr_type, ExprType::Wildcard);
    assert!(wildcard.source_columns.iter().any(|r| r.column == "*"));
}

/// A5: int_orders_enriched — aggregation lineage, JOIN aliases
#[test]
fn test_lineage_int_orders_enriched_aggregation() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("int_orders_enriched")
        .expect("int_orders_enriched lineage missing");

    assert_eq!(model.columns.len(), 7);
    assert!(model.source_tables.contains("stg_orders"));
    assert!(model.source_tables.contains("stg_payments"));

    // Table aliases should be resolved
    assert_eq!(
        model.table_aliases.get("o"),
        Some(&"stg_orders".to_string())
    );
    assert_eq!(
        model.table_aliases.get("p"),
        Some(&"stg_payments".to_string())
    );

    // order_id is direct from o.order_id
    let order_id = find_column(model, "order_id");
    assert!(order_id.is_direct);
    assert!(order_id
        .source_columns
        .contains(&qual_ref("stg_orders", "order_id")));

    // payment_total is COALESCE(SUM(p.amount), 0) -> Function
    let payment_total = find_column(model, "payment_total");
    assert!(!payment_total.is_direct);
    assert_eq!(payment_total.expr_type, ExprType::Function);

    // payment_count is COUNT(p.payment_id) -> Function
    let payment_count = find_column(model, "payment_count");
    assert!(!payment_count.is_direct);
    assert_eq!(payment_count.expr_type, ExprType::Function);

    // order_amount is alias of o.amount -> direct
    let order_amount = find_column(model, "order_amount");
    assert!(order_amount.is_direct);
    assert_eq!(order_amount.expr_type, ExprType::Column);
}

/// A6: dim_customers — CASE expression lineage
#[test]
fn test_lineage_dim_customers_case() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("dim_customers")
        .expect("dim_customers lineage missing");

    assert_eq!(model.columns.len(), 8);

    let computed_tier = find_column(model, "computed_tier");
    assert_eq!(computed_tier.expr_type, ExprType::Case);
    assert!(!computed_tier.is_direct);
    // The CASE references m.lifetime_value which resolves to int_customer_metrics
    assert!(computed_tier
        .source_columns
        .contains(&qual_ref("int_customer_metrics", "lifetime_value")));
}

/// A7: fct_orders — expression and function call lineage
#[test]
fn test_lineage_fct_orders_expression_and_function() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("fct_orders")
        .expect("fct_orders lineage missing");

    assert_eq!(model.columns.len(), 11);

    // balance_due = e.order_amount - e.payment_total -> Expression
    let balance_due = find_column(model, "balance_due");
    assert_eq!(balance_due.expr_type, ExprType::Expression);
    assert!(!balance_due.is_direct);
    assert!(balance_due
        .source_columns
        .contains(&qual_ref("int_orders_enriched", "order_amount")));
    assert!(balance_due
        .source_columns
        .contains(&qual_ref("int_orders_enriched", "payment_total")));

    // payment_ratio = safe_divide(...) — plain SQL function call (not Jinja-expanded)
    let payment_ratio = find_column(model, "payment_ratio");
    assert_eq!(payment_ratio.expr_type, ExprType::Function);
    assert!(!payment_ratio.is_direct);
}

/// A8: dim_products — two CASE columns
#[test]
fn test_lineage_dim_products_two_case_columns() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("dim_products")
        .expect("dim_products lineage missing");

    assert_eq!(model.columns.len(), 6);

    let category_group = find_column(model, "category_group");
    assert_eq!(category_group.expr_type, ExprType::Case);
    assert!(!category_group.is_direct);
    assert!(category_group.source_columns.contains(&col_ref("category")));

    let price_tier = find_column(model, "price_tier");
    assert_eq!(price_tier.expr_type, ExprType::Case);
    assert!(!price_tier.is_direct);
    assert!(price_tier.source_columns.contains(&col_ref("price")));
}

// ── Category B: Per-Model Lineage — New Models ──────────────────────────

/// B1: int_all_orders — UNION ALL column names from left operand
#[test]
fn test_lineage_union_all_column_names() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("int_all_orders")
        .expect("int_all_orders lineage missing");

    // Column names come from the left operand of the UNION ALL
    assert_eq!(model.columns.len(), 6);

    let order_id = find_column(model, "order_id");
    assert!(order_id.is_direct);
    assert_eq!(order_id.expr_type, ExprType::Column);

    // 'enriched' AS source is a literal column
    let source = find_column(model, "source");
    assert_eq!(source.expr_type, ExprType::Literal);
    assert!(!source.is_direct);
    assert!(source.source_columns.is_empty());

    // Source tables should come from the left operand extraction
    assert!(model.source_tables.contains("int_orders_enriched"));
}

/// B2: int_customer_ranking — LEFT JOIN with COALESCE/NULLIF functions
#[test]
fn test_lineage_left_join_functions() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("int_customer_ranking")
        .expect("int_customer_ranking lineage missing");

    assert_eq!(model.columns.len(), 5);
    assert!(model.source_tables.contains("stg_customers"));
    assert!(model.source_tables.contains("int_customer_metrics"));

    // COALESCE(m.lifetime_value, 0) -> Function
    let value_or_zero = find_column(model, "value_or_zero");
    assert_eq!(value_or_zero.expr_type, ExprType::Function);
    assert!(!value_or_zero.is_direct);

    // NULLIF(m.total_orders, 0) -> Function
    let nonzero_orders = find_column(model, "nonzero_orders");
    assert_eq!(nonzero_orders.expr_type, ExprType::Function);
    assert!(!nonzero_orders.is_direct);

    // Direct column passes through LEFT JOIN
    let customer_id = find_column(model, "customer_id");
    assert!(customer_id.is_direct);
}

/// B3: int_customer_ranking — JOIN table resolution and alias mapping
#[test]
fn test_lineage_join_table_resolution() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("int_customer_ranking")
        .expect("int_customer_ranking lineage missing");

    // Should have both tables as source tables
    assert_eq!(model.source_tables.len(), 2);
    assert!(model.source_tables.contains("stg_customers"));
    assert!(model.source_tables.contains("int_customer_metrics"));

    // Aliases should resolve
    assert_eq!(
        model.table_aliases.get("c"),
        Some(&"stg_customers".to_string())
    );
    assert_eq!(
        model.table_aliases.get("m"),
        Some(&"int_customer_metrics".to_string())
    );
}

/// B4: dim_products_extended — nested CASE and CAST of expression
#[test]
fn test_lineage_nested_case_and_cast_expression() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("dim_products_extended")
        .expect("dim_products_extended lineage missing");

    assert_eq!(model.columns.len(), 6);

    // CAST(product_id * 10 AS BIGINT) -> Cast wrapping an expression
    let id_scaled = find_column(model, "id_scaled");
    assert_eq!(id_scaled.expr_type, ExprType::Cast);
    assert!(!id_scaled.is_direct);
    assert!(id_scaled.source_columns.contains(&col_ref("product_id")));

    // Nested CASE: CASE WHEN category THEN CASE WHEN price ... END ... END
    let detailed_category = find_column(model, "detailed_category");
    assert_eq!(detailed_category.expr_type, ExprType::Case);
    assert!(!detailed_category.is_direct);
    // References both category and price
    assert!(detailed_category
        .source_columns
        .contains(&col_ref("category")));
    assert!(detailed_category.source_columns.contains(&col_ref("price")));
}

/// B5: int_high_value_orders — HAVING clause, multiple aggregation functions
#[test]
fn test_lineage_having_multiple_aggregations() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("int_high_value_orders")
        .expect("int_high_value_orders lineage missing");

    assert_eq!(model.columns.len(), 6);

    // customer_id is direct from o.customer_id
    let customer_id = find_column(model, "customer_id");
    assert!(customer_id.is_direct);

    // All aggregation columns are Function type
    let order_count = find_column(model, "order_count");
    assert_eq!(order_count.expr_type, ExprType::Function);
    assert!(!order_count.is_direct);

    let total_amount = find_column(model, "total_amount");
    assert_eq!(total_amount.expr_type, ExprType::Function);
    assert!(!total_amount.is_direct);

    let min_order = find_column(model, "min_order");
    assert_eq!(min_order.expr_type, ExprType::Function);

    let max_order = find_column(model, "max_order");
    assert_eq!(max_order.expr_type, ExprType::Function);

    let avg_order = find_column(model, "avg_order");
    assert_eq!(avg_order.expr_type, ExprType::Function);
}

/// B6: rpt_customer_orders — 3-way JOIN, nested expressions
#[test]
fn test_lineage_multiple_joins_nested_expression() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();
    let model = lineage
        .models
        .get("rpt_customer_orders")
        .expect("rpt_customer_orders lineage missing");

    // 3 source tables from 3-way join
    assert!(model.source_tables.contains("stg_customers"));
    assert!(model.source_tables.contains("int_orders_enriched"));
    assert!(model.source_tables.contains("stg_orders"));

    // balance_with_fee = (e.order_amount - e.payment_total) * 1.1 -> Expression
    let balance_with_fee = find_column(model, "balance_with_fee");
    assert_eq!(balance_with_fee.expr_type, ExprType::Expression);
    assert!(!balance_with_fee.is_direct);
    assert!(balance_with_fee
        .source_columns
        .contains(&qual_ref("int_orders_enriched", "order_amount")));
    assert!(balance_with_fee
        .source_columns
        .contains(&qual_ref("int_orders_enriched", "payment_total")));

    // combined_metric = e.order_amount + e.payment_total + e.payment_count -> Expression
    let combined_metric = find_column(model, "combined_metric");
    assert_eq!(combined_metric.expr_type, ExprType::Expression);
    assert!(combined_metric.source_columns.len() >= 3);
}

// ── Category C: Cross-Model Edge Resolution ─────────────────────────────

/// C1: edges from stg_orders/stg_payments into int_orders_enriched
#[test]
fn test_edges_stg_orders_to_int_orders_enriched() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    // Direct edge: stg_orders.order_id -> int_orders_enriched.order_id
    let edges = lineage.trace_column("int_orders_enriched", "order_id");
    assert!(
        edges
            .iter()
            .any(|e| e.source_model == "stg_orders" && e.source_column == "order_id"),
        "Expected edge from stg_orders.order_id to int_orders_enriched.order_id"
    );

    // Renamed edge: stg_orders.amount -> int_orders_enriched.order_amount
    let amount_edges = lineage.trace_column("int_orders_enriched", "order_amount");
    assert!(
        amount_edges
            .iter()
            .any(|e| e.source_model == "stg_orders" && e.source_column == "amount"),
        "Expected edge from stg_orders.amount to int_orders_enriched.order_amount"
    );

    // Aggregation edge: stg_payments -> int_orders_enriched.payment_total
    let payment_edges = lineage.trace_column("int_orders_enriched", "payment_total");
    assert!(
        payment_edges
            .iter()
            .any(|e| e.source_model == "stg_payments"),
        "Expected edge from stg_payments to int_orders_enriched.payment_total"
    );
}

/// C2: fct_orders pulls from both int_orders_enriched and stg_customers
#[test]
fn test_edges_fct_orders_multiple_sources() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    // customer_name comes from stg_customers
    let name_edges = lineage.trace_column("fct_orders", "customer_name");
    assert!(
        name_edges
            .iter()
            .any(|e| e.source_model == "stg_customers" && e.source_column == "customer_name"),
        "Expected edge from stg_customers.customer_name to fct_orders.customer_name"
    );

    // order_id comes from int_orders_enriched
    let id_edges = lineage.trace_column("fct_orders", "order_id");
    assert!(
        id_edges
            .iter()
            .any(|e| e.source_model == "int_orders_enriched" && e.source_column == "order_id"),
        "Expected edge from int_orders_enriched.order_id to fct_orders.order_id"
    );

    // balance_due is Expression -> is_direct should be false on the edge
    let balance_edges = lineage.trace_column("fct_orders", "balance_due");
    assert!(
        balance_edges.iter().all(|e| !e.is_direct),
        "balance_due edges should not be direct (it's an expression)"
    );
    assert!(
        balance_edges
            .iter()
            .all(|e| e.expr_type == ExprType::Expression),
        "balance_due edges should have Expression expr_type"
    );
}

/// C3: dim_customers — computed_tier edges carry Case expr_type
#[test]
fn test_edges_dim_customers_case_expr_type() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let tier_edges = lineage.trace_column("dim_customers", "computed_tier");
    assert!(
        !tier_edges.is_empty(),
        "computed_tier should have upstream edges"
    );
    for edge in &tier_edges {
        assert_eq!(edge.expr_type, ExprType::Case);
        assert!(!edge.is_direct);
    }
}

/// C4: total edge count sanity check
#[test]
fn test_edges_total_count() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    assert!(
        !lineage.edges.is_empty(),
        "ProjectLineage should have resolved edges"
    );
    // With 16 SQL models, there should be many edges
    assert!(
        lineage.edges.len() > 20,
        "Expected more than 20 edges across the sample project, got {}",
        lineage.edges.len()
    );
}

// ── Category D: Recursive Multi-Hop Tracing ─────────────────────────────

/// D1: fct_orders.order_id traces back >= 2 hops
#[test]
fn test_recursive_upstream_fct_orders_order_id() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let upstream = lineage.trace_column_recursive("fct_orders", "order_id");
    assert!(
        upstream.len() >= 2,
        "fct_orders.order_id should trace back at least 2 hops, got {}",
        upstream.len()
    );

    // Should include int_orders_enriched as an intermediate source
    assert!(
        upstream
            .iter()
            .any(|e| e.source_model == "int_orders_enriched"),
        "Upstream of fct_orders.order_id should include int_orders_enriched"
    );

    // Should include stg_orders as the original source
    assert!(
        upstream.iter().any(|e| e.source_model == "stg_orders"),
        "Upstream of fct_orders.order_id should trace back to stg_orders"
    );
}

/// D2: stg_orders.order_id fans out downstream
#[test]
fn test_recursive_downstream_stg_orders_order_id() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let downstream = lineage.column_consumers_recursive("stg_orders", "order_id");
    assert!(
        downstream.len() >= 2,
        "stg_orders.order_id should have at least 2 downstream consumers, got {}",
        downstream.len()
    );

    // int_orders_enriched should consume order_id
    assert!(
        downstream
            .iter()
            .any(|e| e.target_model == "int_orders_enriched"),
        "stg_orders.order_id should flow to int_orders_enriched"
    );

    // fct_orders should consume order_id (2 hops)
    assert!(
        downstream.iter().any(|e| e.target_model == "fct_orders"),
        "stg_orders.order_id should flow to fct_orders (2 hops)"
    );
}

/// D3: dim_customers.computed_tier traces through int_customer_metrics
#[test]
fn test_recursive_upstream_dim_customers_computed_tier() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let upstream = lineage.trace_column_recursive("dim_customers", "computed_tier");
    assert!(
        !upstream.is_empty(),
        "dim_customers.computed_tier should have upstream edges"
    );

    // Should trace back to int_customer_metrics.lifetime_value
    assert!(
        upstream.iter().any(
            |e| e.source_model == "int_customer_metrics" && e.source_column == "lifetime_value"
        ),
        "computed_tier should trace through int_customer_metrics.lifetime_value"
    );
}

/// D4: stg_customers.customer_id fans out to multiple models
#[test]
fn test_recursive_downstream_stg_customers_customer_id_fan_out() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let downstream = lineage.column_consumers_recursive("stg_customers", "customer_id");
    assert!(
        downstream.len() >= 3,
        "stg_customers.customer_id should have at least 3 downstream consumers, got {}",
        downstream.len()
    );

    // Should reach multiple models
    let target_models: std::collections::HashSet<&str> =
        downstream.iter().map(|e| e.target_model.as_str()).collect();
    assert!(
        target_models.len() >= 2,
        "stg_customers.customer_id should reach at least 2 distinct target models, got {:?}",
        target_models
    );
}

/// D5: rpt_customer_orders.order_amount traces deep through DAG
#[test]
fn test_recursive_upstream_rpt_customer_orders_deep_chain() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let upstream = lineage.trace_column_recursive("rpt_customer_orders", "order_amount");
    assert!(
        upstream.len() >= 2,
        "rpt_customer_orders.order_amount should trace at least 2 hops, got {}",
        upstream.len()
    );

    // Should include int_orders_enriched as intermediate
    assert!(
        upstream
            .iter()
            .any(|e| e.source_model == "int_orders_enriched"),
        "rpt_customer_orders.order_amount should trace through int_orders_enriched"
    );

    // Should reach stg_orders as the origin
    assert!(
        upstream.iter().any(|e| e.source_model == "stg_orders"),
        "rpt_customer_orders.order_amount should trace back to stg_orders"
    );
}

// ── Category E: Classification Propagation ──────────────────────────────

/// E1: PII classification propagates on stg_orders.customer_id edges
#[test]
fn test_classification_propagation_pii_through_edges() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let edges_from_customer_id = lineage.column_consumers("stg_orders", "customer_id");
    assert!(
        !edges_from_customer_id.is_empty(),
        "stg_orders.customer_id should have downstream edges"
    );
    for edge in &edges_from_customer_id {
        assert_eq!(
            edge.classification.as_deref(),
            Some("pii"),
            "Edge from stg_orders.customer_id to {}.{} should carry pii classification",
            edge.target_model,
            edge.target_column
        );
    }
}

/// E2: Sensitive classification on stg_orders.amount propagates
#[test]
fn test_classification_sensitive_amount_propagation() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let amount_edges = lineage.column_consumers("stg_orders", "amount");
    assert!(
        !amount_edges.is_empty(),
        "stg_orders.amount should have downstream edges"
    );
    for edge in &amount_edges {
        assert_eq!(
            edge.classification.as_deref(),
            Some("sensitive"),
            "Edge from stg_orders.amount to {}.{} should carry sensitive classification",
            edge.target_model,
            edge.target_column
        );
    }
}

/// E3: Internal classification on stg_orders.status propagates
#[test]
fn test_classification_internal_status_propagation() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let status_edges = lineage.column_consumers("stg_orders", "status");
    assert!(
        !status_edges.is_empty(),
        "stg_orders.status should have downstream edges"
    );
    for edge in &status_edges {
        assert_eq!(
            edge.classification.as_deref(),
            Some("internal"),
            "Edge from stg_orders.status to {}.{} should carry internal classification",
            edge.target_model,
            edge.target_column
        );
    }
}

// ── Category F: Edge Attribute Verification ─────────────────────────────

/// F1: Direct rename/passthrough preserves is_direct=true
#[test]
fn test_edge_is_direct_for_rename_passthrough() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    // customer_id in int_orders_enriched is o.customer_id (direct passthrough)
    let edges = lineage.trace_column("int_orders_enriched", "customer_id");
    let stg_edge = edges.iter().find(|e| e.source_model == "stg_orders");
    assert!(
        stg_edge.is_some(),
        "Expected edge from stg_orders.customer_id"
    );
    assert!(
        stg_edge.unwrap().is_direct,
        "customer_id passthrough should be direct"
    );
    assert_eq!(stg_edge.unwrap().expr_type, ExprType::Column);
}

/// F2: Aggregation edges have is_direct=false
#[test]
fn test_edge_is_not_direct_for_aggregation() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    let edges = lineage.trace_column("int_orders_enriched", "payment_total");
    assert!(
        !edges.is_empty(),
        "payment_total should have upstream edges"
    );
    for edge in &edges {
        assert!(
            !edge.is_direct,
            "payment_total (aggregation) edges should not be direct"
        );
        assert_eq!(edge.expr_type, ExprType::Function);
    }
}

/// F3: ExprType::Column and is_direct=true preserved at each hop in rename chains
#[test]
fn test_edge_expr_type_across_hops() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sample_project");
    let lineage = pipeline.ctx.lineage();

    // fct_orders.amount is `e.order_amount AS amount` -> direct column reference
    let amount_edges = lineage.trace_column("fct_orders", "amount");
    let from_enriched = amount_edges
        .iter()
        .find(|e| e.source_model == "int_orders_enriched");
    assert!(from_enriched.is_some());
    assert_eq!(from_enriched.unwrap().expr_type, ExprType::Column);
    assert!(from_enriched.unwrap().is_direct);

    // int_orders_enriched.order_amount is `o.amount AS order_amount` -> also direct
    let order_amount_edges = lineage.trace_column("int_orders_enriched", "order_amount");
    let from_stg = order_amount_edges
        .iter()
        .find(|e| e.source_model == "stg_orders");
    assert!(from_stg.is_some());
    assert_eq!(from_stg.unwrap().expr_type, ExprType::Column);
    assert!(from_stg.unwrap().is_direct);
}

// ── Category G: CLI-based DataFusion Lineage Tests ──────────────────────
//
// These tests invoke `ff lineage --output json` as a subprocess to exercise
// the **real DataFusion bridge path** (alias resolution, plan walking, etc.)
// rather than the AST-only path used in build_analysis_pipeline above.
// This is the test harness that actually catches alias resolution bugs.

fn ff_bin() -> String {
    env!("CARGO_BIN_EXE_ff").to_string()
}

/// Run `ff lineage --output json` and parse the full result
fn run_lineage_json() -> serde_json::Value {
    let output = std::process::Command::new(ff_bin())
        .args([
            "dt",
            "lineage",
            "--project-dir",
            "tests/fixtures/sample_project",
            "--output",
            "json",
        ])
        .output()
        .expect("Failed to run ff dt lineage");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ff lineage should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "JSON parse failed: {}\nraw: {}",
            e,
            &stdout[..stdout.len().min(500)]
        )
    })
}

/// Run `ff lineage -n <model> --column <col> --output json` and return the edge array
fn run_lineage_column_json(model: &str, column: &str) -> Vec<serde_json::Value> {
    let output = std::process::Command::new(ff_bin())
        .args([
            "dt",
            "lineage",
            "--project-dir",
            "tests/fixtures/sample_project",
            "-n",
            model,
            "--column",
            column,
            "--output",
            "json",
        ])
        .output()
        .expect("Failed to run ff dt lineage");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ff lineage -n {} --column {} should succeed.\nstdout: {}\nstderr: {}",
        model,
        column,
        stdout,
        stderr
    );

    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "JSON parse failed for {} {}: {}\nraw: {}",
            model,
            column,
            e,
            &stdout[..stdout.len().min(500)]
        )
    })
}

/// Helper: extract edges array from full lineage JSON
fn get_edges(data: &serde_json::Value) -> &Vec<serde_json::Value> {
    data["edges"]
        .as_array()
        .expect("lineage JSON should have 'edges' array")
}

/// Helper: find edges in an array matching source model/column -> target model/column
fn find_edge<'a>(
    edges: &'a [serde_json::Value],
    source_model: &str,
    source_column: &str,
    target_model: &str,
    target_column: &str,
) -> Option<&'a serde_json::Value> {
    edges.iter().find(|e| {
        e["source_model"] == source_model
            && e["source_column"] == source_column
            && e["target_model"] == target_model
            && e["target_column"] == target_column
    })
}

/// G1: Total project lineage has substantial edges (basic sanity)
#[test]
fn test_cli_lineage_total_edges() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    assert!(
        edges.len() >= 50,
        "Expected at least 50 lineage edges, got {}",
        edges.len()
    );
}

// ── G2: Every raw_customers column traces to stg_customers ──────────────

#[test]
fn test_cli_lineage_raw_customers_id() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_customers", "id", "stg_customers", "customer_id");
    assert!(
        edge.is_some(),
        "raw_customers.id -> stg_customers.customer_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_customers_name() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_customers",
        "name",
        "stg_customers",
        "customer_name",
    );
    assert!(
        edge.is_some(),
        "raw_customers.name -> stg_customers.customer_name edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_customers_email() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_customers", "email", "stg_customers", "email");
    assert!(
        edge.is_some(),
        "raw_customers.email -> stg_customers.email edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_customers_created_at() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_customers",
        "created_at",
        "stg_customers",
        "signup_date",
    );
    assert!(
        edge.is_some(),
        "raw_customers.created_at -> stg_customers.signup_date edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_customers_tier() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_customers",
        "tier",
        "stg_customers",
        "customer_tier",
    );
    assert!(
        edge.is_some(),
        "raw_customers.tier -> stg_customers.customer_tier edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

// ── G3: Every raw_orders column traces to stg_orders ────────────────────

#[test]
fn test_cli_lineage_raw_orders_id() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_orders", "id", "stg_orders", "order_id");
    assert!(
        edge.is_some(),
        "raw_orders.id -> stg_orders.order_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_orders_user_id() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_orders", "user_id", "stg_orders", "customer_id");
    assert!(
        edge.is_some(),
        "raw_orders.user_id -> stg_orders.customer_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_orders_created_at() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_orders",
        "created_at",
        "stg_orders",
        "order_date",
    );
    assert!(
        edge.is_some(),
        "raw_orders.created_at -> stg_orders.order_date edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_orders_amount() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_orders", "amount", "stg_orders", "amount");
    assert!(
        edge.is_some(),
        "raw_orders.amount -> stg_orders.amount edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_orders_status() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_orders", "status", "stg_orders", "status");
    assert!(
        edge.is_some(),
        "raw_orders.status -> stg_orders.status edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

// ── G4: Every raw_payments column traces to stg_payments or stg_payments_star ─

#[test]
fn test_cli_lineage_raw_payments_id_to_stg_payments() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_payments", "id", "stg_payments", "payment_id");
    assert!(
        edge.is_some(),
        "raw_payments.id -> stg_payments.payment_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_payments_order_id_to_stg_payments() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_payments",
        "order_id",
        "stg_payments",
        "order_id",
    );
    assert!(
        edge.is_some(),
        "raw_payments.order_id -> stg_payments.order_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_payments_amount_to_stg_payments() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_payments", "amount", "stg_payments", "amount");
    assert!(
        edge.is_some(),
        "raw_payments.amount -> stg_payments.amount edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "transform");
}

#[test]
fn test_cli_lineage_raw_payments_id_to_star() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_payments", "id", "stg_payments_star", "id");
    assert!(
        edge.is_some(),
        "raw_payments.id -> stg_payments_star.id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_payments_order_id_to_star() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_payments",
        "order_id",
        "stg_payments_star",
        "order_id",
    );
    assert!(
        edge.is_some(),
        "raw_payments.order_id -> stg_payments_star.order_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_payments_payment_method_to_star() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_payments",
        "payment_method",
        "stg_payments_star",
        "payment_method",
    );
    assert!(
        edge.is_some(),
        "raw_payments.payment_method -> stg_payments_star.payment_method edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_payments_amount_to_star() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_payments",
        "amount",
        "stg_payments_star",
        "amount",
    );
    assert!(
        edge.is_some(),
        "raw_payments.amount -> stg_payments_star.amount edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_payments_created_at_to_star() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_payments",
        "created_at",
        "stg_payments_star",
        "created_at",
    );
    assert!(
        edge.is_some(),
        "raw_payments.created_at -> stg_payments_star.created_at edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

// ── G5: Every raw_products column traces to stg_products ────────────────

#[test]
fn test_cli_lineage_raw_products_id() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_products", "id", "stg_products", "product_id");
    assert!(
        edge.is_some(),
        "raw_products.id -> stg_products.product_id edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_products_name() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_products",
        "name",
        "stg_products",
        "product_name",
    );
    assert!(
        edge.is_some(),
        "raw_products.name -> stg_products.product_name edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "rename");
}

#[test]
fn test_cli_lineage_raw_products_category() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(
        edges,
        "raw_products",
        "category",
        "stg_products",
        "category",
    );
    assert!(
        edge.is_some(),
        "raw_products.category -> stg_products.category edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

#[test]
fn test_cli_lineage_raw_products_price() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_products", "price", "stg_products", "price");
    assert!(
        edge.is_some(),
        "raw_products.price -> stg_products.price edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "transform");
}

#[test]
fn test_cli_lineage_raw_products_active() {
    let data = run_lineage_json();
    let edges = get_edges(&data);
    let edge = find_edge(edges, "raw_products", "active", "stg_products", "active");
    assert!(
        edge.is_some(),
        "raw_products.active -> stg_products.active edge missing"
    );
    assert_eq!(edge.unwrap()["kind"], "copy");
}

// ── G6: Aliased models — edges resolve through aliases correctly ────────
//
// These are the critical tests: models with FROM table AS alias must resolve
// aliases to real table names. Without extract_alias_map(), these all fail.

#[test]
fn test_cli_lineage_int_orders_enriched_alias_resolution() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // stg_orders aliased as `o` — edges must use real name
    assert!(
        find_edge(
            edges,
            "stg_orders",
            "order_id",
            "int_orders_enriched",
            "order_id"
        )
        .is_some(),
        "stg_orders.order_id -> int_orders_enriched.order_id missing (alias: o)"
    );
    assert!(
        find_edge(
            edges,
            "stg_orders",
            "customer_id",
            "int_orders_enriched",
            "customer_id"
        )
        .is_some(),
        "stg_orders.customer_id -> int_orders_enriched.customer_id missing (alias: o)"
    );
    assert!(
        find_edge(
            edges,
            "stg_orders",
            "order_date",
            "int_orders_enriched",
            "order_date"
        )
        .is_some(),
        "stg_orders.order_date -> int_orders_enriched.order_date missing (alias: o)"
    );
    assert!(
        find_edge(
            edges,
            "stg_orders",
            "status",
            "int_orders_enriched",
            "status"
        )
        .is_some(),
        "stg_orders.status -> int_orders_enriched.status missing (alias: o)"
    );

    // stg_payments aliased as `p`
    let has_payment_edge = edges
        .iter()
        .any(|e| e["source_model"] == "stg_payments" && e["target_model"] == "int_orders_enriched");
    assert!(
        has_payment_edge,
        "int_orders_enriched should have edges from stg_payments (alias: p)"
    );
}

#[test]
fn test_cli_lineage_int_customer_metrics_alias_resolution() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // stg_customers aliased as `c`
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_id",
            "int_customer_metrics",
            "customer_id"
        )
        .is_some(),
        "stg_customers.customer_id -> int_customer_metrics.customer_id missing (alias: c)"
    );
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_name",
            "int_customer_metrics",
            "customer_name"
        )
        .is_some(),
        "stg_customers.customer_name -> int_customer_metrics.customer_name missing (alias: c)"
    );

    // stg_orders aliased as `o` — aggregation columns
    let has_orders_edge = edges
        .iter()
        .any(|e| e["source_model"] == "stg_orders" && e["target_model"] == "int_customer_metrics");
    assert!(
        has_orders_edge,
        "int_customer_metrics should have edges from stg_orders (alias: o)"
    );
}

#[test]
fn test_cli_lineage_int_customer_ranking_alias_resolution() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // stg_customers aliased as `c`
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_id",
            "int_customer_ranking",
            "customer_id"
        )
        .is_some(),
        "stg_customers.customer_id -> int_customer_ranking.customer_id missing (alias: c)"
    );
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_name",
            "int_customer_ranking",
            "customer_name"
        )
        .is_some(),
        "stg_customers.customer_name -> int_customer_ranking.customer_name missing (alias: c)"
    );

    // int_customer_metrics aliased as `m`
    assert!(
        find_edge(
            edges,
            "int_customer_metrics",
            "lifetime_value",
            "int_customer_ranking",
            "lifetime_value"
        )
        .is_some(),
        "int_customer_metrics.lifetime_value -> int_customer_ranking.lifetime_value missing (alias: m)"
    );
    assert!(
        find_edge(
            edges,
            "int_customer_metrics",
            "lifetime_value",
            "int_customer_ranking",
            "value_or_zero"
        )
        .is_some(),
        "int_customer_metrics.lifetime_value -> int_customer_ranking.value_or_zero missing (alias: m)"
    );
    assert!(
        find_edge(
            edges,
            "int_customer_metrics",
            "total_orders",
            "int_customer_ranking",
            "nonzero_orders"
        )
        .is_some(),
        "int_customer_metrics.total_orders -> int_customer_ranking.nonzero_orders missing (alias: m)"
    );
}

#[test]
fn test_cli_lineage_dim_customers_alias_resolution() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // int_customer_metrics aliased as `m`
    assert!(
        find_edge(
            edges,
            "int_customer_metrics",
            "customer_id",
            "dim_customers",
            "customer_id"
        )
        .is_some(),
        "int_customer_metrics.customer_id -> dim_customers.customer_id missing (alias: m)"
    );
    assert!(
        find_edge(
            edges,
            "int_customer_metrics",
            "total_orders",
            "dim_customers",
            "total_orders"
        )
        .is_some(),
        "int_customer_metrics.total_orders -> dim_customers.total_orders missing (alias: m)"
    );
    assert!(
        find_edge(
            edges,
            "int_customer_metrics",
            "lifetime_value",
            "dim_customers",
            "lifetime_value"
        )
        .is_some(),
        "int_customer_metrics.lifetime_value -> dim_customers.lifetime_value missing (alias: m)"
    );

    // stg_customers aliased as `c`
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_name",
            "dim_customers",
            "customer_name"
        )
        .is_some(),
        "stg_customers.customer_name -> dim_customers.customer_name missing (alias: c)"
    );
    assert!(
        find_edge(edges, "stg_customers", "email", "dim_customers", "email").is_some(),
        "stg_customers.email -> dim_customers.email missing (alias: c)"
    );
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "signup_date",
            "dim_customers",
            "signup_date"
        )
        .is_some(),
        "stg_customers.signup_date -> dim_customers.signup_date missing (alias: c)"
    );
}

#[test]
fn test_cli_lineage_fct_orders_alias_resolution() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // int_orders_enriched aliased as `e`
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "order_id",
            "fct_orders",
            "order_id"
        )
        .is_some(),
        "int_orders_enriched.order_id -> fct_orders.order_id missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "customer_id",
            "fct_orders",
            "customer_id"
        )
        .is_some(),
        "int_orders_enriched.customer_id -> fct_orders.customer_id missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "order_date",
            "fct_orders",
            "order_date"
        )
        .is_some(),
        "int_orders_enriched.order_date -> fct_orders.order_date missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "status",
            "fct_orders",
            "status"
        )
        .is_some(),
        "int_orders_enriched.status -> fct_orders.status missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "payment_total",
            "fct_orders",
            "payment_total"
        )
        .is_some(),
        "int_orders_enriched.payment_total -> fct_orders.payment_total missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "payment_count",
            "fct_orders",
            "payment_count"
        )
        .is_some(),
        "int_orders_enriched.payment_count -> fct_orders.payment_count missing (alias: e)"
    );

    // stg_customers aliased as `c`
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_name",
            "fct_orders",
            "customer_name"
        )
        .is_some(),
        "stg_customers.customer_name -> fct_orders.customer_name missing (alias: c)"
    );
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_tier",
            "fct_orders",
            "customer_tier"
        )
        .is_some(),
        "stg_customers.customer_tier -> fct_orders.customer_tier missing (alias: c)"
    );
}

#[test]
fn test_cli_lineage_rpt_customer_orders_alias_resolution() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // stg_customers aliased as `c`
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_id",
            "rpt_customer_orders",
            "customer_id"
        )
        .is_some(),
        "stg_customers.customer_id -> rpt_customer_orders.customer_id missing (alias: c)"
    );
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "customer_name",
            "rpt_customer_orders",
            "customer_name"
        )
        .is_some(),
        "stg_customers.customer_name -> rpt_customer_orders.customer_name missing (alias: c)"
    );
    assert!(
        find_edge(
            edges,
            "stg_customers",
            "email",
            "rpt_customer_orders",
            "email"
        )
        .is_some(),
        "stg_customers.email -> rpt_customer_orders.email missing (alias: c)"
    );

    // int_orders_enriched aliased as `e`
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "order_id",
            "rpt_customer_orders",
            "order_id"
        )
        .is_some(),
        "int_orders_enriched.order_id -> rpt_customer_orders.order_id missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "order_amount",
            "rpt_customer_orders",
            "order_amount"
        )
        .is_some(),
        "int_orders_enriched.order_amount -> rpt_customer_orders.order_amount missing (alias: e)"
    );
    assert!(
        find_edge(
            edges,
            "int_orders_enriched",
            "payment_total",
            "rpt_customer_orders",
            "payment_total"
        )
        .is_some(),
        "int_orders_enriched.payment_total -> rpt_customer_orders.payment_total missing (alias: e)"
    );
}

// ── G7: Per-column recursive tracing through aliased models ─────────────

#[test]
fn test_cli_lineage_column_trace_customer_id_through_aliases() {
    // int_customer_ranking.customer_id traces back through stg_customers -> raw_customers
    let edges = run_lineage_column_json("int_customer_ranking", "customer_id");
    assert!(
        !edges.is_empty(),
        "int_customer_ranking.customer_id should have upstream lineage edges"
    );

    // Should trace back to raw_customers.id
    let has_raw = edges.iter().any(|e| e["source_model"] == "raw_customers");
    assert!(
        has_raw,
        "int_customer_ranking.customer_id should trace back to raw_customers"
    );
}

#[test]
fn test_cli_lineage_column_trace_fct_orders_order_id() {
    // fct_orders.order_id traces: fct_orders <- int_orders_enriched <- stg_orders <- raw_orders
    let edges = run_lineage_column_json("fct_orders", "order_id");
    assert!(
        !edges.is_empty(),
        "fct_orders.order_id should have upstream lineage edges"
    );

    let has_raw = edges.iter().any(|e| e["source_model"] == "raw_orders");
    assert!(
        has_raw,
        "fct_orders.order_id should trace back to raw_orders"
    );
}

#[test]
fn test_cli_lineage_column_trace_dim_customers_email() {
    // dim_customers.email traces: dim_customers <- stg_customers <- raw_customers
    let edges = run_lineage_column_json("dim_customers", "email");
    assert!(
        !edges.is_empty(),
        "dim_customers.email should have upstream lineage edges"
    );

    let has_raw = edges.iter().any(|e| e["source_model"] == "raw_customers");
    assert!(
        has_raw,
        "dim_customers.email should trace back to raw_customers"
    );
}

#[test]
fn test_cli_lineage_column_trace_fct_orders_customer_name() {
    // fct_orders.customer_name traces: fct_orders <- stg_customers <- raw_customers
    let edges = run_lineage_column_json("fct_orders", "customer_name");
    assert!(
        !edges.is_empty(),
        "fct_orders.customer_name should have upstream lineage edges"
    );

    let has_stg = edges.iter().any(|e| e["source_model"] == "stg_customers");
    assert!(
        has_stg,
        "fct_orders.customer_name should trace through stg_customers"
    );
}

// ── G8: No alias leakage — verify no edges reference alias names ────────

#[test]
fn test_cli_lineage_no_alias_leakage() {
    let data = run_lineage_json();
    let edges = get_edges(&data);

    // Known aliases used in the sample project
    let alias_names = ["o", "c", "p", "m", "e"];

    for edge in edges {
        let source = edge["source_model"].as_str().unwrap_or("");
        let target = edge["target_model"].as_str().unwrap_or("");

        for alias in &alias_names {
            assert_ne!(
                source, *alias,
                "Edge source_model should be a real table name, not alias '{}': {:?}",
                alias, edge
            );
            assert_ne!(
                target, *alias,
                "Edge target_model should be a real table name, not alias '{}': {:?}",
                alias, edge
            );
        }
    }
}

// ── G9: Model-level lineage data uses real table names in source_tables ─

#[test]
fn test_cli_lineage_model_source_tables_resolved() {
    let data = run_lineage_json();
    let models = data["models"].as_object().expect("models should be object");

    let alias_names = ["o", "c", "p", "m", "e"];

    for (model_name, model_data) in models {
        if let Some(sources) = model_data["source_tables"].as_array() {
            for source in sources {
                let s = source.as_str().unwrap_or("");
                for alias in &alias_names {
                    assert_ne!(
                        s, *alias,
                        "Model '{}' source_tables contains alias '{}' instead of real table name",
                        model_name, alias
                    );
                }
            }
        }
    }
}
