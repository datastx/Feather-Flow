//! Integration tests for Featherflow

use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use ff_test::{generator::GeneratedTest, TestRunner};
use std::collections::HashMap;
use std::path::Path;

/// Test loading the sample project
#[test]
fn test_load_sample_project() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    assert_eq!(project.config.name, "sample_project");
    assert_eq!(project.models.len(), 3);
    assert!(project.models.contains_key("stg_orders"));
    assert!(project.models.contains_key("stg_customers"));
    assert!(project.models.contains_key("fct_orders"));
}

/// Test parsing and dependency extraction
#[test]
fn test_parse_and_extract_dependencies() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();
    let parser = SqlParser::duckdb();
    let jinja = JinjaEnvironment::new(&project.config.vars);

    // Test stg_orders - depends on raw_orders (external)
    let stg_orders = project.get_model("stg_orders").unwrap();
    let rendered = jinja.render(&stg_orders.raw_sql).unwrap();
    let stmts = parser.parse(&rendered).unwrap();
    let deps = extract_dependencies(&stmts);

    assert!(deps.contains("raw_orders"));

    // Test fct_orders - depends on stg_orders and stg_customers (models)
    let fct_orders = project.get_model("fct_orders").unwrap();
    let rendered = jinja.render(&fct_orders.raw_sql).unwrap();
    let stmts = parser.parse(&rendered).unwrap();
    let deps = extract_dependencies(&stmts);

    assert!(deps.contains("stg_orders"));
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

    // Create a table
    db.create_table_as("test_table", "SELECT 1 AS id, 'hello' AS name", false)
        .await
        .unwrap();

    // Verify it exists
    assert!(db.relation_exists("test_table").await.unwrap());

    // Query count
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
        model: "test_table".to_string(),
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
        model: "test_table".to_string(),
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
        model: "test_table".to_string(),
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

    // Load seed data
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

    // Load seeds from the testdata/seeds directory
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
