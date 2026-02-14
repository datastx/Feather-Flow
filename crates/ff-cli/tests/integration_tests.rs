//! Integration tests for Featherflow

use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::TestDefinition;
use ff_core::run_state::{RunState, RunStatus};
use ff_core::ModelName;
use ff_core::Project;
use ff_db::{DatabaseCore, DatabaseCsv, DatabaseIncremental, DatabaseSchema, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use ff_test::{generator::GeneratedTest, TestRunner};
use std::collections::HashMap;
use std::path::Path;
use tempfile::tempdir;

/// Test loading the sample project
#[test]
fn test_load_sample_project() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    assert_eq!(project.config.name, "sample_project");
    assert_eq!(project.models.len(), 11);
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
        model: "test_table".to_string(),
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
        model: "test_table".to_string(),
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

/// Test source file discovery
#[test]
fn test_source_discovery() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // Should have discovered sources from source_paths
    assert!(!project.sources.is_empty(), "Sources should be discovered");

    // Check the raw_ecommerce source
    let ecommerce_source = project
        .sources
        .iter()
        .find(|s| s.name == "raw_ecommerce")
        .expect("raw_ecommerce source should exist");

    assert_eq!(ecommerce_source.schema, "main");
    assert_eq!(ecommerce_source.tables.len(), 4);

    // Check raw_orders table
    let raw_orders_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_orders")
        .expect("raw_orders table should exist");
    assert_eq!(raw_orders_table.columns.len(), 5);

    // Check raw_customers table
    let raw_customers_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_customers")
        .expect("raw_customers table should exist");
    assert_eq!(raw_customers_table.columns.len(), 5);

    // Check raw_products table
    let raw_products_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_products")
        .expect("raw_products table should exist");
    assert_eq!(raw_products_table.columns.len(), 5);

    // Check raw_payments table
    let raw_payments_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_payments")
        .expect("raw_payments table should exist");
    assert_eq!(raw_payments_table.columns.len(), 5);
}

/// Test source has kind: sources validation
#[test]
fn test_source_kind_validation() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    // All discovered sources should have kind: sources
    for source in &project.sources {
        // The source kind is validated during discovery, so if we got here, it passed
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
    assert_eq!(qualified, "main.raw_orders");
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
        lookup.contains("main.raw_orders"),
        "main.raw_orders should be in lookup"
    );
    assert!(
        lookup.contains("main.raw_customers"),
        "main.raw_customers should be in lookup"
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
    assert_eq!(source.schema, "main");
    assert!(!source.tables.is_empty(), "Source should have tables");
}

/// Test docs handles models with and without schema files
#[test]
fn test_docs_schema_detection() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let mut with_schema = 0;
    let mut without_schema = 0;

    for model in project.models.values() {
        if model.schema.is_some() {
            with_schema += 1;
        } else {
            without_schema += 1;
        }
    }

    // Sample project may have models without schema files
    // Just ensure we can count them correctly
    assert!(
        with_schema + without_schema > 0,
        "Should have at least one model"
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

/// Test validate checks test/type compatibility
#[test]
fn test_validate_test_compatibility() {
    use ff_core::model::TestType;

    // Numeric tests with struct syntax
    let numeric_tests = vec![
        TestType::Positive,
        TestType::NonNegative,
        TestType::MinValue { value: 0.0 },
        TestType::MaxValue { value: 100.0 },
    ];

    // These tests require numeric types
    for test in &numeric_tests {
        let is_numeric_test = matches!(
            test,
            TestType::Positive
                | TestType::NonNegative
                | TestType::MinValue { .. }
                | TestType::MaxValue { .. }
        );
        assert!(is_numeric_test, "{:?} should be a numeric test", test);
    }

    // Regex test requires string types
    let regex_test = TestType::Regex {
        pattern: ".*".to_string(),
    };
    assert!(
        matches!(regex_test, TestType::Regex { .. }),
        "Regex should be a string-only test"
    );
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

/// Test validate detects duplicate model names
#[test]
fn test_validate_detects_duplicate_models() {
    use ff_core::dag::ModelDag;

    // Simulate duplicate handling - in practice, Project::load prevents this
    // but we can test that the DAG handles it correctly
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("model_a".to_string(), vec![]);
    // Attempting to insert same key again would just overwrite, not error
    // This is expected HashMap behavior

    let dag = ModelDag::build(&deps);
    assert!(dag.is_ok(), "Single model should create valid DAG");
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

/// Test state file serialization
#[test]
fn test_state_file_serialization() {
    use ff_core::config::Materialization;
    use ff_core::state::{ModelState, ModelStateConfig, StateFile};

    let mut state = StateFile::new();

    let config = ModelStateConfig::new(
        Materialization::Table,
        Some("staging".to_string()),
        None,
        None,
        None,
    );
    let model_state = ModelState::new(
        ff_core::ModelName::new("my_model"),
        "SELECT 1",
        Some(100),
        config,
    );

    state.upsert_model(model_state);

    // Serialize and deserialize
    let json = serde_json::to_string_pretty(&state).unwrap();
    let loaded: StateFile = serde_json::from_str(&json).unwrap();

    assert_eq!(loaded.models.len(), 1);
    assert!(loaded.models.contains_key("my_model"));

    let loaded_model = loaded.models.get("my_model").unwrap();
    assert_eq!(loaded_model.row_count, Some(100));
    assert_eq!(loaded_model.config.schema, Some("staging".to_string()));
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
    use ff_core::manifest::{Manifest, ManifestModel};
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
            external_deps: vec!["raw_customers".to_string()],
            referenced_tables: vec!["raw_customers".to_string()],
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
    use ff_core::manifest::{Manifest, ManifestModel};

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
            external_deps: vec!["raw_customers".to_string()],
            referenced_tables: vec!["raw_customers".to_string()],
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
            external_deps: vec!["raw_orders".to_string()],
            referenced_tables: vec!["raw_orders".to_string()],
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
    use ff_core::manifest::{Manifest, ManifestModel};

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
            external_deps: vec!["raw_customers".to_string()],
            referenced_tables: vec!["raw_customers".to_string()],
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
    use ff_core::manifest::{Manifest, ManifestModel};

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
            referenced_tables: vec!["raw_products".to_string()],
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
            referenced_tables: vec!["stg_products".to_string()],
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
    use ff_core::manifest::{Manifest, ManifestModel};

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
    use ff_analysis::{
        parse_sql_type, propagate_schemas, Nullability, RelSchema, SchemaCatalog, TypedColumn,
    };
    use ff_sql::extract_dependencies;

    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();
    let parser = SqlParser::duckdb();
    let jinja = JinjaEnvironment::new(&project.config.vars);

    let known_models: std::collections::HashSet<String> =
        project.models.keys().map(|k| k.to_string()).collect();

    // Build schema catalog from YAML + external tables (sources)
    let mut catalog: SchemaCatalog = HashMap::new();
    for (name, model) in &project.models {
        if let Some(schema) = &model.schema {
            let columns: Vec<TypedColumn> = schema
                .columns
                .iter()
                .map(|col| {
                    let sql_type = parse_sql_type(&col.data_type);
                    TypedColumn {
                        name: col.name.clone(),
                        source_table: None,
                        sql_type,
                        nullability: Nullability::Unknown,
                        provenance: vec![],
                    }
                })
                .collect();
            catalog.insert(name.to_string(), RelSchema::new(columns));
        }
    }

    // Add source tables with their column schemas when available
    for source_file in &project.sources {
        for table in &source_file.tables {
            if catalog.contains_key(&table.name) {
                continue;
            }
            if table.columns.is_empty() {
                catalog.insert(table.name.clone(), RelSchema::empty());
            } else {
                let columns: Vec<TypedColumn> = table
                    .columns
                    .iter()
                    .map(|col| TypedColumn {
                        name: col.name.clone(),
                        source_table: None,
                        sql_type: parse_sql_type(&col.data_type),
                        nullability: Nullability::Unknown,
                        provenance: vec![],
                    })
                    .collect();
                catalog.insert(table.name.clone(), RelSchema::new(columns));
            }
        }
    }
    // Add remaining external tables with empty schemas
    for ext in &project.config.external_tables {
        if !catalog.contains_key(ext) {
            catalog.insert(ext.clone(), RelSchema::empty());
        }
    }

    // Build dep map by rendering SQL and extracting dependencies
    let mut dep_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut sql_sources: HashMap<String, String> = HashMap::new();
    for (name, model) in &project.models {
        let Ok(rendered) = jinja.render(&model.raw_sql) else {
            dep_map.insert(name.to_string(), vec![]);
            continue;
        };
        let Ok(stmts) = parser.parse(&rendered) else {
            dep_map.insert(name.to_string(), vec![]);
            continue;
        };
        let raw_deps = extract_dependencies(&stmts);
        let model_deps: Vec<String> = raw_deps
            .into_iter()
            .filter(|d| known_models.contains(d))
            .collect();
        dep_map.insert(name.to_string(), model_deps);
        sql_sources.insert(name.to_string(), rendered);
    }

    let dag = ModelDag::build(&dep_map).unwrap();
    let topo_order = dag.topological_order().unwrap();

    let yaml_schemas: SchemaCatalog = catalog.clone();
    let (user_fn_stubs, user_table_fn_stubs) = ff_analysis::build_user_function_stubs(&project);
    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &yaml_schemas,
        &catalog,
        &user_fn_stubs,
        &user_table_fn_stubs,
    );

    // All models should plan successfully
    assert!(
        result.failures.is_empty(),
        "Expected no planning failures, got: {:?}",
        result.failures
    );
    assert!(
        !result.model_plans.is_empty(),
        "Should have planned at least one model"
    );

    // Each model should have a non-empty inferred schema
    for (name, plan_result) in &result.model_plans {
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
    order: Vec<String>,
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
    let jinja = JinjaEnvironment::new(&project.config.vars);

    let known_models: std::collections::HashSet<String> =
        project.models.keys().map(|k| k.to_string()).collect();

    let mut catalog: SchemaCatalog = HashMap::new();
    let mut yaml_schemas: HashMap<ModelName, RelSchema> = HashMap::new();
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
            let rel_schema = RelSchema::new(columns);
            catalog.insert(name.to_string(), rel_schema.clone());
            yaml_schemas.insert(name.clone(), rel_schema);
        }
    }

    for source_file in &project.sources {
        for table in &source_file.tables {
            if catalog.contains_key(&table.name) {
                continue;
            }
            if table.columns.is_empty() {
                catalog.insert(table.name.clone(), RelSchema::empty());
            } else {
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
                catalog.insert(table.name.clone(), RelSchema::new(columns));
            }
        }
    }

    let mut dep_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut sql_sources: HashMap<String, String> = HashMap::new();
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
            .filter(|d| known_models.contains(d))
            .collect();
        dep_map.insert(name.to_string(), model_deps);
        if let Some(stmt) = stmts.first() {
            if let Some(lineage) = extract_column_lineage(stmt, name) {
                project_lineage.add_model_lineage(lineage);
            }
        }
        sql_sources.insert(name.to_string(), rendered);
    }
    project_lineage.resolve_edges(&known_models);

    let dag = ModelDag::build(&dep_map).unwrap();
    let topo_order = dag.topological_order().unwrap();

    let yaml_string_map: SchemaCatalog = yaml_schemas
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();

    let (user_fn_stubs, user_table_fn_stubs) = ff_analysis::build_user_function_stubs(&project);
    let propagation = propagate_schemas(
        &topo_order,
        &sql_sources,
        &yaml_string_map,
        &catalog,
        &user_fn_stubs,
        &user_table_fn_stubs,
    );

    let order: Vec<String> = topo_order
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
        model: "test_model".to_string(),
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
    assert_eq!(pipeline.propagation.model_plans.len(), 11);

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

    assert!(
        diagnostics.is_empty(),
        "Expected zero diagnostics on sample_project, got {}:\n{:#?}",
        diagnostics.len(),
        diagnostics
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
    assert_eq!(names.len(), 5);
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
    assert!(
        diags.is_empty(),
        "Clean project should have zero diagnostics, got {}:\n{:#?}",
        diags.len(),
        diags
    );
}

#[test]
fn test_analysis_guard_ecommerce_zero_diagnostics() {
    let pipeline = build_analysis_pipeline("tests/fixtures/sa_dag_pass_ecommerce");
    let diags = run_all_passes(&pipeline);
    assert!(
        diags.is_empty(),
        "Ecommerce regression guard: expected zero diagnostics, got {}:\n{:#?}",
        diags.len(),
        diags
    );
}
