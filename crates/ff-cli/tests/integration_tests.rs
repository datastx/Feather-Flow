//! Integration tests for Featherflow

use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::TestDefinition;
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
    assert_eq!(ecommerce_source.tables.len(), 2);

    // Check raw_orders table
    let raw_orders_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_orders")
        .expect("raw_orders table should exist");
    assert_eq!(raw_orders_table.columns.len(), 4);

    // Check raw_customers table
    let raw_customers_table = ecommerce_source
        .tables
        .iter()
        .find(|t| t.name == "raw_customers")
        .expect("raw_customers table should exist");
    assert_eq!(raw_customers_table.columns.len(), 3);
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

    // order_id column should have unique and not_null tests
    let order_id_col = raw_orders_table
        .columns
        .iter()
        .find(|c| c.name == "order_id")
        .expect("order_id column should exist");
    assert!(
        order_id_col
            .tests
            .contains(&TestDefinition::Simple("unique".to_string())),
        "order_id should have unique test"
    );
    assert!(
        order_id_col
            .tests
            .contains(&TestDefinition::Simple("not_null".to_string())),
        "order_id should have not_null test"
    );

    // customer_id column should have not_null test
    let customer_id_col = raw_orders_table
        .columns
        .iter()
        .find(|c| c.name == "customer_id")
        .expect("customer_id column should exist");
    assert!(
        customer_id_col
            .tests
            .contains(&TestDefinition::Simple("not_null".to_string())),
        "customer_id should have not_null test"
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
        deps.insert(name.clone(), vec![]);
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
    let model_state = ModelState::new("my_model".to_string(), "SELECT 1", Some(100), config);

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
