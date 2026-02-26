use super::*;
use crate::error::CoreError;
use tempfile::TempDir;

fn setup_test_project() -> TempDir {
    let dir = TempDir::new().unwrap();

    // Create featherflow.yml
    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
external_tables:
  - raw.orders
"#,
    )
    .unwrap();

    // Create directory-per-model layout under nodes/
    std::fs::create_dir_all(dir.path().join("nodes/stg_orders")).unwrap();

    // Create a model file
    std::fs::write(
        dir.path().join("nodes/stg_orders/stg_orders.sql"),
        "SELECT * FROM raw.orders",
    )
    .unwrap();

    // Create 1:1 schema file for the model
    std::fs::write(
        dir.path().join("nodes/stg_orders/stg_orders.yml"),
        r#"
kind: sql
version: 1
description: "Staged orders"
columns:
  - name: order_id
    type: INTEGER
    tests:
      - unique
"#,
    )
    .unwrap();

    dir
}

#[test]
fn test_load_project() {
    let dir = setup_test_project();
    let project = Project::load(dir.path()).unwrap();

    assert_eq!(project.config.name, "test_project");
    assert_eq!(project.models.len(), 1);
    assert!(project.models.contains_key("stg_orders"));
    assert!(project.sources.is_empty()); // No sources in this test
}

#[test]
fn test_discover_tests() {
    let dir = setup_test_project();
    let project = Project::load(dir.path()).unwrap();

    // Tests come exclusively from 1:1 schema files
    assert_eq!(project.tests.len(), 1);
    assert!(project.tests.iter().all(|t| t.model == "stg_orders"));
    assert!(project.tests.iter().all(|t| t.column == "order_id"));
}

// Note: DuplicateModel detection within a single nodes/ dir is impossible
// (filesystem prevents two dirs with the same name). The DuplicateModel guard
// remains in the code but cannot be triggered in normal usage.

#[test]
fn test_versioned_model_discovery() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    // Create nodes directory with versioned models
    std::fs::create_dir_all(dir.path().join("nodes/fct_orders")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders/fct_orders.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders/fct_orders.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v2")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v2/fct_orders_v2.sql"),
        "SELECT 2 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v2/fct_orders_v2.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v3")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v3/fct_orders_v3.sql"),
        "SELECT 3 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v3/fct_orders_v3.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();

    // All models should be discovered
    assert_eq!(project.models.len(), 3);

    // Check version parsing
    let v2 = project.get_model("fct_orders_v2").unwrap();
    assert!(v2.is_versioned());
    assert_eq!(v2.get_version(), Some(2));
    assert_eq!(v2.get_base_name(), "fct_orders");

    let v3 = project.get_model("fct_orders_v3").unwrap();
    assert_eq!(v3.get_version(), Some(3));

    // Original model should not be versioned
    let original = project.get_model("fct_orders").unwrap();
    assert!(!original.is_versioned());
    assert_eq!(original.get_base_name(), "fct_orders");
}

#[test]
fn test_resolve_latest_version() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v1")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v1/fct_orders_v1.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v1/fct_orders_v1.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v2")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v2/fct_orders_v2.sql"),
        "SELECT 2",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v2/fct_orders_v2.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();

    // get_latest_version should return v2
    let (name, model) = project.get_latest_version("fct_orders").unwrap();
    assert_eq!(name, "fct_orders_v2");
    assert_eq!(model.get_version(), Some(2));

    // get_all_versions should return both in order
    let versions = project.get_all_versions("fct_orders");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].0, "fct_orders_v1");
    assert_eq!(versions[1].0, "fct_orders_v2");
}

#[test]
fn test_resolve_model_reference() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v1")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v1/fct_orders_v1.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v1/fct_orders_v1.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v2")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v2/fct_orders_v2.sql"),
        "SELECT 2",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v2/fct_orders_v2.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("nodes/dim_products")).unwrap();
    std::fs::write(
        dir.path().join("nodes/dim_products/dim_products.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/dim_products/dim_products.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();

    // Exact match for versioned model
    let (model, warnings) = project.resolve_model_reference("fct_orders_v1");
    assert!(model.is_some());
    assert_eq!(model.unwrap().name, "fct_orders_v1");
    assert!(warnings.is_empty());

    // Unversioned reference resolves to latest
    let (model, warnings) = project.resolve_model_reference("fct_orders");
    assert!(model.is_some());
    assert_eq!(model.unwrap().name, "fct_orders_v2");
    assert!(warnings.is_empty());

    // Exact match for unversioned model
    let (model, warnings) = project.resolve_model_reference("dim_products");
    assert!(model.is_some());
    assert_eq!(model.unwrap().name, "dim_products");
    assert!(warnings.is_empty());

    // Non-existent model
    let (model, _) = project.resolve_model_reference("non_existent");
    assert!(model.is_none());
}

#[test]
fn test_missing_schema_file_enforcement() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    // Create a node directory without a matching .yml file — should fail with NodeMissingYaml
    std::fs::create_dir_all(dir.path().join("nodes/no_schema_model")).unwrap();
    std::fs::write(
        dir.path()
            .join("nodes/no_schema_model/no_schema_model.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::NodeMissingYaml { ref directory } if directory == "no_schema_model"),
        "Expected NodeMissingYaml error, got: {:?}",
        err
    );
}

#[test]
fn test_extra_files_in_node_directory_ignored() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/orders")).unwrap();
    std::fs::write(
        dir.path().join("nodes/orders/orders.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/orders/orders.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    // Extra files in a node directory are silently ignored
    std::fs::write(dir.path().join("nodes/orders/notes.txt"), "some notes").unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_ok(), "Extra files should be ignored: {:?}", result.err());
    assert_eq!(result.unwrap().models.len(), 1);
}

#[test]
fn test_hidden_files_in_model_directory_allowed() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/orders")).unwrap();
    std::fs::write(
        dir.path().join("nodes/orders/orders.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/orders/orders.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    // Add hidden files — should be ignored
    std::fs::write(dir.path().join("nodes/orders/.gitkeep"), "").unwrap();
    std::fs::write(dir.path().join("nodes/orders/.DS_Store"), "").unwrap();

    let result = Project::load(dir.path());
    assert!(
        result.is_ok(),
        "Hidden files should be allowed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().models.len(), 1);
}

#[test]
fn test_deprecation_warning_in_resolution() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/fct_orders_v1")).unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v1/fct_orders_v1.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/fct_orders_v1/fct_orders_v1.yml"),
        "kind: sql\nversion: 1\ndeprecated: true\ndeprecation_message: \"Use fct_orders_v2 instead\"\n",
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();

    // Reference to deprecated model should generate warning
    let (model, warnings) = project.resolve_model_reference("fct_orders_v1");
    assert!(model.is_some());
    assert!(!warnings.is_empty());
    assert!(warnings[0].contains("deprecated"));
    assert!(warnings[0].contains("Use fct_orders_v2 instead"));
}

#[test]
fn test_discover_python_model() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    // Create a SQL model that the Python model depends on
    std::fs::create_dir_all(dir.path().join("nodes/stg_source")).unwrap();
    std::fs::write(
        dir.path().join("nodes/stg_source/stg_source.sql"),
        "SELECT 1 AS id, 100.0 AS amount",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/stg_source/stg_source.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    // Create a Python model
    std::fs::create_dir_all(dir.path().join("nodes/py_enriched")).unwrap();
    std::fs::write(
        dir.path().join("nodes/py_enriched/py_enriched.py"),
        "print('hello')",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/py_enriched/py_enriched.yml"),
        r#"
kind: python
version: 1
description: "Python enrichment model"
depends_on:
  - stg_source
columns:
  - name: id
    type: INTEGER
  - name: score
    type: DOUBLE
"#,
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();

    // Both models should be discovered
    assert_eq!(project.models.len(), 2);

    // SQL model
    let sql_model = project.get_model("stg_source").unwrap();
    assert!(!sql_model.is_python());

    // Python model
    let py_model = project.get_model("py_enriched").unwrap();
    assert!(py_model.is_python());
    assert_eq!(py_model.kind, crate::model::schema::ModelKind::Python);
    assert_eq!(py_model.depends_on.len(), 1);
    assert!(py_model
        .depends_on
        .iter()
        .any(|d| d.as_ref() == "stg_source"));

    // Schema should have the columns
    let schema = py_model.schema.as_ref().unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.depends_on, vec!["stg_source"]);
}

#[test]
fn test_python_file_without_python_kind_rejected() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    // Create a .py file but YAML says kind: sql — fails because .sql file is missing
    std::fs::create_dir_all(dir.path().join("nodes/bad_model")).unwrap();
    std::fs::write(
        dir.path().join("nodes/bad_model/bad_model.py"),
        "print('hello')",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/bad_model/bad_model.yml"),
        "kind: sql\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::NodeMissingDataFile { ref directory, .. } if directory == "bad_model"),
        "Expected NodeMissingDataFile error, got: {:?}",
        err
    );
}

#[test]
fn test_sql_file_with_python_kind_rejected() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    // Create a .sql file but YAML says kind: python — fails because .py file is missing
    std::fs::create_dir_all(dir.path().join("nodes/mismatch")).unwrap();
    std::fs::write(
        dir.path().join("nodes/mismatch/mismatch.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/mismatch/mismatch.yml"),
        "kind: python\nversion: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::NodeMissingDataFile { ref directory, .. } if directory == "mismatch"),
        "Expected NodeMissingDataFile error, got: {:?}",
        err
    );
}

#[test]
fn test_loose_py_file_at_model_root_rejected() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test_project\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes")).unwrap();
    std::fs::write(dir.path().join("nodes/loose.py"), "print('hello')").unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::InvalidModelDirectory { ref reason, .. } if reason.contains("loose .py")),
        "Expected error about loose .py files, got: {:?}",
        err
    );
}

// ── Unified node_paths tests ─────────────────────────────────────

fn setup_node_paths_project() -> TempDir {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: node_test\n",
    )
    .unwrap();

    // SQL model node
    std::fs::create_dir_all(dir.path().join("nodes/stg_orders")).unwrap();
    std::fs::write(
        dir.path().join("nodes/stg_orders/stg_orders.sql"),
        "SELECT id AS order_id, amount FROM raw_orders",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/stg_orders/stg_orders.yml"),
        r#"
version: 1
kind: sql
description: "Staged orders"
columns:
  - name: order_id
    type: INTEGER
    tests:
      - unique
      - not_null
"#,
    )
    .unwrap();

    // Seed node
    std::fs::create_dir_all(dir.path().join("nodes/raw_orders")).unwrap();
    std::fs::write(
        dir.path().join("nodes/raw_orders/raw_orders.csv"),
        "id,amount\n1,100\n2,200\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("nodes/raw_orders/raw_orders.yml"),
        "version: 1\nkind: seed\ndescription: \"Raw orders\"\n",
    )
    .unwrap();

    // Source node
    std::fs::create_dir_all(dir.path().join("nodes/raw_ecommerce")).unwrap();
    std::fs::write(
        dir.path().join("nodes/raw_ecommerce/raw_ecommerce.yml"),
        r#"
kind: source
version: 1
name: raw_ecommerce
schema: main
tables:
  - name: raw_orders
    columns:
      - name: id
        type: INTEGER
"#,
    )
    .unwrap();

    // Function node
    std::fs::create_dir_all(dir.path().join("nodes/double_it")).unwrap();
    std::fs::write(dir.path().join("nodes/double_it/double_it.sql"), "x * 2").unwrap();
    std::fs::write(
        dir.path().join("nodes/double_it/double_it.yml"),
        r#"
kind: function
version: 1
name: double_it
function_type: scalar
args:
  - name: x
    data_type: INTEGER
returns:
  data_type: INTEGER
"#,
    )
    .unwrap();

    dir
}

#[test]
fn test_node_paths_discovers_all_kinds() {
    let dir = setup_node_paths_project();
    let project = Project::load(dir.path()).unwrap();

    assert_eq!(project.config.name, "node_test");
    assert_eq!(project.models.len(), 1, "should discover 1 SQL model");
    assert!(project.models.contains_key("stg_orders"));
    assert_eq!(project.seeds.len(), 1, "should discover 1 seed");
    assert_eq!(project.seeds[0].name, "raw_orders");
    assert_eq!(project.sources.len(), 1, "should discover 1 source");
    assert_eq!(project.sources[0].name, "raw_ecommerce");
    assert_eq!(project.functions.len(), 1, "should discover 1 function");
    assert_eq!(project.functions[0].name.as_str(), "double_it");
}

#[test]
fn test_node_paths_schema_tests_extracted() {
    let dir = setup_node_paths_project();
    let project = Project::load(dir.path()).unwrap();

    // stg_orders has 2 tests: unique + not_null on order_id
    assert_eq!(project.tests.len(), 2);
    assert!(project.tests.iter().all(|t| t.model == "stg_orders"));
}

#[test]
fn test_node_paths_missing_kind_fails() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/bad_node")).unwrap();
    std::fs::write(
        dir.path().join("nodes/bad_node/bad_node.yml"),
        "version: 1\ndescription: no kind\n",
    )
    .unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::NodeMissingKind { ref directory } if directory == "bad_node"),
        "Expected NodeMissingKind, got: {:?}",
        err
    );
}

#[test]
fn test_node_paths_missing_yaml_fails() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/no_yaml")).unwrap();
    std::fs::write(dir.path().join("nodes/no_yaml/no_yaml.sql"), "SELECT 1").unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::NodeMissingYaml { ref directory } if directory == "no_yaml"),
        "Expected NodeMissingYaml, got: {:?}",
        err
    );
}

#[test]
fn test_node_paths_legacy_kind_model_accepted() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test\n",
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("nodes/orders")).unwrap();
    std::fs::write(dir.path().join("nodes/orders/orders.sql"), "SELECT 1 AS id").unwrap();
    // Use legacy "kind: model" — should be accepted and normalized to sql
    std::fs::write(
        dir.path().join("nodes/orders/orders.yml"),
        "version: 1\nkind: model\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();
    assert_eq!(project.models.len(), 1);
    assert!(project.models.contains_key("orders"));
}

#[test]
fn test_node_paths_source_with_no_data_file_works() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        "name: test\n",
    )
    .unwrap();

    // Source nodes don't need a companion data file — just the YAML
    std::fs::create_dir_all(dir.path().join("nodes/my_source")).unwrap();
    std::fs::write(
        dir.path().join("nodes/my_source/my_source.yml"),
        r#"
kind: source
version: 1
name: my_source
schema: public
tables:
  - name: events
    columns:
      - name: id
        type: INTEGER
"#,
    )
    .unwrap();

    let project = Project::load(dir.path()).unwrap();
    assert_eq!(project.sources.len(), 1);
    assert_eq!(project.sources[0].name, "my_source");
}
