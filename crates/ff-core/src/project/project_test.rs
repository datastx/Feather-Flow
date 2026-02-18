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
model_paths: ["models"]
external_tables:
  - raw.orders
"#,
    )
    .unwrap();

    // Create directory-per-model layout
    std::fs::create_dir_all(dir.path().join("models/stg_orders")).unwrap();

    // Create a model file
    std::fs::write(
        dir.path().join("models/stg_orders/stg_orders.sql"),
        "SELECT * FROM raw.orders",
    )
    .unwrap();

    // Create 1:1 schema file for the model
    std::fs::write(
        dir.path().join("models/stg_orders/stg_orders.yml"),
        r#"
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

#[test]
fn test_duplicate_model_detection() {
    let dir = TempDir::new().unwrap();

    // Create featherflow.yml with TWO model_paths roots
    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models_a", "models_b"]
"#,
    )
    .unwrap();

    // Create directory-per-model in both roots with the same model name
    std::fs::create_dir_all(dir.path().join("models_a/orders")).unwrap();
    std::fs::create_dir_all(dir.path().join("models_b/orders")).unwrap();

    std::fs::write(
        dir.path().join("models_a/orders/orders.sql"),
        "SELECT * FROM raw_orders",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models_a/orders/orders.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models_b/orders/orders.sql"),
        "SELECT * FROM staging_orders",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models_b/orders/orders.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    // Should fail with DuplicateModel error
    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::DuplicateModel { ref name } if name == "orders"),
        "Expected DuplicateModel error for 'orders', got: {:?}",
        err
    );
}

#[test]
fn test_versioned_model_discovery() {
    let dir = TempDir::new().unwrap();

    // Create minimal featherflow.yml
    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    // Create models directory with versioned models (directory-per-model)
    std::fs::create_dir_all(dir.path().join("models/fct_orders")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders/fct_orders.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders/fct_orders.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("models/fct_orders_v2")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v2/fct_orders_v2.sql"),
        "SELECT 2 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v2/fct_orders_v2.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("models/fct_orders_v3")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v3/fct_orders_v3.sql"),
        "SELECT 3 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v3/fct_orders_v3.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
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
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("models/fct_orders_v1")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v1/fct_orders_v1.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v1/fct_orders_v1.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("models/fct_orders_v2")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v2/fct_orders_v2.sql"),
        "SELECT 2",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v2/fct_orders_v2.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
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
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("models/fct_orders_v1")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v1/fct_orders_v1.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v1/fct_orders_v1.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("models/fct_orders_v2")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v2/fct_orders_v2.sql"),
        "SELECT 2",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v2/fct_orders_v2.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("models/dim_products")).unwrap();
    std::fs::write(
        dir.path().join("models/dim_products/dim_products.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/dim_products/dim_products.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
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

    // Create featherflow.yml (schema files are always required)
    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    // Create a model directory without a matching .yml file
    std::fs::create_dir_all(dir.path().join("models/no_schema_model")).unwrap();
    std::fs::write(
        dir.path()
            .join("models/no_schema_model/no_schema_model.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();

    // Should fail with MissingSchemaFile error
    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::MissingSchemaFile { ref model, .. } if model == "no_schema_model"),
        "Expected MissingSchemaFile error, got: {:?}",
        err
    );
}

#[test]
fn test_extra_files_in_model_directory_rejected() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("models/orders")).unwrap();
    std::fs::write(
        dir.path().join("models/orders/orders.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/orders/orders.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    // Add an extra .txt file
    std::fs::write(dir.path().join("models/orders/notes.txt"), "some notes").unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::ExtraFilesInModelDirectory { ref directory, .. } if directory == "orders"),
        "Expected ExtraFilesInModelDirectory error for 'orders', got: {:?}",
        err
    );
}

#[test]
fn test_hidden_files_in_model_directory_allowed() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("models/orders")).unwrap();
    std::fs::write(
        dir.path().join("models/orders/orders.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/orders/orders.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();
    // Add hidden files — should be ignored
    std::fs::write(dir.path().join("models/orders/.gitkeep"), "").unwrap();
    std::fs::write(dir.path().join("models/orders/.DS_Store"), "").unwrap();

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
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("models/fct_orders_v1")).unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v1/fct_orders_v1.sql"),
        "SELECT 1",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/fct_orders_v1/fct_orders_v1.yml"),
        r#"
version: 1
deprecated: true
deprecation_message: "Use fct_orders_v2 instead"
"#,
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

// ── Unified node_paths tests ─────────────────────────────────────

fn setup_node_paths_project() -> TempDir {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: node_test
node_paths: ["nodes"]
"#,
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
        "name: test\nnode_paths: [\"nodes\"]\n",
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
        "name: test\nnode_paths: [\"nodes\"]\n",
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
        "name: test\nnode_paths: [\"nodes\"]\n",
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
        "name: test\nnode_paths: [\"nodes\"]\n",
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
