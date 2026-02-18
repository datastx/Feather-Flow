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

#[test]
fn test_discover_python_model() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    // Create a SQL model that the Python model depends on
    std::fs::create_dir_all(dir.path().join("models/stg_source")).unwrap();
    std::fs::write(
        dir.path().join("models/stg_source/stg_source.sql"),
        "SELECT 1 AS id, 100.0 AS amount",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/stg_source/stg_source.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    // Create a Python model
    std::fs::create_dir_all(dir.path().join("models/py_enriched")).unwrap();
    std::fs::write(
        dir.path().join("models/py_enriched/py_enriched.py"),
        "print('hello')",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/py_enriched/py_enriched.yml"),
        r#"
version: 1
kind: python
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
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    // Create a .py file but YAML says kind: model (default)
    std::fs::create_dir_all(dir.path().join("models/bad_model")).unwrap();
    std::fs::write(
        dir.path().join("models/bad_model/bad_model.py"),
        "print('hello')",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/bad_model/bad_model.yml"),
        "version: 1\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::InvalidModelDirectory { ref reason, .. } if reason.contains("kind: python")),
        "Expected error about kind: python, got: {:?}",
        err
    );
}

#[test]
fn test_sql_file_with_python_kind_rejected() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    // Create a .sql file but YAML says kind: python
    std::fs::create_dir_all(dir.path().join("models/mismatch")).unwrap();
    std::fs::write(
        dir.path().join("models/mismatch/mismatch.sql"),
        "SELECT 1 AS id",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models/mismatch/mismatch.yml"),
        "version: 1\nkind: python\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::InvalidModelDirectory { ref reason, .. } if reason.contains("kind: python")),
        "Expected error about kind: python mismatch, got: {:?}",
        err
    );
}

#[test]
fn test_loose_py_file_at_model_root_rejected() {
    let dir = TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("featherflow.yml"),
        r#"
name: test_project
model_paths: ["models"]
"#,
    )
    .unwrap();

    std::fs::create_dir_all(dir.path().join("models")).unwrap();
    std::fs::write(dir.path().join("models/loose.py"), "print('hello')").unwrap();

    let result = Project::load(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, CoreError::InvalidModelDirectory { ref reason, .. } if reason.contains("loose .py")),
        "Expected error about loose .py files, got: {:?}",
        err
    );
}
