use super::*;
use ff_core::config::Materialization;
use ff_core::model::Model;
use ff_core::model_name::ModelName;
use std::collections::HashSet;

#[test]
fn test_manifest_serialization() {
    let mut manifest = Manifest::new("test_project");

    let model = Model {
        name: ModelName::new("test_model"),
        path: std::path::PathBuf::from("models/test_model.sql"),
        raw_sql: "SELECT 1".to_string(),
        compiled_sql: Some("SELECT 1".to_string()),
        config: Default::default(),
        depends_on: HashSet::from_iter(vec![ModelName::new("other_model")]),
        external_deps: HashSet::new(),
        schema: None,
        base_name: None,
        version: None,
    };

    manifest.add_model(
        &model,
        Path::new("target/compiled/test_project/models/test_model.sql"),
        Materialization::View,
        None,
    );

    let json = serde_json::to_string(&manifest).unwrap();
    let loaded: Manifest = serde_json::from_str(&json).unwrap();

    assert_eq!(loaded.project_name, "test_project");
    assert_eq!(loaded.model_count(), 1);
}

#[test]
fn test_reference_manifest_trait() {
    let mut manifest = Manifest::new("test_project");

    let model = Model {
        name: ModelName::new("my_model"),
        path: std::path::PathBuf::from("models/my_model/my_model.sql"),
        raw_sql: "SELECT 1".to_string(),
        compiled_sql: None,
        config: Default::default(),
        depends_on: HashSet::new(),
        external_deps: HashSet::new(),
        schema: None,
        base_name: None,
        version: None,
    };

    manifest.add_model(
        &model,
        Path::new("target/compiled/models/my_model.sql"),
        Materialization::Table,
        None,
    );

    assert!(manifest.contains_model("my_model"));
    assert!(!manifest.contains_model("nonexistent"));

    let model_ref = manifest.get_model_ref("my_model").unwrap();
    assert_eq!(model_ref.materialized, Materialization::Table);
    assert!(manifest.get_model_ref("nonexistent").is_none());
}
