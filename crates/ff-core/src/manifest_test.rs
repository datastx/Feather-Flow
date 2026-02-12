use super::*;
use crate::model_name::ModelName;
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
