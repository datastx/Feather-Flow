use super::*;
use crate::config::Materialization;
use crate::model::SchemaColumnDef;

fn make_model(name: &str, deps: Vec<&str>) -> ManifestModel {
    ManifestModel {
        name: ModelName::new(name),
        source_path: format!("models/{}.sql", name),
        compiled_path: format!("target/compiled/{}.sql", name),
        depends_on: deps.into_iter().map(ModelName::new).collect(),
        external_deps: Vec::new(),
        materialized: Materialization::View,
        schema: None,
        unique_key: None,
        incremental_strategy: None,
        on_schema_change: None,
        tags: Vec::new(),
        referenced_tables: Vec::new(),
        pre_hook: Vec::new(),
        post_hook: Vec::new(),
        wap: None,
        sql_checksum: None,
    }
}

fn make_manifest(models: Vec<ManifestModel>) -> Manifest {
    Manifest::new_with_models("test_project", models)
}

#[test]
fn test_detect_model_removal() {
    let previous = make_manifest(vec![
        make_model("model_a", vec![]),
        make_model("model_b", vec!["model_a"]),
    ]);

    let current: HashMap<ModelName, ManifestModel> = vec![make_model("model_b", vec!["model_a"])]
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect();

    let report = detect_breaking_changes_simple(&previous, &current);

    assert!(report.has_breaking_changes());
    assert_eq!(report.changes.len(), 1);
    assert!(matches!(
        report.changes[0].change_type,
        BreakingChangeType::ModelRemoved
    ));
    assert_eq!(report.changes[0].model, "model_a");
    assert!(report.changes[0]
        .downstream_models
        .contains(&"model_b".to_string()));
}

#[test]
fn test_detect_materialization_change() {
    let mut prev_model = make_model("model_a", vec![]);
    prev_model.materialized = Materialization::View;

    let mut curr_model = make_model("model_a", vec![]);
    curr_model.materialized = Materialization::Table;

    let previous = make_manifest(vec![prev_model]);
    let current: HashMap<ModelName, ManifestModel> = vec![curr_model]
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect();

    let report = detect_breaking_changes_simple(&previous, &current);

    assert!(report.has_breaking_changes());
    assert!(matches!(
        report.changes[0].change_type,
        BreakingChangeType::MaterializationChanged { .. }
    ));
}

#[test]
fn test_detect_new_model() {
    let previous = make_manifest(vec![make_model("model_a", vec![])]);

    let current: HashMap<ModelName, ManifestModel> =
        vec![make_model("model_a", vec![]), make_model("model_b", vec![])]
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();

    let report = detect_breaking_changes_simple(&previous, &current);

    assert!(!report.has_breaking_changes());
    assert!(report.models_added.contains(&"model_b".to_string()));
}

#[test]
fn test_detect_column_removal() {
    let previous = make_manifest(vec![make_model("model_a", vec![])]);

    let current: HashMap<ModelName, ManifestModel> = vec![make_model("model_a", vec![])]
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect();

    let prev_schemas: HashMap<String, ModelSchema> = [(
        "model_a".to_string(),
        ModelSchema {
            version: 1,
            name: None,
            description: None,
            owner: None,
            meta: std::collections::HashMap::new(),
            tags: Vec::new(),
            columns: vec![
                SchemaColumnDef {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    description: None,
                    primary_key: false,
                    constraints: Vec::new(),
                    tests: Vec::new(),
                    references: None,
                    classification: None,
                },
                SchemaColumnDef {
                    name: "removed_col".to_string(),
                    data_type: "VARCHAR".to_string(),
                    description: None,
                    primary_key: false,
                    constraints: Vec::new(),
                    tests: Vec::new(),
                    references: None,
                    classification: None,
                },
            ],
            contract: None,
            freshness: None,
            deprecated: false,
            deprecation_message: None,
        },
    )]
    .into_iter()
    .collect();

    let curr_schemas: HashMap<String, ModelSchema> = [(
        "model_a".to_string(),
        ModelSchema {
            version: 1,
            name: None,
            description: None,
            owner: None,
            meta: std::collections::HashMap::new(),
            tags: Vec::new(),
            columns: vec![SchemaColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                description: None,
                primary_key: false,
                constraints: Vec::new(),
                tests: Vec::new(),
                references: None,
                classification: None,
            }],
            contract: None,
            freshness: None,
            deprecated: false,
            deprecation_message: None,
        },
    )]
    .into_iter()
    .collect();

    let report = detect_breaking_changes(&previous, &current, &prev_schemas, &curr_schemas);

    assert!(report.has_breaking_changes());
    let col_removed = report.changes.iter().find(|c| {
        matches!(&c.change_type, BreakingChangeType::ColumnRemoved { column } if column == "removed_col")
    });
    assert!(col_removed.is_some());
}

#[test]
fn test_detect_type_change() {
    let previous = make_manifest(vec![make_model("model_a", vec![])]);

    let current: HashMap<ModelName, ManifestModel> = vec![make_model("model_a", vec![])]
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect();

    let prev_schemas: HashMap<String, ModelSchema> = [(
        "model_a".to_string(),
        ModelSchema {
            version: 1,
            name: None,
            description: None,
            owner: None,
            meta: std::collections::HashMap::new(),
            tags: Vec::new(),
            columns: vec![SchemaColumnDef {
                name: "amount".to_string(),
                data_type: "INTEGER".to_string(),
                description: None,
                primary_key: false,
                constraints: Vec::new(),
                tests: Vec::new(),
                references: None,
                classification: None,
            }],
            contract: None,
            freshness: None,
            deprecated: false,
            deprecation_message: None,
        },
    )]
    .into_iter()
    .collect();

    let curr_schemas: HashMap<String, ModelSchema> = [(
        "model_a".to_string(),
        ModelSchema {
            version: 1,
            name: None,
            description: None,
            owner: None,
            meta: std::collections::HashMap::new(),
            tags: Vec::new(),
            columns: vec![SchemaColumnDef {
                name: "amount".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                description: None,
                primary_key: false,
                constraints: Vec::new(),
                tests: Vec::new(),
                references: None,
                classification: None,
            }],
            contract: None,
            freshness: None,
            deprecated: false,
            deprecation_message: None,
        },
    )]
    .into_iter()
    .collect();

    let report = detect_breaking_changes(&previous, &current, &prev_schemas, &curr_schemas);

    assert!(report.has_breaking_changes());
    let type_changed = report.changes.iter().find(|c| {
        matches!(&c.change_type, BreakingChangeType::TypeChanged { column, .. } if column == "amount")
    });
    assert!(type_changed.is_some());
}

#[test]
fn test_no_changes() {
    let model = make_model("model_a", vec![]);
    let previous = make_manifest(vec![model.clone()]);

    let current: HashMap<ModelName, ManifestModel> = vec![model]
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect();

    let report = detect_breaking_changes_simple(&previous, &current);

    assert!(!report.has_breaking_changes());
    assert!(report.models_added.is_empty());
}
