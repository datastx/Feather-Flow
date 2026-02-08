//! Breaking change detection for schema evolution
//!
//! This module compares current model schemas against a previous manifest
//! to detect potentially breaking changes that could affect downstream consumers.

use crate::manifest::{Manifest, ManifestModel};
use crate::model::ModelSchema;
use std::collections::HashMap;

/// Type of breaking change detected
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakingChangeType {
    /// A model was removed
    ModelRemoved,
    /// A column was removed from a model
    ColumnRemoved { column: String },
    /// A column's type was changed
    TypeChanged {
        column: String,
        old_type: String,
        new_type: String,
    },
    /// A model's materialization changed
    MaterializationChanged { old: String, new: String },
    /// A model's schema changed
    SchemaChanged {
        old_schema: String,
        new_schema: String,
    },
}

impl BreakingChangeType {
    /// Get the severity of this change (higher = more severe)
    pub fn severity(&self) -> u8 {
        match self {
            BreakingChangeType::ModelRemoved => 5,
            BreakingChangeType::ColumnRemoved { .. } => 4,
            BreakingChangeType::TypeChanged { .. } => 3,
            BreakingChangeType::SchemaChanged { .. } => 2,
            BreakingChangeType::MaterializationChanged { .. } => 1,
        }
    }

    /// Get a human-readable description of the change
    pub fn description(&self) -> String {
        match self {
            BreakingChangeType::ModelRemoved => "Model was removed".to_string(),
            BreakingChangeType::ColumnRemoved { column } => {
                format!("Column '{}' was removed", column)
            }
            BreakingChangeType::TypeChanged {
                column,
                old_type,
                new_type,
            } => {
                format!(
                    "Column '{}' type changed from '{}' to '{}'",
                    column, old_type, new_type
                )
            }
            BreakingChangeType::MaterializationChanged { old, new } => {
                format!("Materialization changed from '{}' to '{}'", old, new)
            }
            BreakingChangeType::SchemaChanged {
                old_schema,
                new_schema,
            } => {
                format!("Schema changed from '{}' to '{}'", old_schema, new_schema)
            }
        }
    }
}

/// A breaking change detected for a specific model
#[derive(Debug, Clone)]
pub struct BreakingChange {
    /// Model name where the change was detected
    pub model: String,
    /// Type of breaking change
    pub change_type: BreakingChangeType,
    /// Models that depend on this model (potentially affected)
    pub downstream_models: Vec<String>,
}

impl BreakingChange {
    /// Create a new breaking change
    pub fn new(model: &str, change_type: BreakingChangeType) -> Self {
        Self {
            model: model.to_string(),
            change_type,
            downstream_models: Vec::new(),
        }
    }

    /// Add downstream models that might be affected
    pub fn with_downstream(mut self, downstream: Vec<String>) -> Self {
        self.downstream_models = downstream;
        self
    }
}

/// Result of breaking change detection
#[derive(Debug, Clone, Default)]
pub struct BreakingChangeReport {
    /// All detected breaking changes
    pub changes: Vec<BreakingChange>,
    /// Models that were added (informational, not breaking)
    pub models_added: Vec<String>,
    /// Columns that were added (informational, not breaking)
    pub columns_added: HashMap<String, Vec<String>>,
}

impl BreakingChangeReport {
    /// Create a new empty report
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if there are any breaking changes
    pub fn has_breaking_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Get the highest severity of any change
    pub fn max_severity(&self) -> u8 {
        self.changes
            .iter()
            .map(|c| c.change_type.severity())
            .max()
            .unwrap_or(0)
    }

    /// Get all changes for a specific model
    pub fn changes_for_model(&self, model: &str) -> Vec<&BreakingChange> {
        self.changes.iter().filter(|c| c.model == model).collect()
    }

    /// Add a breaking change
    pub fn add_change(&mut self, change: BreakingChange) {
        self.changes.push(change);
    }

    /// Add a model addition (not breaking)
    pub fn add_new_model(&mut self, model: String) {
        self.models_added.push(model);
    }

    /// Add a column addition (not breaking)
    pub fn add_new_column(&mut self, model: &str, column: String) {
        self.columns_added
            .entry(model.to_string())
            .or_default()
            .push(column);
    }
}

/// Detect breaking changes between previous and current state
pub fn detect_breaking_changes(
    previous: &Manifest,
    current_models: &HashMap<String, ManifestModel>,
    previous_schemas: &HashMap<String, ModelSchema>,
    current_schemas: &HashMap<String, ModelSchema>,
) -> BreakingChangeReport {
    let mut report = BreakingChangeReport::new();

    // Build reverse dependency map for downstream impact analysis
    let downstream_map = build_downstream_map(current_models);

    // Check for removed models
    for model_name in previous.models.keys() {
        if !current_models.contains_key(model_name) {
            let downstream = downstream_map.get(model_name).cloned().unwrap_or_default();
            report.add_change(
                BreakingChange::new(model_name, BreakingChangeType::ModelRemoved)
                    .with_downstream(downstream),
            );
        }
    }

    // Check for changes in existing models
    for (model_name, prev_model) in &previous.models {
        if let Some(curr_model) = current_models.get(model_name) {
            let downstream = downstream_map.get(model_name).cloned().unwrap_or_default();

            // Check materialization change
            if prev_model.materialized != curr_model.materialized {
                report.add_change(
                    BreakingChange::new(
                        model_name,
                        BreakingChangeType::MaterializationChanged {
                            old: prev_model.materialized.to_string(),
                            new: curr_model.materialized.to_string(),
                        },
                    )
                    .with_downstream(downstream.clone()),
                );
            }

            // Check schema change (database schema, not model schema file)
            if prev_model.schema != curr_model.schema {
                if let (Some(old), Some(new)) = (&prev_model.schema, &curr_model.schema) {
                    report.add_change(
                        BreakingChange::new(
                            model_name,
                            BreakingChangeType::SchemaChanged {
                                old_schema: old.clone(),
                                new_schema: new.clone(),
                            },
                        )
                        .with_downstream(downstream.clone()),
                    );
                }
            }

            // Check for column changes (if we have schema information)
            if let (Some(prev_schema), Some(curr_schema)) = (
                previous_schemas.get(model_name),
                current_schemas.get(model_name),
            ) {
                compare_schemas(
                    model_name,
                    prev_schema,
                    curr_schema,
                    &downstream,
                    &mut report,
                );
            }
        }
    }

    // Check for new models (informational)
    for model_name in current_models.keys() {
        if !previous.models.contains_key(model_name) {
            report.add_new_model(model_name.clone());
        }
    }

    report
}

/// Build a map of model -> models that depend on it
fn build_downstream_map(models: &HashMap<String, ManifestModel>) -> HashMap<String, Vec<String>> {
    let mut downstream: HashMap<String, Vec<String>> = HashMap::new();

    for (model_name, model) in models {
        for dep in &model.depends_on {
            downstream
                .entry(dep.clone())
                .or_default()
                .push(model_name.clone());
        }
    }

    downstream
}

/// Compare two schemas for breaking changes
fn compare_schemas(
    model_name: &str,
    prev_schema: &ModelSchema,
    curr_schema: &ModelSchema,
    downstream: &[String],
    report: &mut BreakingChangeReport,
) {
    // SchemaColumnDef has name and data_type
    let prev_cols: HashMap<&str, Option<&str>> = prev_schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_deref()))
        .collect();

    let curr_cols: HashMap<&str, Option<&str>> = curr_schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_deref()))
        .collect();

    // Check for removed columns
    for col_name in prev_cols.keys() {
        if !curr_cols.contains_key(col_name) {
            report.add_change(
                BreakingChange::new(
                    model_name,
                    BreakingChangeType::ColumnRemoved {
                        column: col_name.to_string(),
                    },
                )
                .with_downstream(downstream.to_vec()),
            );
        }
    }

    // Check for type changes
    for (col_name, prev_type) in &prev_cols {
        if let Some(curr_type) = curr_cols.get(col_name) {
            if prev_type != curr_type {
                if let (Some(old), Some(new)) = (prev_type, curr_type) {
                    report.add_change(
                        BreakingChange::new(
                            model_name,
                            BreakingChangeType::TypeChanged {
                                column: col_name.to_string(),
                                old_type: old.to_string(),
                                new_type: new.to_string(),
                            },
                        )
                        .with_downstream(downstream.to_vec()),
                    );
                }
            }
        }
    }

    // Check for new columns (informational)
    for col_name in curr_cols.keys() {
        if !prev_cols.contains_key(col_name) {
            report.add_new_column(model_name, col_name.to_string());
        }
    }
}

/// Simple check without full schema comparison
pub fn detect_breaking_changes_simple(
    previous: &Manifest,
    current_models: &HashMap<String, ManifestModel>,
) -> BreakingChangeReport {
    detect_breaking_changes(previous, current_models, &HashMap::new(), &HashMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Materialization;
    use crate::model::SchemaColumnDef;

    fn make_model(name: &str, deps: Vec<&str>) -> ManifestModel {
        ManifestModel {
            name: name.to_string(),
            source_path: format!("models/{}.sql", name),
            compiled_path: format!("target/compiled/{}.sql", name),
            depends_on: deps.into_iter().map(String::from).collect(),
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

        let current: HashMap<String, ManifestModel> = vec![make_model("model_b", vec!["model_a"])]
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
        let current: HashMap<String, ManifestModel> = vec![curr_model]
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

        let current: HashMap<String, ManifestModel> =
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

        let current: HashMap<String, ManifestModel> = vec![make_model("model_a", vec![])]
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();

        let prev_schemas: HashMap<String, ModelSchema> = [(
            "model_a".to_string(),
            ModelSchema {
                version: 1,
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: Vec::new(),
                columns: vec![
                    SchemaColumnDef {
                        name: "id".to_string(),
                        data_type: Some("INTEGER".to_string()),
                        description: None,
                        primary_key: false,
                        constraints: Vec::new(),
                        tests: Vec::new(),
                        references: None,
                        classification: None,
                    },
                    SchemaColumnDef {
                        name: "removed_col".to_string(),
                        data_type: Some("VARCHAR".to_string()),
                        description: None,
                        primary_key: false,
                        constraints: Vec::new(),
                        tests: Vec::new(),
                        references: None,
                        classification: None,
                    },
                ],
                config: None,
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
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: Vec::new(),
                columns: vec![SchemaColumnDef {
                    name: "id".to_string(),
                    data_type: Some("INTEGER".to_string()),
                    description: None,
                    primary_key: false,
                    constraints: Vec::new(),
                    tests: Vec::new(),
                    references: None,
                    classification: None,
                }],
                config: None,
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

        let current: HashMap<String, ManifestModel> = vec![make_model("model_a", vec![])]
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();

        let prev_schemas: HashMap<String, ModelSchema> = [(
            "model_a".to_string(),
            ModelSchema {
                version: 1,
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: Vec::new(),
                columns: vec![SchemaColumnDef {
                    name: "amount".to_string(),
                    data_type: Some("INTEGER".to_string()),
                    description: None,
                    primary_key: false,
                    constraints: Vec::new(),
                    tests: Vec::new(),
                    references: None,
                    classification: None,
                }],
                config: None,
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
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: Vec::new(),
                columns: vec![SchemaColumnDef {
                    name: "amount".to_string(),
                    data_type: Some("DECIMAL(10,2)".to_string()),
                    description: None,
                    primary_key: false,
                    constraints: Vec::new(),
                    tests: Vec::new(),
                    references: None,
                    classification: None,
                }],
                config: None,
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

        let current: HashMap<String, ManifestModel> = vec![model]
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();

        let report = detect_breaking_changes_simple(&previous, &current);

        assert!(!report.has_breaking_changes());
        assert!(report.models_added.is_empty());
    }
}
