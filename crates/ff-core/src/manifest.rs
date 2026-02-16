//! Manifest types for compiled project output

use crate::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use crate::model::Model;
use crate::model_name::ModelName;
use crate::source::SourceFile;
use crate::table_name::TableName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// The manifest file containing compiled project metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Project name
    pub project_name: String,

    /// Timestamp when compiled
    pub compiled_at: String,

    /// All models in the project
    pub models: HashMap<ModelName, ManifestModel>,

    /// All sources in the project
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sources: HashMap<String, ManifestSource>,
}

/// A model entry in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestModel {
    /// Model name
    pub name: ModelName,

    /// Path to source SQL file
    pub source_path: String,

    /// Path to compiled SQL file
    pub compiled_path: String,

    /// Materialization type
    pub materialized: Materialization,

    /// Target schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Model tags
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// Dependencies on other models
    pub depends_on: Vec<ModelName>,

    /// Dependencies on external tables
    pub external_deps: Vec<TableName>,

    /// All tables referenced in the SQL
    pub referenced_tables: Vec<TableName>,

    /// Unique key column(s) for incremental models
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_key: Option<Vec<String>>,

    /// Incremental strategy (append, merge, delete+insert)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incremental_strategy: Option<IncrementalStrategy>,

    /// How to handle schema changes for incremental models
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_schema_change: Option<OnSchemaChange>,

    /// SQL statements to execute before the model runs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pre_hook: Vec<String>,

    /// SQL statements to execute after the model runs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_hook: Vec<String>,

    /// Whether this model uses Write-Audit-Publish pattern
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wap: Option<bool>,

    /// SHA-256 checksum of the raw SQL content (for change detection)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_checksum: Option<String>,
}

/// A source entry in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSource {
    /// Table name
    pub name: String,

    /// Source group name
    pub source_name: String,

    /// Database schema
    pub schema: String,

    /// Database name (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// Actual table name if different from name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,

    /// Table description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Column definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ManifestSourceColumn>,
}

/// A column in a manifest source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSourceColumn {
    /// Column name
    pub name: String,

    /// Column data type
    pub data_type: String,

    /// Column description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Tests defined for this column
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tests: Vec<String>,
}

impl Manifest {
    /// Create a new manifest
    pub fn new(project_name: &str) -> Self {
        Self {
            project_name: project_name.to_string(),
            compiled_at: chrono_lite_now(),
            models: HashMap::new(),
            sources: HashMap::new(),
        }
    }

    /// Create a new manifest with pre-populated models (for testing)
    #[cfg(test)]
    pub fn new_with_models(project_name: &str, models: Vec<ManifestModel>) -> Self {
        let models_map: HashMap<ModelName, ManifestModel> =
            models.into_iter().map(|m| (m.name.clone(), m)).collect();
        Self {
            project_name: project_name.to_string(),
            compiled_at: chrono_lite_now(),
            models: models_map,
            sources: HashMap::new(),
        }
    }

    /// Add a source to the manifest
    pub fn add_source(&mut self, source: &SourceFile) {
        for table in &source.tables {
            let key = format!("{}.{}", source.name, table.name);
            let columns = table
                .columns
                .iter()
                .map(Self::source_column_to_manifest)
                .collect();

            let manifest_source = ManifestSource {
                name: table.name.clone(),
                source_name: source.name.clone(),
                schema: source.schema.clone(),
                database: source.database.clone(),
                identifier: table.identifier.clone(),
                description: table.description.clone(),
                columns,
            };

            self.sources.insert(key, manifest_source);
        }
    }

    fn source_column_to_manifest(col: &crate::source::SourceColumn) -> ManifestSourceColumn {
        let tests = col.tests.iter().map(Self::test_definition_name).collect();
        ManifestSourceColumn {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            description: col.description.clone(),
            tests,
        }
    }

    fn test_definition_name(t: &crate::model::TestDefinition) -> String {
        match t {
            crate::model::TestDefinition::Simple(name) => name.clone(),
            crate::model::TestDefinition::Parameterized(map) => {
                map.keys().next().cloned().unwrap_or_default()
            }
        }
    }

    /// Add a model to the manifest
    pub fn add_model(
        &mut self,
        model: &Model,
        compiled_path: &Path,
        default_materialization: Materialization,
        default_schema: Option<&str>,
    ) {
        let source_path = model.path.display().to_string();
        let compiled_path_str = compiled_path.display().to_string();

        self.insert_model(
            model,
            source_path,
            compiled_path_str,
            default_materialization,
            default_schema,
        );
    }

    /// Add a model to the manifest with paths relative to project root
    pub fn add_model_relative(
        &mut self,
        model: &Model,
        compiled_path: &Path,
        project_root: &Path,
        default_materialization: Materialization,
        default_schema: Option<&str>,
    ) {
        // Compute relative paths from project root
        let source_path = model
            .path
            .strip_prefix(project_root)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| model.path.display().to_string());

        let compiled_path_str = compiled_path
            .strip_prefix(project_root)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| compiled_path.display().to_string());

        self.insert_model(
            model,
            source_path,
            compiled_path_str,
            default_materialization,
            default_schema,
        );
    }

    /// Build a `ManifestModel` from pre-resolved paths and insert it into the manifest.
    ///
    /// This is the shared implementation used by both `add_model` and `add_model_relative`.
    fn insert_model(
        &mut self,
        model: &Model,
        source_path: String,
        compiled_path: String,
        default_materialization: Materialization,
        default_schema: Option<&str>,
    ) {
        // Get tags from schema file if available
        let tags = model
            .schema
            .as_ref()
            .map(|s| s.tags.clone())
            .unwrap_or_default();

        let materialized = model.materialization(default_materialization);

        // Only include incremental fields if model is incremental
        let (unique_key, incremental_strategy, on_schema_change) =
            if materialized == Materialization::Incremental {
                (
                    model.unique_key(),
                    Some(model.incremental_strategy()),
                    Some(model.on_schema_change()),
                )
            } else {
                (None, None, None)
            };

        // Resolve WAP setting
        let wap = if model.wap_enabled() {
            Some(true)
        } else {
            None
        };

        let manifest_model = ManifestModel {
            name: model.name.clone(),
            source_path,
            compiled_path,
            materialized,
            schema: model.target_schema(default_schema).map(String::from),
            tags,
            depends_on: model.depends_on.iter().cloned().collect(),
            external_deps: model.external_deps.iter().cloned().collect(),
            referenced_tables: model
                .all_dependencies()
                .into_iter()
                .map(TableName::new)
                .collect(),
            unique_key,
            incremental_strategy,
            on_schema_change,
            pre_hook: model.config.pre_hook.clone(),
            post_hook: model.config.post_hook.clone(),
            wap,
            sql_checksum: Some(model.sql_checksum()),
        };

        self.models.insert(model.name.clone(), manifest_model);
    }

    /// Save the manifest to a file atomically
    ///
    /// Uses write-to-temp-then-rename pattern to prevent corruption.
    /// The temp file includes the process ID to avoid races when multiple
    /// processes compile the same project concurrently (e.g. parallel tests).
    pub fn save(&self, path: &Path) -> crate::error::CoreResult<()> {
        let json = serde_json::to_string_pretty(self)?;

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| crate::error::CoreError::IoWithPath {
                path: parent.display().to_string(),
                source: e,
            })?;
        }

        let temp_path = path.with_extension(format!("json.{}.tmp", std::process::id()));
        std::fs::write(&temp_path, &json).map_err(|e| crate::error::CoreError::IoWithPath {
            path: temp_path.display().to_string(),
            source: e,
        })?;
        std::fs::rename(&temp_path, path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            crate::error::CoreError::IoWithPath {
                path: path.display().to_string(),
                source: e,
            }
        })?;
        Ok(())
    }

    /// Load a manifest from a file
    pub fn load(path: &Path) -> crate::error::CoreResult<Self> {
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::error::CoreError::IoWithPath {
                path: path.display().to_string(),
                source: e,
            })?;
        serde_json::from_str(&content).map_err(|e| crate::error::CoreError::ConfigParseError {
            message: format!("Failed to parse manifest '{}': {}", path.display(), e),
        })
    }

    /// Get a model by name
    pub fn get_model(&self, name: &str) -> Option<&ManifestModel> {
        self.models.get(name)
    }

    /// Get all model names
    pub fn model_names(&self) -> Vec<&str> {
        self.models.keys().map(|s| s.as_str()).collect()
    }

    /// Build dependency map for DAG construction
    pub fn dependency_map(&self) -> HashMap<ModelName, Vec<ModelName>> {
        self.models
            .iter()
            .map(|(name, model)| (name.clone(), model.depends_on.clone()))
            .collect()
    }

    /// Total number of models in the manifest
    pub fn model_count(&self) -> usize {
        self.models.len()
    }
}

/// Current UTC timestamp as ISO 8601 string
fn chrono_lite_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
