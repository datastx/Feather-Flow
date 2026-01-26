//! Manifest types for compiled project output

use crate::config::Materialization;
use crate::model::Model;
use crate::source::SourceFile;
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
    pub models: HashMap<String, ManifestModel>,

    /// All sources in the project
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sources: HashMap<String, ManifestSource>,

    /// Total number of models
    pub model_count: usize,
}

/// A model entry in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestModel {
    /// Model name
    pub name: String,

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
    pub depends_on: Vec<String>,

    /// Dependencies on external tables
    pub external_deps: Vec<String>,

    /// All tables referenced in the SQL
    pub referenced_tables: Vec<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,

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
            model_count: 0,
        }
    }

    /// Add a source to the manifest
    pub fn add_source(&mut self, source: &SourceFile) {
        for table in &source.tables {
            let key = format!("{}.{}", source.name, table.name);

            let columns: Vec<ManifestSourceColumn> = table
                .columns
                .iter()
                .map(|col| {
                    let tests: Vec<String> = col
                        .tests
                        .iter()
                        .map(|t| match t {
                            crate::model::TestDefinition::Simple(name) => name.clone(),
                            crate::model::TestDefinition::Parameterized(map) => {
                                map.keys().next().cloned().unwrap_or_default()
                            }
                        })
                        .collect();
                    ManifestSourceColumn {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        description: col.description.clone(),
                        tests,
                    }
                })
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

    /// Add a model to the manifest
    pub fn add_model(
        &mut self,
        model: &Model,
        compiled_path: &Path,
        default_materialization: Materialization,
        default_schema: Option<&str>,
    ) {
        // Get tags from schema file if available
        let tags = model
            .schema
            .as_ref()
            .map(|s| s.tags.clone())
            .unwrap_or_default();

        // Use paths as-is (caller should provide relative paths)
        let manifest_model = ManifestModel {
            name: model.name.clone(),
            source_path: model.path.display().to_string(),
            compiled_path: compiled_path.display().to_string(),
            materialized: model.materialization(default_materialization),
            schema: model.target_schema(default_schema),
            tags,
            depends_on: model.depends_on.iter().cloned().collect(),
            external_deps: model.external_deps.iter().cloned().collect(),
            referenced_tables: model.all_dependencies().into_iter().collect(),
        };

        self.models.insert(model.name.clone(), manifest_model);
        self.model_count = self.models.len();
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
        // Get tags from schema file if available
        let tags = model
            .schema
            .as_ref()
            .map(|s| s.tags.clone())
            .unwrap_or_default();

        // Compute relative paths from project root
        let source_path = model
            .path
            .strip_prefix(project_root)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| model.path.display().to_string());

        let compiled_path_rel = compiled_path
            .strip_prefix(project_root)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| compiled_path.display().to_string());

        let manifest_model = ManifestModel {
            name: model.name.clone(),
            source_path,
            compiled_path: compiled_path_rel,
            materialized: model.materialization(default_materialization),
            schema: model.target_schema(default_schema),
            tags,
            depends_on: model.depends_on.iter().cloned().collect(),
            external_deps: model.external_deps.iter().cloned().collect(),
            referenced_tables: model.all_dependencies().into_iter().collect(),
        };

        self.models.insert(model.name.clone(), manifest_model);
        self.model_count = self.models.len();
    }

    /// Save the manifest to a file
    pub fn save(&self, path: &Path) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(path, json)
    }

    /// Load a manifest from a file
    pub fn load(path: &Path) -> Result<Self, std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
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
    pub fn dependency_map(&self) -> HashMap<String, Vec<String>> {
        self.models
            .iter()
            .map(|(name, model)| (name.clone(), model.depends_on.clone()))
            .collect()
    }
}

/// Simple timestamp function (no external dependencies)
fn chrono_lite_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let secs = duration.as_secs();

    // Calculate date/time components
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple year calculation (not accounting for leap years perfectly, but good enough)
    let mut year = 1970;
    let mut remaining_days = days_since_epoch as i64;

    while remaining_days >= 365 {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days >= days_in_year {
            remaining_days -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    // Calculate month and day
    let days_in_months = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for days in days_in_months.iter() {
        if remaining_days < *days as i64 {
            break;
        }
        remaining_days -= *days as i64;
        month += 1;
    }
    let day = remaining_days + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_manifest_serialization() {
        let mut manifest = Manifest::new("test_project");

        let model = Model {
            name: "test_model".to_string(),
            path: std::path::PathBuf::from("models/test_model.sql"),
            raw_sql: "SELECT 1".to_string(),
            compiled_sql: Some("SELECT 1".to_string()),
            config: Default::default(),
            depends_on: HashSet::from_iter(vec!["other_model".to_string()]),
            external_deps: HashSet::new(),
            schema: None,
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
        assert_eq!(loaded.model_count, 1);
    }
}
