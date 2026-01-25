//! Configuration types and parsing for featherflow.yml

use crate::error::{CoreError, CoreResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Main project configuration from featherflow.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Project name
    pub name: String,

    /// Project version
    #[serde(default = "default_version")]
    pub version: String,

    /// Directories containing model SQL files
    #[serde(default = "default_model_paths")]
    pub model_paths: Vec<String>,

    /// Directories containing seed CSV files
    #[serde(default = "default_seed_paths")]
    pub seed_paths: Vec<String>,

    /// Output directory for compiled SQL and manifest
    #[serde(default = "default_target_path")]
    pub target_path: String,

    /// Default materialization for models (view or table)
    #[serde(default = "default_materialization")]
    pub materialization: Materialization,

    /// Default schema for models
    #[serde(default)]
    pub schema: Option<String>,

    /// SQL dialect for parsing
    #[serde(default = "default_dialect")]
    pub dialect: Dialect,

    /// Database connection configuration
    #[serde(default)]
    pub database: DatabaseConfig,

    /// External tables not managed by Featherflow
    #[serde(default)]
    pub external_tables: Vec<String>,

    /// Variables available in Jinja templates
    #[serde(default)]
    pub vars: HashMap<String, serde_yaml::Value>,
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseConfig {
    /// Database type (duckdb or snowflake)
    #[serde(rename = "type", default = "default_db_type")]
    pub db_type: String,

    /// Database path (for DuckDB file-based or :memory:)
    #[serde(default = "default_db_path")]
    pub path: String,
}

/// Materialization type for models
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Materialization {
    /// Create a view
    #[default]
    View,
    /// Create a table
    Table,
}

/// SQL dialect
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    /// DuckDB SQL dialect
    #[default]
    DuckDb,
    /// Snowflake SQL dialect
    Snowflake,
}

// Default value functions
fn default_version() -> String {
    "1.0.0".to_string()
}

fn default_model_paths() -> Vec<String> {
    vec!["models".to_string()]
}

fn default_seed_paths() -> Vec<String> {
    vec!["seeds".to_string()]
}

fn default_target_path() -> String {
    "target".to_string()
}

fn default_materialization() -> Materialization {
    Materialization::View
}

fn default_dialect() -> Dialect {
    Dialect::DuckDb
}

fn default_db_type() -> String {
    "duckdb".to_string()
}

fn default_db_path() -> String {
    ":memory:".to_string()
}

impl Config {
    /// Load configuration from a file path
    pub fn load(path: &Path) -> CoreResult<Self> {
        if !path.exists() {
            return Err(CoreError::ConfigNotFound {
                path: path.display().to_string(),
            });
        }

        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a project directory
    /// Looks for featherflow.yml or featherflow.yaml
    pub fn load_from_dir(dir: &Path) -> CoreResult<Self> {
        let yml_path = dir.join("featherflow.yml");
        let yaml_path = dir.join("featherflow.yaml");

        if yml_path.exists() {
            Self::load(&yml_path)
        } else if yaml_path.exists() {
            Self::load(&yaml_path)
        } else {
            Err(CoreError::ConfigNotFound {
                path: dir.join("featherflow.yml").display().to_string(),
            })
        }
    }

    /// Validate the configuration
    fn validate(&self) -> CoreResult<()> {
        if self.name.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: "Project name cannot be empty".to_string(),
            });
        }

        if self.model_paths.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: "At least one model path must be specified".to_string(),
            });
        }

        Ok(())
    }

    /// Get a variable value by name
    pub fn get_var(&self, name: &str) -> Option<&serde_yaml::Value> {
        self.vars.get(name)
    }

    /// Check if a table is an external table
    pub fn is_external_table(&self, table: &str) -> bool {
        self.external_tables.iter().any(|t| t == table)
    }

    /// Get absolute model paths relative to a project root
    pub fn model_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        self.model_paths.iter().map(|p| root.join(p)).collect()
    }

    /// Get absolute seed paths relative to a project root
    pub fn seed_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        self.seed_paths.iter().map(|p| root.join(p)).collect()
    }

    /// Get absolute target path relative to a project root
    pub fn target_path_absolute(&self, root: &Path) -> PathBuf {
        root.join(&self.target_path)
    }
}

impl std::fmt::Display for Materialization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Materialization::View => write!(f, "view"),
            Materialization::Table => write!(f, "table"),
        }
    }
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialect::DuckDb => write!(f, "duckdb"),
            Dialect::Snowflake => write!(f, "snowflake"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let yaml = r#"
name: test_project
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.name, "test_project");
        assert_eq!(config.model_paths, vec!["models"]);
        assert_eq!(config.target_path, "target");
    }

    #[test]
    fn test_parse_full_config() {
        let yaml = r#"
name: my_analytics_project
version: "1.0.0"
model_paths: ["models"]
seed_paths: ["seeds"]
target_path: "target"
materialization: view
schema: analytics
dialect: duckdb
database:
  type: duckdb
  path: "./warehouse.duckdb"
external_tables:
  - raw.orders
  - raw.customers
vars:
  start_date: "2024-01-01"
  environment: dev
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.name, "my_analytics_project");
        assert_eq!(config.external_tables.len(), 2);
        assert!(config.is_external_table("raw.orders"));
        assert!(!config.is_external_table("stg_orders"));
    }

    #[test]
    fn test_materialization_default() {
        let config: Config = serde_yaml::from_str("name: test").unwrap();
        assert_eq!(config.materialization, Materialization::View);
    }

    #[test]
    fn test_dialect_default() {
        let config: Config = serde_yaml::from_str("name: test").unwrap();
        assert_eq!(config.dialect, Dialect::DuckDb);
    }
}
