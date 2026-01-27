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

    /// Directories containing macro files
    #[serde(default = "default_macro_paths")]
    pub macro_paths: Vec<String>,

    /// Directories containing source definitions
    #[serde(default = "default_source_paths")]
    pub source_paths: Vec<String>,

    /// Directories containing singular test SQL files
    #[serde(default = "default_test_paths")]
    pub test_paths: Vec<String>,

    /// Directories containing snapshot YAML files
    #[serde(default = "default_snapshot_paths")]
    pub snapshot_paths: Vec<String>,

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

    /// Directories to clean with `ff clean`
    #[serde(default = "default_clean_targets")]
    pub clean_targets: Vec<String>,

    /// SQL statements to execute before any model runs
    #[serde(default)]
    pub on_run_start: Vec<String>,

    /// SQL statements to execute after all models complete
    #[serde(default)]
    pub on_run_end: Vec<String>,
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
    /// Incremental table (only process new/changed data)
    Incremental,
}

/// Incremental strategy for incremental models
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum IncrementalStrategy {
    /// INSERT new rows only (default)
    #[default]
    Append,
    /// UPSERT based on unique_key
    Merge,
    /// DELETE matching rows then INSERT
    DeleteInsert,
}

/// Schema change handling for incremental models
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    /// Ignore schema changes (default)
    #[default]
    Ignore,
    /// Fail on schema changes
    Fail,
    /// Add new columns
    AppendNewColumns,
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

fn default_macro_paths() -> Vec<String> {
    vec!["macros".to_string()]
}

fn default_source_paths() -> Vec<String> {
    vec!["sources".to_string()]
}

fn default_test_paths() -> Vec<String> {
    vec!["tests".to_string()]
}

fn default_snapshot_paths() -> Vec<String> {
    vec!["snapshots".to_string()]
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

fn default_clean_targets() -> Vec<String> {
    vec!["target".to_string()]
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

    /// Get absolute macro paths relative to a project root
    pub fn macro_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        self.macro_paths.iter().map(|p| root.join(p)).collect()
    }

    /// Get absolute source paths relative to a project root
    pub fn source_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        self.source_paths.iter().map(|p| root.join(p)).collect()
    }

    /// Get absolute test paths relative to a project root
    pub fn test_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        self.test_paths.iter().map(|p| root.join(p)).collect()
    }

    /// Get absolute snapshot paths relative to a project root
    pub fn snapshot_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        self.snapshot_paths.iter().map(|p| root.join(p)).collect()
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
            Materialization::Incremental => write!(f, "incremental"),
        }
    }
}

impl std::fmt::Display for IncrementalStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrementalStrategy::Append => write!(f, "append"),
            IncrementalStrategy::Merge => write!(f, "merge"),
            IncrementalStrategy::DeleteInsert => write!(f, "delete+insert"),
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
        assert_eq!(config.seed_paths, vec!["seeds"]);
        assert_eq!(config.macro_paths, vec!["macros"]);
        assert_eq!(config.source_paths, vec!["sources"]);
        assert_eq!(config.target_path, "target");
    }

    #[test]
    fn test_parse_full_config() {
        let yaml = r#"
name: my_analytics_project
version: "1.0.0"
model_paths: ["models"]
seed_paths: ["seeds"]
macro_paths: ["macros", "shared_macros"]
source_paths: ["sources"]
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
        assert_eq!(config.macro_paths, vec!["macros", "shared_macros"]);
        assert_eq!(config.source_paths, vec!["sources"]);
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

    #[test]
    fn test_run_hooks_default() {
        let config: Config = serde_yaml::from_str("name: test").unwrap();
        assert!(config.on_run_start.is_empty());
        assert!(config.on_run_end.is_empty());
    }

    #[test]
    fn test_run_hooks_parsing() {
        let yaml = r#"
name: test_project
on_run_start:
  - "CREATE SCHEMA IF NOT EXISTS staging"
  - "SET timezone = 'UTC'"
on_run_end:
  - "ANALYZE staging.final_table"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.on_run_start.len(), 2);
        assert_eq!(
            config.on_run_start[0],
            "CREATE SCHEMA IF NOT EXISTS staging"
        );
        assert_eq!(config.on_run_start[1], "SET timezone = 'UTC'");
        assert_eq!(config.on_run_end.len(), 1);
        assert_eq!(config.on_run_end[0], "ANALYZE staging.final_table");
    }
}
