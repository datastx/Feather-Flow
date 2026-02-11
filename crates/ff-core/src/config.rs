//! Configuration types and parsing for featherflow.yml

use crate::error::{CoreError, CoreResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Main project configuration from featherflow.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Directories containing exposure YAML files
    #[serde(default = "default_exposure_paths")]
    pub exposure_paths: Vec<String>,

    /// Directories containing user-defined function definitions
    #[serde(default = "default_function_paths")]
    pub function_paths: Vec<String>,

    /// Output directory for compiled SQL and manifest
    #[serde(default = "default_target_path")]
    pub target_path: String,

    /// Default materialization for models (view or table)
    #[serde(default)]
    pub materialization: Materialization,

    /// Default schema for models
    #[serde(default)]
    pub schema: Option<String>,

    /// Private schema for Write-Audit-Publish pattern
    #[serde(default)]
    pub wap_schema: Option<String>,

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

    /// SQL hooks to execute before each model runs (applied to all models)
    #[serde(default)]
    pub pre_hook: Vec<String>,

    /// SQL hooks to execute after each model runs (applied to all models)
    #[serde(default)]
    pub post_hook: Vec<String>,

    /// Named target configurations (e.g., dev, staging, prod)
    /// Each target can override database settings and variables
    #[serde(default)]
    pub targets: HashMap<String, TargetConfig>,

    /// Data classification governance settings
    #[serde(default)]
    pub data_classification: DataClassificationConfig,

    /// Query comment configuration for SQL observability
    #[serde(default)]
    pub query_comment: QueryCommentConfig,
}

/// Target-specific configuration overrides
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetConfig {
    /// Database configuration override
    #[serde(default)]
    pub database: Option<DatabaseConfig>,

    /// Schema override
    #[serde(default)]
    pub schema: Option<String>,

    /// WAP schema override
    #[serde(default)]
    pub wap_schema: Option<String>,

    /// Variable overrides (merged with base vars)
    #[serde(default)]
    pub vars: HashMap<String, serde_yaml::Value>,
}

/// Database type selector
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DbType {
    /// DuckDB (default)
    #[default]
    DuckDb,
    /// Snowflake
    Snowflake,
}

impl std::fmt::Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbType::DuckDb => write!(f, "duckdb"),
            DbType::Snowflake => write!(f, "snowflake"),
        }
    }
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseConfig {
    /// Database type (duckdb or snowflake)
    #[serde(rename = "type", default)]
    pub db_type: DbType,

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
    /// Ephemeral model (inlined as CTE, no database object created)
    Ephemeral,
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

fn default_exposure_paths() -> Vec<String> {
    vec!["exposures".to_string()]
}

fn default_function_paths() -> Vec<String> {
    vec!["functions".to_string()]
}

fn default_target_path() -> String {
    "target".to_string()
}

fn default_dialect() -> Dialect {
    Dialect::DuckDb
}

/// Default database path (in-memory)
pub const DEFAULT_DB_PATH: &str = ":memory:";

/// Default target/output directory name
pub const DEFAULT_TARGET_DIR: &str = "target";

fn default_db_path() -> String {
    DEFAULT_DB_PATH.to_string()
}

fn default_clean_targets() -> Vec<String> {
    vec![DEFAULT_TARGET_DIR.to_string()]
}

impl Config {
    /// Load configuration from a file path
    pub fn load(path: &Path) -> CoreResult<Self> {
        if !path.exists() {
            return Err(CoreError::ConfigNotFound {
                path: path.display().to_string(),
            });
        }

        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
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

    /// Check if a table is an external table (O(n) linear scan).
    ///
    /// For batch lookups consider [`external_tables_as_set`](Self::external_tables_as_set).
    pub fn is_external_table(&self, table: &str) -> bool {
        self.external_tables.iter().any(|t| t == table)
    }

    /// Return the external tables as a `HashSet` for O(1) lookups in batch scenarios.
    pub fn external_tables_as_set(&self) -> std::collections::HashSet<&str> {
        self.external_tables.iter().map(|s| s.as_str()).collect()
    }

    /// Resolve relative path strings to absolute paths against a root directory
    fn paths_absolute(paths: &[String], root: &Path) -> Vec<PathBuf> {
        paths.iter().map(|p| root.join(p)).collect()
    }

    /// Get absolute model paths relative to a project root
    pub fn model_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.model_paths, root)
    }

    /// Get absolute seed paths relative to a project root
    pub fn seed_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.seed_paths, root)
    }

    /// Get absolute macro paths relative to a project root
    pub fn macro_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.macro_paths, root)
    }

    /// Get absolute source paths relative to a project root
    pub fn source_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.source_paths, root)
    }

    /// Get absolute test paths relative to a project root
    pub fn test_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.test_paths, root)
    }

    /// Get absolute snapshot paths relative to a project root
    pub fn snapshot_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.snapshot_paths, root)
    }

    /// Get absolute exposure paths relative to a project root
    pub fn exposure_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.exposure_paths, root)
    }

    /// Get absolute function paths relative to a project root
    pub fn function_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.function_paths, root)
    }

    /// Get absolute target path relative to a project root
    pub fn target_path_absolute(&self, root: &Path) -> PathBuf {
        root.join(&self.target_path)
    }

    /// Get the list of available target names
    pub fn available_targets(&self) -> Vec<&str> {
        self.targets.keys().map(|s| s.as_str()).collect()
    }

    /// Get target configuration by name
    pub fn get_target(&self, name: &str) -> Option<&TargetConfig> {
        self.targets.get(name)
    }

    /// Get database configuration, optionally applying target overrides
    ///
    /// If target is specified and exists, uses target's database config.
    /// Otherwise, uses the base database config.
    pub fn get_database_config(&self, target: Option<&str>) -> CoreResult<DatabaseConfig> {
        match target {
            Some(name) => {
                let target_config =
                    self.targets
                        .get(name)
                        .ok_or_else(|| CoreError::ConfigInvalid {
                            message: format!(
                                "Target '{}' not found. Available targets: {}",
                                name,
                                self.targets.keys().cloned().collect::<Vec<_>>().join(", ")
                            ),
                        })?;

                // Use target's database config if specified, otherwise fall back to base
                Ok(target_config
                    .database
                    .clone()
                    .unwrap_or_else(|| self.database.clone()))
            }
            None => Ok(self.database.clone()),
        }
    }

    /// Get schema, optionally applying target overrides
    pub fn get_schema(&self, target: Option<&str>) -> Option<String> {
        if let Some(schema) = target
            .and_then(|name| self.targets.get(name))
            .and_then(|tc| tc.schema.clone())
        {
            return Some(schema);
        }
        self.schema.clone()
    }

    /// Get WAP schema, optionally applying target overrides
    pub fn get_wap_schema(&self, target: Option<&str>) -> Option<String> {
        if let Some(schema) = target
            .and_then(|name| self.targets.get(name))
            .and_then(|tc| tc.wap_schema.clone())
        {
            return Some(schema);
        }
        self.wap_schema.clone()
    }

    /// Get merged variables, with target overrides taking precedence
    pub fn get_merged_vars(&self, target: Option<&str>) -> HashMap<String, serde_yaml::Value> {
        let mut vars = self.vars.clone();

        if let Some(name) = target {
            if let Some(target_config) = self.targets.get(name) {
                // Target vars override base vars
                for (key, value) in &target_config.vars {
                    vars.insert(key.clone(), value.clone());
                }
            }
        }

        vars
    }

    /// Resolve target from CLI flag or FF_TARGET environment variable
    ///
    /// Priority: CLI flag > FF_TARGET env var > None
    pub fn resolve_target(cli_target: Option<&str>) -> Option<String> {
        if let Some(target) = cli_target {
            return Some(target.to_string());
        }

        std::env::var("FF_TARGET").ok()
    }
}

impl std::fmt::Display for Materialization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Materialization::View => write!(f, "view"),
            Materialization::Table => write!(f, "table"),
            Materialization::Incremental => write!(f, "incremental"),
            Materialization::Ephemeral => write!(f, "ephemeral"),
        }
    }
}

impl Materialization {
    /// Returns true if this is an ephemeral materialization
    pub fn is_ephemeral(&self) -> bool {
        matches!(self, Materialization::Ephemeral)
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

/// Data classification governance settings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataClassificationConfig {
    /// Whether all columns must have a classification assigned
    #[serde(default)]
    pub require_classification: bool,

    /// Default classification for columns that don't specify one
    #[serde(default)]
    pub default_classification: Option<crate::model::DataClassification>,

    /// Whether classification propagates through lineage (default: true)
    #[serde(default = "default_true")]
    pub propagate: bool,
}

/// Query comment configuration for SQL observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCommentConfig {
    /// Whether to append query comments to compiled SQL (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for QueryCommentConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

fn default_true() -> bool {
    true
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
    fn test_materialization_ephemeral() {
        let config: Config =
            serde_yaml::from_str("name: test\nmaterialization: ephemeral").unwrap();
        assert_eq!(config.materialization, Materialization::Ephemeral);
        assert!(config.materialization.is_ephemeral());
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

    #[test]
    fn test_targets_parsing() {
        let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./dev.duckdb
schema: dev_schema
vars:
  environment: dev
targets:
  prod:
    database:
      type: duckdb
      path: ./prod.duckdb
    schema: prod_schema
    vars:
      environment: prod
      debug_mode: false
  staging:
    database:
      type: duckdb
      path: ./staging.duckdb
    schema: staging_schema
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.targets.len(), 2);
        assert!(config.targets.contains_key("prod"));
        assert!(config.targets.contains_key("staging"));

        let prod = config.targets.get("prod").unwrap();
        assert_eq!(prod.database.as_ref().unwrap().path, "./prod.duckdb");
        assert_eq!(prod.schema.as_ref().unwrap(), "prod_schema");
        assert!(prod.vars.contains_key("environment"));
    }

    #[test]
    fn test_get_database_config_base() {
        let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let db = config.get_database_config(None).unwrap();
        assert_eq!(db.path, "./base.duckdb");
    }

    #[test]
    fn test_get_database_config_with_target() {
        let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
targets:
  prod:
    database:
      type: duckdb
      path: ./prod.duckdb
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let db = config.get_database_config(Some("prod")).unwrap();
        assert_eq!(db.path, "./prod.duckdb");
    }

    #[test]
    fn test_get_database_config_target_without_db_override() {
        let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
targets:
  prod:
    schema: prod_schema
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        // Target exists but has no database override, should use base
        let db = config.get_database_config(Some("prod")).unwrap();
        assert_eq!(db.path, "./base.duckdb");
    }

    #[test]
    fn test_get_database_config_invalid_target() {
        let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
targets:
  prod:
    database:
      path: ./prod.duckdb
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let result = config.get_database_config(Some("nonexistent"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Target 'nonexistent' not found"));
        assert!(err.contains("prod"));
    }

    #[test]
    fn test_get_schema_with_target_override() {
        let yaml = r#"
name: test_project
schema: base_schema
targets:
  prod:
    schema: prod_schema
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.get_schema(None), Some("base_schema".to_string()));
        assert_eq!(
            config.get_schema(Some("prod")),
            Some("prod_schema".to_string())
        );
    }

    #[test]
    fn test_get_merged_vars() {
        let yaml = r#"
name: test_project
vars:
  environment: dev
  debug: true
  common_key: base_value
targets:
  prod:
    vars:
      environment: prod
      debug: false
      extra_key: prod_only
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();

        // Base vars
        let base_vars = config.get_merged_vars(None);
        assert_eq!(base_vars.get("environment").unwrap().as_str(), Some("dev"));
        assert_eq!(base_vars.get("debug").unwrap().as_bool(), Some(true));

        // Merged vars with target
        let merged = config.get_merged_vars(Some("prod"));
        assert_eq!(merged.get("environment").unwrap().as_str(), Some("prod"));
        assert_eq!(merged.get("debug").unwrap().as_bool(), Some(false));
        assert_eq!(
            merged.get("common_key").unwrap().as_str(),
            Some("base_value")
        );
        assert_eq!(merged.get("extra_key").unwrap().as_str(), Some("prod_only"));
    }

    #[test]
    fn test_available_targets() {
        let yaml = r#"
name: test_project
targets:
  dev: {}
  staging: {}
  prod: {}
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let targets = config.available_targets();
        assert_eq!(targets.len(), 3);
        assert!(targets.contains(&"dev"));
        assert!(targets.contains(&"staging"));
        assert!(targets.contains(&"prod"));
    }

    // These tests modify environment variables and must run serially
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_resolve_target_cli_takes_precedence() {
        // CLI flag should take precedence over env var
        let original = std::env::var("FF_TARGET").ok();
        std::env::set_var("FF_TARGET", "staging");
        let result = Config::resolve_target(Some("prod"));
        assert_eq!(result, Some("prod".to_string()));
        // Restore original state
        match original {
            Some(v) => std::env::set_var("FF_TARGET", v),
            None => std::env::remove_var("FF_TARGET"),
        }
    }

    #[test]
    #[serial]
    fn test_resolve_target_uses_env_var() {
        let original = std::env::var("FF_TARGET").ok();
        std::env::set_var("FF_TARGET", "staging");
        let result = Config::resolve_target(None);
        assert_eq!(result, Some("staging".to_string()));
        // Restore original state
        match original {
            Some(v) => std::env::set_var("FF_TARGET", v),
            None => std::env::remove_var("FF_TARGET"),
        }
    }

    #[test]
    #[serial]
    fn test_resolve_target_none_when_not_set() {
        let original = std::env::var("FF_TARGET").ok();
        std::env::remove_var("FF_TARGET");
        let result = Config::resolve_target(None);
        assert_eq!(result, None);
        // Restore original state
        if let Some(v) = original {
            std::env::set_var("FF_TARGET", v);
        }
    }

    #[test]
    fn test_project_hooks_default_empty() {
        let config: Config = serde_yaml::from_str("name: test").unwrap();
        assert!(config.pre_hook.is_empty());
        assert!(config.post_hook.is_empty());
    }

    #[test]
    fn test_project_hooks_parsing() {
        let yaml = r#"
name: test_project
pre_hook:
  - "CREATE SCHEMA IF NOT EXISTS staging"
post_hook:
  - "ANALYZE {{ this }}"
  - "GRANT SELECT ON {{ this }} TO analyst"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.pre_hook.len(), 1);
        assert_eq!(config.post_hook.len(), 2);
        assert!(config.pre_hook[0].contains("CREATE SCHEMA"));
        assert!(config.post_hook[0].contains("ANALYZE"));
    }
}
