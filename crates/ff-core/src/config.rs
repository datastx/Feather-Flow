//! Configuration types and parsing for featherflow.yml

use crate::error::{CoreError, CoreResult};
use crate::serde_helpers::default_true;
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
    pub fn get_schema(&self, target: Option<&str>) -> Option<&str> {
        target
            .and_then(|name| self.targets.get(name))
            .and_then(|tc| tc.schema.as_deref())
            .or(self.schema.as_deref())
    }

    /// Get WAP schema, optionally applying target overrides
    pub fn get_wap_schema(&self, target: Option<&str>) -> Option<&str> {
        target
            .and_then(|name| self.targets.get(name))
            .and_then(|tc| tc.wap_schema.as_deref())
            .or(self.wap_schema.as_deref())
    }

    /// Get merged variables, with target overrides taking precedence
    pub fn get_merged_vars(&self, target: Option<&str>) -> HashMap<String, serde_yaml::Value> {
        let target_config = target.and_then(|name| self.targets.get(name));
        match target_config.filter(|tc| !tc.vars.is_empty()) {
            Some(tc) => {
                let mut vars = self.vars.clone();
                for (key, value) in &tc.vars {
                    vars.insert(key.clone(), value.clone());
                }
                vars
            }
            None => self.vars.clone(),
        }
    }

    /// Resolve target from CLI flag or FF_TARGET environment variable
    ///
    /// Priority: CLI flag > FF_TARGET env var > None
    pub fn resolve_target(cli_target: Option<&str>) -> Option<String> {
        cli_target
            .map(String::from)
            .or_else(|| std::env::var("FF_TARGET").ok())
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

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
