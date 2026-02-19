//! Configuration types and parsing for featherflow.yml

use crate::error::{CoreError, CoreResult};
use crate::serde_helpers::default_true;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
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

    /// Directories containing all node types (unified layout).
    ///
    /// When set, Featherflow discovers models, seeds, sources, and functions
    /// from these directories based on the `kind` field in each node's YAML
    /// configuration file.  Takes precedence over the legacy per-type path
    /// fields (`model_paths`, `source_paths`, `function_paths`).
    #[serde(default)]
    pub node_paths: Vec<String>,

    /// Directories containing model SQL files (legacy — prefer `node_paths`)
    #[serde(default = "default_model_paths")]
    pub model_paths: Vec<String>,

    /// Directories containing macro files
    #[serde(default = "default_macro_paths")]
    pub macro_paths: Vec<String>,

    /// Directories containing source definitions (legacy — prefer `node_paths`)
    #[serde(default = "default_source_paths")]
    pub source_paths: Vec<String>,

    /// Directories containing singular test SQL files
    #[serde(default = "default_test_paths")]
    pub test_paths: Vec<String>,

    /// Directories containing user-defined function definitions (legacy — prefer `node_paths`)
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

    /// Named target configurations (e.g., dev, staging, prod)
    /// Each target can override database settings and variables
    #[serde(default)]
    pub targets: HashMap<String, TargetConfig>,

    /// Analysis diagnostic configuration
    #[serde(default)]
    pub analysis: AnalysisConfig,

    /// Data classification governance settings
    #[serde(default)]
    pub data_classification: DataClassificationConfig,

    /// Query comment configuration for SQL observability
    #[serde(default)]
    pub query_comment: QueryCommentConfig,

    /// SQL rules engine configuration
    #[serde(default)]
    pub rules: Option<crate::rules::RulesConfig>,

    /// Documentation enforcement settings
    #[serde(default)]
    pub documentation: DocumentationConfig,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database type (duckdb or snowflake)
    #[serde(rename = "type", default)]
    pub db_type: DbType,

    /// Database path (for DuckDB file-based or :memory:)
    #[serde(default = "default_db_path")]
    pub path: String,

    /// Logical database name for fully-qualified references (default: "main")
    #[serde(default = "default_db_name")]
    pub name: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_type: DbType::default(),
            path: default_db_path(),
            name: default_db_name(),
        }
    }
}

fn default_db_name() -> String {
    "main".to_string()
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

fn default_macro_paths() -> Vec<String> {
    vec!["macros".to_string()]
}

fn default_source_paths() -> Vec<String> {
    vec!["sources".to_string()]
}

fn default_test_paths() -> Vec<String> {
    vec!["tests".to_string()]
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

const DEFAULT_DB_PATH: &str = ":memory:";

const DEFAULT_TARGET_DIR: &str = "target";

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

        // At least one of node_paths or model_paths must be specified
        if self.node_paths.is_empty() && self.model_paths.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: "At least one node_paths or model_paths entry must be specified"
                    .to_string(),
            });
        }

        // Validate diagnostic code keys in severity_overrides
        for code in self.analysis.severity_overrides.keys() {
            if !VALID_DIAGNOSTIC_CODES.contains(&code.as_str()) {
                return Err(CoreError::ConfigInvalid {
                    message: format!(
                        "Unknown diagnostic code '{}' in analysis.severity_overrides. Valid codes: {}",
                        code,
                        VALID_DIAGNOSTIC_CODES.join(", ")
                    ),
                });
            }
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

    /// Returns `true` when the project uses the unified `node_paths` layout.
    pub fn uses_node_paths(&self) -> bool {
        !self.node_paths.is_empty()
    }

    /// Get absolute node paths relative to a project root
    pub fn node_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.node_paths, root)
    }

    /// Get absolute model paths relative to a project root
    pub fn model_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        Self::paths_absolute(&self.model_paths, root)
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
                                self.targets
                                    .keys()
                                    .map(|k| k.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
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

    /// Get merged variables, with target overrides taking precedence.
    ///
    /// Returns a borrowed reference when no target overrides apply,
    /// avoiding an unnecessary clone of the base vars map.
    pub fn get_merged_vars(
        &self,
        target: Option<&str>,
    ) -> Cow<'_, HashMap<String, serde_yaml::Value>> {
        let target_config = target.and_then(|name| self.targets.get(name));
        match target_config.filter(|tc| !tc.vars.is_empty()) {
            Some(tc) => {
                let mut vars = self.vars.clone();
                for (key, value) in &tc.vars {
                    vars.insert(key.clone(), value.clone());
                }
                Cow::Owned(vars)
            }
            None => Cow::Borrowed(&self.vars),
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

/// Severity level for analysis diagnostic overrides
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigSeverity {
    /// Informational — no action required
    Info,
    /// Warning — potential issue worth reviewing
    Warning,
    /// Error — likely bug or incorrect behavior
    Error,
    /// Disabled — suppress the diagnostic entirely
    Off,
}

/// Analysis configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalysisConfig {
    /// Override default severities for specific diagnostic codes.
    ///
    /// Keys are diagnostic code strings (e.g. "A020", "SA01").
    /// Values are severity levels: info, warning, error, or off.
    #[serde(default)]
    pub severity_overrides: HashMap<String, ConfigSeverity>,
}

/// Valid diagnostic codes that can be overridden in `analysis.severity_overrides`
const VALID_DIAGNOSTIC_CODES: &[&str] = &[
    "A002", "A003", "A004", "A005", "A010", "A011", "A012", "A020", "A030", "A032", "A033", "A040",
    "A041", "SA01", "SA02",
];

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

/// Documentation enforcement settings
///
/// When enabled, `ff validate` will report errors for models or columns
/// that are missing a `description` field in their schema YAML. This ensures
/// every node and column is documented, which is critical for AI/LLM
/// discoverability and team onboarding.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DocumentationConfig {
    /// Whether every model must have a non-empty `description` in its schema YAML
    #[serde(default)]
    pub require_model_descriptions: bool,

    /// Whether every column must have a non-empty `description` in its schema YAML
    #[serde(default)]
    pub require_column_descriptions: bool,
}

/// Where to place the query comment relative to the SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CommentPlacement {
    /// Append the comment after the SQL (default).
    #[default]
    Append,
    /// Prepend the comment before the SQL.
    Prepend,
}

impl std::fmt::Display for CommentPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Append => write!(f, "append"),
            Self::Prepend => write!(f, "prepend"),
        }
    }
}

/// How to format the query comment JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CommentStyle {
    /// Compact single-line JSON (default).
    #[default]
    Compact,
    /// Pretty-printed, human-readable JSON.
    Pretty,
}

impl std::fmt::Display for CommentStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compact => write!(f, "compact"),
            Self::Pretty => write!(f, "pretty"),
        }
    }
}

/// Which metadata fields to include in query comments.
///
/// All fields default to `true`. Set individual fields to `false` to suppress
/// them from the generated comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentInclude {
    /// Include the model name (default: true)
    #[serde(default = "default_true")]
    pub model: bool,
    /// Include the project name (default: true)
    #[serde(default = "default_true")]
    pub project: bool,
    /// Include the materialization type (default: true)
    #[serde(default = "default_true")]
    pub materialization: bool,
    /// Include the compilation timestamp (default: true)
    #[serde(default = "default_true")]
    pub compiled_at: bool,
    /// Include the target name (default: true)
    #[serde(default = "default_true")]
    pub target: bool,
    /// Include the invocation ID (default: true)
    #[serde(default = "default_true")]
    pub invocation_id: bool,
    /// Include the OS user (default: true)
    #[serde(default = "default_true")]
    pub user: bool,
    /// Include the featherflow version (default: true)
    #[serde(default = "default_true")]
    pub featherflow_version: bool,
    /// Include the model's relative file path (default: true)
    #[serde(default = "default_true")]
    pub node_path: bool,
    /// Include the target schema (default: true)
    #[serde(default = "default_true")]
    pub schema: bool,
}

impl Default for CommentInclude {
    fn default() -> Self {
        Self {
            model: true,
            project: true,
            materialization: true,
            compiled_at: true,
            target: true,
            invocation_id: true,
            user: true,
            featherflow_version: true,
            node_path: true,
            schema: true,
        }
    }
}

/// Query comment configuration for SQL observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCommentConfig {
    /// Whether to append query comments to compiled SQL (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Where to place the comment: `append` (default) or `prepend`
    #[serde(default)]
    pub placement: CommentPlacement,

    /// JSON formatting style: `compact` (default) or `pretty`
    #[serde(default)]
    pub style: CommentStyle,

    /// Which fields to include in the comment
    #[serde(default)]
    pub include: CommentInclude,

    /// Custom key-value pairs added to every query comment
    #[serde(default)]
    pub custom_vars: HashMap<String, String>,
}

impl Default for QueryCommentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            placement: CommentPlacement::default(),
            style: CommentStyle::default(),
            include: CommentInclude::default(),
            custom_vars: HashMap::new(),
        }
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
