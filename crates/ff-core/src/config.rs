//! Configuration types and parsing for featherflow.yml

use crate::error::{CoreError, CoreResult};
use crate::serde_helpers::default_true;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Hardcoded node directory name.
const NODE_DIR: &str = "nodes";

/// Hardcoded macro directory name.
const MACRO_DIR: &str = "macros";

/// Hardcoded test directory name.
const TEST_DIR: &str = "tests";

/// Hardcoded output/target directory name.
const DEFAULT_TARGET_DIR: &str = "target";

/// Hardcoded clean targets.
const DEFAULT_CLEAN_DIRS: &[&str] = &[DEFAULT_TARGET_DIR];

/// Main project configuration from featherflow.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Project name
    pub name: String,

    /// Project version
    #[serde(default = "default_version")]
    pub version: String,

    /// Default materialization for models (view or table)
    #[serde(default)]
    pub materialization: Materialization,

    /// SQL dialect for parsing
    #[serde(default = "default_dialect")]
    pub dialect: Dialect,

    /// Named database connections (must contain a "default" key)
    #[serde(default)]
    pub database: DatabaseMap,

    /// External tables not managed by Featherflow
    #[serde(default)]
    pub external_tables: Vec<String>,

    /// Variables available in Jinja templates
    #[serde(default)]
    pub vars: HashMap<String, serde_yaml::Value>,

    /// SQL statements to execute before any model runs
    #[serde(default)]
    pub on_run_start: Vec<String>,

    /// SQL statements to execute after all models complete
    #[serde(default)]
    pub on_run_end: Vec<String>,

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

    /// SQL formatting configuration
    #[serde(default)]
    pub format: FormatConfig,

    /// Run/build mode defaults
    #[serde(default)]
    pub run: Option<RunConfig>,

    /// Named run groups — collections of nodes with preset run parameters
    #[serde(default)]
    pub run_groups: Option<HashMap<String, RunGroupConfig>>,
}

/// Database connection configuration.
///
/// In the new config format, each named connection under `database:` is a
/// `DatabaseConfig`. The `schema` and `wap_schema` fields that were previously
/// top-level are now per-connection.
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

    /// Default schema for models in this connection
    #[serde(default)]
    pub schema: Option<String>,

    /// Private schema for Write-Audit-Publish pattern
    #[serde(default)]
    pub wap_schema: Option<String>,

    /// Variable overrides for this connection (merged with base vars)
    #[serde(default)]
    pub vars: HashMap<String, serde_yaml::Value>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_type: DbType::default(),
            path: default_db_path(),
            name: default_db_name(),
            schema: None,
            wap_schema: None,
            vars: HashMap::new(),
        }
    }
}

/// Named database connections with a required `default` key.
///
/// Custom deserialization supports both legacy single-object format and the
/// new named-connection map format:
///
/// **Legacy** (auto-migrated to `{"default": <obj>}`):
/// ```yaml
/// database:
///   type: duckdb
///   path: "target/dev.duckdb"
/// ```
///
/// **New**:
/// ```yaml
/// database:
///   default:
///     type: duckdb
///     path: "target/dev.duckdb"
///   prod:
///     type: duckdb
///     path: "/data/prod.duckdb"
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseMap(pub HashMap<String, DatabaseConfig>);

impl Default for DatabaseMap {
    fn default() -> Self {
        let mut map = HashMap::new();
        map.insert("default".to_string(), DatabaseConfig::default());
        DatabaseMap(map)
    }
}

impl<'de> Deserialize<'de> for DatabaseMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map: HashMap<String, DatabaseConfig> =
            HashMap::deserialize(deserializer).unwrap_or_default();
        Ok(DatabaseMap(map))
    }
}

impl DatabaseMap {
    /// Get a connection by name, defaulting to "default".
    pub fn get(&self, name: Option<&str>) -> Option<&DatabaseConfig> {
        let key = name.unwrap_or("default");
        self.0.get(key)
    }

    /// List all available connection names.
    pub fn connection_names(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
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

impl Materialization {
    /// Parse a materialization from a string, defaulting to View for unrecognized values.
    pub fn parse(s: &str) -> Self {
        match s {
            "table" => Materialization::Table,
            "incremental" => Materialization::Incremental,
            "ephemeral" => Materialization::Ephemeral,
            _ => Materialization::View,
        }
    }
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

impl IncrementalStrategy {
    /// Parse an incremental strategy from a string, defaulting to Append for unrecognized values.
    pub fn parse(s: &str) -> Self {
        match s {
            "merge" => IncrementalStrategy::Merge,
            "delete+insert" | "delete_insert" => IncrementalStrategy::DeleteInsert,
            _ => IncrementalStrategy::Append,
        }
    }
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

impl OnSchemaChange {
    /// Parse an on_schema_change policy from a string, defaulting to Ignore for unrecognized values.
    pub fn parse(s: &str) -> Self {
        match s {
            "fail" => OnSchemaChange::Fail,
            "append_new_columns" => OnSchemaChange::AppendNewColumns,
            _ => OnSchemaChange::Ignore,
        }
    }
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

/// SQL formatting configuration for `ff fmt`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatConfig {
    /// Maximum line length for formatted SQL (default: 88)
    #[serde(default = "default_format_line_length")]
    pub line_length: usize,

    /// Disable Jinja formatting within SQL files (default: false)
    #[serde(default)]
    pub no_jinjafmt: bool,
}

impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            line_length: default_format_line_length(),
            no_jinjafmt: false,
        }
    }
}

fn default_format_line_length() -> usize {
    88
}

/// Run/build mode configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    /// Default run mode: "models" | "test" | "build"
    #[serde(default)]
    pub default_mode: RunMode,
}

/// Named run group configuration.
///
/// Run groups define named collections of node selectors with preset run
/// parameters. They can be invoked via the `run-group:<name>` selector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunGroupConfig {
    /// Optional description for documentation / `ff dt ls`
    #[serde(default)]
    pub description: Option<String>,

    /// Node selectors (same syntax as `-n` CLI flag: model names,
    /// `tag:X`, `path:X`, `+model`, `model+`, etc.)
    pub nodes: Vec<String>,

    /// Override run mode for this group
    #[serde(default)]
    pub mode: Option<RunMode>,

    /// Override full-refresh for this group
    #[serde(default)]
    pub full_refresh: Option<bool>,

    /// Override fail-fast for this group
    #[serde(default)]
    pub fail_fast: Option<bool>,
}

/// Execution mode for `ff run`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    /// Just execute models, no tests
    Models,
    /// Just run tests against existing tables
    Test,
    /// Run model then test in DAG order (default)
    #[default]
    Build,
}

impl std::fmt::Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunMode::Models => write!(f, "models"),
            RunMode::Test => write!(f, "test"),
            RunMode::Build => write!(f, "build"),
        }
    }
}

fn default_version() -> String {
    "1.0.0".to_string()
}

fn default_dialect() -> Dialect {
    Dialect::DuckDb
}

const DEFAULT_DB_PATH: &str = ":memory:";

fn default_db_path() -> String {
    DEFAULT_DB_PATH.to_string()
}

fn default_db_name() -> String {
    "main".to_string()
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
        let config: Config =
            serde_yaml::from_str(&content).map_err(|e| CoreError::ConfigParseError {
                message: format!("{}: {}", path.display(), e),
            })?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a project directory
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

        if !self.database.0.contains_key("default") {
            return Err(CoreError::ConfigInvalid {
                message: "database map must contain a 'default' connection".to_string(),
            });
        }

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
    pub fn is_external_table(&self, table: &str) -> bool {
        self.external_tables.iter().any(|t| t == table)
    }

    /// Return the external tables as a `HashSet` for O(1) lookups.
    pub fn external_tables_as_set(&self) -> std::collections::HashSet<&str> {
        self.external_tables.iter().map(|s| s.as_str()).collect()
    }

    /// Get absolute node paths (always `<root>/nodes/`)
    pub fn node_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        vec![root.join(NODE_DIR)]
    }

    /// Get absolute macro paths (always `<root>/macros/`)
    pub fn macro_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        vec![root.join(MACRO_DIR)]
    }

    /// Get absolute test paths (always `<root>/tests/`)
    pub fn test_paths_absolute(&self, root: &Path) -> Vec<PathBuf> {
        vec![root.join(TEST_DIR)]
    }

    /// Get absolute target path (always `<root>/target/`)
    pub fn target_path_absolute(&self, root: &Path) -> PathBuf {
        root.join(DEFAULT_TARGET_DIR)
    }

    /// Get clean targets (hardcoded).
    pub fn clean_targets(&self) -> Vec<&str> {
        DEFAULT_CLEAN_DIRS.to_vec()
    }

    /// Get a database connection by name. `None` returns the "default" connection.
    pub fn get_database_config(&self, db: Option<&str>) -> CoreResult<&DatabaseConfig> {
        let key = db.unwrap_or("default");
        self.database
            .0
            .get(key)
            .ok_or_else(|| CoreError::ConfigInvalid {
                message: format!(
                    "Database connection '{}' not found. Available connections: {}",
                    key,
                    self.database.connection_names().join(", ")
                ),
            })
    }

    /// Get schema from the selected database connection.
    pub fn get_schema(&self, db: Option<&str>) -> Option<&str> {
        self.get_database_config(db)
            .ok()
            .and_then(|c| c.schema.as_deref())
    }

    /// Get WAP schema from the selected database connection.
    pub fn get_wap_schema(&self, db: Option<&str>) -> Option<&str> {
        self.get_database_config(db)
            .ok()
            .and_then(|c| c.wap_schema.as_deref())
    }

    /// Get merged variables: base vars + connection-specific vars.
    pub fn get_merged_vars(&self, db: Option<&str>) -> Cow<'_, HashMap<String, serde_yaml::Value>> {
        let conn_vars = self
            .get_database_config(db)
            .ok()
            .filter(|c| !c.vars.is_empty());

        match conn_vars {
            Some(c) => {
                let mut vars = self.vars.clone();
                vars.extend(c.vars.iter().map(|(k, v)| (k.clone(), v.clone())));
                Cow::Owned(vars)
            }
            None => Cow::Borrowed(&self.vars),
        }
    }

    /// Get the list of available database connection names.
    pub fn available_databases(&self) -> Vec<&str> {
        self.database.connection_names()
    }

    /// Resolve database connection from CLI flag or FF_DATABASE environment variable.
    ///
    /// Priority: CLI flag > FF_DATABASE env var > None (uses "default")
    pub fn resolve_database(cli_database: Option<&str>) -> Option<String> {
        cli_database
            .map(String::from)
            .or_else(|| std::env::var("FF_DATABASE").ok())
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
    /// Return the materialization name as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Materialization::View => "view",
            Materialization::Table => "table",
            Materialization::Incremental => "incremental",
            Materialization::Ephemeral => "ephemeral",
        }
    }

    /// Returns true if this is an ephemeral materialization
    pub fn is_ephemeral(&self) -> bool {
        matches!(self, Materialization::Ephemeral)
    }
}

impl IncrementalStrategy {
    /// Return the strategy name as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            IncrementalStrategy::Append => "append",
            IncrementalStrategy::Merge => "merge",
            IncrementalStrategy::DeleteInsert => "delete+insert",
        }
    }
}

impl OnSchemaChange {
    /// Return the schema-change policy name as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            OnSchemaChange::Ignore => "ignore",
            OnSchemaChange::Fail => "fail",
            OnSchemaChange::AppendNewColumns => "append_new_columns",
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

/// Severity level for analysis diagnostic overrides
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigSeverity {
    Info,
    Warning,
    Error,
    Off,
}

/// Analysis configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalysisConfig {
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
    #[serde(default)]
    pub require_classification: bool,
    #[serde(default)]
    pub default_classification: Option<crate::model::DataClassification>,
    #[serde(default = "default_true")]
    pub propagate: bool,
}

/// Documentation enforcement settings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DocumentationConfig {
    #[serde(default)]
    pub require_model_descriptions: bool,
    #[serde(default)]
    pub require_column_descriptions: bool,
}

/// Where to place the query comment relative to the SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CommentPlacement {
    #[default]
    Append,
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
    #[default]
    Compact,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentInclude {
    #[serde(default = "default_true")]
    pub model: bool,
    #[serde(default = "default_true")]
    pub project: bool,
    #[serde(default = "default_true")]
    pub materialization: bool,
    #[serde(default = "default_true")]
    pub compiled_at: bool,
    #[serde(default = "default_true")]
    pub target: bool,
    #[serde(default = "default_true")]
    pub invocation_id: bool,
    #[serde(default = "default_true")]
    pub user: bool,
    #[serde(default = "default_true")]
    pub featherflow_version: bool,
    #[serde(default = "default_true")]
    pub node_path: bool,
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
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub placement: CommentPlacement,
    #[serde(default)]
    pub style: CommentStyle,
    #[serde(default)]
    pub include: CommentInclude,
    #[serde(default)]
    pub custom_vars: HashMap<String, String>,
    /// Custom key-value pairs to include in query comments.
    /// Values support environment variable interpolation (e.g., "$CI_JOB_ID", "$GIT_SHA").
    #[serde(default)]
    pub custom_fields: Option<HashMap<String, String>>,
}

impl Default for QueryCommentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            placement: CommentPlacement::default(),
            style: CommentStyle::default(),
            include: CommentInclude::default(),
            custom_vars: HashMap::new(),
            custom_fields: None,
        }
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
