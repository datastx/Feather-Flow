//! Model representation

use crate::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use crate::error::CoreError;
use crate::model_name::ModelName;
use crate::table_name::TableName;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

/// Represents a SQL model in the project
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Model name (derived from filename without extension)
    pub name: ModelName,

    /// Path to the source SQL file
    pub path: PathBuf,

    /// Raw SQL content (before Jinja rendering)
    pub raw_sql: String,

    /// Compiled SQL content (after Jinja rendering)
    #[serde(default)]
    pub compiled_sql: Option<String>,

    /// Model configuration from config() function
    #[serde(default)]
    pub config: ModelConfig,

    /// Dependencies on other models
    #[serde(default)]
    pub depends_on: HashSet<ModelName>,

    /// Dependencies on external tables
    #[serde(default)]
    pub external_deps: HashSet<TableName>,

    /// Schema metadata from 1:1 .yml file (optional)
    #[serde(default)]
    pub schema: Option<ModelSchema>,

    /// Base name without version suffix (e.g., "fct_orders" for "fct_orders_v2")
    #[serde(default)]
    pub base_name: Option<String>,

    /// Version number if model follows _v{N} naming convention (e.g., 2 for "fct_orders_v2")
    #[serde(default)]
    pub version: Option<u32>,
}

/// Schema metadata for a single model (from 1:1 .yml file)
///
/// This follows the 1:1 naming convention where each model's schema file
/// has the same name as its SQL file (e.g., stg_orders.sql + stg_orders.yml)
///
/// Uses `deny_unknown_fields` so that any YAML containing a `config:` key
/// (which is no longer supported) will fail with a clear deserialization error.
/// Config should only be specified via the SQL `{{ config() }}` function.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSchema {
    /// Schema format version
    pub version: u32,

    /// Model name (optional, must match SQL file if provided)
    #[serde(default)]
    pub name: Option<String>,

    /// Model description
    #[serde(default)]
    pub description: Option<String>,

    /// Model owner (team or person) - deprecated, use meta.owner
    #[serde(default)]
    pub owner: Option<String>,

    /// Arbitrary metadata (owner, team, slack_channel, etc.)
    #[serde(default)]
    pub meta: std::collections::HashMap<String, serde_yaml::Value>,

    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,

    /// Data contract definition for schema enforcement
    #[serde(default)]
    pub contract: Option<SchemaContract>,

    /// Freshness configuration for SLA monitoring
    #[serde(default)]
    pub freshness: Option<FreshnessConfig>,

    /// Column definitions
    #[serde(default)]
    pub columns: Vec<SchemaColumnDef>,

    /// Whether this model is deprecated
    #[serde(default)]
    pub deprecated: bool,

    /// Deprecation message to show users (e.g., "Use fct_orders_v2 instead")
    #[serde(default)]
    pub deprecation_message: Option<String>,
}

/// Freshness configuration for SLA monitoring
///
/// Defines when a model should be considered stale based on
/// the maximum value of a timestamp column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessConfig {
    /// Column containing row timestamps (e.g., "updated_at", "loaded_at")
    pub loaded_at_field: String,

    /// Threshold after which to show a warning
    #[serde(default)]
    pub warn_after: Option<FreshnessThreshold>,

    /// Threshold after which to show an error
    #[serde(default)]
    pub error_after: Option<FreshnessThreshold>,
}

/// A freshness threshold (count + period)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessThreshold {
    /// Number of periods
    pub count: u32,

    /// Time period unit
    pub period: FreshnessPeriod,
}

impl FreshnessThreshold {
    /// Create a new threshold
    pub fn new(count: u32, period: FreshnessPeriod) -> Self {
        Self { count, period }
    }

    /// Convert the threshold to seconds
    pub fn to_seconds(&self) -> u64 {
        const SECS_PER_MINUTE: u64 = 60;
        const SECS_PER_HOUR: u64 = 3600;
        const SECS_PER_DAY: u64 = 86_400;

        let period_seconds = match self.period {
            FreshnessPeriod::Minute => SECS_PER_MINUTE,
            FreshnessPeriod::Hour => SECS_PER_HOUR,
            FreshnessPeriod::Day => SECS_PER_DAY,
        };
        self.count as u64 * period_seconds
    }
}

/// Time period unit for freshness thresholds
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessPeriod {
    /// Minutes
    Minute,
    /// Hours
    Hour,
    /// Days
    Day,
}

impl std::fmt::Display for FreshnessPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FreshnessPeriod::Minute => write!(f, "minute"),
            FreshnessPeriod::Hour => write!(f, "hour"),
            FreshnessPeriod::Day => write!(f, "day"),
        }
    }
}

/// Data contract definition for enforcing schema stability
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaContract {
    /// Whether the contract is enforced (error on violation) or advisory (warning)
    #[serde(default)]
    pub enforced: bool,
}

/// Column constraint types for contracts
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnConstraint {
    /// Column must not contain NULL values
    NotNull,
    /// Column is the primary key
    PrimaryKey,
    /// Column values must be unique
    Unique,
}

impl ModelSchema {
    /// Load schema from a file path
    pub fn load(path: &std::path::Path) -> Result<Self, CoreError> {
        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
        let schema: ModelSchema = serde_yaml::from_str(&content)?;
        Ok(schema)
    }

    /// Check if this model has an enforced contract
    pub fn has_enforced_contract(&self) -> bool {
        self.contract.as_ref().map(|c| c.enforced).unwrap_or(false)
    }

    /// Get the contract if defined
    pub fn get_contract(&self) -> Option<&SchemaContract> {
        self.contract.as_ref()
    }

    /// Get column definition by name
    pub fn get_column(&self, name: &str) -> Option<&SchemaColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get all column names defined in the schema
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Check if this model has freshness configuration
    pub fn has_freshness(&self) -> bool {
        self.freshness.is_some()
    }

    /// Get the freshness config if defined
    pub fn get_freshness(&self) -> Option<&FreshnessConfig> {
        self.freshness.as_ref()
    }

    /// Get owner - prefers direct owner field, falls back to meta.owner
    pub fn get_owner(&self) -> Option<String> {
        // First check direct owner field
        if let Some(owner) = &self.owner {
            return Some(owner.clone());
        }
        // Fall back to meta.owner
        self.get_meta_string("owner")
    }

    /// Get a metadata value as a string
    pub fn get_meta_string(&self, key: &str) -> Option<String> {
        self.meta.get(key).and_then(|v| match v {
            serde_yaml::Value::String(s) => Some(s.clone()),
            _ => None,
        })
    }

    /// Get a metadata value
    pub fn get_meta(&self, key: &str) -> Option<&serde_yaml::Value> {
        self.meta.get(key)
    }

    /// Extract tests from this schema
    pub fn extract_tests(&self, model_name: &str) -> Vec<SchemaTest> {
        let mut tests = Vec::new();

        for column in &self.columns {
            for test_def in &column.tests {
                if let Some(test_type) = parse_test_definition(test_def) {
                    tests.push(SchemaTest {
                        test_type,
                        column: column.name.clone(),
                        model: model_name.to_string(),
                        config: TestConfig::default(),
                    });
                }
            }
        }

        tests
    }
}

/// Parse a test definition into a TestType
pub fn parse_test_definition(test_def: &TestDefinition) -> Option<TestType> {
    match test_def {
        TestDefinition::Simple(name) => match name.as_str() {
            "unique" => Some(TestType::Unique),
            "not_null" => Some(TestType::NotNull),
            "positive" => Some(TestType::Positive),
            "non_negative" => Some(TestType::NonNegative),
            _ => None, // Skip unknown test types
        },
        TestDefinition::Parameterized(map) => {
            // Get the first (and should be only) key-value pair
            let (test_name, params) = map.iter().next()?;

            match test_name.as_str() {
                "accepted_values" => {
                    let values: Vec<String> = params
                        .values
                        .iter()
                        .map(|v| match v {
                            serde_yaml::Value::String(s) => s.clone(),
                            serde_yaml::Value::Number(n) => n.to_string(),
                            serde_yaml::Value::Bool(b) => b.to_string(),
                            _ => v.as_str().unwrap_or("").to_string(),
                        })
                        .collect();
                    Some(TestType::AcceptedValues {
                        values,
                        quote: params.quote,
                    })
                }
                "min_value" => params.value.map(|value| TestType::MinValue { value }),
                "max_value" => params.value.map(|value| TestType::MaxValue { value }),
                "regex" => params
                    .pattern
                    .clone()
                    .map(|pattern| TestType::Regex { pattern }),
                "relationship" | "relationships" => {
                    params.to.clone().map(|to| TestType::Relationship {
                        to,
                        field: params.field.clone(),
                    })
                }
                _ => None,
            }
        }
    }
}

/// Configuration for a model extracted from config() function
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Materialization type (view, table, or incremental)
    #[serde(default)]
    pub materialized: Option<Materialization>,

    /// Target schema
    #[serde(default)]
    pub schema: Option<String>,

    /// Additional tags
    #[serde(default)]
    pub tags: Vec<String>,

    /// Unique key column(s) for incremental merge
    /// Can be a single column name or comma-separated list
    #[serde(default)]
    pub unique_key: Option<String>,

    /// Incremental strategy (append, merge, delete+insert)
    #[serde(default)]
    pub incremental_strategy: Option<IncrementalStrategy>,

    /// Schema change handling for incremental models
    #[serde(default)]
    pub on_schema_change: Option<OnSchemaChange>,

    /// SQL statements to execute before the model runs
    #[serde(default)]
    pub pre_hook: Vec<String>,

    /// SQL statements to execute after the model runs
    #[serde(default)]
    pub post_hook: Vec<String>,

    /// Whether this model uses Write-Audit-Publish pattern
    #[serde(default)]
    pub wap: Option<bool>,
}

/// Schema test definition from schema.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaTest {
    /// Test type (unique, not_null, etc.)
    pub test_type: TestType,

    /// Column name to test
    pub column: String,

    /// Model name
    pub model: String,

    /// Test configuration (severity, where, limit, etc.)
    #[serde(default)]
    pub config: TestConfig,
}

/// Singular test - standalone SQL test file that should return 0 rows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingularTest {
    /// Test name (derived from filename without extension)
    pub name: String,

    /// Path to the SQL test file
    pub path: PathBuf,

    /// SQL content - query that should return 0 rows if test passes
    pub sql: String,
}

impl SingularTest {
    /// Load a singular test from a SQL file
    pub fn from_file(path: PathBuf) -> Result<Self, CoreError> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| CoreError::ModelParseError {
                name: path.display().to_string(),
                message: "Invalid file name".to_string(),
            })?
            .to_string();

        let sql = std::fs::read_to_string(&path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;

        if sql.trim().is_empty() {
            return Err(CoreError::ModelParseError {
                name: name.clone(),
                message: "Test file is empty".to_string(),
            });
        }

        Ok(Self { name, path, sql })
    }
}

/// Types of schema tests
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestType {
    /// Column values must be unique
    Unique,
    /// Column values must not be null
    NotNull,
    /// Column values must be > 0
    Positive,
    /// Column values must be >= 0
    NonNegative,
    /// Column values must be in the allowed list
    AcceptedValues {
        /// List of allowed values
        values: Vec<String>,
        /// Whether to quote string values in SQL
        #[serde(default)]
        quote: bool,
    },
    /// Column values must be >= threshold
    MinValue {
        /// Minimum allowed value
        value: f64,
    },
    /// Column values must be <= threshold
    MaxValue {
        /// Maximum allowed value
        value: f64,
    },
    /// Column values must match the regex pattern
    Regex {
        /// Regex pattern to match
        pattern: String,
    },
    /// Column values must exist in referenced table (foreign key relationship)
    Relationship {
        /// Referenced model/table name
        to: String,
        /// Column in the referenced table (defaults to same column name)
        #[serde(default)]
        field: Option<String>,
    },
    /// Custom test macro (user-defined)
    Custom {
        /// Name of the test macro (without the test_ prefix)
        name: String,
        /// Additional keyword arguments passed to the macro
        #[serde(default, flatten)]
        kwargs: std::collections::HashMap<String, serde_json::Value>,
    },
}

/// Test severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TestSeverity {
    /// Test failure causes run to fail (default)
    #[default]
    Error,
    /// Test failure is logged as warning but doesn't fail run
    Warn,
}

impl std::fmt::Display for TestSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestSeverity::Error => write!(f, "error"),
            TestSeverity::Warn => write!(f, "warn"),
        }
    }
}

/// Configuration options for tests
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestConfig {
    /// Test severity level
    #[serde(default)]
    pub severity: TestSeverity,

    /// SQL WHERE clause to filter test
    #[serde(default)]
    pub where_clause: Option<String>,

    /// Max failing rows to return
    #[serde(default)]
    pub limit: Option<usize>,

    /// SQL condition that triggers error (e.g., "> 100")
    #[serde(default)]
    pub error_if: Option<String>,

    /// SQL condition that triggers warning (e.g., "> 10")
    #[serde(default)]
    pub warn_if: Option<String>,
}

/// Column definition in a model's schema YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumnDef {
    /// Column name
    pub name: String,

    /// SQL data type (e.g., VARCHAR, INT, TIMESTAMP, DECIMAL(10,2))
    #[serde(rename = "type", alias = "data_type")]
    pub data_type: String,

    /// Column description
    #[serde(default)]
    pub description: Option<String>,

    /// Whether this column is a primary key
    #[serde(default)]
    pub primary_key: bool,

    /// Column constraints for schema contracts (not_null, primary_key, unique)
    #[serde(default)]
    pub constraints: Vec<ColumnConstraint>,

    /// Tests to run on this column
    #[serde(default)]
    pub tests: Vec<TestDefinition>,

    /// Foreign key reference to another model's column
    #[serde(default)]
    pub references: Option<ColumnReference>,

    /// Data classification for governance (pii, sensitive, internal, public)
    #[serde(default)]
    pub classification: Option<DataClassification>,
}

/// Data classification level for governance and compliance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    /// Personally Identifiable Information (highest sensitivity)
    Pii,
    /// Sensitive business data
    Sensitive,
    /// Internal-only data
    Internal,
    /// Public data (lowest sensitivity)
    Public,
}

impl std::fmt::Display for DataClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataClassification::Pii => write!(f, "pii"),
            DataClassification::Sensitive => write!(f, "sensitive"),
            DataClassification::Internal => write!(f, "internal"),
            DataClassification::Public => write!(f, "public"),
        }
    }
}

/// Foreign key reference to another model's column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnReference {
    /// Referenced model name
    pub model: ModelName,
    /// Referenced column name
    pub column: String,
}

/// A test definition that can be either a simple string or a parameterized test
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TestDefinition {
    /// Simple test with no parameters (e.g., "unique", "not_null")
    Simple(String),
    /// Parameterized test (e.g., accepted_values with values list)
    Parameterized(std::collections::HashMap<String, TestParams>),
}

/// Parameters for parameterized tests
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestParams {
    /// Values for accepted_values test
    #[serde(default)]
    pub values: Vec<serde_yaml::Value>,
    /// Whether to quote values in SQL
    #[serde(default)]
    pub quote: bool,
    /// Threshold value for min_value/max_value tests
    #[serde(default)]
    pub value: Option<f64>,
    /// Pattern for regex tests
    #[serde(default)]
    pub pattern: Option<String>,
    /// Referenced model for relationship tests
    #[serde(default)]
    pub to: Option<String>,
    /// Referenced field for relationship tests (defaults to same column name)
    #[serde(default)]
    pub field: Option<String>,
}

impl Model {
    /// Create a new model from a file path
    ///
    /// This also looks for a matching 1:1 schema file (same name with .yml or .yaml extension).
    /// Every model must have a corresponding YAML schema file — returns an error if missing.
    pub fn from_file(path: PathBuf) -> Result<Self, crate::error::CoreError> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| crate::error::CoreError::ModelParseError {
                name: path.display().to_string(),
                message: "Cannot extract model name from path".to_string(),
            })?
            .to_string();

        let raw_sql = std::fs::read_to_string(&path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;

        // Look for matching 1:1 schema file (required)
        let yml_path = path.with_extension("yml");
        let yaml_path = path.with_extension("yaml");

        let schema = if yml_path.exists() {
            Some(ModelSchema::load(&yml_path)?)
        } else if yaml_path.exists() {
            Some(ModelSchema::load(&yaml_path)?)
        } else {
            return Err(crate::error::CoreError::MissingSchemaFile {
                model: name,
                expected_path: yml_path.display().to_string(),
            });
        };

        // Parse version from name (e.g., "fct_orders_v2" -> base="fct_orders", version=2)
        let (base_name, version) = Self::parse_version(&name);

        Ok(Self {
            name: ModelName::new(name),
            path,
            raw_sql,
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema,
            base_name,
            version,
        })
    }

    /// Parse version suffix from model name
    ///
    /// Returns (base_name, version) where base_name is Some if the model follows _v{N} convention
    /// Examples:
    /// - "fct_orders_v2" -> (Some("fct_orders"), Some(2))
    /// - "fct_orders_v10" -> (Some("fct_orders"), Some(10))
    /// - "fct_orders" -> (None, None)
    /// - "v2_model" -> (None, None) // v must be suffix, not prefix
    pub fn parse_version(name: &str) -> (Option<String>, Option<u32>) {
        // Look for _v{N} pattern at the end of the name
        if let Some(idx) = name.rfind("_v") {
            let suffix = &name[idx + 2..];
            if let Ok(version) = suffix.parse::<u32>() {
                let base_name = name[..idx].to_string();
                return (Some(base_name), Some(version));
            }
        }
        (None, None)
    }

    /// Get the base name for this model (name without version suffix)
    pub fn get_base_name(&self) -> &str {
        self.base_name.as_deref().unwrap_or(&self.name)
    }

    /// Check if this model is a versioned model (has _v{N} suffix)
    pub fn is_versioned(&self) -> bool {
        self.version.is_some()
    }

    /// Get the version number if this is a versioned model
    pub fn get_version(&self) -> Option<u32> {
        self.version
    }

    /// Check if this model is deprecated
    pub fn is_deprecated(&self) -> bool {
        self.schema.as_ref().map(|s| s.deprecated).unwrap_or(false)
    }

    /// Get the deprecation message if this model is deprecated
    pub fn get_deprecation_message(&self) -> Option<&str> {
        self.schema
            .as_ref()
            .and_then(|s| s.deprecation_message.as_deref())
    }

    /// Get the materialization for this model, falling back through the precedence chain:
    /// 1. SQL config() function (self.config.materialized)
    /// 2. Project default
    pub fn materialization(&self, default: Materialization) -> Materialization {
        self.config.materialized.unwrap_or(default)
    }

    /// Get the schema for this model, falling back through the precedence chain:
    /// 1. SQL config() function (self.config.schema)
    /// 2. Project default
    pub fn target_schema(&self, default: Option<&str>) -> Option<String> {
        self.config
            .schema
            .clone()
            .or_else(|| default.map(String::from))
    }

    /// Check if WAP is enabled for this model
    ///
    /// Follows the precedence chain:
    /// 1. SQL config() function (self.config.wap)
    /// 2. Default: false
    pub fn wap_enabled(&self) -> bool {
        self.config.wap.unwrap_or(false)
    }

    /// Get all dependencies (both model and external)
    pub fn all_dependencies(&self) -> HashSet<String> {
        let mut deps: HashSet<String> = self.depends_on.iter().map(|m| m.to_string()).collect();
        deps.extend(self.external_deps.iter().map(|t| t.to_string()));
        deps
    }

    /// Get tests from the model's 1:1 schema file
    pub fn get_schema_tests(&self) -> Vec<SchemaTest> {
        match &self.schema {
            Some(schema) => schema.extract_tests(&self.name),
            None => Vec::new(),
        }
    }

    /// Check if this model is configured for incremental materialization
    pub fn is_incremental_model(&self, default: Materialization) -> bool {
        self.materialization(default) == Materialization::Incremental
    }

    /// Get the incremental strategy for this model
    pub fn incremental_strategy(&self) -> IncrementalStrategy {
        self.config
            .incremental_strategy
            .unwrap_or(IncrementalStrategy::Append)
    }

    /// Get the unique key(s) for incremental merge
    pub fn unique_key(&self) -> Option<Vec<String>> {
        self.config.unique_key.as_ref().map(|k| {
            k.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    }

    /// Get the on_schema_change behavior
    pub fn on_schema_change(&self) -> OnSchemaChange {
        self.config
            .on_schema_change
            .unwrap_or(OnSchemaChange::Ignore)
    }

    /// Compute a SHA-256 checksum of the raw SQL content.
    pub fn sql_checksum(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.raw_sql.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Get the owner for this model from schema metadata
    pub fn get_owner(&self) -> Option<String> {
        self.schema.as_ref().and_then(|s| s.get_owner())
    }

    /// Get a metadata value from the model's schema
    pub fn get_meta(&self, key: &str) -> Option<&serde_yaml::Value> {
        self.schema.as_ref().and_then(|s| s.get_meta(key))
    }

    /// Get a metadata value as a string
    pub fn get_meta_string(&self, key: &str) -> Option<String> {
        self.schema.as_ref().and_then(|s| s.get_meta_string(key))
    }
}

// SchemaYml impl moved to #[cfg(test)] — only used in tests

impl std::fmt::Display for TestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestType::Unique => write!(f, "unique"),
            TestType::NotNull => write!(f, "not_null"),
            TestType::Positive => write!(f, "positive"),
            TestType::NonNegative => write!(f, "non_negative"),
            TestType::AcceptedValues { .. } => write!(f, "accepted_values"),
            TestType::MinValue { .. } => write!(f, "min_value"),
            TestType::MaxValue { .. } => write!(f, "max_value"),
            TestType::Regex { .. } => write!(f, "regex"),
            TestType::Relationship { .. } => write!(f, "relationship"),
            TestType::Custom { name, .. } => write!(f, "{}", name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Legacy schema.yml container type (only used in tests)
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SchemaYml {
        version: u32,
        #[serde(default)]
        models: Vec<SchemaModelDef>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SchemaModelDef {
        name: String,
        #[serde(default)]
        columns: Vec<SchemaColumnDef>,
    }

    impl SchemaYml {
        fn parse(content: &str) -> Result<Self, serde_yaml::Error> {
            serde_yaml::from_str(content)
        }

        fn extract_tests(&self) -> Vec<SchemaTest> {
            let mut tests = Vec::new();
            for model_def in &self.models {
                for column_def in &model_def.columns {
                    for test_def in &column_def.tests {
                        if let Some(test_type) = parse_test_definition(test_def) {
                            tests.push(SchemaTest {
                                test_type,
                                column: column_def.name.clone(),
                                model: model_def.name.clone(),
                                config: TestConfig::default(),
                            });
                        }
                    }
                }
            }
            tests
        }
    }

    #[test]
    fn test_parse_schema_yml() {
        let yaml = r#"
version: 1

models:
  - name: stg_orders
    columns:
      - name: order_id
        type: INTEGER
        tests:
          - unique
          - not_null
      - name: customer_id
        type: INTEGER
        tests:
          - not_null
"#;
        let schema = SchemaYml::parse(yaml).unwrap();
        assert_eq!(schema.version, 1);
        assert_eq!(schema.models.len(), 1);
        assert_eq!(schema.models[0].columns.len(), 2);
    }

    #[test]
    fn test_extract_tests() {
        let yaml = r#"
version: 1
models:
  - name: stg_orders
    columns:
      - name: order_id
        type: INTEGER
        tests:
          - unique
          - not_null
"#;
        let schema = SchemaYml::parse(yaml).unwrap();
        let tests = schema.extract_tests();

        assert_eq!(tests.len(), 2);
        assert_eq!(tests[0].model, "stg_orders");
        assert_eq!(tests[0].column, "order_id");
        assert_eq!(tests[0].test_type, TestType::Unique);
    }

    #[test]
    fn test_parse_model_schema_1to1() {
        let yaml = r#"
version: 1
description: "Staged orders from raw source"
owner: data-team
tags:
  - staging
  - orders
columns:
  - name: order_id
    type: INTEGER
    description: "Unique identifier for the order"
    tests:
      - unique
      - not_null
  - name: customer_id
    type: INTEGER
    tests:
      - not_null
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(schema.version, 1);
        assert_eq!(
            schema.description,
            Some("Staged orders from raw source".to_string())
        );
        assert_eq!(schema.owner, Some("data-team".to_string()));
        assert_eq!(schema.tags, vec!["staging", "orders"]);
        assert_eq!(schema.columns.len(), 2);
        // get_owner should return the direct owner field
        assert_eq!(schema.get_owner(), Some("data-team".to_string()));
    }

    #[test]
    fn test_parse_owner_metadata() {
        let yaml = r##"
version: 1
meta:
  owner: analytics-team@example.com
  team: Analytics
  slack_channel: "#data-alerts"
  pagerduty_service: data-platform
"##;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        // Direct owner field should be None
        assert!(schema.owner.is_none());

        // Meta fields should be populated
        assert_eq!(
            schema.get_meta_string("owner"),
            Some("analytics-team@example.com".to_string())
        );
        assert_eq!(
            schema.get_meta_string("team"),
            Some("Analytics".to_string())
        );
        assert_eq!(
            schema.get_meta_string("slack_channel"),
            Some("#data-alerts".to_string())
        );

        // get_owner should fall back to meta.owner
        assert_eq!(
            schema.get_owner(),
            Some("analytics-team@example.com".to_string())
        );
    }

    #[test]
    fn test_owner_direct_takes_precedence_over_meta() {
        let yaml = r#"
version: 1
owner: direct-owner
meta:
  owner: meta-owner
  team: Analytics
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        // get_owner should return direct owner over meta.owner
        assert_eq!(schema.get_owner(), Some("direct-owner".to_string()));

        // But we can still access meta.owner directly
        assert_eq!(
            schema.get_meta_string("owner"),
            Some("meta-owner".to_string())
        );
    }

    #[test]
    fn test_model_schema_extract_tests() {
        let yaml = r#"
version: 1
columns:
  - name: order_id
    type: INTEGER
    tests:
      - unique
      - not_null
  - name: customer_id
    type: INTEGER
    tests:
      - not_null
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("stg_orders");

        assert_eq!(tests.len(), 3);
        assert!(tests
            .iter()
            .any(|t| t.column == "order_id" && t.test_type == TestType::Unique));
        assert!(tests
            .iter()
            .any(|t| t.column == "order_id" && t.test_type == TestType::NotNull));
        assert!(tests
            .iter()
            .any(|t| t.column == "customer_id" && t.test_type == TestType::NotNull));
    }

    #[test]
    fn test_parse_positive_test() {
        let yaml = r#"
version: 1
columns:
  - name: amount
    type: DECIMAL(10,2)
    tests:
      - positive
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("orders");

        assert_eq!(tests.len(), 1);
        assert_eq!(tests[0].test_type, TestType::Positive);
    }

    #[test]
    fn test_parse_non_negative_test() {
        let yaml = r#"
version: 1
columns:
  - name: quantity
    type: INTEGER
    tests:
      - non_negative
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("orders");

        assert_eq!(tests.len(), 1);
        assert_eq!(tests[0].test_type, TestType::NonNegative);
    }

    #[test]
    fn test_parse_accepted_values_test() {
        let yaml = r#"
version: 1
columns:
  - name: status
    type: VARCHAR
    tests:
      - accepted_values:
          values: [pending, completed, cancelled]
          quote: true
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("orders");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::AcceptedValues { values, quote } => {
                assert_eq!(values, &["pending", "completed", "cancelled"]);
                assert!(*quote);
            }
            _ => panic!("Expected AcceptedValues test type"),
        }
    }

    #[test]
    fn test_parse_min_value_test() {
        let yaml = r#"
version: 1
columns:
  - name: price
    type: DECIMAL(10,2)
    tests:
      - min_value:
          value: 0.0
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("products");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::MinValue { value } => {
                assert_eq!(*value, 0.0);
            }
            _ => panic!("Expected MinValue test type"),
        }
    }

    #[test]
    fn test_parse_max_value_test() {
        let yaml = r#"
version: 1
columns:
  - name: discount
    type: DECIMAL(10,2)
    tests:
      - max_value:
          value: 100.0
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("products");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::MaxValue { value } => {
                assert_eq!(*value, 100.0);
            }
            _ => panic!("Expected MaxValue test type"),
        }
    }

    #[test]
    fn test_parse_regex_test() {
        let yaml = r#"
version: 1
columns:
  - name: email
    type: VARCHAR
    tests:
      - regex:
          pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("users");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::Regex { pattern } => {
                assert!(pattern.contains("@"));
            }
            _ => panic!("Expected Regex test type"),
        }
    }

    #[test]
    fn test_parse_mixed_tests() {
        let yaml = r#"
version: 1
columns:
  - name: order_id
    type: INTEGER
    tests:
      - unique
      - not_null
  - name: amount
    type: DECIMAL(10,2)
    tests:
      - positive
      - min_value:
          value: 1.0
  - name: status
    type: VARCHAR
    tests:
      - accepted_values:
          values: [pending, completed]
          quote: true
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("orders");

        assert_eq!(tests.len(), 5);
    }

    #[test]
    fn test_parse_column_full_schema() {
        let yaml = r#"
version: 1
columns:
  - name: user_id
    type: BIGINT
    description: "Unique identifier for the user"
    primary_key: true
    tests:
      - unique
      - not_null
  - name: customer_id
    type: BIGINT
    description: "Foreign key to customers table"
    references:
      model: dim_customers
      column: customer_id
    tests:
      - not_null
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(schema.columns.len(), 2);

        let user_col = &schema.columns[0];
        assert_eq!(user_col.name, "user_id");
        assert_eq!(user_col.data_type, "BIGINT");
        assert_eq!(
            user_col.description,
            Some("Unique identifier for the user".to_string())
        );
        assert!(user_col.primary_key);
        assert!(user_col.references.is_none());

        let customer_col = &schema.columns[1];
        assert_eq!(customer_col.name, "customer_id");
        assert!(!customer_col.primary_key);
        assert!(customer_col.references.is_some());
        let refs = customer_col.references.as_ref().unwrap();
        assert_eq!(refs.model, "dim_customers");
        assert_eq!(refs.column, "customer_id");
    }

    #[test]
    fn test_yaml_with_config_key_fails_to_parse() {
        let yaml = r#"
version: 1
description: "Test model"
config:
  materialized: table
  schema: staging
columns:
  - name: id
    type: INTEGER
    tests:
      - unique
"#;
        let result: Result<ModelSchema, _> = serde_yaml::from_str(yaml);
        assert!(
            result.is_err(),
            "YAML with config: key should fail to parse"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field, got: {err}"
        );
    }

    #[test]
    fn test_config_precedence_sql_wins() {
        use crate::config::Materialization;

        // Create a model with SQL config set
        let model = Model {
            name: ModelName::new("test"),
            path: std::path::PathBuf::from("test.sql"),
            raw_sql: String::new(),
            compiled_sql: None,
            config: ModelConfig {
                materialized: Some(Materialization::Table),
                schema: Some("sql_schema".to_string()),
                tags: vec![],
                unique_key: None,
                incremental_strategy: None,
                on_schema_change: None,
                pre_hook: vec![],
                post_hook: vec![],
                wap: None,
            },
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema: Some(ModelSchema {
                version: 1,
                name: None,
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: vec![],
                contract: None,
                freshness: None,
                columns: vec![],
                deprecated: false,
                deprecation_message: None,
            }),
            base_name: None,
            version: None,
        };

        // SQL config should win over project default
        assert_eq!(
            model.materialization(Materialization::View),
            Materialization::Table
        );
        assert_eq!(model.target_schema(None), Some("sql_schema".to_string()));
    }

    #[test]
    fn test_config_precedence_falls_back_to_project_default() {
        use crate::config::Materialization;

        // Create a model with no SQL config — should fall back to project default
        let model = Model {
            name: ModelName::new("test"),
            path: std::path::PathBuf::from("test.sql"),
            raw_sql: String::new(),
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema: Some(ModelSchema {
                version: 1,
                name: None,
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: vec![],
                contract: None,
                freshness: None,
                columns: vec![],
                deprecated: false,
                deprecation_message: None,
            }),
            base_name: None,
            version: None,
        };

        // Should use the passed-in project default
        assert_eq!(
            model.materialization(Materialization::View),
            Materialization::View
        );
        assert_eq!(model.target_schema(None), None);
        assert_eq!(
            model.target_schema(Some("default_schema")),
            Some("default_schema".to_string())
        );
    }

    #[test]
    fn test_config_precedence_project_default() {
        use crate::config::Materialization;

        // Create a model with no config
        let model = Model {
            name: ModelName::new("test"),
            path: std::path::PathBuf::from("test.sql"),
            raw_sql: String::new(),
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema: None,
            base_name: None,
            version: None,
        };

        // Project default should be used
        assert_eq!(
            model.materialization(Materialization::View),
            Materialization::View
        );
        assert_eq!(
            model.target_schema(Some("default_schema")),
            Some("default_schema".to_string())
        );
    }

    #[test]
    fn test_model_config_hooks_default() {
        let config = ModelConfig::default();
        assert!(config.pre_hook.is_empty());
        assert!(config.post_hook.is_empty());
    }

    #[test]
    fn test_model_config_with_hooks() {
        let config = ModelConfig {
            materialized: None,
            schema: None,
            tags: vec![],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec!["CREATE INDEX IF NOT EXISTS idx_id ON {{ this }}(id)".to_string()],
            post_hook: vec![
                "ANALYZE {{ this }}".to_string(),
                "GRANT SELECT ON {{ this }} TO analyst".to_string(),
            ],
            wap: None,
        };
        assert_eq!(config.pre_hook.len(), 1);
        assert_eq!(config.post_hook.len(), 2);
        assert!(config.pre_hook[0].contains("CREATE INDEX"));
        assert!(config.post_hook[0].contains("ANALYZE"));
    }

    #[test]
    fn test_test_severity_default() {
        let severity = TestSeverity::default();
        assert_eq!(severity, TestSeverity::Error);
    }

    #[test]
    fn test_test_severity_display() {
        assert_eq!(TestSeverity::Error.to_string(), "error");
        assert_eq!(TestSeverity::Warn.to_string(), "warn");
    }

    #[test]
    fn test_test_config_default() {
        let config = TestConfig::default();
        assert_eq!(config.severity, TestSeverity::Error);
        assert!(config.where_clause.is_none());
        assert!(config.limit.is_none());
        assert!(config.error_if.is_none());
        assert!(config.warn_if.is_none());
    }

    #[test]
    fn test_test_config_with_severity() {
        let config = TestConfig {
            severity: TestSeverity::Warn,
            where_clause: Some("status = 'active'".to_string()),
            limit: Some(100),
            error_if: Some("> 100".to_string()),
            warn_if: Some("> 10".to_string()),
        };
        assert_eq!(config.severity, TestSeverity::Warn);
        assert_eq!(config.where_clause, Some("status = 'active'".to_string()));
        assert_eq!(config.limit, Some(100));
        assert_eq!(config.error_if, Some("> 100".to_string()));
        assert_eq!(config.warn_if, Some("> 10".to_string()));
    }

    #[test]
    fn test_schema_test_with_config() {
        let test = SchemaTest {
            test_type: TestType::Unique,
            column: "id".to_string(),
            model: "users".to_string(),
            config: TestConfig {
                severity: TestSeverity::Warn,
                ..Default::default()
            },
        };
        assert_eq!(test.config.severity, TestSeverity::Warn);
    }

    #[test]
    fn test_parse_relationship_test() {
        let yaml = r#"
version: 1
columns:
  - name: customer_id
    type: INTEGER
    tests:
      - relationship:
          to: customers
          field: id
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("orders");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::Relationship { to, field } => {
                assert_eq!(to, "customers");
                assert_eq!(field, &Some("id".to_string()));
            }
            _ => panic!("Expected Relationship test type"),
        }
    }

    #[test]
    fn test_parse_relationship_test_default_field() {
        let yaml = r#"
version: 1
columns:
  - name: user_id
    type: INTEGER
    tests:
      - relationship:
          to: users
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("posts");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::Relationship { to, field } => {
                assert_eq!(to, "users");
                assert!(field.is_none());
            }
            _ => panic!("Expected Relationship test type"),
        }
    }

    #[test]
    fn test_relationship_test_display() {
        let test_type = TestType::Relationship {
            to: "customers".to_string(),
            field: Some("id".to_string()),
        };
        assert_eq!(test_type.to_string(), "relationship");
    }

    #[test]
    fn test_parse_relationships_alias() {
        // dbt uses "relationships" (plural) - we support both
        let yaml = r#"
version: 1
columns:
  - name: order_id
    type: INTEGER
    tests:
      - relationships:
          to: orders
          field: id
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        let tests = schema.extract_tests("order_items");

        assert_eq!(tests.len(), 1);
        match &tests[0].test_type {
            TestType::Relationship { to, field } => {
                assert_eq!(to, "orders");
                assert_eq!(field, &Some("id".to_string()));
            }
            _ => panic!("Expected Relationship test type"),
        }
    }

    #[test]
    fn test_parse_contract_definition() {
        let yaml = r#"
version: 1
name: fct_orders
contract:
  enforced: true
columns:
  - name: order_id
    data_type: INTEGER
    constraints:
      - not_null
      - primary_key
  - name: customer_id
    data_type: INTEGER
    constraints:
      - not_null
  - name: total_amount
    data_type: DECIMAL
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        // Verify contract is parsed
        assert!(schema.contract.is_some());
        let contract = schema.contract.unwrap();
        assert!(contract.enforced);

        // Verify column constraints are parsed
        assert_eq!(schema.columns.len(), 3);

        let order_id_col = &schema.columns[0];
        assert_eq!(order_id_col.name, "order_id");
        assert_eq!(order_id_col.data_type, "INTEGER");
        assert_eq!(order_id_col.constraints.len(), 2);
        assert!(order_id_col
            .constraints
            .contains(&ColumnConstraint::NotNull));
        assert!(order_id_col
            .constraints
            .contains(&ColumnConstraint::PrimaryKey));

        let customer_id_col = &schema.columns[1];
        assert_eq!(customer_id_col.constraints.len(), 1);
        assert!(customer_id_col
            .constraints
            .contains(&ColumnConstraint::NotNull));

        let total_amount_col = &schema.columns[2];
        assert!(total_amount_col.constraints.is_empty());
    }

    #[test]
    fn test_contract_not_enforced() {
        let yaml = r#"
version: 1
contract:
  enforced: false
columns:
  - name: id
    type: INTEGER
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        assert!(schema.contract.is_some());
        assert!(!schema.has_enforced_contract());
        let contract = schema.contract.as_ref().unwrap();
        assert!(!contract.enforced);
    }

    #[test]
    fn test_no_contract_section() {
        let yaml = r#"
version: 1
columns:
  - name: id
    type: INTEGER
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        assert!(schema.contract.is_none());
        assert!(!schema.has_enforced_contract());
    }

    #[test]
    fn test_contract_helper_methods() {
        let yaml = r#"
version: 1
contract:
  enforced: true
columns:
  - name: order_id
    data_type: INTEGER
  - name: customer_id
    data_type: VARCHAR
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        // Test has_enforced_contract
        assert!(schema.has_enforced_contract());

        // Test get_contract
        assert!(schema.get_contract().is_some());

        // Test get_column
        let order_id = schema.get_column("order_id");
        assert!(order_id.is_some());
        assert_eq!(order_id.unwrap().data_type, "INTEGER");

        // Case-insensitive lookup
        let order_id_upper = schema.get_column("ORDER_ID");
        assert!(order_id_upper.is_some());

        // Non-existent column
        let missing = schema.get_column("nonexistent");
        assert!(missing.is_none());

        // Test column_names
        let names = schema.column_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"order_id"));
        assert!(names.contains(&"customer_id"));
    }

    #[test]
    fn test_column_constraint_unique() {
        let yaml = r#"
version: 1
columns:
  - name: email
    type: VARCHAR
    constraints:
      - unique
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        let email_col = &schema.columns[0];
        assert_eq!(email_col.constraints.len(), 1);
        assert!(email_col.constraints.contains(&ColumnConstraint::Unique));
    }

    #[test]
    fn test_parse_model_freshness() {
        let yaml = r#"
version: 1
freshness:
  loaded_at_field: updated_at
  warn_after:
    count: 4
    period: hour
  error_after:
    count: 8
    period: hour
columns:
  - name: id
    type: INTEGER
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        assert!(schema.has_freshness());
        let freshness = schema.get_freshness().unwrap();
        assert_eq!(freshness.loaded_at_field, "updated_at");

        let warn = freshness.warn_after.as_ref().unwrap();
        assert_eq!(warn.count, 4);
        assert_eq!(warn.period, FreshnessPeriod::Hour);
        assert_eq!(warn.to_seconds(), 4 * 3600);

        let error = freshness.error_after.as_ref().unwrap();
        assert_eq!(error.count, 8);
        assert_eq!(error.period, FreshnessPeriod::Hour);
        assert_eq!(error.to_seconds(), 8 * 3600);
    }

    #[test]
    fn test_freshness_warn_only() {
        let yaml = r#"
version: 1
freshness:
  loaded_at_field: created_at
  warn_after:
    count: 2
    period: day
columns:
  - name: id
    type: INTEGER
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        let freshness = schema.get_freshness().unwrap();
        assert_eq!(freshness.loaded_at_field, "created_at");

        let warn = freshness.warn_after.as_ref().unwrap();
        assert_eq!(warn.count, 2);
        assert_eq!(warn.period, FreshnessPeriod::Day);
        assert_eq!(warn.to_seconds(), 2 * 86400);

        // No error_after
        assert!(freshness.error_after.is_none());
    }

    #[test]
    fn test_freshness_minutes() {
        let yaml = r#"
version: 1
freshness:
  loaded_at_field: last_sync
  warn_after:
    count: 30
    period: minute
  error_after:
    count: 60
    period: minute
columns: []
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        let freshness = schema.get_freshness().unwrap();
        assert_eq!(freshness.loaded_at_field, "last_sync");

        let warn = freshness.warn_after.as_ref().unwrap();
        assert_eq!(warn.count, 30);
        assert_eq!(warn.period, FreshnessPeriod::Minute);
        assert_eq!(warn.to_seconds(), 30 * 60);

        let error = freshness.error_after.as_ref().unwrap();
        assert_eq!(error.to_seconds(), 60 * 60);
    }

    #[test]
    fn test_no_freshness() {
        let yaml = r#"
version: 1
columns:
  - name: id
    type: INTEGER
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        assert!(!schema.has_freshness());
        assert!(schema.get_freshness().is_none());
    }

    #[test]
    fn test_freshness_period_display() {
        assert_eq!(FreshnessPeriod::Minute.to_string(), "minute");
        assert_eq!(FreshnessPeriod::Hour.to_string(), "hour");
        assert_eq!(FreshnessPeriod::Day.to_string(), "day");
    }

    #[test]
    fn test_freshness_threshold_conversions() {
        // Test various threshold conversions
        assert_eq!(
            FreshnessThreshold::new(1, FreshnessPeriod::Minute).to_seconds(),
            60
        );
        assert_eq!(
            FreshnessThreshold::new(1, FreshnessPeriod::Hour).to_seconds(),
            3600
        );
        assert_eq!(
            FreshnessThreshold::new(1, FreshnessPeriod::Day).to_seconds(),
            86400
        );

        // Test larger counts
        assert_eq!(
            FreshnessThreshold::new(24, FreshnessPeriod::Hour).to_seconds(),
            24 * 3600
        );
        assert_eq!(
            FreshnessThreshold::new(7, FreshnessPeriod::Day).to_seconds(),
            7 * 86400
        );
    }

    #[test]
    fn test_parse_version_suffix() {
        // Standard version suffix
        let (base, version) = Model::parse_version("fct_orders_v2");
        assert_eq!(base, Some("fct_orders".to_string()));
        assert_eq!(version, Some(2));

        // Larger version number
        let (base, version) = Model::parse_version("stg_customers_v10");
        assert_eq!(base, Some("stg_customers".to_string()));
        assert_eq!(version, Some(10));

        // No version suffix
        let (base, version) = Model::parse_version("dim_products");
        assert_eq!(base, None);
        assert_eq!(version, None);

        // v at start (should NOT match)
        let (base, version) = Model::parse_version("v2_model");
        assert_eq!(base, None);
        assert_eq!(version, None);

        // Underscore but no number
        let (base, version) = Model::parse_version("model_vx");
        assert_eq!(base, None);
        assert_eq!(version, None);

        // Multiple underscores
        let (base, version) = Model::parse_version("my_cool_model_v3");
        assert_eq!(base, Some("my_cool_model".to_string()));
        assert_eq!(version, Some(3));
    }

    #[test]
    fn test_model_version_methods() {
        // Create a versioned model
        let mut model = Model {
            name: ModelName::new("fct_orders_v2"),
            path: std::path::PathBuf::from("models/fct_orders_v2.sql"),
            raw_sql: "SELECT 1".to_string(),
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: std::collections::HashSet::new(),
            external_deps: std::collections::HashSet::new(),
            schema: None,
            base_name: Some("fct_orders".to_string()),
            version: Some(2),
        };

        assert!(model.is_versioned());
        assert_eq!(model.get_version(), Some(2));
        assert_eq!(model.get_base_name(), "fct_orders");

        // Non-versioned model
        model.base_name = None;
        model.version = None;
        model.name = ModelName::new("dim_products");

        assert!(!model.is_versioned());
        assert_eq!(model.get_version(), None);
        assert_eq!(model.get_base_name(), "dim_products");
    }

    #[test]
    fn test_deprecated_model() {
        let yaml = r#"
version: 1
deprecated: true
deprecation_message: "Use fct_orders_v2 instead"
columns:
  - name: id
    type: INTEGER
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

        assert!(schema.deprecated);
        assert_eq!(
            schema.deprecation_message,
            Some("Use fct_orders_v2 instead".to_string())
        );
    }

    #[test]
    fn test_deprecated_model_via_model() {
        let mut model = Model {
            name: ModelName::new("fct_orders_v1"),
            path: std::path::PathBuf::from("models/fct_orders_v1.sql"),
            raw_sql: "SELECT 1".to_string(),
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: std::collections::HashSet::new(),
            external_deps: std::collections::HashSet::new(),
            schema: Some(ModelSchema {
                version: 1,
                name: None,
                description: None,
                owner: None,
                meta: std::collections::HashMap::new(),
                tags: vec![],
                contract: None,
                freshness: None,
                columns: vec![],
                deprecated: true,
                deprecation_message: Some("Use v2".to_string()),
            }),
            base_name: Some("fct_orders".to_string()),
            version: Some(1),
        };

        assert!(model.is_deprecated());
        assert_eq!(model.get_deprecation_message(), Some("Use v2"));

        // Non-deprecated model
        model.schema.as_mut().unwrap().deprecated = false;
        assert!(!model.is_deprecated());
    }
}
