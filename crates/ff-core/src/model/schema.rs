//! Schema metadata types for model YAML files

use super::testing::{parse_test_definition, SchemaTest, TestConfig, TestDefinition};
use crate::error::CoreError;
use crate::model_name::ModelName;
use serde::{Deserialize, Serialize};

/// The kind of resource described by a model directory.
///
/// Accepts both modern (`sql`) and legacy (`model`) names.
/// Comparison via [`PartialEq`] treats `Sql` and `Model` as equivalent.
#[derive(Debug, Clone, Copy, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ModelKind {
    /// A SQL model (default) — directory contains a `.sql` file
    #[default]
    Model,
    /// Modern alias for [`ModelKind::Model`]
    Sql,
    /// A CSV seed — directory contains a `.csv` file
    Seed,
    /// A Python model — directory contains a `.py` file, executed via `uv run`
    Python,
}

impl PartialEq for ModelKind {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(&self.normalize()) == std::mem::discriminant(&other.normalize())
    }
}

impl ModelKind {
    /// Collapse legacy `Model` to the canonical `Sql` form.
    fn normalize(self) -> ModelKind {
        match self {
            ModelKind::Model | ModelKind::Sql => ModelKind::Sql,
            other => other,
        }
    }

    /// Returns `true` if this is a SQL model kind (either `model` or `sql`).
    pub fn is_sql(&self) -> bool {
        matches!(self, ModelKind::Model | ModelKind::Sql)
    }
}

impl std::fmt::Display for ModelKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelKind::Model | ModelKind::Sql => write!(f, "sql"),
            ModelKind::Seed => write!(f, "seed"),
            ModelKind::Python => write!(f, "python"),
        }
    }
}

/// Python-specific configuration for `kind: python` models
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PythonConfig {
    /// Minimum Python version requirement (e.g., ">=3.11")
    #[serde(default, rename = "requires-python")]
    pub requires_python: Option<String>,

    /// Python package dependencies (e.g., ["pandas>=2.0", "scikit-learn>=1.3"])
    #[serde(default)]
    pub dependencies: Vec<String>,
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

    /// Resource kind: `model` (default) or `seed`
    #[serde(default)]
    pub kind: ModelKind,

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

    /// Column definitions
    #[serde(default)]
    pub columns: Vec<SchemaColumnDef>,

    /// Whether this model is deprecated
    #[serde(default)]
    pub deprecated: bool,

    /// Deprecation message to show users (e.g., "Use fct_orders_v2 instead")
    #[serde(default)]
    pub deprecation_message: Option<String>,

    // ── Python-specific fields (only relevant when kind: python) ──────
    /// Explicit dependency list for Python models (kind: python only).
    /// Since Python scripts cannot be parsed for SQL table references,
    /// dependencies must be declared here.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Python-specific configuration (kind: python only)
    #[serde(default)]
    pub python: Option<PythonConfig>,

    // ── Seed-specific fields (only relevant when kind: seed) ─────────
    /// Override target schema for seed loading (kind: seed only)
    #[serde(default)]
    pub schema: Option<String>,

    /// Force column quoting for seed CSV (kind: seed only)
    #[serde(default)]
    pub quote_columns: bool,

    /// Override inferred types for specific columns (kind: seed only).
    /// Key: column name, Value: SQL type (e.g., "VARCHAR", "INTEGER")
    #[serde(default)]
    pub column_types: std::collections::HashMap<String, String>,

    /// CSV delimiter for seed loading (kind: seed only, default: ',')
    #[serde(default = "default_delimiter")]
    pub delimiter: char,

    /// Enable/disable this seed (kind: seed only, default: true)
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_delimiter() -> char {
    ','
}

fn default_enabled() -> bool {
    true
}

impl Default for ModelSchema {
    fn default() -> Self {
        Self {
            version: 1,
            kind: ModelKind::default(),
            name: None,
            description: None,
            owner: None,
            meta: std::collections::HashMap::new(),
            tags: Vec::new(),
            contract: None,
            columns: Vec::new(),
            deprecated: false,
            deprecation_message: None,
            depends_on: Vec::new(),
            python: None,
            schema: None,
            quote_columns: false,
            column_types: std::collections::HashMap::new(),
            delimiter: default_delimiter(),
            enabled: default_enabled(),
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
        let schema: ModelSchema = serde_yaml::from_str(&content).map_err(|e| {
            use serde::de::Error as _;
            CoreError::YamlParse(serde_yaml::Error::custom(format!(
                "{}: {}",
                path.display(),
                e
            )))
        })?;
        if schema.version != 1 {
            return Err(CoreError::UnsupportedSchemaVersion {
                version: schema.version,
            });
        }
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

    /// Get owner - prefers direct owner field, falls back to meta.owner
    pub fn get_owner(&self) -> Option<String> {
        if let Some(owner) = &self.owner {
            return Some(owner.clone());
        }
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
        self.columns
            .iter()
            .flat_map(|column| {
                column.tests.iter().filter_map(move |test_def| {
                    parse_test_definition(test_def).map(|test_type| SchemaTest {
                        test_type,
                        column: column.name.clone(),
                        model: crate::model_name::ModelName::new(model_name),
                        config: TestConfig::default(),
                    })
                })
            })
            .collect()
    }
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

#[cfg(test)]
#[path = "schema_test.rs"]
mod tests;
