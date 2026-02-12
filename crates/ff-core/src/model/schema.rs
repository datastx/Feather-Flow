//! Schema metadata types for model YAML files

use super::freshness::FreshnessConfig;
use super::testing::{parse_test_definition, SchemaTest, TestConfig, TestDefinition};
use crate::error::CoreError;
use crate::model_name::ModelName;
use serde::{Deserialize, Serialize};

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
