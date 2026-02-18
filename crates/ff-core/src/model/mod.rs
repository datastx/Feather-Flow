//! Model representation

pub mod schema;
pub mod testing;

// Re-export all public types to preserve existing `ff_core::model::*` paths
pub use schema::{
    ColumnConstraint, ColumnReference, DataClassification, ModelKind, ModelSchema, SchemaColumnDef,
    SchemaContract,
};
pub use testing::{
    parse_test_definition, SchemaTest, SingularTest, TestConfig, TestDefinition, TestParams,
    TestSeverity, TestType,
};

use crate::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use crate::error::CoreError;
use crate::model_name::ModelName;
use crate::table_name::TableName;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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

    /// Arbitrary key-value metadata from the config() function
    #[serde(default)]
    pub meta: HashMap<String, String>,
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

        if raw_sql.trim().is_empty() {
            return Err(CoreError::ModelParseError {
                name,
                message: "SQL file is empty".into(),
            });
        }

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
    pub fn target_schema<'a>(&'a self, default: Option<&'a str>) -> Option<&'a str> {
        self.config.schema.as_deref().or(default)
    }

    /// Check if WAP is enabled for this model
    ///
    /// Follows the precedence chain:
    /// 1. SQL config() function (self.config.wap)
    /// 2. Default: false
    pub fn wap_enabled(&self) -> bool {
        self.config.wap.unwrap_or(false)
    }

    /// Get all dependencies (both model and external) as plain strings.
    ///
    /// Note: this erases the distinction between `ModelName` and `TableName`,
    /// returning both as `String`. Callers that need to distinguish between
    /// model dependencies and external table dependencies should use
    /// `depends_on` and `external_deps` fields directly.
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
        crate::compute_checksum(&self.raw_sql)
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

#[cfg(test)]
#[path = "model_test.rs"]
mod tests;
