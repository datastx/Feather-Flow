//! Model representation

use crate::config::Materialization;
use crate::error::CoreError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

/// Represents a SQL model in the project
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Model name (derived from filename without extension)
    pub name: String,

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
    pub depends_on: HashSet<String>,

    /// Dependencies on external tables
    #[serde(default)]
    pub external_deps: HashSet<String>,

    /// Schema metadata from 1:1 .yml file (optional)
    #[serde(default)]
    pub schema: Option<ModelSchema>,
}

/// Schema metadata for a single model (from 1:1 .yml file)
///
/// This follows the 1:1 naming convention where each model's schema file
/// has the same name as its SQL file (e.g., stg_orders.sql + stg_orders.yml)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSchema {
    /// Schema format version
    pub version: u32,

    /// Model description
    #[serde(default)]
    pub description: Option<String>,

    /// Model owner (team or person)
    #[serde(default)]
    pub owner: Option<String>,

    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,

    /// Column definitions
    #[serde(default)]
    pub columns: Vec<SchemaColumnDef>,
}

impl ModelSchema {
    /// Load schema from a file path
    pub fn load(path: &std::path::Path) -> Result<Self, CoreError> {
        let content = std::fs::read_to_string(path)?;
        let schema: ModelSchema = serde_yaml::from_str(&content)?;
        Ok(schema)
    }

    /// Extract tests from this schema
    pub fn extract_tests(&self, model_name: &str) -> Vec<SchemaTest> {
        let mut tests = Vec::new();

        for column in &self.columns {
            for test_name in &column.tests {
                let test_type = match test_name.as_str() {
                    "unique" => TestType::Unique,
                    "not_null" => TestType::NotNull,
                    _ => continue, // Skip unknown test types
                };

                tests.push(SchemaTest {
                    test_type,
                    column: column.name.clone(),
                    model: model_name.to_string(),
                });
            }
        }

        tests
    }
}

/// Configuration for a model extracted from config() function
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Materialization type (view or table)
    #[serde(default)]
    pub materialized: Option<Materialization>,

    /// Target schema
    #[serde(default)]
    pub schema: Option<String>,

    /// Additional tags
    #[serde(default)]
    pub tags: Vec<String>,
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
}

/// Types of schema tests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestType {
    /// Column values must be unique
    Unique,
    /// Column values must not be null
    NotNull,
}

/// Schema definition from schema.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaYml {
    /// Version of the schema file format
    pub version: u32,

    /// Model definitions with column tests
    #[serde(default)]
    pub models: Vec<SchemaModelDef>,
}

/// Model definition in schema.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaModelDef {
    /// Model name
    pub name: String,

    /// Column definitions with tests
    #[serde(default)]
    pub columns: Vec<SchemaColumnDef>,
}

/// Column definition in schema.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumnDef {
    /// Column name
    pub name: String,

    /// Tests to run on this column
    #[serde(default)]
    pub tests: Vec<String>,
}

impl Model {
    /// Create a new model from a file path
    ///
    /// This also looks for a matching 1:1 schema file (same name with .yml or .yaml extension)
    pub fn from_file(path: PathBuf) -> Result<Self, std::io::Error> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let raw_sql = std::fs::read_to_string(&path)?;

        // Look for matching 1:1 schema file
        let yml_path = path.with_extension("yml");
        let yaml_path = path.with_extension("yaml");

        let schema = if yml_path.exists() {
            ModelSchema::load(&yml_path).ok()
        } else if yaml_path.exists() {
            ModelSchema::load(&yaml_path).ok()
        } else {
            None
        };

        Ok(Self {
            name,
            path,
            raw_sql,
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema,
        })
    }

    /// Get the materialization for this model, falling back to project default
    pub fn materialization(&self, default: Materialization) -> Materialization {
        self.config.materialized.unwrap_or(default)
    }

    /// Get the schema for this model
    pub fn schema(&self, default: Option<&str>) -> Option<String> {
        self.config
            .schema
            .clone()
            .or_else(|| default.map(String::from))
    }

    /// Get all dependencies (both model and external)
    pub fn all_dependencies(&self) -> HashSet<String> {
        self.depends_on
            .union(&self.external_deps)
            .cloned()
            .collect()
    }

    /// Get tests from the model's 1:1 schema file
    pub fn get_schema_tests(&self) -> Vec<SchemaTest> {
        match &self.schema {
            Some(schema) => schema.extract_tests(&self.name),
            None => Vec::new(),
        }
    }
}

impl SchemaYml {
    /// Parse schema.yml from a string
    pub fn parse(content: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(content)
    }

    /// Load schema.yml from a file
    pub fn load(path: &std::path::Path) -> Result<Self, crate::error::CoreError> {
        let content = std::fs::read_to_string(path)?;
        Ok(Self::parse(&content)?)
    }

    /// Extract all schema tests from this schema definition
    pub fn extract_tests(&self) -> Vec<SchemaTest> {
        let mut tests = Vec::new();

        for model_def in &self.models {
            for column_def in &model_def.columns {
                for test_name in &column_def.tests {
                    let test_type = match test_name.as_str() {
                        "unique" => TestType::Unique,
                        "not_null" => TestType::NotNull,
                        _ => continue, // Skip unknown test types
                    };

                    tests.push(SchemaTest {
                        test_type,
                        column: column_def.name.clone(),
                        model: model_def.name.clone(),
                    });
                }
            }
        }

        tests
    }
}

impl std::fmt::Display for TestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestType::Unique => write!(f, "unique"),
            TestType::NotNull => write!(f, "not_null"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema_yml() {
        let yaml = r#"
version: 1

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
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
    description: "Unique identifier for the order"
    tests:
      - unique
      - not_null
  - name: customer_id
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
    }

    #[test]
    fn test_model_schema_extract_tests() {
        let yaml = r#"
version: 1
columns:
  - name: order_id
    tests:
      - unique
      - not_null
  - name: customer_id
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
}
