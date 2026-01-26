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

    /// Model-level configuration (can override project defaults)
    #[serde(default)]
    pub config: Option<SchemaConfig>,

    /// Column definitions
    #[serde(default)]
    pub columns: Vec<SchemaColumnDef>,
}

/// Configuration from schema YAML that can override project defaults
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// Materialization type (view or table)
    #[serde(default)]
    pub materialized: Option<Materialization>,

    /// Target schema
    #[serde(default)]
    pub schema: Option<String>,
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
            for test_def in &column.tests {
                if let Some(test_type) = parse_test_definition(test_def) {
                    tests.push(SchemaTest {
                        test_type,
                        column: column.name.clone(),
                        model: model_name.to_string(),
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
                _ => None,
            }
        }
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

    /// SQL data type (e.g., VARCHAR, INT, TIMESTAMP)
    #[serde(rename = "type", default)]
    pub data_type: Option<String>,

    /// Column description
    #[serde(default)]
    pub description: Option<String>,

    /// Whether this column is a primary key
    #[serde(default)]
    pub primary_key: bool,

    /// Tests to run on this column
    #[serde(default)]
    pub tests: Vec<TestDefinition>,

    /// Foreign key reference to another model's column
    #[serde(default)]
    pub references: Option<ColumnReference>,
}

/// Foreign key reference to another model's column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnReference {
    /// Referenced model name
    pub model: String,
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

    /// Get the materialization for this model, falling back through the precedence chain:
    /// 1. SQL config() function (self.config.materialized)
    /// 2. Schema YAML config section (self.schema.config.materialized)
    /// 3. Project default
    pub fn materialization(&self, default: Materialization) -> Materialization {
        // First check SQL config
        if let Some(mat) = self.config.materialized {
            return mat;
        }

        // Then check schema YAML config
        if let Some(schema) = &self.schema {
            if let Some(config) = &schema.config {
                if let Some(mat) = config.materialized {
                    return mat;
                }
            }
        }

        // Finally use project default
        default
    }

    /// Get the schema for this model, falling back through the precedence chain:
    /// 1. SQL config() function (self.config.schema)
    /// 2. Schema YAML config section (self.schema.config.schema)
    /// 3. Project default
    pub fn target_schema(&self, default: Option<&str>) -> Option<String> {
        // First check SQL config
        if let Some(s) = &self.config.schema {
            return Some(s.clone());
        }

        // Then check schema YAML config
        if let Some(schema) = &self.schema {
            if let Some(config) = &schema.config {
                if let Some(s) = &config.schema {
                    return Some(s.clone());
                }
            }
        }

        // Finally use project default
        default.map(String::from)
    }

    /// Get the schema for this model (deprecated, use target_schema instead)
    #[deprecated(note = "Use target_schema instead for clearer naming")]
    pub fn schema(&self, default: Option<&str>) -> Option<String> {
        self.target_schema(default)
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
                for test_def in &column_def.tests {
                    if let Some(test_type) = parse_test_definition(test_def) {
                        tests.push(SchemaTest {
                            test_type,
                            column: column_def.name.clone(),
                            model: model_def.name.clone(),
                        });
                    }
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
            TestType::Positive => write!(f, "positive"),
            TestType::NonNegative => write!(f, "non_negative"),
            TestType::AcceptedValues { .. } => write!(f, "accepted_values"),
            TestType::MinValue { .. } => write!(f, "min_value"),
            TestType::MaxValue { .. } => write!(f, "max_value"),
            TestType::Regex { .. } => write!(f, "regex"),
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

    #[test]
    fn test_parse_positive_test() {
        let yaml = r#"
version: 1
columns:
  - name: amount
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
    tests:
      - unique
      - not_null
  - name: amount
    tests:
      - positive
      - min_value:
          value: 1.0
  - name: status
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
        assert_eq!(user_col.data_type, Some("BIGINT".to_string()));
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
    fn test_parse_schema_config() {
        let yaml = r#"
version: 1
description: "Test model"
config:
  materialized: table
  schema: staging
columns:
  - name: id
    tests:
      - unique
"#;
        let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
        assert!(schema.config.is_some());
        let config = schema.config.unwrap();
        assert_eq!(config.materialized, Some(Materialization::Table));
        assert_eq!(config.schema, Some("staging".to_string()));
    }

    #[test]
    fn test_config_precedence_sql_wins() {
        use crate::config::Materialization;

        // Create a model with SQL config and schema config
        let model = Model {
            name: "test".to_string(),
            path: std::path::PathBuf::from("test.sql"),
            raw_sql: String::new(),
            compiled_sql: None,
            config: ModelConfig {
                materialized: Some(Materialization::Table), // SQL wins
                schema: Some("sql_schema".to_string()),
                tags: vec![],
            },
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema: Some(ModelSchema {
                version: 1,
                description: None,
                owner: None,
                tags: vec![],
                config: Some(SchemaConfig {
                    materialized: Some(Materialization::View), // Should be ignored
                    schema: Some("yaml_schema".to_string()),   // Should be ignored
                }),
                columns: vec![],
            }),
        };

        // SQL config should win
        assert_eq!(
            model.materialization(Materialization::View),
            Materialization::Table
        );
        assert_eq!(model.target_schema(None), Some("sql_schema".to_string()));
    }

    #[test]
    fn test_config_precedence_yaml_fallback() {
        use crate::config::Materialization;

        // Create a model with only schema config (no SQL config)
        let model = Model {
            name: "test".to_string(),
            path: std::path::PathBuf::from("test.sql"),
            raw_sql: String::new(),
            compiled_sql: None,
            config: ModelConfig::default(), // No SQL config
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema: Some(ModelSchema {
                version: 1,
                description: None,
                owner: None,
                tags: vec![],
                config: Some(SchemaConfig {
                    materialized: Some(Materialization::Table), // Should be used
                    schema: Some("yaml_schema".to_string()),    // Should be used
                }),
                columns: vec![],
            }),
        };

        // YAML config should be used when SQL config is not set
        assert_eq!(
            model.materialization(Materialization::View),
            Materialization::Table
        );
        assert_eq!(model.target_schema(None), Some("yaml_schema".to_string()));
    }

    #[test]
    fn test_config_precedence_project_default() {
        use crate::config::Materialization;

        // Create a model with no config
        let model = Model {
            name: "test".to_string(),
            path: std::path::PathBuf::from("test.sql"),
            raw_sql: String::new(),
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: HashSet::new(),
            external_deps: HashSet::new(),
            schema: None,
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
}
