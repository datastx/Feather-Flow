//! Test types and parsing for model schema tests

use crate::error::CoreError;
use crate::model_name::ModelName;
use crate::test_name::TestName;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Parse a test definition into a TestType
pub fn parse_test_definition(test_def: &TestDefinition) -> Option<TestType> {
    match test_def {
        TestDefinition::Simple(name) => parse_simple_test(name),
        TestDefinition::Parameterized(map) => {
            let (test_name, params) = map.iter().next()?;
            parse_parameterized_test(test_name, params)
        }
    }
}

/// Parse a simple (unparameterized) test name.
fn parse_simple_test(name: &str) -> Option<TestType> {
    match name {
        "unique" => Some(TestType::Unique),
        "not_null" => Some(TestType::NotNull),
        "positive" => Some(TestType::Positive),
        "non_negative" => Some(TestType::NonNegative),
        _ => None,
    }
}

/// Parse a parameterized test definition.
fn parse_parameterized_test(test_name: &str, params: &TestParams) -> Option<TestType> {
    match test_name {
        "accepted_values" => parse_accepted_values(params),
        "min_value" => params.value.map(|value| TestType::MinValue { value }),
        "max_value" => params.value.map(|value| TestType::MaxValue { value }),
        "regex" => params
            .pattern
            .clone()
            .map(|pattern| TestType::Regex { pattern }),
        "relationship" | "relationships" => params.to.clone().map(|to| TestType::Relationship {
            to,
            field: params.field.clone(),
        }),
        _ => None,
    }
}

/// Parse accepted_values test parameters into a TestType.
fn parse_accepted_values(params: &TestParams) -> Option<TestType> {
    let values: Vec<String> = params
        .values
        .iter()
        .filter_map(|v| match v {
            serde_yaml::Value::String(s) => Some(s.clone()),
            serde_yaml::Value::Number(n) => Some(n.to_string()),
            serde_yaml::Value::Bool(b) => Some(b.to_string()),
            _ => None,
        })
        .collect();
    if values.is_empty() {
        return None;
    }
    Some(TestType::AcceptedValues {
        values,
        quote: params.quote,
    })
}

/// Schema test definition from schema.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaTest {
    /// Test type (unique, not_null, etc.)
    pub test_type: TestType,

    /// Column name to test
    pub column: String,

    /// Model name
    pub model: ModelName,

    /// Test configuration (severity, where, limit, etc.)
    #[serde(default)]
    pub config: TestConfig,
}

/// Singular test - standalone SQL test file that should return 0 rows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingularTest {
    /// Test name (derived from filename without extension)
    pub name: TestName,

    /// Path to the SQL test file
    pub path: PathBuf,

    /// SQL content - query that should return 0 rows if test passes
    pub sql: String,
}

impl SingularTest {
    /// Load a singular test from a SQL file
    pub fn from_file(path: PathBuf) -> Result<Self, CoreError> {
        let name = TestName::new(path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
            CoreError::TestValidationError {
                name: path.display().to_string(),
                message: "Invalid file name".to_string(),
            }
        })?);

        let sql = std::fs::read_to_string(&path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;

        if sql.trim().is_empty() {
            return Err(CoreError::TestValidationError {
                name: name.to_string(),
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

#[cfg(test)]
#[path = "testing_test.rs"]
mod tests;
