//! Schema contract validation
//!
//! This module provides functionality for validating that model outputs
//! match their defined schema contracts.

use crate::model::{ColumnConstraint, ModelSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A violation of a schema contract
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    /// The model that has the violation
    pub model: String,
    /// Type of violation
    pub violation_type: ViolationType,
    /// Human-readable description
    pub message: String,
}

/// Types of contract violations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationType {
    /// Expected column not found in output
    MissingColumn { column: String },
    /// Column type doesn't match contract
    TypeMismatch {
        column: String,
        expected: String,
        actual: String,
    },
    /// Extra column found that wasn't in contract (warning only)
    ExtraColumn { column: String },
    /// Constraint violation (not_null, unique, primary_key)
    ConstraintNotMet {
        column: String,
        constraint: ColumnConstraint,
    },
}

/// Result of validating a schema contract
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContractValidationResult {
    /// The model that was validated
    pub model: String,
    /// Whether the contract was enforced (errors vs warnings)
    pub enforced: bool,
    /// List of violations found
    pub violations: Vec<ContractViolation>,
    /// Whether validation passed (no violations, or violations are warnings only)
    pub passed: bool,
}

impl ContractValidationResult {
    /// Create a new result
    pub fn new(model: impl Into<String>, enforced: bool) -> Self {
        Self {
            model: model.into(),
            enforced,
            violations: Vec::new(),
            passed: true,
        }
    }

    /// Add a violation
    pub fn add_violation(&mut self, violation_type: ViolationType, message: impl Into<String>) {
        self.violations.push(ContractViolation {
            model: self.model.clone(),
            violation_type,
            message: message.into(),
        });
        // If enforced, mark as failed
        if self.enforced {
            self.passed = false;
        }
    }

    /// Check if there are any violations
    pub fn has_violations(&self) -> bool {
        !self.violations.is_empty()
    }
}

/// Validate a model's actual schema against its contract
///
/// # Arguments
/// * `model_name` - Name of the model being validated
/// * `schema` - The model's schema definition with contract
/// * `actual_columns` - Columns from the actual table (name, type)
///
/// # Returns
/// A validation result with any violations found
pub fn validate_contract(
    model_name: &str,
    schema: &ModelSchema,
    actual_columns: &[(String, String)],
) -> ContractValidationResult {
    let enforced = schema.has_enforced_contract();
    let mut result = ContractValidationResult::new(model_name, enforced);

    // Build a map of actual columns for quick lookup (case-insensitive)
    let actual_map: HashMap<String, &str> = actual_columns
        .iter()
        .map(|(name, dtype)| (name.to_lowercase(), dtype.as_str()))
        .collect();

    // Check each contracted column
    for column_def in &schema.columns {
        let column_lower = column_def.name.to_lowercase();

        // Check if column exists
        match actual_map.get(&column_lower) {
            None => {
                result.add_violation(
                    ViolationType::MissingColumn {
                        column: column_def.name.clone(),
                    },
                    format!(
                        "Column '{}' defined in contract is missing from model output",
                        column_def.name
                    ),
                );
            }
            Some(actual_type) => {
                // Check type compatibility if type is specified in contract
                if let Some(ref expected_type) = column_def.data_type {
                    if !types_compatible(expected_type, actual_type) {
                        result.add_violation(
                            ViolationType::TypeMismatch {
                                column: column_def.name.clone(),
                                expected: expected_type.clone(),
                                actual: actual_type.to_string(),
                            },
                            format!(
                                "Column '{}' type mismatch: contract specifies {}, but got {}",
                                column_def.name, expected_type, actual_type
                            ),
                        );
                    }
                }
            }
        }
    }

    // Optionally check for extra columns (as warnings only)
    let contracted_columns: std::collections::HashSet<String> = schema
        .columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .collect();

    for (name, _) in actual_columns {
        if !contracted_columns.contains(&name.to_lowercase()) {
            // Extra columns are informational, don't fail the contract
            result.violations.push(ContractViolation {
                model: model_name.to_string(),
                violation_type: ViolationType::ExtraColumn {
                    column: name.clone(),
                },
                message: format!(
                    "Column '{}' found in output but not defined in contract",
                    name
                ),
            });
        }
    }

    result
}

/// Check if two SQL types are compatible
///
/// This is a simplified compatibility check. In a real implementation,
/// you'd want to handle:
/// - Type aliases (INT vs INTEGER)
/// - Precision/scale for decimals
/// - Size variations (VARCHAR(50) vs VARCHAR(100))
fn types_compatible(expected: &str, actual: &str) -> bool {
    let expected_norm = normalize_type(expected);
    let actual_norm = normalize_type(actual);

    // Exact match after normalization
    if expected_norm == actual_norm {
        return true;
    }

    // Check type families
    let expected_family = type_family(&expected_norm);
    let actual_family = type_family(&actual_norm);

    expected_family == actual_family
}

/// Normalize a SQL type for comparison
fn normalize_type(t: &str) -> String {
    let t = t.to_uppercase();

    // Remove parentheses for precision/scale
    let base = if let Some(paren) = t.find('(') {
        &t[..paren]
    } else {
        &t
    };

    // Normalize common aliases
    match base.trim() {
        "INT" => "INTEGER".to_string(),
        "BOOL" => "BOOLEAN".to_string(),
        "STRING" => "VARCHAR".to_string(),
        "TEXT" => "VARCHAR".to_string(),
        "FLOAT" => "DOUBLE".to_string(),
        "REAL" => "DOUBLE".to_string(),
        "NUMERIC" => "DECIMAL".to_string(),
        other => other.to_string(),
    }
}

/// SQL type families for compatibility checking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeFamily {
    Integer,
    Floating,
    Decimal,
    String,
    Boolean,
    Date,
    Time,
    Timestamp,
    Binary,
    Other,
}

/// Get the type family for looser compatibility checks
fn type_family(normalized_type: &str) -> TypeFamily {
    match normalized_type {
        "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "HUGEINT" | "UBIGINT" | "UINTEGER"
        | "USMALLINT" | "UTINYINT" => TypeFamily::Integer,
        "DOUBLE" | "FLOAT" | "REAL" => TypeFamily::Floating,
        "DECIMAL" => TypeFamily::Decimal,
        "VARCHAR" | "CHAR" | "TEXT" | "STRING" => TypeFamily::String,
        "BOOLEAN" | "BOOL" => TypeFamily::Boolean,
        "DATE" => TypeFamily::Date,
        "TIME" => TypeFamily::Time,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => TypeFamily::Timestamp,
        "BLOB" | "BYTEA" | "BINARY" => TypeFamily::Binary,
        _ => TypeFamily::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{SchemaColumnDef, SchemaContract};

    fn make_schema(columns: Vec<(&str, Option<&str>)>, enforced: bool) -> ModelSchema {
        ModelSchema {
            version: 1,
            description: None,
            owner: None,
            meta: std::collections::HashMap::new(),
            tags: vec![],
            config: None,
            contract: Some(SchemaContract { enforced }),
            freshness: None,
            columns: columns
                .into_iter()
                .map(|(name, dtype)| SchemaColumnDef {
                    name: name.to_string(),
                    data_type: dtype.map(|s| s.to_string()),
                    description: None,
                    primary_key: false,
                    constraints: vec![],
                    tests: vec![],
                    references: None,
                })
                .collect(),
            deprecated: false,
            deprecation_message: None,
        }
    }

    #[test]
    fn test_contract_passes() {
        let schema = make_schema(
            vec![("id", Some("INTEGER")), ("name", Some("VARCHAR"))],
            true,
        );
        let actual = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "VARCHAR".to_string()),
        ];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(result.passed);
        assert!(!result.has_violations());
    }

    #[test]
    fn test_missing_column_enforced() {
        let schema = make_schema(
            vec![("id", Some("INTEGER")), ("name", Some("VARCHAR"))],
            true,
        );
        let actual = vec![("id".to_string(), "INTEGER".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(!result.passed);
        assert!(result.has_violations());
        assert!(result.violations.iter().any(|v| matches!(
            &v.violation_type,
            ViolationType::MissingColumn { column } if column == "name"
        )));
    }

    #[test]
    fn test_missing_column_not_enforced() {
        let schema = make_schema(
            vec![("id", Some("INTEGER")), ("name", Some("VARCHAR"))],
            false,
        );
        let actual = vec![("id".to_string(), "INTEGER".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        // Not enforced, so it still "passes"
        assert!(result.passed);
        // But there are violations (warnings)
        assert!(result.has_violations());
    }

    #[test]
    fn test_type_mismatch() {
        let schema = make_schema(vec![("id", Some("INTEGER"))], true);
        let actual = vec![("id".to_string(), "VARCHAR".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(!result.passed);
        assert!(result.violations.iter().any(|v| matches!(
            &v.violation_type,
            ViolationType::TypeMismatch { column, .. } if column == "id"
        )));
    }

    #[test]
    fn test_type_compatible_int_variants() {
        let schema = make_schema(vec![("id", Some("INT"))], true);
        let actual = vec![("id".to_string(), "INTEGER".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(result.passed);
    }

    #[test]
    fn test_type_compatible_varchar_text() {
        let schema = make_schema(vec![("name", Some("VARCHAR"))], true);
        let actual = vec![("name".to_string(), "TEXT".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(result.passed);
    }

    #[test]
    fn test_extra_column_warning() {
        let schema = make_schema(vec![("id", Some("INTEGER"))], true);
        let actual = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("extra_col".to_string(), "VARCHAR".to_string()),
        ];

        let result = validate_contract("test_model", &schema, &actual);
        // Extra columns don't cause failure
        assert!(result.passed);
        // But they're reported as violations
        assert!(result.violations.iter().any(|v| matches!(
            &v.violation_type,
            ViolationType::ExtraColumn { column } if column == "extra_col"
        )));
    }

    #[test]
    fn test_case_insensitive_column_match() {
        let schema = make_schema(vec![("OrderId", Some("INTEGER"))], true);
        let actual = vec![("orderid".to_string(), "INTEGER".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(result.passed);
    }

    #[test]
    fn test_no_type_in_contract() {
        // When contract doesn't specify type, any type is acceptable
        let schema = make_schema(vec![("id", None)], true);
        let actual = vec![("id".to_string(), "VARCHAR".to_string())];

        let result = validate_contract("test_model", &schema, &actual);
        assert!(result.passed);
    }

    #[test]
    fn test_type_family_integer() {
        assert!(types_compatible("INTEGER", "BIGINT"));
        assert!(types_compatible("INT", "INTEGER"));
        assert!(types_compatible("SMALLINT", "TINYINT"));
    }

    #[test]
    fn test_type_family_string() {
        assert!(types_compatible("VARCHAR", "TEXT"));
        assert!(types_compatible("STRING", "VARCHAR"));
        assert!(types_compatible("CHAR", "VARCHAR"));
    }

    #[test]
    fn test_incompatible_types() {
        assert!(!types_compatible("INTEGER", "VARCHAR"));
        assert!(!types_compatible("BOOLEAN", "INTEGER"));
        assert!(!types_compatible("DATE", "TIMESTAMP"));
    }
}
