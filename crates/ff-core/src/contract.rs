//! Schema contract validation
//!
//! This module provides functionality for validating that model outputs
//! match their defined schema contracts.

use crate::model::{ColumnConstraint, ModelSchema};
use crate::model_name::ModelName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A violation of a schema contract
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    /// The model that has the violation
    pub model: ModelName,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractValidationResult {
    /// The model that was validated
    pub model: ModelName,
    /// Whether the contract was enforced (errors vs warnings)
    pub enforced: bool,
    /// List of violations found
    pub violations: Vec<ContractViolation>,
    /// Whether validation passed (no violations, or violations are warnings only)
    pub passed: bool,
}

impl ContractValidationResult {
    /// Create a new result
    pub fn new(model: &str, enforced: bool) -> Self {
        Self {
            model: ModelName::new(model),
            enforced,
            violations: Vec::new(),
            passed: true,
        }
    }

    /// Add a violation that respects the enforced flag
    pub fn add_violation(&mut self, violation_type: ViolationType, message: impl Into<String>) {
        self.violations.push(ContractViolation {
            model: self.model.clone(),
            violation_type,
            message: message.into(),
        });
        if self.enforced {
            self.passed = false;
        }
    }

    /// Add a warning violation that never marks the result as failed
    pub fn add_warning(&mut self, violation_type: ViolationType, message: impl Into<String>) {
        self.violations.push(ContractViolation {
            model: self.model.clone(),
            violation_type,
            message: message.into(),
        });
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

    let actual_map: HashMap<String, &str> = actual_columns
        .iter()
        .map(|(name, dtype)| (name.to_lowercase(), dtype.as_str()))
        .collect();

    for column_def in &schema.columns {
        let column_lower = column_def.name.to_lowercase();

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
                if !types_compatible(&column_def.data_type, actual_type) {
                    result.add_violation(
                        ViolationType::TypeMismatch {
                            column: column_def.name.clone(),
                            expected: column_def.data_type.clone(),
                            actual: actual_type.to_string(),
                        },
                        format!(
                            "Column '{}' type mismatch: contract specifies {}, but got {}",
                            column_def.name, column_def.data_type, actual_type
                        ),
                    );
                }
            }
        }
    }

    // Check for extra columns (as warnings only — never fail the contract)
    let contracted_columns: std::collections::HashSet<String> = schema
        .columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .collect();

    for (name, _) in actual_columns {
        if !contracted_columns.contains(&name.to_lowercase()) {
            result.add_warning(
                ViolationType::ExtraColumn {
                    column: name.clone(),
                },
                format!(
                    "Column '{}' found in output but not defined in contract",
                    name
                ),
            );
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
pub(crate) fn types_compatible(expected: &str, actual: &str) -> bool {
    let expected_norm = normalize_type(expected);
    let actual_norm = normalize_type(actual);

    if expected_norm == actual_norm {
        return true;
    }

    let expected_family = type_family(&expected_norm);
    let actual_family = type_family(&actual_norm);

    expected_family == actual_family
}

/// Normalize a SQL type for comparison
fn normalize_type(t: &str) -> String {
    let t = t.to_uppercase();

    let base = if let Some(paren) = t.find('(') {
        &t[..paren]
    } else {
        &t
    };

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
#[path = "contract_test.rs"]
mod tests;
