use super::*;
use crate::model::{SchemaColumnDef, SchemaContract};

fn make_schema(columns: Vec<(&str, &str)>, enforced: bool) -> ModelSchema {
    ModelSchema {
        contract: Some(SchemaContract { enforced }),
        columns: columns
            .into_iter()
            .map(|(name, dtype)| SchemaColumnDef {
                name: name.to_string(),
                data_type: dtype.to_string(),
                description: None,
                description_ai_generated: None,
                primary_key: false,
                constraints: vec![],
                tests: vec![],
                references: None,
                classification: None,
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn test_contract_passes() {
    let schema = make_schema(vec![("id", "INTEGER"), ("name", "VARCHAR")], true);
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
    let schema = make_schema(vec![("id", "INTEGER"), ("name", "VARCHAR")], true);
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
    let schema = make_schema(vec![("id", "INTEGER"), ("name", "VARCHAR")], false);
    let actual = vec![("id".to_string(), "INTEGER".to_string())];

    let result = validate_contract("test_model", &schema, &actual);
    // Not enforced, so it still "passes"
    assert!(result.passed);
    // But there are violations (warnings)
    assert!(result.has_violations());
}

#[test]
fn test_type_mismatch() {
    let schema = make_schema(vec![("id", "INTEGER")], true);
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
    let schema = make_schema(vec![("id", "INT")], true);
    let actual = vec![("id".to_string(), "INTEGER".to_string())];

    let result = validate_contract("test_model", &schema, &actual);
    assert!(result.passed);
}

#[test]
fn test_type_compatible_varchar_text() {
    let schema = make_schema(vec![("name", "VARCHAR")], true);
    let actual = vec![("name".to_string(), "TEXT".to_string())];

    let result = validate_contract("test_model", &schema, &actual);
    assert!(result.passed);
}

#[test]
fn test_extra_column_warning() {
    let schema = make_schema(vec![("id", "INTEGER")], true);
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
    let schema = make_schema(vec![("OrderId", "INTEGER")], true);
    let actual = vec![("orderid".to_string(), "INTEGER".to_string())];

    let result = validate_contract("test_model", &schema, &actual);
    assert!(result.passed);
}

#[test]
fn test_compatible_type_in_contract() {
    // When contract specifies a compatible type family, validation passes
    let schema = make_schema(vec![("id", "VARCHAR")], true);
    let actual = vec![("id".to_string(), "TEXT".to_string())];

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
