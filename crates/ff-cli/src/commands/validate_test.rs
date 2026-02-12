use super::*;
use std::collections::HashMap;

#[test]
fn test_numeric_test_on_string_type() {
    let test = TestDefinition::Simple("positive".to_string());
    let result = check_test_type_compatibility(&test, "VARCHAR", "amount", "test_model");
    assert!(result.is_some());
    assert!(result.unwrap().contains("numeric test on string type"));
}

#[test]
fn test_numeric_test_on_numeric_type() {
    let test = TestDefinition::Simple("positive".to_string());
    let result = check_test_type_compatibility(&test, "INTEGER", "amount", "test_model");
    assert!(result.is_none());
}

#[test]
fn test_regex_on_integer_type() {
    let mut params = HashMap::new();
    params.insert(
        "regex".to_string(),
        ff_core::model::TestParams {
            values: vec![],
            quote: false,
            value: None,
            pattern: Some(".*".to_string()),
            to: None,
            field: None,
        },
    );
    let test = TestDefinition::Parameterized(params);
    let result = check_test_type_compatibility(&test, "INTEGER", "code", "test_model");
    assert!(result.is_some());
    assert!(result.unwrap().contains("regex test on non-string type"));
}

#[test]
fn test_regex_on_varchar_type() {
    let mut params = HashMap::new();
    params.insert(
        "regex".to_string(),
        ff_core::model::TestParams {
            values: vec![],
            quote: false,
            value: None,
            pattern: Some(".*".to_string()),
            to: None,
            field: None,
        },
    );
    let test = TestDefinition::Parameterized(params);
    let result = check_test_type_compatibility(&test, "VARCHAR", "email", "test_model");
    assert!(result.is_none());
}

#[test]
fn test_min_value_on_text_type() {
    let mut params = HashMap::new();
    params.insert(
        "min_value".to_string(),
        ff_core::model::TestParams {
            values: vec![],
            quote: false,
            value: Some(0.0),
            pattern: None,
            to: None,
            field: None,
        },
    );
    let test = TestDefinition::Parameterized(params);
    let result = check_test_type_compatibility(&test, "TEXT", "count", "test_model");
    assert!(result.is_some());
}

#[test]
fn test_not_null_on_any_type() {
    let test = TestDefinition::Simple("not_null".to_string());
    let result1 = check_test_type_compatibility(&test, "VARCHAR", "name", "test_model");
    let result2 = check_test_type_compatibility(&test, "INTEGER", "id", "test_model");
    assert!(result1.is_none());
    assert!(result2.is_none());
}

#[test]
fn test_unique_on_any_type() {
    let test = TestDefinition::Simple("unique".to_string());
    let result1 = check_test_type_compatibility(&test, "VARCHAR", "name", "test_model");
    let result2 = check_test_type_compatibility(&test, "INTEGER", "id", "test_model");
    assert!(result1.is_none());
    assert!(result2.is_none());
}
