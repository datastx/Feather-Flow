use super::*;
use ff_core::model::{ModelSchema, SchemaColumnDef};
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

// ── Documentation enforcement tests ─────────────────────────────────

#[test]
fn test_model_missing_description_produces_error() {
    let schema = ModelSchema {
        description: None,
        ..Default::default()
    };
    let mut ctx = ValidationContext::new();
    let issues = check_model_description(&schema, "my_model", "models/my_model.yml", &mut ctx);
    assert_eq!(issues, 1);
    assert_eq!(ctx.error_count(), 1);
    assert!(ctx.issues[0].code == "D001");
    assert!(ctx.issues[0].message.contains("my_model"));
}

#[test]
fn test_model_empty_description_produces_error() {
    let schema = ModelSchema {
        description: Some("   ".to_string()),
        ..Default::default()
    };
    let mut ctx = ValidationContext::new();
    let issues = check_model_description(&schema, "my_model", "models/my_model.yml", &mut ctx);
    assert_eq!(issues, 1);
    assert_eq!(ctx.error_count(), 1);
    assert!(ctx.issues[0].code == "D001");
}

#[test]
fn test_model_with_description_passes() {
    let schema = ModelSchema {
        description: Some("Staging orders table".to_string()),
        ..Default::default()
    };
    let mut ctx = ValidationContext::new();
    let issues = check_model_description(&schema, "stg_orders", "models/stg_orders.yml", &mut ctx);
    assert_eq!(issues, 0);
    assert_eq!(ctx.error_count(), 0);
}

#[test]
fn test_column_missing_description_produces_error() {
    let column = SchemaColumnDef {
        name: "user_id".to_string(),
        data_type: "INTEGER".to_string(),
        description: None,
        description_ai_generated: None,
        primary_key: false,
        constraints: vec![],
        tests: vec![],
        references: None,
        classification: None,
    };
    let mut ctx = ValidationContext::new();
    let issues = check_column_description(&column, "my_model", "models/my_model.yml", &mut ctx);
    assert_eq!(issues, 1);
    assert_eq!(ctx.error_count(), 1);
    assert!(ctx.issues[0].code == "D002");
    assert!(ctx.issues[0].message.contains("user_id"));
    assert!(ctx.issues[0].message.contains("my_model"));
}

#[test]
fn test_column_empty_description_produces_error() {
    let column = SchemaColumnDef {
        name: "user_id".to_string(),
        data_type: "INTEGER".to_string(),
        description: Some("".to_string()),
        description_ai_generated: None,
        primary_key: false,
        constraints: vec![],
        tests: vec![],
        references: None,
        classification: None,
    };
    let mut ctx = ValidationContext::new();
    let issues = check_column_description(&column, "my_model", "models/my_model.yml", &mut ctx);
    assert_eq!(issues, 1);
    assert_eq!(ctx.error_count(), 1);
    assert!(ctx.issues[0].code == "D002");
}

#[test]
fn test_column_with_description_passes() {
    let column = SchemaColumnDef {
        name: "user_id".to_string(),
        data_type: "INTEGER".to_string(),
        description: Some("Unique identifier for the user".to_string()),
        description_ai_generated: None,
        primary_key: false,
        constraints: vec![],
        tests: vec![],
        references: None,
        classification: None,
    };
    let mut ctx = ValidationContext::new();
    let issues = check_column_description(&column, "my_model", "models/my_model.yml", &mut ctx);
    assert_eq!(issues, 0);
    assert_eq!(ctx.error_count(), 0);
}

// ── E013: Qualified output uniqueness tests ─────────────────────────

#[test]
fn test_qualified_uniqueness_no_collision() {
    let mut map = HashMap::new();
    map.insert(
        "model_a".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "model_a".to_string(),
        },
    );
    map.insert(
        "model_b".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "model_b".to_string(),
        },
    );
    let ephemeral = HashSet::new();
    let mut ctx = ValidationContext::new();
    validate_qualified_uniqueness(&map, &ephemeral, &mut ctx);
    assert_eq!(ctx.error_count(), 0);
}

#[test]
fn test_qualified_uniqueness_collision_detected() {
    let mut map = HashMap::new();
    map.insert(
        "model_a".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "fct_orders".to_string(),
        },
    );
    map.insert(
        "model_b".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "fct_orders".to_string(),
        },
    );
    let ephemeral = HashSet::new();
    let mut ctx = ValidationContext::new();
    validate_qualified_uniqueness(&map, &ephemeral, &mut ctx);
    assert_eq!(ctx.error_count(), 1);
    assert_eq!(ctx.issues[0].code, "E013");
    assert!(ctx.issues[0].message.contains("fct_orders"));
}

#[test]
fn test_qualified_uniqueness_ephemeral_skipped() {
    let mut map = HashMap::new();
    map.insert(
        "model_a".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "fct_orders".to_string(),
        },
    );
    map.insert(
        "model_b".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "fct_orders".to_string(),
        },
    );
    let mut ephemeral = HashSet::new();
    ephemeral.insert("model_b".to_string());
    let mut ctx = ValidationContext::new();
    validate_qualified_uniqueness(&map, &ephemeral, &mut ctx);
    assert_eq!(ctx.error_count(), 0);
}

#[test]
fn test_qualified_uniqueness_cross_db_no_collision() {
    let mut map = HashMap::new();
    map.insert(
        "model_a".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("main".to_string()),
            schema: "analytics".to_string(),
            table: "orders".to_string(),
        },
    );
    map.insert(
        "ext_orders".to_string(),
        ff_sql::qualify::QualifiedRef {
            database: Some("ext_db".to_string()),
            schema: "analytics".to_string(),
            table: "orders".to_string(),
        },
    );
    let ephemeral = HashSet::new();
    let mut ctx = ValidationContext::new();
    validate_qualified_uniqueness(&map, &ephemeral, &mut ctx);
    // Different databases → no collision
    assert_eq!(ctx.error_count(), 0);
}
