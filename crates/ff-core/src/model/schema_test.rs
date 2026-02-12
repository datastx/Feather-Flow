use super::*;
use crate::model::freshness::FreshnessPeriod;
use crate::model::testing::TestType;

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
    type: INTEGER
    description: "Unique identifier for the order"
    tests:
      - unique
      - not_null
  - name: customer_id
    type: INTEGER
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
    // get_owner should return the direct owner field
    assert_eq!(schema.get_owner(), Some("data-team".to_string()));
}

#[test]
fn test_parse_owner_metadata() {
    let yaml = r##"
version: 1
meta:
  owner: analytics-team@example.com
  team: Analytics
  slack_channel: "#data-alerts"
  pagerduty_service: data-platform
"##;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    // Direct owner field should be None
    assert!(schema.owner.is_none());

    // Meta fields should be populated
    assert_eq!(
        schema.get_meta_string("owner"),
        Some("analytics-team@example.com".to_string())
    );
    assert_eq!(
        schema.get_meta_string("team"),
        Some("Analytics".to_string())
    );
    assert_eq!(
        schema.get_meta_string("slack_channel"),
        Some("#data-alerts".to_string())
    );

    // get_owner should fall back to meta.owner
    assert_eq!(
        schema.get_owner(),
        Some("analytics-team@example.com".to_string())
    );
}

#[test]
fn test_owner_direct_takes_precedence_over_meta() {
    let yaml = r#"
version: 1
owner: direct-owner
meta:
  owner: meta-owner
  team: Analytics
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    // get_owner should return direct owner over meta.owner
    assert_eq!(schema.get_owner(), Some("direct-owner".to_string()));

    // But we can still access meta.owner directly
    assert_eq!(
        schema.get_meta_string("owner"),
        Some("meta-owner".to_string())
    );
}

#[test]
fn test_model_schema_extract_tests() {
    let yaml = r#"
version: 1
columns:
  - name: order_id
    type: INTEGER
    tests:
      - unique
      - not_null
  - name: customer_id
    type: INTEGER
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
    type: DECIMAL(10,2)
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
    type: INTEGER
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
    type: VARCHAR
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
    type: DECIMAL(10,2)
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
    type: DECIMAL(10,2)
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
    type: VARCHAR
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
    type: INTEGER
    tests:
      - unique
      - not_null
  - name: amount
    type: DECIMAL(10,2)
    tests:
      - positive
      - min_value:
          value: 1.0
  - name: status
    type: VARCHAR
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
    assert_eq!(user_col.data_type, "BIGINT");
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
fn test_yaml_with_config_key_fails_to_parse() {
    let yaml = r#"
version: 1
description: "Test model"
config:
  materialized: table
  schema: staging
columns:
  - name: id
    type: INTEGER
    tests:
      - unique
"#;
    let result: Result<ModelSchema, _> = serde_yaml::from_str(yaml);
    assert!(
        result.is_err(),
        "YAML with config: key should fail to parse"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("unknown field"),
        "Error should mention unknown field, got: {err}"
    );
}

#[test]
fn test_parse_relationship_test() {
    let yaml = r#"
version: 1
columns:
  - name: customer_id
    type: INTEGER
    tests:
      - relationship:
          to: customers
          field: id
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
    let tests = schema.extract_tests("orders");

    assert_eq!(tests.len(), 1);
    match &tests[0].test_type {
        TestType::Relationship { to, field } => {
            assert_eq!(to, "customers");
            assert_eq!(field, &Some("id".to_string()));
        }
        _ => panic!("Expected Relationship test type"),
    }
}

#[test]
fn test_parse_relationship_test_default_field() {
    let yaml = r#"
version: 1
columns:
  - name: user_id
    type: INTEGER
    tests:
      - relationship:
          to: users
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
    let tests = schema.extract_tests("posts");

    assert_eq!(tests.len(), 1);
    match &tests[0].test_type {
        TestType::Relationship { to, field } => {
            assert_eq!(to, "users");
            assert!(field.is_none());
        }
        _ => panic!("Expected Relationship test type"),
    }
}

#[test]
fn test_parse_relationships_alias() {
    // dbt uses "relationships" (plural) - we support both
    let yaml = r#"
version: 1
columns:
  - name: order_id
    type: INTEGER
    tests:
      - relationships:
          to: orders
          field: id
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();
    let tests = schema.extract_tests("order_items");

    assert_eq!(tests.len(), 1);
    match &tests[0].test_type {
        TestType::Relationship { to, field } => {
            assert_eq!(to, "orders");
            assert_eq!(field, &Some("id".to_string()));
        }
        _ => panic!("Expected Relationship test type"),
    }
}

#[test]
fn test_parse_contract_definition() {
    let yaml = r#"
version: 1
name: fct_orders
contract:
  enforced: true
columns:
  - name: order_id
    data_type: INTEGER
    constraints:
      - not_null
      - primary_key
  - name: customer_id
    data_type: INTEGER
    constraints:
      - not_null
  - name: total_amount
    data_type: DECIMAL
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    // Verify contract is parsed
    assert!(schema.contract.is_some());
    let contract = schema.contract.unwrap();
    assert!(contract.enforced);

    // Verify column constraints are parsed
    assert_eq!(schema.columns.len(), 3);

    let order_id_col = &schema.columns[0];
    assert_eq!(order_id_col.name, "order_id");
    assert_eq!(order_id_col.data_type, "INTEGER");
    assert_eq!(order_id_col.constraints.len(), 2);
    assert!(order_id_col
        .constraints
        .contains(&ColumnConstraint::NotNull));
    assert!(order_id_col
        .constraints
        .contains(&ColumnConstraint::PrimaryKey));

    let customer_id_col = &schema.columns[1];
    assert_eq!(customer_id_col.constraints.len(), 1);
    assert!(customer_id_col
        .constraints
        .contains(&ColumnConstraint::NotNull));

    let total_amount_col = &schema.columns[2];
    assert!(total_amount_col.constraints.is_empty());
}

#[test]
fn test_contract_not_enforced() {
    let yaml = r#"
version: 1
contract:
  enforced: false
columns:
  - name: id
    type: INTEGER
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    assert!(schema.contract.is_some());
    assert!(!schema.has_enforced_contract());
    let contract = schema.contract.as_ref().unwrap();
    assert!(!contract.enforced);
}

#[test]
fn test_no_contract_section() {
    let yaml = r#"
version: 1
columns:
  - name: id
    type: INTEGER
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    assert!(schema.contract.is_none());
    assert!(!schema.has_enforced_contract());
}

#[test]
fn test_contract_helper_methods() {
    let yaml = r#"
version: 1
contract:
  enforced: true
columns:
  - name: order_id
    data_type: INTEGER
  - name: customer_id
    data_type: VARCHAR
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    // Test has_enforced_contract
    assert!(schema.has_enforced_contract());

    // Test get_contract
    assert!(schema.get_contract().is_some());

    // Test get_column
    let order_id = schema.get_column("order_id");
    assert!(order_id.is_some());
    assert_eq!(order_id.unwrap().data_type, "INTEGER");

    // Case-insensitive lookup
    let order_id_upper = schema.get_column("ORDER_ID");
    assert!(order_id_upper.is_some());

    // Non-existent column
    let missing = schema.get_column("nonexistent");
    assert!(missing.is_none());

    // Test column_names
    let names = schema.column_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"order_id"));
    assert!(names.contains(&"customer_id"));
}

#[test]
fn test_column_constraint_unique() {
    let yaml = r#"
version: 1
columns:
  - name: email
    type: VARCHAR
    constraints:
      - unique
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    let email_col = &schema.columns[0];
    assert_eq!(email_col.constraints.len(), 1);
    assert!(email_col.constraints.contains(&ColumnConstraint::Unique));
}

#[test]
fn test_parse_model_freshness() {
    let yaml = r#"
version: 1
freshness:
  loaded_at_field: updated_at
  warn_after:
    count: 4
    period: hour
  error_after:
    count: 8
    period: hour
columns:
  - name: id
    type: INTEGER
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    assert!(schema.has_freshness());
    let freshness = schema.get_freshness().unwrap();
    assert_eq!(freshness.loaded_at_field, "updated_at");

    let warn = freshness.warn_after.as_ref().unwrap();
    assert_eq!(warn.count, 4);
    assert_eq!(warn.period, FreshnessPeriod::Hour);
    assert_eq!(warn.to_seconds(), 4 * 3600);

    let error = freshness.error_after.as_ref().unwrap();
    assert_eq!(error.count, 8);
    assert_eq!(error.period, FreshnessPeriod::Hour);
    assert_eq!(error.to_seconds(), 8 * 3600);
}

#[test]
fn test_freshness_warn_only() {
    let yaml = r#"
version: 1
freshness:
  loaded_at_field: created_at
  warn_after:
    count: 2
    period: day
columns:
  - name: id
    type: INTEGER
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    let freshness = schema.get_freshness().unwrap();
    assert_eq!(freshness.loaded_at_field, "created_at");

    let warn = freshness.warn_after.as_ref().unwrap();
    assert_eq!(warn.count, 2);
    assert_eq!(warn.period, FreshnessPeriod::Day);
    assert_eq!(warn.to_seconds(), 2 * 86400);

    // No error_after
    assert!(freshness.error_after.is_none());
}

#[test]
fn test_freshness_minutes() {
    let yaml = r#"
version: 1
freshness:
  loaded_at_field: last_sync
  warn_after:
    count: 30
    period: minute
  error_after:
    count: 60
    period: minute
columns: []
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    let freshness = schema.get_freshness().unwrap();
    assert_eq!(freshness.loaded_at_field, "last_sync");

    let warn = freshness.warn_after.as_ref().unwrap();
    assert_eq!(warn.count, 30);
    assert_eq!(warn.period, FreshnessPeriod::Minute);
    assert_eq!(warn.to_seconds(), 30 * 60);

    let error = freshness.error_after.as_ref().unwrap();
    assert_eq!(error.to_seconds(), 60 * 60);
}

#[test]
fn test_no_freshness() {
    let yaml = r#"
version: 1
columns:
  - name: id
    type: INTEGER
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    assert!(!schema.has_freshness());
    assert!(schema.get_freshness().is_none());
}

#[test]
fn test_unsupported_schema_version() {
    let dir = tempfile::TempDir::new().unwrap();
    let yml_path = dir.path().join("test.yml");
    std::fs::write(
        &yml_path,
        "version: 2\ncolumns:\n  - name: id\n    type: INTEGER\n",
    )
    .unwrap();

    let result = ModelSchema::load(&yml_path);
    assert!(result.is_err(), "version 2 should be rejected");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("E018"),
        "Error should reference E018, got: {err}"
    );
    assert!(
        err.contains("version 2"),
        "Error should mention version 2, got: {err}"
    );
}

#[test]
fn test_deprecated_model() {
    let yaml = r#"
version: 1
deprecated: true
deprecation_message: "Use fct_orders_v2 instead"
columns:
  - name: id
    type: INTEGER
"#;
    let schema: ModelSchema = serde_yaml::from_str(yaml).unwrap();

    assert!(schema.deprecated);
    assert_eq!(
        schema.deprecation_message,
        Some("Use fct_orders_v2 instead".to_string())
    );
}
