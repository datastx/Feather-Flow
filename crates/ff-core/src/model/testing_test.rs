use super::*;

#[test]
fn test_test_severity_default() {
    let severity = TestSeverity::default();
    assert_eq!(severity, TestSeverity::Error);
}

#[test]
fn test_test_severity_display() {
    assert_eq!(TestSeverity::Error.to_string(), "error");
    assert_eq!(TestSeverity::Warn.to_string(), "warn");
}

#[test]
fn test_test_config_default() {
    let config = TestConfig::default();
    assert_eq!(config.severity, TestSeverity::Error);
    assert!(config.where_clause.is_none());
    assert!(config.limit.is_none());
    assert!(config.error_if.is_none());
    assert!(config.warn_if.is_none());
}

#[test]
fn test_test_config_with_severity() {
    let config = TestConfig {
        severity: TestSeverity::Warn,
        where_clause: Some("status = 'active'".to_string()),
        limit: Some(100),
        error_if: Some("> 100".to_string()),
        warn_if: Some("> 10".to_string()),
    };
    assert_eq!(config.severity, TestSeverity::Warn);
    assert_eq!(config.where_clause, Some("status = 'active'".to_string()));
    assert_eq!(config.limit, Some(100));
    assert_eq!(config.error_if, Some("> 100".to_string()));
    assert_eq!(config.warn_if, Some("> 10".to_string()));
}

#[test]
fn test_schema_test_with_config() {
    let test = SchemaTest {
        test_type: TestType::Unique,
        column: "id".to_string(),
        model: crate::model_name::ModelName::new("users"),
        config: TestConfig {
            severity: TestSeverity::Warn,
            ..Default::default()
        },
    };
    assert_eq!(test.config.severity, TestSeverity::Warn);
}

#[test]
fn test_relationship_test_display() {
    let test_type = TestType::Relationship {
        to: "customers".to_string(),
        field: Some("id".to_string()),
    };
    assert_eq!(test_type.to_string(), "relationship");
}
