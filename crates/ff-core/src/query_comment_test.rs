use super::*;

#[test]
fn test_build_query_comment() {
    let ctx = QueryCommentContext::new("my_project", Some("dev"));
    let metadata = ctx.build_metadata("stg_orders", "table");
    let comment = build_query_comment(&metadata);
    assert!(comment.starts_with("\n/* ff_metadata:"));
    assert!(comment.ends_with("*/"));
    assert!(comment.contains("stg_orders"));
    assert!(comment.contains("my_project"));
    assert!(comment.contains("\"materialization\":\"table\""));
    assert!(comment.contains("\"target\":\"dev\""));
}

#[test]
fn test_append_and_strip() {
    let sql = "SELECT * FROM orders";
    let comment = "\n/* ff_metadata: {\"model\":\"test\"} */";
    let with_comment = append_query_comment(sql, comment);
    assert!(with_comment.contains("ff_metadata"));
    let stripped = strip_query_comment(&with_comment);
    assert_eq!(stripped, sql);
}

#[test]
fn test_strip_no_comment() {
    let sql = "SELECT * FROM orders";
    assert_eq!(strip_query_comment(sql), sql);
}

#[test]
fn test_whoami() {
    let user = whoami();
    assert!(!user.is_empty());
}

#[test]
fn test_metadata_serialization() {
    let ctx = QueryCommentContext::new("proj", None);
    let metadata = ctx.build_metadata("my_model", "view");
    let json = serde_json::to_string(&metadata).unwrap();
    assert!(json.contains("\"model\":\"my_model\""));
    assert!(json.contains("\"project\":\"proj\""));
    assert!(json.contains("\"target\":null"));
}

#[test]
fn test_context_fields() {
    let ctx = QueryCommentContext::new("test_proj", Some("prod"));
    assert_eq!(ctx.project_name, "test_proj");
    assert_eq!(ctx.target, Some("prod".to_string()));
    assert!(!ctx.invocation_id.is_empty());
    assert!(!ctx.user.is_empty());
    assert!(!ctx.featherflow_version.is_empty());
}

#[test]
fn test_config_default_enabled() {
    let config = crate::config::QueryCommentConfig::default();
    assert!(config.enabled);
}
