use super::*;
use crate::config::{CommentInclude, CommentPlacement, CommentStyle, QueryCommentConfig};
use std::collections::HashMap;

fn default_config() -> QueryCommentConfig {
    QueryCommentConfig::default()
}

fn make_input<'a>(
    name: &'a str,
    mat: &'a str,
    path: Option<&'a str>,
    schema: Option<&'a str>,
) -> ModelCommentInput<'a> {
    ModelCommentInput {
        model_name: name,
        materialization: mat,
        node_path: path,
        schema,
    }
}

#[test]
fn test_build_comment_compact_default() {
    let ctx = QueryCommentContext::new("my_project", Some("dev"), default_config());
    let input = make_input("stg_orders", "table", Some("models/stg_orders.sql"), None);
    let comment = ctx.build_comment(&input);
    assert!(comment.starts_with("/* ff_metadata:"));
    assert!(comment.ends_with("*/"));
    assert!(comment.contains("stg_orders"));
    assert!(comment.contains("my_project"));
    assert!(comment.contains("\"materialization\":\"table\""));
    assert!(comment.contains("\"target\":\"dev\""));
    assert!(comment.contains("\"node_path\":\"models/stg_orders.sql\""));
}

#[test]
fn test_build_comment_pretty_style() {
    let config = QueryCommentConfig {
        style: CommentStyle::Pretty,
        ..default_config()
    };
    let ctx = QueryCommentContext::new("my_project", None, config);
    let input = make_input("dim_users", "view", None, None);
    let comment = ctx.build_comment(&input);
    assert!(comment.starts_with("/*\nff_metadata:\n"));
    assert!(comment.ends_with("\n*/"));
    assert!(comment.contains("\"model\": \"dim_users\""));
}

#[test]
fn test_attach_append_placement() {
    let sql = "SELECT * FROM orders";
    let comment = "/* ff_metadata: {} */";
    let result = attach_query_comment(sql, comment, CommentPlacement::Append);
    assert!(result.starts_with("SELECT * FROM orders\n"));
    assert!(result.ends_with("/* ff_metadata: {} */"));
}

#[test]
fn test_attach_prepend_placement() {
    let sql = "SELECT * FROM orders";
    let comment = "/* ff_metadata: {} */";
    let result = attach_query_comment(sql, comment, CommentPlacement::Prepend);
    assert!(result.starts_with("/* ff_metadata: {} */\n"));
    assert!(result.ends_with("SELECT * FROM orders"));
}

#[test]
fn test_strip_appended_compact_comment() {
    let sql = "SELECT * FROM orders\n/* ff_metadata: {\"model\":\"test\"} */";
    let stripped = strip_query_comment(sql);
    assert_eq!(stripped, "SELECT * FROM orders");
}

#[test]
fn test_strip_appended_pretty_comment() {
    let sql = "SELECT * FROM orders\n/*\nff_metadata:\n{\"model\":\"test\"}\n*/";
    let stripped = strip_query_comment(sql);
    assert_eq!(stripped, "SELECT * FROM orders");
}

#[test]
fn test_strip_prepended_compact_comment() {
    let sql = "/* ff_metadata: {\"model\":\"test\"} */\nSELECT * FROM orders";
    let stripped = strip_query_comment(sql);
    assert_eq!(stripped, "SELECT * FROM orders");
}

#[test]
fn test_strip_prepended_pretty_comment() {
    let sql = "/*\nff_metadata:\n{\"model\":\"test\"}\n*/\nSELECT * FROM orders";
    let stripped = strip_query_comment(sql);
    assert_eq!(stripped, "SELECT * FROM orders");
}

#[test]
fn test_strip_no_comment() {
    let sql = "SELECT * FROM orders";
    assert_eq!(strip_query_comment(sql), sql);
}

#[test]
fn test_include_filter_suppresses_fields() {
    let config = QueryCommentConfig {
        include: CommentInclude {
            model: true,
            project: true,
            materialization: false,
            compiled_at: false,
            target: false,
            invocation_id: false,
            user: false,
            featherflow_version: false,
            node_path: false,
            schema: false,
        },
        ..default_config()
    };
    let ctx = QueryCommentContext::new("proj", Some("dev"), config);
    let input = make_input("m", "table", Some("models/m.sql"), Some("analytics"));
    let comment = ctx.build_comment(&input);
    assert!(comment.contains("\"model\":\"m\""));
    assert!(comment.contains("\"project\":\"proj\""));
    assert!(!comment.contains("materialization"));
    assert!(!comment.contains("compiled_at"));
    assert!(!comment.contains("\"target\""));
    assert!(!comment.contains("invocation_id"));
    assert!(!comment.contains("\"user\""));
    assert!(!comment.contains("featherflow_version"));
    assert!(!comment.contains("node_path"));
    assert!(!comment.contains("schema"));
}

#[test]
fn test_custom_vars_appear_in_comment() {
    let mut custom = HashMap::new();
    custom.insert("team".to_string(), "data-eng".to_string());
    custom.insert("env_id".to_string(), "ci-42".to_string());
    let config = QueryCommentConfig {
        custom_vars: custom,
        ..default_config()
    };
    let ctx = QueryCommentContext::new("proj", None, config);
    let input = make_input("m", "view", None, None);
    let comment = ctx.build_comment(&input);
    assert!(comment.contains("\"team\":\"data-eng\""));
    assert!(comment.contains("\"env_id\":\"ci-42\""));
}

#[test]
fn test_schema_and_node_path_in_metadata() {
    let ctx = QueryCommentContext::new("proj", None, default_config());
    let input = make_input("m", "table", Some("models/staging/m.sql"), Some("staging"));
    let metadata = ctx.build_metadata(&input);
    assert_eq!(metadata.node_path.as_deref(), Some("models/staging/m.sql"));
    assert_eq!(metadata.schema.as_deref(), Some("staging"));
}

#[test]
fn test_whoami() {
    let user = whoami();
    assert!(!user.is_empty());
}

#[test]
fn test_context_fields() {
    let ctx = QueryCommentContext::new("test_proj", Some("prod"), default_config());
    assert_eq!(ctx.project_name, "test_proj");
    assert_eq!(ctx.target, Some("prod".to_string()));
    assert!(!ctx.invocation_id.is_empty());
    assert!(!ctx.user.is_empty());
    assert!(!ctx.featherflow_version.is_empty());
}

#[test]
fn test_config_default_enabled() {
    let config = QueryCommentConfig::default();
    assert!(config.enabled);
    assert_eq!(config.placement, CommentPlacement::Append);
    assert_eq!(config.style, CommentStyle::Compact);
    assert!(config.custom_vars.is_empty());
    assert!(config.custom_fields.is_none());
    assert!(config.include.model);
    assert!(config.include.node_path);
    assert!(config.include.schema);
}

#[test]
fn test_interpolate_env_vars_known_var() {
    std::env::set_var("FF_TEST_VAR_123", "hello_world");
    let result = interpolate_env_vars("prefix-$FF_TEST_VAR_123-suffix");
    assert_eq!(result, "prefix-hello_world-suffix");
    std::env::remove_var("FF_TEST_VAR_123");
}

#[test]
fn test_interpolate_env_vars_unknown_var() {
    std::env::remove_var("FF_NONEXISTENT_VAR_XYZ");
    let result = interpolate_env_vars("before-$FF_NONEXISTENT_VAR_XYZ-after");
    assert_eq!(result, "before--after");
}

#[test]
fn test_interpolate_env_vars_no_vars() {
    let result = interpolate_env_vars("plain text no vars");
    assert_eq!(result, "plain text no vars");
}

#[test]
fn test_interpolate_env_vars_dollar_at_end() {
    let result = interpolate_env_vars("trailing$");
    assert_eq!(result, "trailing$");
}

#[test]
fn test_interpolate_env_vars_multiple_vars() {
    std::env::set_var("FF_TEST_A", "aaa");
    std::env::set_var("FF_TEST_B", "bbb");
    let result = interpolate_env_vars("$FF_TEST_A and $FF_TEST_B");
    assert_eq!(result, "aaa and bbb");
    std::env::remove_var("FF_TEST_A");
    std::env::remove_var("FF_TEST_B");
}

#[test]
fn test_custom_fields_appear_in_comment() {
    let mut fields = HashMap::new();
    fields.insert("ci_job".to_string(), "static-value".to_string());
    let config = QueryCommentConfig {
        custom_fields: Some(fields),
        ..default_config()
    };
    let ctx = QueryCommentContext::new("proj", None, config);
    let input = make_input("m", "view", None, None);
    let comment = ctx.build_comment(&input);
    assert!(comment.contains("\"ci_job\":\"static-value\""));
}

#[test]
fn test_custom_fields_env_interpolation() {
    std::env::set_var("FF_TEST_CI_JOB", "job-42");
    let mut fields = HashMap::new();
    fields.insert("ci_job".to_string(), "$FF_TEST_CI_JOB".to_string());
    let config = QueryCommentConfig {
        custom_fields: Some(fields),
        ..default_config()
    };
    let ctx = QueryCommentContext::new("proj", None, config);
    let input = make_input("m", "view", None, None);
    let metadata = ctx.build_metadata(&input);
    assert_eq!(
        metadata.custom_fields.as_ref().unwrap().get("ci_job"),
        Some(&"job-42".to_string())
    );
    std::env::remove_var("FF_TEST_CI_JOB");
}

#[test]
fn test_runtime_fields_none_by_default() {
    let ctx = QueryCommentContext::new("proj", None, default_config());
    let input = make_input("m", "view", None, None);
    let metadata = ctx.build_metadata(&input);
    assert!(metadata.execution_id.is_none());
    assert!(metadata.run_mode.is_none());
    assert!(metadata.is_full_refresh.is_none());
}

#[test]
fn test_runtime_fields_populated_via_with_runtime_fields() {
    let ctx = QueryCommentContext::new("proj", None, default_config())
        .with_runtime_fields("models", true);
    let input = make_input("m", "table", None, None);
    let metadata = ctx.build_metadata(&input);
    assert!(metadata.execution_id.is_some());
    assert!(!metadata.execution_id.as_ref().unwrap().is_empty());
    assert_eq!(metadata.run_mode.as_deref(), Some("models"));
    assert_eq!(metadata.is_full_refresh, Some(true));
}

#[test]
fn test_runtime_fields_in_comment_output() {
    let ctx = QueryCommentContext::new("proj", None, default_config())
        .with_runtime_fields("build", false);
    let input = make_input("m", "view", None, None);
    let comment = ctx.build_comment(&input);
    assert!(comment.contains("\"run_mode\":\"build\""));
    assert!(comment.contains("\"is_full_refresh\":false"));
    assert!(comment.contains("\"execution_id\":\""));
}

#[test]
fn test_runtime_fields_not_in_compile_only_comment() {
    let ctx = QueryCommentContext::new("proj", None, default_config());
    let input = make_input("m", "view", None, None);
    let comment = ctx.build_comment(&input);
    assert!(!comment.contains("execution_id"));
    assert!(!comment.contains("run_mode"));
    assert!(!comment.contains("is_full_refresh"));
}

#[test]
fn test_legacy_build_query_comment() {
    let ctx = QueryCommentContext::new("proj", None, default_config());
    let input = make_input("my_model", "view", None, None);
    let metadata = ctx.build_metadata(&input);
    let comment = build_query_comment(&metadata);
    assert!(comment.starts_with("/* ff_metadata:"));
    assert!(comment.contains("\"model\":\"my_model\""));
    assert!(comment.contains("\"project\":\"proj\""));
    assert!(comment.contains("\"target\":null"));
}

#[test]
fn test_legacy_append_query_comment() {
    let sql = "SELECT 1";
    let comment = "/* ff_metadata: {} */";
    let result = append_query_comment(sql, comment);
    assert_eq!(result, "SELECT 1\n/* ff_metadata: {} */");
}

#[test]
fn test_round_trip_append_strip() {
    let sql = "SELECT * FROM orders";
    let ctx = QueryCommentContext::new("proj", Some("dev"), default_config());
    let input = make_input("orders", "table", Some("models/orders.sql"), Some("main"));
    let comment = ctx.build_comment(&input);
    let with_comment = attach_query_comment(sql, &comment, CommentPlacement::Append);
    let stripped = strip_query_comment(&with_comment);
    assert_eq!(stripped, sql);
}

#[test]
fn test_round_trip_prepend_strip() {
    let sql = "SELECT * FROM orders";
    let ctx = QueryCommentContext::new("proj", Some("dev"), default_config());
    let input = make_input("orders", "table", None, None);
    let comment = ctx.build_comment(&input);
    let with_comment = attach_query_comment(sql, &comment, CommentPlacement::Prepend);
    let stripped = strip_query_comment(&with_comment);
    assert_eq!(stripped, sql);
}

#[test]
fn test_round_trip_pretty_append() {
    let sql = "SELECT * FROM orders";
    let config = QueryCommentConfig {
        style: CommentStyle::Pretty,
        ..default_config()
    };
    let ctx = QueryCommentContext::new("proj", None, config);
    let input = make_input("orders", "view", None, None);
    let comment = ctx.build_comment(&input);
    let with_comment = attach_query_comment(sql, &comment, CommentPlacement::Append);
    let stripped = strip_query_comment(&with_comment);
    assert_eq!(stripped, sql);
}

#[test]
fn test_round_trip_pretty_prepend() {
    let sql = "SELECT * FROM orders";
    let config = QueryCommentConfig {
        style: CommentStyle::Pretty,
        placement: CommentPlacement::Prepend,
        ..default_config()
    };
    let ctx = QueryCommentContext::new("proj", None, config);
    let input = make_input("orders", "view", None, None);
    let comment = ctx.build_comment(&input);
    let with_comment = attach_query_comment(sql, &comment, CommentPlacement::Prepend);
    let stripped = strip_query_comment(&with_comment);
    assert_eq!(stripped, sql);
}
