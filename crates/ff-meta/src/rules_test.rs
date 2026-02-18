use super::*;
use crate::MetaDb;
use ff_core::rules::{RuleFile, RuleSeverity};
use std::path::PathBuf;

fn open_meta() -> MetaDb {
    MetaDb::open_memory().unwrap()
}

fn make_rule(name: &str, sql: &str, severity: RuleSeverity) -> RuleFile {
    RuleFile {
        name: name.to_string(),
        severity,
        description: Some(format!("Test rule: {name}")),
        sql: sql.to_string(),
        path: PathBuf::from(format!("rules/{name}.sql")),
    }
}

#[test]
fn rule_returning_zero_rows_passes() {
    let meta = open_meta();
    let rule = make_rule("no_violations", "SELECT 1 WHERE false", RuleSeverity::Error);
    let (result, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert!(result.passed);
    assert_eq!(result.violation_count, 0);
    assert!(violations.is_empty());
    assert!(result.error.is_none());
}

#[test]
fn rule_returning_rows_fails() {
    let meta = open_meta();
    let rule = make_rule(
        "always_fails",
        "SELECT 'model_a' AS model_name, 'bad practice' AS violation",
        RuleSeverity::Error,
    );
    let (result, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert!(!result.passed);
    assert_eq!(result.violation_count, 1);
    assert_eq!(violations[0].message, "bad practice");
    assert_eq!(violations[0].entity_name.as_deref(), Some("model_a"));
    assert_eq!(violations[0].severity, RuleSeverity::Error);
}

#[test]
fn rule_uses_message_column() {
    let meta = open_meta();
    let rule = make_rule(
        "msg_col",
        "SELECT 'something went wrong' AS message",
        RuleSeverity::Warn,
    );
    let (_, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert_eq!(violations[0].message, "something went wrong");
}

#[test]
fn rule_uses_entity_name_column() {
    let meta = open_meta();
    let rule = make_rule(
        "entity_col",
        "SELECT 'my_model' AS entity_name, 'issue found' AS violation",
        RuleSeverity::Error,
    );
    let (_, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert_eq!(violations[0].entity_name.as_deref(), Some("my_model"));
}

#[test]
fn rule_extra_columns_in_context_json() {
    let meta = open_meta();
    let rule = make_rule(
        "extra_cols",
        "SELECT 'bad' AS violation, 'models/a.sql' AS source_path, 42 AS count",
        RuleSeverity::Error,
    );
    let (_, violations) = execute_rule(meta.conn(), &rule).unwrap();

    let ctx = violations[0].context_json.as_deref().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(ctx).unwrap();
    assert_eq!(parsed["source_path"], "models/a.sql");
    assert_eq!(parsed["count"], "42");
}

#[test]
fn rule_invalid_sql_returns_error() {
    let meta = open_meta();
    let rule = make_rule("bad_sql", "SELECTTTT garbage", RuleSeverity::Error);
    let (result, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert!(!result.passed);
    assert!(result.error.is_some());
    assert!(violations.is_empty());
}

#[test]
fn rule_severity_is_preserved() {
    let meta = open_meta();
    let rule = make_rule(
        "warn_rule",
        "SELECT 'warning' AS violation",
        RuleSeverity::Warn,
    );
    let (result, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert_eq!(result.severity, RuleSeverity::Warn);
    assert_eq!(violations[0].severity, RuleSeverity::Warn);
}

#[test]
fn execute_all_rules_combines_results() {
    let meta = open_meta();
    let rules = vec![
        make_rule("pass", "SELECT 1 WHERE false", RuleSeverity::Error),
        make_rule("fail", "SELECT 'oops' AS violation", RuleSeverity::Error),
    ];
    let (results, violations) = execute_all_rules(meta.conn(), &rules).unwrap();

    assert_eq!(results.len(), 2);
    assert!(results[0].passed);
    assert!(!results[1].passed);
    assert_eq!(violations.len(), 1);
}

#[test]
fn populate_rule_violations_inserts_rows() {
    let meta = open_meta();
    let conn = meta.conn();

    conn.execute(
        "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('test', '/tmp', '/tmp/dev.duckdb')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES (1, 'rules')",
        [],
    )
    .unwrap();
    let run_id: i64 = conn
        .query_row(
            "SELECT run_id FROM ff_meta.compilation_runs LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();

    let violations = vec![
        RuleViolation {
            rule_name: "test_rule".to_string(),
            rule_path: "rules/test.sql".to_string(),
            severity: RuleSeverity::Error,
            entity_name: Some("model_a".to_string()),
            message: "violation found".to_string(),
            context_json: Some(r#"{"count":"5"}"#.to_string()),
        },
        RuleViolation {
            rule_name: "test_rule".to_string(),
            rule_path: "rules/test.sql".to_string(),
            severity: RuleSeverity::Warn,
            entity_name: None,
            message: "minor issue".to_string(),
            context_json: None,
        },
    ];

    populate_rule_violations(conn, run_id, &violations).unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM ff_meta.rule_violations WHERE run_id = ?",
            duckdb::params![run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    let msg: String = conn
        .query_row(
            "SELECT message FROM ff_meta.rule_violations WHERE entity_name = 'model_a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(msg, "violation found");
}

#[test]
fn rule_no_message_column_uses_first_value() {
    let meta = open_meta();
    let rule = make_rule(
        "numeric_only",
        "SELECT 42 AS some_count, 'info text' AS detail",
        RuleSeverity::Error,
    );
    let (_, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert_eq!(violations[0].message, "42");
}

#[test]
fn rule_with_no_columns_zero_violations() {
    let meta = open_meta();
    let rule = make_rule("empty", "SELECT 1 WHERE false", RuleSeverity::Error);
    let (result, violations) = execute_rule(meta.conn(), &rule).unwrap();

    assert!(result.passed);
    assert!(violations.is_empty());
}
