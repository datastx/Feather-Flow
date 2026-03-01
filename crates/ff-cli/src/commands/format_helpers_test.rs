use super::*;
use ff_core::config::{Dialect, FormatConfig};

#[test]
fn test_build_mode_duckdb_dialect() {
    let config = FormatConfig::default();
    let mode = build_sqlfmt_mode(&config, Dialect::DuckDb);
    assert_eq!(mode.dialect_name, "duckdb");
    assert_eq!(mode.line_length, 88);
    assert!(!mode.no_jinjafmt);
    assert!(mode.quiet);
}

#[test]
fn test_build_mode_snowflake_dialect() {
    let config = FormatConfig::default();
    let mode = build_sqlfmt_mode(&config, Dialect::Snowflake);
    assert_eq!(mode.dialect_name, "polyglot");
}

#[test]
fn test_build_mode_custom_line_length() {
    let config = FormatConfig {
        line_length: 120,
        ..Default::default()
    };
    let mode = build_sqlfmt_mode(&config, Dialect::DuckDb);
    assert_eq!(mode.line_length, 120);
}

#[test]
fn test_build_mode_no_jinjafmt() {
    let config = FormatConfig {
        no_jinjafmt: true,
        ..Default::default()
    };
    let mode = build_sqlfmt_mode(&config, Dialect::DuckDb);
    assert!(mode.no_jinjafmt);
}

#[test]
fn test_format_string_basic() {
    let mode = build_sqlfmt_mode(&FormatConfig::default(), Dialect::DuckDb);
    let result = sqlfmt::format_string("select 1", &mode);
    assert!(result.is_ok());
    assert!(!result.unwrap().is_empty());
}

#[test]
fn test_jinja_preservation() {
    let mode = build_sqlfmt_mode(&FormatConfig::default(), Dialect::DuckDb);
    let sql = "select {{ config() }}, id from orders";
    let result = sqlfmt::format_string(sql, &mode).unwrap();
    assert!(result.contains("{{ config() }}") || result.contains("{{config()}}"));
}
