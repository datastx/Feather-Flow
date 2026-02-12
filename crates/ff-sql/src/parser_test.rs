use super::*;

#[test]
fn test_parse_select() {
    let parser = SqlParser::duckdb();
    let stmts = parser
        .parse("SELECT id, name FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_parse_multiple_statements() {
    let parser = SqlParser::duckdb();
    let stmts = parser.parse("SELECT 1; SELECT 2;").unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_parse_empty() {
    let parser = SqlParser::duckdb();
    let result = parser.parse("");
    assert!(matches!(result, Err(SqlError::EmptySql)));
}

#[test]
fn test_parse_error() {
    let parser = SqlParser::duckdb();
    let result = parser.parse("SELECT FROM");
    assert!(result.is_err());
}

#[test]
fn test_from_dialect_name() {
    let parser = SqlParser::from_dialect_name("duckdb").unwrap();
    assert_eq!(parser.dialect_name(), "duckdb");

    let parser = SqlParser::from_dialect_name("snowflake").unwrap();
    assert_eq!(parser.dialect_name(), "snowflake");

    let result = SqlParser::from_dialect_name("unknown");
    assert!(result.is_err());
}
