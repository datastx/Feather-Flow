use super::*;
use crate::error::{SqlError, SqlResult};
use crate::parser::SqlParser;
use sqlparser::ast::Statement;

fn validate_statements(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        validate_statement(stmt)?;
    }
    Ok(())
}

fn validate_statement(statement: &Statement) -> SqlResult<()> {
    match statement {
        Statement::Query(_) => Ok(()),
        Statement::Insert(_) => Err(SqlError::UnsupportedStatement(
            "INSERT statements are not allowed in models".to_string(),
        )),
        Statement::Update { .. } => Err(SqlError::UnsupportedStatement(
            "UPDATE statements are not allowed in models".to_string(),
        )),
        Statement::Delete(_) => Err(SqlError::UnsupportedStatement(
            "DELETE statements are not allowed in models".to_string(),
        )),
        Statement::Drop { .. } => Err(SqlError::UnsupportedStatement(
            "DROP statements are not allowed in models".to_string(),
        )),
        Statement::Truncate { .. } => Err(SqlError::UnsupportedStatement(
            "TRUNCATE statements are not allowed in models".to_string(),
        )),
        _ => Ok(()),
    }
}

fn is_select_statement(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_))
}

fn validate_sql(sql: &str) -> SqlResult<()> {
    let parser = SqlParser::duckdb();
    let stmts = parser.parse(sql)?;
    validate_statements(&stmts)
}

fn validate_no_complex(sql: &str) -> SqlResult<()> {
    let parser = SqlParser::duckdb();
    let stmts = parser.parse(sql)?;
    validate_no_complex_queries(&stmts)
}

#[test]
fn test_validate_select() {
    assert!(validate_sql("SELECT id FROM users").is_ok());
}

#[test]
fn test_validate_insert_fails() {
    assert!(validate_sql("INSERT INTO users VALUES (1)").is_err());
}

#[test]
fn test_validate_update_fails() {
    assert!(validate_sql("UPDATE users SET name = 'test'").is_err());
}

#[test]
fn test_validate_delete_fails() {
    assert!(validate_sql("DELETE FROM users").is_err());
}

#[test]
fn test_is_select() {
    let parser = SqlParser::duckdb();
    let stmt = parser.parse_single("SELECT 1").unwrap();
    assert!(is_select_statement(&stmt));
}

#[test]
fn test_cte_rejected() {
    let result =
        validate_no_complex("WITH staged AS (SELECT id FROM raw_users) SELECT id FROM staged");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, SqlError::CteNotAllowed { ref cte_names } if cte_names == &["staged"]),
        "Expected CteNotAllowed, got: {:?}",
        err
    );
}

#[test]
fn test_multiple_ctes_rejected() {
    let result = validate_no_complex(
        "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT a.id FROM a JOIN b ON true",
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, SqlError::CteNotAllowed { ref cte_names } if cte_names.contains(&"a".to_string()) && cte_names.contains(&"b".to_string())),
    );
}

#[test]
fn test_derived_table_rejected() {
    let result = validate_no_complex("SELECT id FROM (SELECT id, name FROM users) AS sub");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SqlError::DerivedTableNotAllowed
    ));
}

#[test]
fn test_derived_table_in_join_rejected() {
    let result = validate_no_complex(
        "SELECT o.id FROM orders o JOIN (SELECT id FROM customers) AS c ON o.customer_id = c.id",
    );
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SqlError::DerivedTableNotAllowed
    ));
}

#[test]
fn test_scalar_subquery_allowed() {
    let result =
        validate_no_complex("SELECT id, (SELECT MAX(amount) FROM orders) AS max_amount FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_where_subquery_allowed() {
    let result =
        validate_no_complex("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)");
    assert!(result.is_ok());
}

#[test]
fn test_simple_select_passes() {
    let result = validate_no_complex("SELECT id, name FROM users WHERE active = true");
    assert!(result.is_ok());
}

#[test]
fn test_join_without_derived_passes() {
    let result = validate_no_complex(
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_select_star_rejected() {
    let result = validate_no_complex("SELECT * FROM users");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SqlError::SelectStarNotAllowed
    ));
}

#[test]
fn test_qualified_wildcard_rejected() {
    let result = validate_no_complex("SELECT t.* FROM users t");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SqlError::SelectStarNotAllowed
    ));
}

#[test]
fn test_select_star_in_union_rejected() {
    let result = validate_no_complex("SELECT * FROM users UNION ALL SELECT * FROM customers");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SqlError::SelectStarNotAllowed
    ));
}

#[test]
fn test_explicit_columns_allowed() {
    let result = validate_no_complex("SELECT id, name FROM users");
    assert!(result.is_ok());
}
