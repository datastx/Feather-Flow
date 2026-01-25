//! SQL validation utilities

use crate::error::{SqlError, SqlResult};
use sqlparser::ast::Statement;

/// Validate that SQL contains only supported statements
pub fn validate_statements(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        validate_statement(stmt)?;
    }
    Ok(())
}

/// Validate a single SQL statement
pub fn validate_statement(statement: &Statement) -> SqlResult<()> {
    match statement {
        // Supported statements for models
        Statement::Query(_) => Ok(()),

        // Unsupported statements
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

        // Other statements - allow for now (CREATE, etc. might be used in edge cases)
        _ => Ok(()),
    }
}

/// Check if SQL is a SELECT statement
pub fn is_select_statement(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn validate_sql(sql: &str) -> SqlResult<()> {
        let parser = SqlParser::duckdb();
        let stmts = parser.parse(sql)?;
        validate_statements(&stmts)
    }

    #[test]
    fn test_validate_select() {
        assert!(validate_sql("SELECT * FROM users").is_ok());
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
}
