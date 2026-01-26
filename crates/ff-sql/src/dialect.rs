//! SQL dialect abstraction

use sqlparser::ast::Statement;
use sqlparser::dialect::{
    Dialect, DuckDbDialect as SqlParserDuckDb, SnowflakeDialect as SqlParserSnowflake,
};
use sqlparser::parser::Parser;

use crate::error::{SqlError, SqlResult};

/// Trait for SQL dialect implementations
pub trait SqlDialect: Send + Sync {
    /// Get the underlying sqlparser dialect
    fn parser_dialect(&self) -> &dyn Dialect;

    /// Parse SQL into AST statements
    fn parse(&self, sql: &str) -> SqlResult<Vec<Statement>> {
        Parser::parse_sql(self.parser_dialect(), sql).map_err(|e| {
            let msg = e.to_string();
            // Extract line/column from error message (format: "... at Line: X, Column: Y")
            let (line, column) = parse_location_from_error(&msg);
            SqlError::ParseError {
                message: msg,
                line,
                column,
            }
        })
    }

    /// Quote an identifier for this dialect
    fn quote_ident(&self, ident: &str) -> String;

    /// Get the dialect name
    fn name(&self) -> &'static str;
}

/// Parse line and column from sqlparser error message
fn parse_location_from_error(msg: &str) -> (usize, usize) {
    // Look for pattern "Line: N, Column: M"
    if let Some(line_idx) = msg.find("Line: ") {
        let line_start = line_idx + 6;
        if let Some(comma_idx) = msg[line_start..].find(',') {
            if let Ok(line) = msg[line_start..line_start + comma_idx]
                .trim()
                .parse::<usize>()
            {
                if let Some(col_idx) = msg.find("Column: ") {
                    let col_start = col_idx + 8;
                    // Find end of number (could be end of string or non-digit)
                    let col_end = msg[col_start..]
                        .find(|c: char| !c.is_ascii_digit())
                        .map(|i| col_start + i)
                        .unwrap_or(msg.len());
                    if let Ok(column) = msg[col_start..col_end].trim().parse::<usize>() {
                        return (line, column);
                    }
                }
            }
        }
    }
    (0, 0)
}

/// DuckDB SQL dialect
pub struct DuckDbDialect {
    dialect: SqlParserDuckDb,
}

impl DuckDbDialect {
    /// Create a new DuckDB dialect
    pub fn new() -> Self {
        Self {
            dialect: SqlParserDuckDb {},
        }
    }
}

impl Default for DuckDbDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDialect for DuckDbDialect {
    fn parser_dialect(&self) -> &dyn Dialect {
        &self.dialect
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn name(&self) -> &'static str {
        "duckdb"
    }
}

/// Snowflake SQL dialect
pub struct SnowflakeDialect {
    dialect: SqlParserSnowflake,
}

impl SnowflakeDialect {
    /// Create a new Snowflake dialect
    pub fn new() -> Self {
        Self {
            dialect: SqlParserSnowflake {},
        }
    }
}

impl Default for SnowflakeDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDialect for SnowflakeDialect {
    fn parser_dialect(&self) -> &dyn Dialect {
        &self.dialect
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn name(&self) -> &'static str {
        "snowflake"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_parse() {
        let dialect = DuckDbDialect::new();
        let stmts = dialect.parse("SELECT * FROM users").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_snowflake_parse() {
        let dialect = SnowflakeDialect::new();
        let stmts = dialect.parse("SELECT * FROM users").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_quote_ident() {
        let dialect = DuckDbDialect::new();
        assert_eq!(dialect.quote_ident("user"), "\"user\"");
        assert_eq!(dialect.quote_ident("user\"name"), "\"user\"\"name\"");
    }

    #[test]
    fn test_parse_error_location() {
        let dialect = DuckDbDialect::new();
        let result = dialect.parse("SELECT\nFROM users");
        assert!(result.is_err());
        if let Err(crate::error::SqlError::ParseError { line, column, .. }) = result {
            // The error should have a non-zero line number
            assert!(
                line > 0 || column > 0,
                "Expected non-zero location, got line: {}, column: {}",
                line,
                column
            );
        }
    }

    #[test]
    fn test_parse_location_extraction() {
        // Test the helper function
        let (line, col) =
            super::parse_location_from_error("Expected: something at Line: 5, Column: 10");
        assert_eq!(line, 5);
        assert_eq!(col, 10);

        // Test with no location info
        let (line, col) = super::parse_location_from_error("Some error without location");
        assert_eq!(line, 0);
        assert_eq!(col, 0);
    }
}
