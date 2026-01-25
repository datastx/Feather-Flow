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
            // Extract line/column if possible
            let msg = e.to_string();
            SqlError::ParseError {
                message: msg,
                line: 0,
                column: 0,
            }
        })
    }

    /// Quote an identifier for this dialect
    fn quote_ident(&self, ident: &str) -> String;

    /// Get the dialect name
    fn name(&self) -> &'static str;
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
}
