//! SQL parser wrapper

use crate::dialect::{DuckDbDialect, SnowflakeDialect, SqlDialect};
use crate::error::{SqlError, SqlResult};
use sqlparser::ast::Statement;

/// SQL parser that wraps sqlparser-rs with dialect support
pub struct SqlParser {
    dialect: Box<dyn SqlDialect>,
}

impl SqlParser {
    /// Create a new parser with DuckDB dialect
    pub fn duckdb() -> Self {
        Self {
            dialect: Box::new(DuckDbDialect::new()),
        }
    }

    /// Create a new parser with Snowflake dialect
    pub fn snowflake() -> Self {
        Self {
            dialect: Box::new(SnowflakeDialect::new()),
        }
    }

    /// Create a parser from dialect name
    pub fn from_dialect_name(name: &str) -> SqlResult<Self> {
        match name.to_lowercase().as_str() {
            "duckdb" => Ok(Self::duckdb()),
            "snowflake" => Ok(Self::snowflake()),
            _ => Err(SqlError::UnknownDialect(name.to_string())),
        }
    }

    /// Parse SQL into AST statements
    pub fn parse(&self, sql: &str) -> SqlResult<Vec<Statement>> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(SqlError::EmptySql);
        }

        self.dialect.parse(sql)
    }

    /// Parse SQL and return the first statement
    pub fn parse_single(&self, sql: &str) -> SqlResult<Statement> {
        let stmts = self.parse(sql)?;
        stmts.into_iter().next().ok_or_else(|| SqlError::EmptySql)
    }

    /// Get the dialect name
    pub fn dialect_name(&self) -> &'static str {
        self.dialect.name()
    }

    /// Quote an identifier for the current dialect
    pub fn quote_ident(&self, ident: &str) -> String {
        self.dialect.quote_ident(ident)
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::duckdb()
    }
}

#[cfg(test)]
#[path = "parser_test.rs"]
mod tests;
