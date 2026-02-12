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

/// Parse line and column from sqlparser error message.
///
/// sqlparser 0.60's `ParserError` is a simple string wrapper with no structured
/// location data, so we extract "Line: N, Column: M" from the error message text.
fn parse_location_from_error(msg: &str) -> (usize, usize) {
    let Some(line_idx) = msg.find("Line: ") else {
        return (0, 0);
    };
    let line_start = line_idx + 6;
    let Some(comma_idx) = msg[line_start..].find(',') else {
        return (0, 0);
    };
    let Ok(line) = msg[line_start..line_start + comma_idx]
        .trim()
        .parse::<usize>()
    else {
        return (0, 0);
    };
    let Some(col_idx) = msg.find("Column: ") else {
        return (0, 0);
    };
    let col_start = col_idx + 8;
    let col_end = msg[col_start..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| col_start + i)
        .unwrap_or(msg.len());
    let Ok(column) = msg[col_start..col_end].trim().parse::<usize>() else {
        return (0, 0);
    };
    (line, column)
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
#[path = "dialect_test.rs"]
mod tests;
