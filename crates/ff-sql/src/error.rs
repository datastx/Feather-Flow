//! Error types for ff-sql

use thiserror::Error;

/// SQL parsing and analysis errors
#[derive(Error, Debug)]
pub enum SqlError {
    /// SQL parse error
    #[error("SQL parse error at line {line}, column {column}: {message}")]
    ParseError {
        message: String,
        line: usize,
        column: usize,
    },

    /// Empty SQL
    #[error("SQL is empty")]
    EmptySql,

    /// Unsupported SQL statement
    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),

    /// Validation error
    #[error("SQL validation error: {0}")]
    ValidationError(String),
}

/// Result type alias for SqlError
pub type SqlResult<T> = Result<T, SqlError>;
