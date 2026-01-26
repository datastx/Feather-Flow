//! Error types for ff-sql

use thiserror::Error;

/// SQL parsing and analysis errors
#[derive(Error, Debug)]
pub enum SqlError {
    /// SQL parse error (S001)
    #[error("[S001] SQL parse error at line {line}, column {column}: {message}")]
    ParseError {
        message: String,
        line: usize,
        column: usize,
    },

    /// Empty SQL (S002)
    #[error("[S002] SQL is empty")]
    EmptySql,

    /// Unsupported SQL statement (S003)
    #[error("[S003] Unsupported SQL statement type: {0}")]
    UnsupportedStatement(String),

    /// Validation error (S004)
    #[error("[S004] SQL validation failed: {0}")]
    ValidationError(String),
}

/// Result type alias for SqlError
pub type SqlResult<T> = Result<T, SqlError>;
