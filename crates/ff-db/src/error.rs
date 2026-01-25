//! Error types for ff-db

use thiserror::Error;

/// Database operation errors
#[derive(Error, Debug)]
pub enum DbError {
    /// Connection error
    #[error("Database connection error: {0}")]
    ConnectionError(String),

    /// Query execution error
    #[error("Query execution error: {0}")]
    ExecutionError(String),

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// CSV loading error
    #[error("CSV loading error: {0}")]
    CsvError(String),

    /// Not implemented
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Internal error
    #[error("Internal database error: {0}")]
    Internal(String),
}

/// Result type alias for DbError
pub type DbResult<T> = Result<T, DbError>;

impl From<duckdb::Error> for DbError {
    fn from(err: duckdb::Error) -> Self {
        DbError::ExecutionError(err.to_string())
    }
}
