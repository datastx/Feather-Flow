//! Error types for ff-db

use thiserror::Error;

/// Database operation errors
#[derive(Error, Debug)]
pub enum DbError {
    /// Connection error (D001)
    #[error("[D001] Database connection failed: {0}")]
    ConnectionError(String),

    /// Query execution error (D002)
    #[error("[D002] SQL execution failed: {0}")]
    ExecutionError(String),

    /// Table not found (D003)
    #[error("[D003] Table or view not found: {0}")]
    TableNotFound(String),

    /// CSV loading error (D004)
    #[error("[D004] CSV load failed: {0}")]
    CsvError(String),

    /// Not implemented (D005)
    #[error("[D005] Feature not implemented for {backend}: {feature}")]
    NotImplemented { backend: String, feature: String },

    /// Mutex poisoned (D006)
    #[error("[D006] Database mutex poisoned: {0}")]
    MutexPoisoned(String),

    /// Internal error (D007)
    #[error("[D007] Internal database error: {0}")]
    Internal(String),
}

/// Result type alias for DbError
pub type DbResult<T> = Result<T, DbError>;

impl From<duckdb::Error> for DbError {
    fn from(err: duckdb::Error) -> Self {
        // Classify DuckDB errors by inspecting the error message.
        // duckdb::Error does not expose structured variants, so string
        // matching is the only reliable approach. We use narrow patterns
        // to avoid misclassifying function/type/schema errors.
        let msg = err.to_string();
        if msg.contains("Table with name")
            || msg.contains("View with name")
            || msg.contains("Table or view with name")
            || (msg.contains("Catalog Error") && msg.contains("Table") && msg.contains("not found"))
        {
            DbError::TableNotFound(msg)
        } else {
            DbError::ExecutionError(msg)
        }
    }
}
