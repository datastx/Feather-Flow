//! Error types for ff-db

use thiserror::Error;

/// Database operation errors
#[derive(Error, Debug)]
pub enum DbError {
    /// Connection error with preserved source chain (D001)
    #[error("[D001] Database connection failed: {message}")]
    ConnectionFailed {
        message: String,
        #[source]
        source: duckdb::Error,
    },

    /// Query execution error (D002)
    #[error("[D002] SQL execution failed: {0}")]
    ExecutionError(String),

    /// Query execution error with preserved source chain (D002)
    #[error("[D002] SQL execution failed: {context}")]
    ExecutionFailed {
        context: String,
        #[source]
        source: duckdb::Error,
    },

    /// DuckDB driver error with preserved source chain (D002)
    #[error("[D002] SQL execution failed")]
    DuckDb(#[source] duckdb::Error),

    /// Table not found (D003)
    #[error("[D003] Table or view not found: {0}")]
    TableNotFound(String),

    /// Not implemented (D005)
    #[error("[D005] Feature not implemented for {backend}: {feature}")]
    NotImplemented { backend: String, feature: String },

    /// Mutex poisoned (D006)
    #[error("[D006] Database mutex poisoned: {0}")]
    MutexPoisoned(String),
}

impl DbError {
    /// Check whether this is a DuckDB "wrong relation type" error (e.g. trying
    /// to DROP VIEW on a TABLE). Used by `drop_if_exists` to silently skip
    /// type mismatches.
    pub(crate) fn is_wrong_relation_type(&self) -> bool {
        match self {
            DbError::ExecutionError(msg) => msg.contains("trying to drop type"),
            DbError::ExecutionFailed { source, .. } => {
                source.to_string().contains("trying to drop type")
            }
            _ => false,
        }
    }
}

/// Result type alias for DbError
pub type DbResult<T> = Result<T, DbError>;

impl From<duckdb::Error> for DbError {
    fn from(err: duckdb::Error) -> Self {
        let msg = err.to_string();
        if is_table_not_found(&msg) {
            DbError::TableNotFound(msg)
        } else {
            DbError::DuckDb(err)
        }
    }
}

fn is_table_not_found(msg: &str) -> bool {
    msg.contains("Table with name")
        || msg.contains("View with name")
        || msg.contains("Table or view with name")
        || (msg.contains("Catalog Error") && msg.contains("Table") && msg.contains("not found"))
}
