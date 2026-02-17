//! Error types for the meta database.

use thiserror::Error;

/// Meta database errors.
#[derive(Error, Debug)]
pub enum MetaError {
    /// Failed to open or create the meta database (M001).
    #[error("[M001] Meta database connection failed: {0}")]
    ConnectionError(String),

    /// Schema migration failed (M002).
    #[error("[M002] Meta database migration failed: {0}")]
    MigrationError(String),

    /// SQL execution error inside the meta database (M003).
    #[error("[M003] Meta database query failed: {0}")]
    QueryError(String),

    /// Transaction management error (M004).
    #[error("[M004] Meta database transaction failed: {0}")]
    TransactionError(String),

    /// Population error — data could not be inserted (M005).
    #[error("[M005] Meta database population failed: {0}")]
    PopulationError(String),

    /// Rule execution error (M006).
    #[error("[M006] Meta database rule execution failed: {0}")]
    RuleError(String),

    /// DuckDB driver error with preserved source chain (M007).
    #[error("[M007] DuckDB error")]
    DuckDb(#[source] duckdb::Error),
}

/// Result type alias for [`MetaError`].
pub type MetaResult<T> = Result<T, MetaError>;

impl From<duckdb::Error> for MetaError {
    fn from(err: duckdb::Error) -> Self {
        MetaError::DuckDb(err)
    }
}
