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

    // S004: Reserved/retired — was previously used for an error code that has been removed.
    // Do not reuse this code to avoid confusion with historical diagnostics.
    /// CTE not allowed (S005)
    #[error("[S005] CTEs are not allowed — each transform must be its own model. Found CTE(s): {}", cte_names.join(", "))]
    CteNotAllowed { cte_names: Vec<String> },

    /// Derived table not allowed (S006)
    #[error("[S006] Derived tables (subqueries in FROM clause) are not allowed — each transform must be its own model")]
    DerivedTableNotAllowed,

    /// Unknown SQL dialect (S007)
    #[error("[S007] Unknown SQL dialect: {0}")]
    UnknownDialect(String),

    /// Ephemeral model inlining failed (S008)
    #[error("[S008] Failed to inline ephemeral model '{model_name}': {reason}")]
    InlineError { model_name: String, reason: String },
    // S009: Reserved/retired — SELECT * is now allowed (DataFusion expands wildcards transparently).
    // Do not reuse this code to avoid confusion with historical diagnostics.
}

/// Result type alias for SqlError
pub type SqlResult<T> = Result<T, SqlError>;
