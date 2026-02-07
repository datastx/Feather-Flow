//! Error types for ff-analysis

use thiserror::Error;

/// Analysis error type
#[derive(Error, Debug)]
pub enum AnalysisError {
    /// A001: Failed to lower SQL statement to IR
    #[error("[A001] Failed to lower SQL for model '{model}': {message}")]
    LoweringFailed { model: String, message: String },

    /// A002: Unsupported SQL construct during lowering
    #[error("[A002] Unsupported SQL construct in '{model}': {construct}")]
    UnsupportedConstruct { model: String, construct: String },

    /// A003: Schema catalog lookup failed
    #[error("[A003] Unknown table '{table}' referenced in model '{model}'")]
    UnknownTable { model: String, table: String },

    /// A004: Column resolution failed
    #[error("[A004] Cannot resolve column '{column}' in model '{model}'")]
    UnresolvedColumn { model: String, column: String },

    /// A005: SQL parse error during analysis
    #[error("[A005] SQL parse error: {0}")]
    SqlParse(String),

    /// A006: Core error propagation
    #[error("[A006] Core error: {0}")]
    Core(#[from] ff_core::CoreError),

    /// A007: SQL crate error propagation
    #[error("[A007] SQL error: {0}")]
    Sql(#[from] ff_sql::SqlError),
}

/// Result type alias for AnalysisError
pub type AnalysisResult<T> = Result<T, AnalysisError>;
