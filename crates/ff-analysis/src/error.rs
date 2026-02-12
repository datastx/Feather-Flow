//! Error types for ff-analysis

use thiserror::Error;

/// Analysis error type
///
/// These use the `AE` prefix (Analysis Error) to avoid collisions with
/// analysis pass diagnostic codes which use plain `A` codes (e.g. A002-A005
/// in `plan_type_inference`).
#[derive(Error, Debug)]
pub enum AnalysisError {
    /// AE003: Schema catalog lookup failed
    #[error("[AE003] Unknown table '{table}' referenced in model '{model}'")]
    UnknownTable { model: String, table: String },

    /// AE004: Column resolution failed
    #[error("[AE004] Cannot resolve column '{column}' in model '{model}'")]
    UnresolvedColumn { model: String, column: String },

    /// AE005: SQL parse error during analysis
    #[error("[AE005] SQL parse error: {0}")]
    SqlParse(String),

    /// AE006: Core error propagation
    #[error("[AE006] Core error: {0}")]
    Core(#[from] ff_core::CoreError),

    /// AE007: SQL crate error propagation
    #[error("[AE007] SQL error: {0}")]
    Sql(#[from] ff_sql::SqlError),

    /// AE008: DataFusion planning error
    #[error("[AE008] Planning error: {0}")]
    PlanningError(String),
}

/// Result type alias for AnalysisError
pub type AnalysisResult<T> = Result<T, AnalysisError>;
