//! Error types for ff-jinja

use thiserror::Error;

/// Jinja templating errors
#[derive(Error, Debug)]
pub enum JinjaError {
    /// Template render error (J001)
    #[error("[J001] Jinja render error: {0}")]
    RenderError(String),

    /// Unknown variable (J002)
    #[error("[J002] Undefined variable '{name}'. Define it in vars: section of featherflow.yml")]
    UnknownVariable { name: String },

    /// Invalid config key (J003)
    #[error("[J003] Invalid config key '{key}'. Valid keys: materialized, schema, tags")]
    InvalidConfigKey { key: String },

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for JinjaError
pub type JinjaResult<T> = Result<T, JinjaError>;

impl From<minijinja::Error> for JinjaError {
    fn from(err: minijinja::Error) -> Self {
        JinjaError::RenderError(err.to_string())
    }
}
