//! Error types for ff-jinja

use thiserror::Error;

/// Jinja templating errors
#[derive(Error, Debug)]
pub enum JinjaError {
    /// Template render error
    #[error("Template render error: {0}")]
    RenderError(String),

    /// Unknown variable
    #[error("Unknown variable: {name}")]
    UnknownVariable { name: String },

    /// Invalid config key
    #[error("Invalid config key: {key}")]
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
