//! Error types for ff-jinja

use thiserror::Error;

/// Jinja templating errors
#[derive(Error, Debug)]
pub enum JinjaError {
    /// Template render error (J001)
    #[error("[J001] Jinja render error: {0}")]
    RenderError(String),

    /// Template render error with source (J001)
    #[error("[J001] Jinja render error: {0}")]
    RenderErrorSource(#[source] minijinja::Error),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for JinjaError
pub type JinjaResult<T> = Result<T, JinjaError>;

impl From<minijinja::Error> for JinjaError {
    fn from(err: minijinja::Error) -> Self {
        JinjaError::RenderErrorSource(err)
    }
}
