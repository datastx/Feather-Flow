//! Error types for ff-core

use thiserror::Error;

/// Core error type for Featherflow
#[derive(Error, Debug)]
pub enum CoreError {
    /// Configuration file not found
    #[error("Configuration file not found: {path}")]
    ConfigNotFound { path: String },

    /// Failed to parse configuration file
    #[error("Failed to parse configuration: {message}")]
    ConfigParseError { message: String },

    /// Invalid configuration value
    #[error("Invalid configuration: {message}")]
    ConfigInvalid { message: String },

    /// Project directory not found
    #[error("Project directory not found: {path}")]
    ProjectNotFound { path: String },

    /// Model file not found
    #[error("Model file not found: {name}")]
    ModelNotFound { name: String },

    /// Model parse error
    #[error("Failed to parse model {name}: {message}")]
    ModelParseError { name: String, message: String },

    /// Circular dependency detected
    #[error("Circular dependency detected: {cycle}")]
    CircularDependency { cycle: String },

    /// Duplicate model name
    #[error("Duplicate model name: {name}")]
    DuplicateModel { name: String },

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// YAML parse error
    #[error("YAML parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    /// JSON error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type alias for CoreError
pub type CoreResult<T> = Result<T, CoreError>;
