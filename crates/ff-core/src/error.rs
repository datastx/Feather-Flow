//! Error types for ff-core

use thiserror::Error;

/// Core error type for Featherflow
#[derive(Error, Debug)]
pub enum CoreError {
    /// E001: Configuration file not found
    #[error("[E001] Config file not found: {path}")]
    ConfigNotFound { path: String },

    /// E002: Failed to parse configuration file
    #[error("[E002] Failed to parse config: {message}")]
    ConfigParseError { message: String },

    /// E003: Invalid configuration value
    #[error("[E003] Invalid config: {message}")]
    ConfigInvalid { message: String },

    /// E004: Project directory not found
    #[error("[E004] Project directory not found: {path}")]
    ProjectNotFound { path: String },

    /// E005: Model file not found
    #[error("[E005] Model not found: {name}")]
    ModelNotFound { name: String },

    /// E006: Model parse error
    #[error("[E006] SQL parse error in {name}: {message}")]
    ModelParseError { name: String, message: String },

    /// E007: Circular dependency detected
    #[error("[E007] Circular dependency detected: {cycle}")]
    CircularDependency { cycle: String },

    /// E008: Duplicate model name
    #[error("[E008] Duplicate model name: {name}")]
    DuplicateModel { name: String },

    /// E009: Invalid selector
    #[error("[E009] Invalid selector '{selector}': {reason}")]
    InvalidSelector { selector: String, reason: String },

    // Source error types (SRC001-SRC007)
    /// SRC001: Source file missing required 'kind' field
    #[error("[SRC001] Source file missing required 'kind' field: {path}. Add `kind: sources`")]
    SourceMissingKind { path: String },

    /// SRC002: Invalid 'kind' in source file
    #[error("[SRC002] Invalid 'kind' in {path}: expected 'sources', found '{found}'")]
    SourceInvalidKind { path: String, found: String },

    /// SRC003: Source missing required 'schema' field
    #[error("[SRC003] Source '{name}' missing required 'schema' field in {path}")]
    SourceMissingSchema { name: String, path: String },

    /// SRC004: Source has no tables defined
    #[error("[SRC004] Source '{name}' has no tables defined in {path}")]
    SourceEmptyTables { name: String, path: String },

    /// SRC005: Failed to parse source file
    #[error("[SRC005] Failed to parse source file {path}: {details}")]
    SourceParseError { path: String, details: String },

    /// SRC006: Duplicate source name
    #[error("[SRC006] Duplicate source name '{name}' in {path1} and {path2}")]
    SourceDuplicateName {
        name: String,
        path1: String,
        path2: String,
    },

    /// SRC007: Duplicate table in source
    #[error("[SRC007] Duplicate table '{table}' in source '{source_name}'")]
    SourceDuplicateTable { table: String, source_name: String },

    /// E009: IO error
    #[error("[E009] IO error: {0}")]
    Io(#[from] std::io::Error),

    /// E010: Schema/YAML parse error
    #[error("[E010] Schema parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    /// JSON error (not in spec but needed for functionality)
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type alias for CoreError
pub type CoreResult<T> = Result<T, CoreError>;
