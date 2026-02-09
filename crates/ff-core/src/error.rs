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

    /// E010: Missing schema file for model
    #[error("[E010] Model '{model}' is missing a required schema file ({expected_path}). Every model must have a corresponding YAML file.")]
    MissingSchemaFile {
        model: String,
        expected_path: String,
    },

    /// E011: Invalid model directory structure
    #[error("[E011] Invalid model directory at '{path}': {reason}")]
    InvalidModelDirectory { path: String, reason: String },

    /// E012: Model directory name doesn't match SQL file name
    #[error("[E012] Model directory mismatch: directory '{directory}' contains SQL file '{sql_file}' (must match)")]
    ModelDirectoryMismatch { directory: String, sql_file: String },

    /// E013: Model directory contains unexpected extra files
    #[error("[E013] Model directory '{directory}' contains unexpected files: {files}. Each model directory must contain exactly one .sql and one .yml/.yaml file.")]
    ExtraFilesInModelDirectory { directory: String, files: String },

    /// E014: IO error
    #[error("[E014] IO error: {0}")]
    Io(#[from] std::io::Error),

    /// E016: IO error with file path context
    #[error("[E016] Failed to read '{path}': {source}")]
    IoWithPath {
        path: String,
        source: std::io::Error,
    },

    /// E015: Schema/YAML parse error
    #[error("[E015] Schema parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type alias for CoreError
pub type CoreResult<T> = Result<T, CoreError>;
