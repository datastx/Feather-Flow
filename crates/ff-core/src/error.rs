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

    /// EXP001: Duplicate exposure name
    #[error("[EXP001] Duplicate exposure name '{name}' in {path1} and {path2}")]
    ExposureDuplicateName {
        name: String,
        path1: String,
        path2: String,
    },

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

    /// E014: IO error (no path context — prefer [`IoWithPath`](Self::IoWithPath) for file operations)
    #[error("[E014] IO error: {0}")]
    Io(#[from] std::io::Error),

    /// E015: Schema/YAML parse error
    #[error("[E015] Schema parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    /// E016: IO error with file path context (preferred over E014 for file operations)
    #[error("[E016] Failed to read '{path}': {source}")]
    IoWithPath {
        path: String,
        source: std::io::Error,
    },

    /// E017: JSON serialization/deserialization error
    #[error("[E017] JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// T001: Singular test validation error
    #[error("[T001] Test '{name}': {message}")]
    TestValidationError { name: String, message: String },

    // Function error types (FN001-FN012)
    /// FN001: Function YAML without matching SQL file
    #[error("[FN001] Function '{name}': missing .sql file ({yaml_path})")]
    FunctionMissingSqlFile { name: String, yaml_path: String },

    /// FN002: Function SQL file is empty
    #[error("[FN002] Function '{name}': .sql file is empty ({sql_path})")]
    FunctionEmptySqlFile { name: String, sql_path: String },

    /// FN003: Duplicate function name across directories
    #[error("[FN003] Function '{name}': duplicate definition in {path1} and {path2}")]
    FunctionDuplicateName {
        name: String,
        path1: String,
        path2: String,
    },

    // FN004: Reserved for FunctionShadowsBuiltin
    /// FN005: Non-default argument after default argument
    #[error("[FN005] Function '{name}': non-default argument '{arg}' follows a default argument")]
    FunctionArgOrderError { name: String, arg: String },

    /// FN006: Table function with empty return columns
    #[error("[FN006] Function '{name}': table function must define at least one return column")]
    FunctionTableMissingColumns { name: String },

    /// FN007: Invalid function name (not a valid SQL identifier)
    #[error("[FN007] Function '{name}': invalid name in {path}, must be a valid SQL identifier")]
    FunctionInvalidName { name: String, path: String },

    /// FN008: Function argument validation error
    #[error("[FN008] Function '{name}': {details}")]
    FunctionArgError { name: String, details: String },

    /// FN009: Function YAML parse error
    #[error("[FN009] Function parse error in {path}: {details}")]
    FunctionParseError { path: String, details: String },

    // FN010: Reserved for FunctionUnknownTable
    // FN011: Reserved for FunctionUnknownFunction
    /// FN012: Function deployment error
    #[error("[FN012] Function '{name}': deployment failed: {details}")]
    FunctionDeployError { name: String, details: String },

    /// E018: Unsupported schema version
    #[error("[E018] Unsupported schema version {version}, only version 1 is supported")]
    UnsupportedSchemaVersion { version: u32 },

    /// E019: Empty name where a non-empty name is required
    #[error("[E019] Empty name: {context}")]
    EmptyName { context: String },
}

/// Result type alias for CoreError
pub type CoreResult<T> = Result<T, CoreError>;
