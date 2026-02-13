//! ff-core - Core library for Featherflow
//!
//! This crate provides shared types, configuration parsing, project discovery,
//! DAG building, manifest types, and breaking change detection used across all
//! Featherflow components.

pub mod breaking_changes;
pub mod classification;
pub mod config;
pub mod contract;
pub mod dag;
pub mod error;
pub mod exposure;
pub mod function;
pub mod function_name;
pub mod manifest;
pub mod model;
pub mod model_name;
mod newtype_string;
pub mod project;
pub mod query_comment;
pub mod run_state;
pub mod seed;
pub mod selector;
pub(crate) mod serde_helpers;
pub mod source;
pub mod sql_utils;
pub mod state;
pub mod table_name;

pub use breaking_changes::{
    detect_breaking_changes, detect_breaking_changes_simple, BreakingChange, BreakingChangeReport,
    BreakingChangeType,
};
pub use config::{Config, DbType};
pub use contract::{validate_contract, ContractValidationResult, ContractViolation, ViolationType};
pub use error::CoreError;
pub use exposure::{discover_exposures, Exposure, ExposureMaturity, ExposureOwner, ExposureType};
pub use function::{
    build_function_lookup, discover_functions, FunctionArg, FunctionConfig, FunctionDef,
    FunctionReturn, FunctionReturnColumn, FunctionSignature, FunctionType,
};
pub use function_name::FunctionName;
pub use manifest::Manifest;
pub use model::{ColumnConstraint, DataClassification, Model, ModelSchema, SchemaContract};
pub use model_name::ModelName;
pub use project::{Project, ProjectParts};
pub use query_comment::{QueryCommentContext, QueryCommentMetadata};
pub use run_state::{CompletedModel, FailedModel, RunState, RunStateSummary, RunStatus};
pub use seed::{Seed, SeedConfig};
pub use selector::{apply_selectors, Selector, TraversalDepth};
pub use source::{SourceFile, SourceTable};
pub use state::{ModelState, ModelStateConfig, StateFile};
pub use table_name::TableName;
