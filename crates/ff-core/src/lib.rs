//! ff-core - Core library for Featherflow
//!
//! This crate provides shared types, configuration parsing, project discovery,
//! DAG building, and reference manifest traits used across all Featherflow
//! components.

pub mod checksum;
pub mod classification;
pub mod config;
pub mod contract;
pub mod dag;
pub mod error;
pub mod function;
pub mod function_name;
pub mod model;
pub mod model_name;
mod newtype_string;
pub mod node;
pub mod project;
pub mod query_comment;
pub mod reference_manifest;
pub mod rules;
pub mod run_state;
pub mod seed;
pub mod selector;
pub(crate) mod serde_helpers;
pub mod source;
pub mod sql_utils;
pub mod table_name;

pub use checksum::compute_checksum;
pub use config::{Config, DbType};
pub use contract::{validate_contract, ContractValidationResult, ContractViolation, ViolationType};
pub use error::CoreError;
pub use function::{
    build_function_lookup, discover_functions, FunctionArg, FunctionConfig, FunctionDef,
    FunctionReturn, FunctionReturnColumn, FunctionSignature, FunctionType,
};
pub use function_name::FunctionName;
pub use model::{
    ColumnConstraint, DataClassification, Model, ModelKind, ModelSchema, SchemaContract,
};
pub use model_name::ModelName;
pub use node::NodeKind;
pub use project::{Project, ProjectParts};
pub use query_comment::{QueryCommentContext, QueryCommentMetadata};
pub use reference_manifest::{ReferenceManifest, ReferenceModelRef};
pub use run_state::{CompletedModel, FailedModel, RunState, RunStateSummary, RunStatus};
pub use seed::Seed;
pub use selector::{apply_selectors, Selector, TraversalDepth};
pub use source::{SourceFile, SourceTable};
pub use table_name::TableName;
