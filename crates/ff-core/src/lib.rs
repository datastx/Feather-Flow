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
pub mod manifest;
pub mod metric;
pub mod model;
pub mod project;
pub mod query_comment;
pub mod run_state;
pub mod seed;
pub mod selector;
pub mod snapshot;
pub mod source;
pub mod sql_utils;
pub mod state;

pub use breaking_changes::{
    detect_breaking_changes, detect_breaking_changes_simple, BreakingChange, BreakingChangeReport,
    BreakingChangeType,
};
pub use config::{Config, DbType};
pub use contract::{validate_contract, ContractValidationResult, ContractViolation, ViolationType};
pub use error::CoreError;
pub use exposure::{discover_exposures, Exposure, ExposureMaturity, ExposureOwner, ExposureType};
pub use manifest::Manifest;
pub use metric::{discover_metrics, Metric, MetricCalculation};
pub use model::{
    ColumnConstraint, DataClassification, FreshnessConfig, FreshnessPeriod, FreshnessThreshold,
    Model, ModelSchema, SchemaContract,
};
pub use project::Project;
pub use query_comment::{QueryCommentContext, QueryCommentMetadata};
pub use run_state::{CompletedModel, FailedModel, RunState, RunStateSummary, RunStatus};
pub use seed::{Seed, SeedConfig};
pub use selector::Selector;
pub use snapshot::{discover_snapshots, Snapshot, SnapshotConfig, SnapshotStrategy};
pub use source::{SourceFile, SourceTable};
pub use state::{ModelState, ModelStateConfig, StateFile};
