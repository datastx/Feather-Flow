//! ff-core - Core library for Featherflow
//!
//! This crate provides shared types, configuration parsing, project discovery,
//! DAG building, manifest types, and breaking change detection used across all
//! Featherflow components.

pub mod breaking_changes;
pub mod config;
pub mod dag;
pub mod error;
pub mod manifest;
pub mod model;
pub mod project;
pub mod seed;
pub mod selector;
pub mod snapshot;
pub mod source;
pub mod state;

pub use breaking_changes::{
    detect_breaking_changes, detect_breaking_changes_simple, BreakingChange, BreakingChangeReport,
    BreakingChangeType,
};
pub use config::Config;
pub use error::CoreError;
pub use manifest::Manifest;
pub use model::Model;
pub use project::Project;
pub use seed::{Seed, SeedConfig};
pub use selector::Selector;
pub use snapshot::{discover_snapshots, Snapshot, SnapshotConfig, SnapshotStrategy};
pub use source::{SourceFile, SourceTable};
pub use state::{ModelState, ModelStateConfig, StateFile};
