//! ff-core - Core library for Featherflow
//!
//! This crate provides shared types, configuration parsing, project discovery,
//! DAG building, and manifest types used across all Featherflow components.

pub mod config;
pub mod dag;
pub mod error;
pub mod manifest;
pub mod model;
pub mod project;

pub use config::Config;
pub use error::CoreError;
pub use manifest::Manifest;
pub use model::Model;
pub use project::Project;
