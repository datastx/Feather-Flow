//! ff-jinja - Jinja templating layer for Featherflow
//!
//! This crate provides a simplified Jinja templating environment
//! with `config()`, `var()`, `is_incremental()`, and `this` functions,
//! as well as built-in macros for common SQL operations.

pub mod builtins;
pub mod environment;
pub mod error;
pub mod functions;

pub use environment::JinjaEnvironment;
pub use error::JinjaError;
pub use functions::IncrementalState;
