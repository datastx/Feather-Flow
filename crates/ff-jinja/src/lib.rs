//! ff-jinja - Jinja templating layer for Featherflow
//!
//! This crate provides a simplified Jinja templating environment
//! with only `config()` and `var()` functions.

pub mod environment;
pub mod error;
pub mod functions;

pub use environment::JinjaEnvironment;
pub use error::JinjaError;
