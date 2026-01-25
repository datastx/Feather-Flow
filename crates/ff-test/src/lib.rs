//! ff-test - Schema test generation for Featherflow
//!
//! This crate provides test SQL generation and execution
//! for schema tests (unique, not_null).

pub mod generator;
pub mod runner;

pub use generator::{generate_not_null_test, generate_unique_test};
pub use runner::TestRunner;
