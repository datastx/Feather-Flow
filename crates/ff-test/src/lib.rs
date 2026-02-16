//! ff-test - Schema test generation for Featherflow
//!
//! This crate provides test SQL generation and execution
//! for schema tests (unique, not_null).

pub mod generator;
pub mod runner;

pub use generator::{generate_test_sql, GeneratedTest, TestGenError, TestGenResult};
pub use runner::{TestResult, TestRunner, TestSummary};
