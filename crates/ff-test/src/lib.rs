//! ff-test - Schema test generation for Featherflow
//!
//! This crate provides test SQL generation and execution
//! for schema tests (unique, not_null).

pub mod generator;
pub mod runner;

pub use generator::{
    generate_accepted_values_test, generate_max_value_test, generate_min_value_test,
    generate_non_negative_test, generate_not_null_test, generate_positive_test,
    generate_regex_test, generate_relationship_test, generate_test_sql, generate_unique_test,
    GeneratedTest,
};
pub use runner::{TestResult, TestRunner, TestSummary};
