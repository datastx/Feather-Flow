//! ff-sql - SQL parsing layer for Featherflow
//!
//! This crate provides SQL parsing using sqlparser-rs with dialect support,
//! table dependency extraction, and column-level lineage via AST visitor.

pub mod dialect;
pub mod error;
pub mod extractor;
pub mod lineage;
pub mod parser;
pub mod suggestions;
pub mod validator;

pub use dialect::{DuckDbDialect, SnowflakeDialect, SqlDialect};
pub use error::SqlError;
pub use extractor::extract_dependencies;
pub use lineage::{extract_column_lineage, ColumnLineage, ColumnRef, ModelLineage};
pub use parser::SqlParser;
pub use suggestions::{suggest_tests, ColumnSuggestions, ModelSuggestions, TestSuggestion};
