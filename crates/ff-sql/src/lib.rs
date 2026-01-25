//! ff-sql - SQL parsing layer for Featherflow
//!
//! This crate provides SQL parsing using sqlparser-rs with dialect support,
//! and table dependency extraction via AST visitor.

pub mod dialect;
pub mod error;
pub mod extractor;
pub mod parser;
pub mod validator;

pub use dialect::{DuckDbDialect, SnowflakeDialect, SqlDialect};
pub use error::SqlError;
pub use extractor::extract_dependencies;
pub use parser::SqlParser;
