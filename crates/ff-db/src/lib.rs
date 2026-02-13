//! ff-db - Database abstraction layer for Featherflow
//!
//! This crate provides the `Database` trait and implementations
//! for DuckDB (and a Snowflake stub for future implementation).

pub mod duckdb;
pub mod error;
pub(crate) mod snowflake;
pub mod traits;

pub use duckdb::DuckDbBackend;
pub use error::DbError;
pub use traits::{
    CsvLoadOptions, Database, DatabaseCore, DatabaseCsv, DatabaseFunction, DatabaseIncremental,
    DatabaseSchema,
};
