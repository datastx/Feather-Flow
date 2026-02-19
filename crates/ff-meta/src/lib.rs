//! Meta database for Featherflow.
//!
//! Provides a DuckDB-backed store for project metadata, analysis results,
//! execution state, and a SQL rules engine. Replaces `manifest.json` and
//! `state.json` with a queryable database at `target/meta.duckdb`.

pub mod connection;
pub mod ddl;
pub mod error;
pub mod manifest;
pub mod migration;
pub mod populate;
pub mod query;
pub(crate) mod row_helpers;
pub mod rules;

pub use connection::MetaDb;
pub use error::{MetaError, MetaResult};
pub use manifest::Manifest;
