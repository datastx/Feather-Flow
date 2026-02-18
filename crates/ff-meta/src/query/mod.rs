//! Query helpers for reading data from the meta database.
//!
//! - [`adhoc`] — Ad-hoc SQL queries, table listing, row counts
//! - [`state`] — Smart build queries (model modification detection)

pub mod adhoc;
pub mod state;

pub use adhoc::{execute_query, list_tables, table_row_count, QueryResult};
pub use state::is_model_modified;
