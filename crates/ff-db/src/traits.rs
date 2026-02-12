//! Database trait definition
//!
//! The `Database` trait is split into focused sub-traits for modularity:
//! - [`DatabaseCore`]: Execute SQL, check existence, query
//! - [`DatabaseSchema`]: DDL operations (create table/view, drop, alter)
//! - [`DatabaseCsv`]: CSV loading and schema inference
//! - [`DatabaseIncremental`]: Merge/delete-insert for incremental models
//! - [`DatabaseSnapshot`]: SCD Type 2 snapshot operations
//!
//! The [`Database`] super-trait combines all of them. Consumers that need
//! all capabilities use `Arc<dyn Database>`.

use crate::error::DbResult;
use async_trait::async_trait;
use std::collections::HashMap;

/// Options for loading CSV files
#[derive(Debug, Clone, Default)]
pub struct CsvLoadOptions {
    delimiter: Option<char>,
    column_types: HashMap<String, String>,
    quote_columns: bool,
    schema: Option<String>,
}

impl CsvLoadOptions {
    /// Create a new CsvLoadOptions with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the CSV delimiter character
    #[must_use]
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set column type overrides
    #[must_use]
    pub fn with_column_types(mut self, types: HashMap<String, String>) -> Self {
        self.column_types = types;
        self
    }

    /// Add a single column type override
    #[must_use]
    pub fn with_column_type(mut self, column: impl Into<String>, dtype: impl Into<String>) -> Self {
        self.column_types.insert(column.into(), dtype.into());
        self
    }

    /// Enable column quoting
    #[must_use]
    pub fn with_quote_columns(mut self, quote: bool) -> Self {
        self.quote_columns = quote;
        self
    }

    /// Set target schema for the table
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Get the delimiter
    pub fn delimiter(&self) -> Option<char> {
        self.delimiter
    }

    /// Get the column types
    pub fn column_types(&self) -> &HashMap<String, String> {
        &self.column_types
    }

    /// Check if column quoting is enabled
    pub fn quote_columns(&self) -> bool {
        self.quote_columns
    }

    /// Get the schema
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

/// Core database operations: execute SQL, query, check existence.
///
/// # Trusted-input contract
///
/// [`execute`](Self::execute) and [`execute_batch`](Self::execute_batch) pass
/// `sql` directly to the database engine without sanitisation. Callers **must**
/// ensure the SQL is constructed from trusted sources (compiled model SQL,
/// internal framework queries, or parameterised values that have already been
/// validated/escaped). Never pass unsanitised user input to these methods.
#[async_trait]
pub trait DatabaseCore: Send + Sync {
    /// Execute SQL that modifies data, returns affected rows.
    ///
    /// The caller is responsible for ensuring `sql` does not contain untrusted input.
    /// See the trait-level safety contract for details.
    async fn execute(&self, sql: &str) -> DbResult<usize>;

    /// Execute multiple SQL statements in a single batch.
    ///
    /// The caller is responsible for ensuring `sql` does not contain untrusted input.
    /// See the trait-level safety contract for details.
    async fn execute_batch(&self, sql: &str) -> DbResult<()>;

    /// Check if a table or view exists
    async fn relation_exists(&self, name: &str) -> DbResult<bool>;

    /// Execute query returning row count (for tests)
    async fn query_count(&self, sql: &str) -> DbResult<usize>;

    /// Query and return sample rows as formatted strings
    async fn query_sample_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<String>>;

    /// Query and return rows as structured column vectors
    ///
    /// Each inner `Vec<String>` represents one row with one entry per column.
    /// Use this instead of [`Self::query_sample_rows`] when you need to distinguish
    /// individual column values (e.g. for diff comparisons).
    async fn query_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<Vec<String>>>;

    /// Query and return a single string value from the first row, first column
    async fn query_one(&self, sql: &str) -> DbResult<Option<String>>;

    /// Database type identifier for logging
    fn db_type(&self) -> &'static str;
}

/// DDL operations: create/drop tables and views, alter schema.
#[async_trait]
pub trait DatabaseSchema: Send + Sync {
    /// Create table from SELECT statement
    async fn create_table_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()>;

    /// Create view from SELECT statement
    async fn create_view_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()>;

    /// Drop a table or view if it exists
    async fn drop_if_exists(&self, name: &str) -> DbResult<()>;

    /// Create a schema if it does not exist
    async fn create_schema_if_not_exists(&self, schema: &str) -> DbResult<()>;

    /// Get the schema (column names and types) for a table
    async fn get_table_schema(&self, table: &str) -> DbResult<Vec<(String, String)>>;

    /// Get the schema for a SELECT query without executing it
    async fn describe_query(&self, sql: &str) -> DbResult<Vec<(String, String)>>;

    /// Add columns to an existing table
    async fn add_columns(&self, table: &str, columns: &[(String, String)]) -> DbResult<()>;
}

/// CSV loading and schema inference.
#[async_trait]
pub trait DatabaseCsv: Send + Sync {
    /// Load CSV file into table
    async fn load_csv(&self, table: &str, path: &str) -> DbResult<()>;

    /// Load CSV file into table with options
    async fn load_csv_with_options(
        &self,
        table: &str,
        path: &str,
        options: CsvLoadOptions,
    ) -> DbResult<()>;

    /// Get inferred schema for a CSV file without loading it
    async fn infer_csv_schema(&self, path: &str) -> DbResult<Vec<(String, String)>>;
}

/// Merge and delete-insert operations for incremental models.
#[async_trait]
pub trait DatabaseIncremental: Send + Sync {
    /// Execute a MERGE/UPSERT operation for incremental models
    async fn merge_into(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()>;

    /// Execute a delete+insert operation for incremental models
    async fn delete_insert(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()>;
}

/// SCD Type 2 snapshot operations.
#[async_trait]
pub trait DatabaseSnapshot: Send + Sync {
    /// Execute a snapshot operation (SCD Type 2)
    async fn execute_snapshot(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
        invalidate_hard_deletes: bool,
    ) -> DbResult<SnapshotResult>;

    /// Insert new records into snapshot.
    ///
    /// **Warning**: This method runs outside a transaction. For atomic snapshot
    /// operations, use [`execute_snapshot`](Self::execute_snapshot) instead.
    async fn snapshot_insert_new(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize>;

    /// Update changed records in snapshot (set valid_to, insert new version).
    ///
    /// **Warning**: This method runs outside a transaction. For atomic snapshot
    /// operations, use [`execute_snapshot`](Self::execute_snapshot) instead.
    async fn snapshot_update_changed(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
    ) -> DbResult<usize>;

    /// Handle hard deletes by setting valid_to on records missing from source.
    ///
    /// **Warning**: This method runs outside a transaction. For atomic snapshot
    /// operations, use [`execute_snapshot`](Self::execute_snapshot) instead.
    async fn snapshot_invalidate_deleted(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize>;
}

/// User-defined function management operations.
#[async_trait]
pub trait DatabaseFunction: Send + Sync {
    /// Deploy a function by executing its CREATE MACRO SQL
    async fn deploy_function(&self, create_sql: &str) -> DbResult<()>;

    /// Drop a function by executing its DROP MACRO SQL
    async fn drop_function(&self, drop_sql: &str) -> DbResult<()>;

    /// Check if a user-defined function (macro) exists
    async fn function_exists(&self, name: &str) -> DbResult<bool>;

    /// List all user-defined macro names
    async fn list_user_functions(&self) -> DbResult<Vec<String>>;
}

/// Combined database trait providing all capabilities.
///
/// This is the main trait used by CLI commands via `Arc<dyn Database>`.
/// It inherits from all sub-traits, so any type implementing `Database`
/// must implement all sub-trait methods.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support:
/// - Concurrent model execution with `--threads` flag
/// - Async operations across multiple tokio tasks
/// - Shared access via `Arc<dyn Database>`
///
/// # Implementors
///
/// - [`DuckDbBackend`](crate::DuckDbBackend) - Primary implementation using DuckDB
pub trait Database:
    DatabaseCore
    + DatabaseSchema
    + DatabaseCsv
    + DatabaseIncremental
    + DatabaseSnapshot
    + DatabaseFunction
{
}

/// Blanket implementation: any type that implements all sub-traits also implements Database.
impl<T> Database for T where
    T: DatabaseCore
        + DatabaseSchema
        + DatabaseCsv
        + DatabaseIncremental
        + DatabaseSnapshot
        + DatabaseFunction
{
}

/// Result of a snapshot execution
#[derive(Debug, Clone, Default)]
pub struct SnapshotResult {
    /// Number of new records inserted
    pub new_records: usize,
    /// Number of records that were updated (changed)
    pub updated_records: usize,
    /// Number of records that were invalidated (hard deleted)
    pub deleted_records: usize,
}
