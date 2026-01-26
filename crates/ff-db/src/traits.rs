//! Database trait definition

use crate::error::DbResult;
use async_trait::async_trait;
use std::collections::HashMap;

/// Options for loading CSV files
#[derive(Debug, Clone, Default)]
pub struct CsvLoadOptions {
    /// CSV delimiter character (default: comma)
    pub delimiter: Option<char>,
    /// Override inferred types for specific columns
    pub column_types: HashMap<String, String>,
    /// Force column quoting
    pub quote_columns: bool,
    /// Target schema for the table
    pub schema: Option<String>,
}

/// Database abstraction trait for Featherflow
///
/// Implementations must be Send + Sync for async operation.
#[async_trait]
pub trait Database: Send + Sync {
    /// Execute SQL that modifies data, returns affected rows
    async fn execute(&self, sql: &str) -> DbResult<usize>;

    /// Execute multiple SQL statements
    async fn execute_batch(&self, sql: &str) -> DbResult<()>;

    /// Create table from SELECT statement
    async fn create_table_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()>;

    /// Create view from SELECT statement
    async fn create_view_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()>;

    /// Check if a table or view exists
    async fn relation_exists(&self, name: &str) -> DbResult<bool>;

    /// Execute query returning row count (for tests)
    async fn query_count(&self, sql: &str) -> DbResult<usize>;

    /// Load CSV file into table
    async fn load_csv(&self, table: &str, path: &str) -> DbResult<()>;

    /// Load CSV file into table with options
    ///
    /// Options:
    /// - `delimiter`: CSV delimiter character
    /// - `column_types`: Override inferred types for specific columns
    /// - `quote_columns`: Force column quoting
    async fn load_csv_with_options(
        &self,
        table: &str,
        path: &str,
        options: CsvLoadOptions,
    ) -> DbResult<()>;

    /// Get inferred schema for a CSV file without loading it
    ///
    /// Returns a list of (column_name, inferred_type) tuples
    async fn infer_csv_schema(&self, path: &str) -> DbResult<Vec<(String, String)>>;

    /// Database type identifier for logging
    fn db_type(&self) -> &'static str;

    /// Drop a table or view if it exists
    async fn drop_if_exists(&self, name: &str) -> DbResult<()>;

    /// Create a schema if it does not exist
    async fn create_schema_if_not_exists(&self, schema: &str) -> DbResult<()>;

    /// Query and return sample rows as formatted strings
    /// Returns up to `limit` rows, each as a comma-separated string
    async fn query_sample_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<String>>;

    /// Query and return a single string value from the first row, first column
    async fn query_one(&self, sql: &str) -> DbResult<Option<String>>;

    /// Execute a MERGE/UPSERT operation for incremental models
    ///
    /// This merges source data into an existing target table based on unique keys.
    async fn merge_into(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()>;

    /// Execute a delete+insert operation for incremental models
    ///
    /// This deletes matching rows from target and inserts from source.
    async fn delete_insert(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()>;

    /// Get the schema (column names and types) for a table
    ///
    /// Returns a list of (column_name, column_type) tuples
    async fn get_table_schema(&self, table: &str) -> DbResult<Vec<(String, String)>>;

    /// Get the schema for a SELECT query without executing it
    ///
    /// Returns a list of (column_name, column_type) tuples
    async fn describe_query(&self, sql: &str) -> DbResult<Vec<(String, String)>>;

    /// Add columns to an existing table
    ///
    /// Used for on_schema_change: append_new_columns
    async fn add_columns(&self, table: &str, columns: &[(String, String)]) -> DbResult<()>;

    // ===== Snapshot Operations =====

    /// Execute a snapshot operation (SCD Type 2)
    ///
    /// This handles the full snapshot workflow:
    /// 1. Create snapshot table if it doesn't exist
    /// 2. Invalidate changed records (set valid_to)
    /// 3. Insert new records
    /// 4. Handle hard deletes if configured
    async fn execute_snapshot(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
        invalidate_hard_deletes: bool,
    ) -> DbResult<SnapshotResult>;

    /// Insert new records into snapshot
    async fn snapshot_insert_new(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize>;

    /// Update changed records in snapshot (set valid_to, insert new version)
    async fn snapshot_update_changed(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
    ) -> DbResult<usize>;

    /// Handle hard deletes by setting valid_to on records missing from source
    async fn snapshot_invalidate_deleted(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize>;
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
