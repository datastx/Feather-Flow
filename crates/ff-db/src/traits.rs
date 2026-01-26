//! Database trait definition

use crate::error::DbResult;
use async_trait::async_trait;

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

    /// Database type identifier for logging
    fn db_type(&self) -> &'static str;

    /// Drop a table or view if it exists
    async fn drop_if_exists(&self, name: &str) -> DbResult<()>;

    /// Create a schema if it does not exist
    async fn create_schema_if_not_exists(&self, schema: &str) -> DbResult<()>;

    /// Query and return sample rows as formatted strings
    /// Returns up to `limit` rows, each as a comma-separated string
    async fn query_sample_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<String>>;
}
