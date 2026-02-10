//! Snowflake database backend stub

use crate::error::{DbError, DbResult};
use crate::traits::{
    CsvLoadOptions, DatabaseCore, DatabaseCsv, DatabaseFunction, DatabaseIncremental,
    DatabaseSchema, DatabaseSnapshot, SnapshotResult,
};
use async_trait::async_trait;

/// Snowflake database backend (stub implementation)
///
/// This is a placeholder for future Snowflake support.
pub struct SnowflakeBackend {
    // Connection details would go here
}

/// Helper to create a `NotImplemented` error for Snowflake features
fn not_impl(feature: &str) -> DbError {
    DbError::NotImplemented {
        backend: "snowflake".to_string(),
        feature: feature.to_string(),
    }
}

impl SnowflakeBackend {
    /// Create a new Snowflake backend (not yet implemented)
    pub fn new(_connection_string: &str) -> DbResult<Self> {
        Err(not_impl("connection"))
    }
}

#[async_trait]
impl DatabaseCore for SnowflakeBackend {
    async fn execute(&self, _sql: &str) -> DbResult<usize> {
        Err(not_impl("execute"))
    }

    async fn execute_batch(&self, _sql: &str) -> DbResult<()> {
        Err(not_impl("execute_batch"))
    }

    async fn relation_exists(&self, _name: &str) -> DbResult<bool> {
        Err(not_impl("relation_exists"))
    }

    async fn query_count(&self, _sql: &str) -> DbResult<usize> {
        Err(not_impl("query_count"))
    }

    async fn query_sample_rows(&self, _sql: &str, _limit: usize) -> DbResult<Vec<String>> {
        Err(not_impl("query_sample_rows"))
    }

    async fn query_rows(&self, _sql: &str, _limit: usize) -> DbResult<Vec<Vec<String>>> {
        Err(not_impl("query_rows"))
    }

    async fn query_one(&self, _sql: &str) -> DbResult<Option<String>> {
        Err(not_impl("query_one"))
    }

    fn db_type(&self) -> &'static str {
        "snowflake"
    }
}

#[async_trait]
impl DatabaseSchema for SnowflakeBackend {
    async fn create_table_as(&self, _name: &str, _select: &str, _replace: bool) -> DbResult<()> {
        Err(not_impl("create_table_as"))
    }

    async fn create_view_as(&self, _name: &str, _select: &str, _replace: bool) -> DbResult<()> {
        Err(not_impl("create_view_as"))
    }

    async fn drop_if_exists(&self, _name: &str) -> DbResult<()> {
        Err(not_impl("drop_if_exists"))
    }

    async fn create_schema_if_not_exists(&self, _schema: &str) -> DbResult<()> {
        Err(not_impl("create_schema_if_not_exists"))
    }

    async fn get_table_schema(&self, _table: &str) -> DbResult<Vec<(String, String)>> {
        Err(not_impl("get_table_schema"))
    }

    async fn describe_query(&self, _sql: &str) -> DbResult<Vec<(String, String)>> {
        Err(not_impl("describe_query"))
    }

    async fn add_columns(&self, _table: &str, _columns: &[(String, String)]) -> DbResult<()> {
        Err(not_impl("add_columns"))
    }
}

#[async_trait]
impl DatabaseCsv for SnowflakeBackend {
    async fn load_csv(&self, _table: &str, _path: &str) -> DbResult<()> {
        Err(not_impl("load_csv"))
    }

    async fn load_csv_with_options(
        &self,
        _table: &str,
        _path: &str,
        _options: CsvLoadOptions,
    ) -> DbResult<()> {
        Err(not_impl("load_csv_with_options"))
    }

    async fn infer_csv_schema(&self, _path: &str) -> DbResult<Vec<(String, String)>> {
        Err(not_impl("infer_csv_schema"))
    }
}

#[async_trait]
impl DatabaseIncremental for SnowflakeBackend {
    async fn merge_into(
        &self,
        _target_table: &str,
        _source_sql: &str,
        _unique_keys: &[String],
    ) -> DbResult<()> {
        Err(not_impl("merge_into"))
    }

    async fn delete_insert(
        &self,
        _target_table: &str,
        _source_sql: &str,
        _unique_keys: &[String],
    ) -> DbResult<()> {
        Err(not_impl("delete_insert"))
    }
}

#[async_trait]
impl DatabaseSnapshot for SnowflakeBackend {
    async fn execute_snapshot(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
        _updated_at_column: Option<&str>,
        _check_cols: Option<&[String]>,
        _invalidate_hard_deletes: bool,
    ) -> DbResult<SnapshotResult> {
        Err(not_impl("execute_snapshot"))
    }

    async fn snapshot_insert_new(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
    ) -> DbResult<usize> {
        Err(not_impl("snapshot_insert_new"))
    }

    async fn snapshot_update_changed(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
        _updated_at_column: Option<&str>,
        _check_cols: Option<&[String]>,
    ) -> DbResult<usize> {
        Err(not_impl("snapshot_update_changed"))
    }

    async fn snapshot_invalidate_deleted(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
    ) -> DbResult<usize> {
        Err(not_impl("snapshot_invalidate_deleted"))
    }
}

#[async_trait]
impl DatabaseFunction for SnowflakeBackend {
    async fn deploy_function(&self, _create_sql: &str) -> DbResult<()> {
        Err(not_impl("deploy_function"))
    }

    async fn drop_function(&self, _drop_sql: &str) -> DbResult<()> {
        Err(not_impl("drop_function"))
    }

    async fn function_exists(&self, _name: &str) -> DbResult<bool> {
        Err(not_impl("function_exists"))
    }

    async fn list_user_functions(&self) -> DbResult<Vec<String>> {
        Err(not_impl("list_user_functions"))
    }
}
