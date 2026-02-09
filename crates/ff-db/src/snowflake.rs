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

impl SnowflakeBackend {
    /// Create a new Snowflake backend (not yet implemented)
    pub fn new(_connection_string: &str) -> DbResult<Self> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "connection".to_string(),
        })
    }
}

#[async_trait]
impl DatabaseCore for SnowflakeBackend {
    async fn execute(&self, _sql: &str) -> DbResult<usize> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "execute".to_string(),
        })
    }

    async fn execute_batch(&self, _sql: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "execute_batch".to_string(),
        })
    }

    async fn relation_exists(&self, _name: &str) -> DbResult<bool> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "relation_exists".to_string(),
        })
    }

    async fn query_count(&self, _sql: &str) -> DbResult<usize> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "query_count".to_string(),
        })
    }

    async fn query_sample_rows(&self, _sql: &str, _limit: usize) -> DbResult<Vec<String>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "query_sample_rows".to_string(),
        })
    }

    async fn query_one(&self, _sql: &str) -> DbResult<Option<String>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "query_one".to_string(),
        })
    }

    fn db_type(&self) -> &'static str {
        "snowflake"
    }
}

#[async_trait]
impl DatabaseSchema for SnowflakeBackend {
    async fn create_table_as(&self, _name: &str, _select: &str, _replace: bool) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "create_table_as".to_string(),
        })
    }

    async fn create_view_as(&self, _name: &str, _select: &str, _replace: bool) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "create_view_as".to_string(),
        })
    }

    async fn drop_if_exists(&self, _name: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "drop_if_exists".to_string(),
        })
    }

    async fn create_schema_if_not_exists(&self, _schema: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "create_schema_if_not_exists".to_string(),
        })
    }

    async fn get_table_schema(&self, _table: &str) -> DbResult<Vec<(String, String)>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "get_table_schema".to_string(),
        })
    }

    async fn describe_query(&self, _sql: &str) -> DbResult<Vec<(String, String)>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "describe_query".to_string(),
        })
    }

    async fn add_columns(&self, _table: &str, _columns: &[(String, String)]) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "add_columns".to_string(),
        })
    }
}

#[async_trait]
impl DatabaseCsv for SnowflakeBackend {
    async fn load_csv(&self, _table: &str, _path: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "load_csv".to_string(),
        })
    }

    async fn load_csv_with_options(
        &self,
        _table: &str,
        _path: &str,
        _options: CsvLoadOptions,
    ) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "load_csv_with_options".to_string(),
        })
    }

    async fn infer_csv_schema(&self, _path: &str) -> DbResult<Vec<(String, String)>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "infer_csv_schema".to_string(),
        })
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
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "merge_into".to_string(),
        })
    }

    async fn delete_insert(
        &self,
        _target_table: &str,
        _source_sql: &str,
        _unique_keys: &[String],
    ) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "delete_insert".to_string(),
        })
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
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "execute_snapshot".to_string(),
        })
    }

    async fn snapshot_insert_new(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
    ) -> DbResult<usize> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "snapshot_insert_new".to_string(),
        })
    }

    async fn snapshot_update_changed(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
        _updated_at_column: Option<&str>,
        _check_cols: Option<&[String]>,
    ) -> DbResult<usize> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "snapshot_update_changed".to_string(),
        })
    }

    async fn snapshot_invalidate_deleted(
        &self,
        _snapshot_table: &str,
        _source_table: &str,
        _unique_keys: &[String],
    ) -> DbResult<usize> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "snapshot_invalidate_deleted".to_string(),
        })
    }
}

#[async_trait]
impl DatabaseFunction for SnowflakeBackend {
    async fn deploy_function(&self, _create_sql: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "deploy_function".to_string(),
        })
    }

    async fn drop_function(&self, _drop_sql: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "drop_function".to_string(),
        })
    }

    async fn function_exists(&self, _name: &str) -> DbResult<bool> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "function_exists".to_string(),
        })
    }

    async fn list_user_functions(&self) -> DbResult<Vec<String>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "list_user_functions".to_string(),
        })
    }
}
