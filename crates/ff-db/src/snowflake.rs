//! Snowflake database backend stub

use crate::error::{DbError, DbResult};
use crate::traits::Database;
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
impl Database for SnowflakeBackend {
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

    async fn load_csv(&self, _table: &str, _path: &str) -> DbResult<()> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "load_csv".to_string(),
        })
    }

    fn db_type(&self) -> &'static str {
        "snowflake"
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

    async fn query_sample_rows(&self, _sql: &str, _limit: usize) -> DbResult<Vec<String>> {
        Err(DbError::NotImplemented {
            backend: "snowflake".to_string(),
            feature: "query_sample_rows".to_string(),
        })
    }
}
