//! DuckDB database backend implementation

use crate::error::{DbError, DbResult};
use crate::traits::Database;
use async_trait::async_trait;
use duckdb::Connection;
use std::path::Path;
use std::sync::Mutex;

/// DuckDB database backend
pub struct DuckDbBackend {
    conn: Mutex<Connection>,
}

impl DuckDbBackend {
    /// Create a new in-memory DuckDB connection
    pub fn in_memory() -> DbResult<Self> {
        let conn =
            Connection::open_in_memory().map_err(|e| DbError::ConnectionError(e.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create a new DuckDB connection from a file path
    pub fn from_path(path: &Path) -> DbResult<Self> {
        let conn = Connection::open(path).map_err(|e| DbError::ConnectionError(e.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create from path string (handles :memory: special case)
    pub fn new(path: &str) -> DbResult<Self> {
        if path == ":memory:" {
            Self::in_memory()
        } else {
            Self::from_path(Path::new(path))
        }
    }

    /// Execute SQL synchronously
    fn execute_sync(&self, sql: &str) -> DbResult<usize> {
        let conn = self.conn.lock().unwrap();
        conn.execute(sql, [])
            .map_err(|e| DbError::ExecutionError(format!("{}: {}", e, sql)))
    }

    /// Execute batch SQL synchronously
    fn execute_batch_sync(&self, sql: &str) -> DbResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(sql)
            .map_err(|e| DbError::ExecutionError(e.to_string()))
    }

    /// Query count synchronously
    fn query_count_sync(&self, sql: &str) -> DbResult<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM ({})", sql), [], |row| {
                row.get(0)
            })
            .map_err(|e| DbError::ExecutionError(e.to_string()))?;
        Ok(count as usize)
    }

    /// Check if relation exists synchronously
    fn relation_exists_sync(&self, name: &str) -> DbResult<bool> {
        let conn = self.conn.lock().unwrap();

        // Handle schema-qualified names
        let (schema, table) = if let Some(pos) = name.rfind('.') {
            (&name[..pos], &name[pos + 1..])
        } else {
            ("main", name)
        };

        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}'",
            schema, table
        );

        let count: i64 = conn
            .query_row(&sql, [], |row| row.get(0))
            .map_err(|e| DbError::ExecutionError(e.to_string()))?;

        Ok(count > 0)
    }
}

#[async_trait]
impl Database for DuckDbBackend {
    async fn execute(&self, sql: &str) -> DbResult<usize> {
        self.execute_sync(sql)
    }

    async fn execute_batch(&self, sql: &str) -> DbResult<()> {
        self.execute_batch_sync(sql)
    }

    async fn create_table_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()> {
        let sql = if replace {
            format!("CREATE OR REPLACE TABLE {} AS {}", name, select)
        } else {
            format!("CREATE TABLE {} AS {}", name, select)
        };
        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn create_view_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()> {
        let sql = if replace {
            format!("CREATE OR REPLACE VIEW {} AS {}", name, select)
        } else {
            format!("CREATE VIEW {} AS {}", name, select)
        };
        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn relation_exists(&self, name: &str) -> DbResult<bool> {
        self.relation_exists_sync(name)
    }

    async fn query_count(&self, sql: &str) -> DbResult<usize> {
        self.query_count_sync(sql)
    }

    async fn load_csv(&self, table: &str, path: &str) -> DbResult<()> {
        let sql = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_csv_auto('{}')",
            table, path
        );
        self.execute_sync(&sql)?;
        Ok(())
    }

    fn db_type(&self) -> &'static str {
        "duckdb"
    }

    async fn drop_if_exists(&self, name: &str) -> DbResult<()> {
        // Try dropping as view first, then as table
        let _ = self.execute_sync(&format!("DROP VIEW IF EXISTS {}", name));
        let _ = self.execute_sync(&format!("DROP TABLE IF EXISTS {}", name));
        Ok(())
    }

    async fn create_schema_if_not_exists(&self, schema: &str) -> DbResult<()> {
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", schema);
        self.execute_sync(&sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory() {
        let db = DuckDbBackend::in_memory().unwrap();
        assert_eq!(db.db_type(), "duckdb");
    }

    #[tokio::test]
    async fn test_create_table_as() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.create_table_as("test_table", "SELECT 1 AS id, 'hello' AS name", false)
            .await
            .unwrap();

        assert!(db.relation_exists("test_table").await.unwrap());
    }

    #[tokio::test]
    async fn test_create_view_as() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.create_view_as("test_view", "SELECT 1 AS id", false)
            .await
            .unwrap();

        assert!(db.relation_exists("test_view").await.unwrap());
    }

    #[tokio::test]
    async fn test_query_count() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch("CREATE TABLE nums AS SELECT * FROM range(10) t(n)")
            .await
            .unwrap();

        let count = db.query_count("SELECT * FROM nums").await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_execute_batch() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT); INSERT INTO t1 VALUES (1);",
        )
        .await
        .unwrap();

        assert!(db.relation_exists("t1").await.unwrap());
        assert!(db.relation_exists("t2").await.unwrap());
    }

    #[tokio::test]
    async fn test_relation_not_exists() {
        let db = DuckDbBackend::in_memory().unwrap();
        assert!(!db.relation_exists("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_if_exists() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.create_table_as("to_drop", "SELECT 1 AS id", false)
            .await
            .unwrap();

        assert!(db.relation_exists("to_drop").await.unwrap());

        db.drop_if_exists("to_drop").await.unwrap();

        assert!(!db.relation_exists("to_drop").await.unwrap());
    }

    #[tokio::test]
    async fn test_create_schema_if_not_exists() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create a schema
        db.create_schema_if_not_exists("staging").await.unwrap();

        // Create a table in the schema
        db.create_table_as("staging.test_table", "SELECT 1 AS id", false)
            .await
            .unwrap();

        // Verify the table exists in the schema
        assert!(db.relation_exists("staging.test_table").await.unwrap());

        // Creating the same schema again should not fail (IF NOT EXISTS)
        db.create_schema_if_not_exists("staging").await.unwrap();
    }
}
