//! DuckDB database backend implementation

use crate::error::{DbError, DbResult};
use crate::traits::{CsvLoadOptions, Database, SnapshotResult};
use async_trait::async_trait;
use duckdb::Connection;
use std::path::Path;
use std::sync::Mutex;

/// Extension trait for converting `duckdb::Error` into `DbResult`.
///
/// Reduces boilerplate when propagating database errors through the crate.
trait DuckDbResultExt<T> {
    fn to_db_err(self) -> DbResult<T>;
}

impl<T> DuckDbResultExt<T> for Result<T, duckdb::Error> {
    fn to_db_err(self) -> DbResult<T> {
        self.map_err(|e| DbError::ExecutionError(e.to_string()))
    }
}

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

    async fn load_csv_with_options(
        &self,
        table: &str,
        path: &str,
        options: CsvLoadOptions,
    ) -> DbResult<()> {
        let mut csv_options = Vec::new();

        if let Some(delim) = options.delimiter() {
            let delim_str = if delim == '\t' {
                "\\t".to_string()
            } else {
                delim.to_string()
            };
            csv_options.push(format!("delim = '{}'", delim_str));
        }

        if options.quote_columns() {
            csv_options.push("quote = '\"'".to_string());
        }

        let type_casts: Vec<String> = options
            .column_types()
            .iter()
            .map(|(col, typ)| format!("CAST({} AS {}) AS {}", col, typ, col))
            .collect();

        let csv_opts_str = if csv_options.is_empty() {
            String::new()
        } else {
            format!(", {}", csv_options.join(", "))
        };

        let target_table = if let Some(schema) = options.schema() {
            self.create_schema_if_not_exists(schema).await?;
            format!("{}.{}", schema, table)
        } else {
            table.to_string()
        };

        let sql = if type_casts.is_empty() {
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_csv_auto('{}'{})",
                target_table, path, csv_opts_str
            )
        } else {
            let typed_columns = type_casts.join(", ");
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT {}, * EXCLUDE ({}) FROM read_csv_auto('{}'{})",
                target_table,
                typed_columns,
                options.column_types().keys().cloned().collect::<Vec<_>>().join(", "),
                path,
                csv_opts_str
            )
        };

        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn infer_csv_schema(&self, path: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        let sql = format!("DESCRIBE SELECT * FROM read_csv_auto('{}')", path);

        let mut stmt = conn.prepare(&sql).to_db_err()?;
        let mut schema: Vec<(String, String)> = Vec::new();
        let mut rows = stmt.query([]).to_db_err()?;

        while let Some(row) = rows.next().to_db_err()? {
            let column_name: String = row.get(0).to_db_err()?;
            let column_type: String = row.get(1).to_db_err()?;
            schema.push((column_name, column_type));
        }

        Ok(schema)
    }

    fn db_type(&self) -> &'static str {
        "duckdb"
    }

    async fn drop_if_exists(&self, name: &str) -> DbResult<()> {
        let _ = self.execute_sync(&format!("DROP VIEW IF EXISTS {}", name));
        let _ = self.execute_sync(&format!("DROP TABLE IF EXISTS {}", name));
        Ok(())
    }

    async fn create_schema_if_not_exists(&self, schema: &str) -> DbResult<()> {
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", schema);
        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn query_sample_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let limited_sql = format!("SELECT * FROM ({}) AS subq LIMIT {}", sql, limit);

        let mut stmt = conn.prepare(&limited_sql).to_db_err()?;
        let mut rows: Vec<String> = Vec::new();
        let mut result_rows = stmt.query([]).to_db_err()?;
        let column_count = result_rows.as_ref().map_or(0, |r| r.column_count());

        while let Some(row) = result_rows.next().to_db_err()? {
            let mut values: Vec<String> = Vec::new();
            for i in 0..column_count {
                let str_val: String = row.get::<_, String>(i).unwrap_or_else(|_| {
                    row.get::<_, i64>(i)
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| {
                            row.get::<_, f64>(i)
                                .map(|n| n.to_string())
                                .unwrap_or_else(|_| "NULL".to_string())
                        })
                });
                values.push(str_val);
            }
            rows.push(values.join(", "));
        }

        Ok(rows)
    }

    async fn query_one(&self, sql: &str) -> DbResult<Option<String>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(sql).to_db_err()?;
        let mut result_rows = stmt.query([]).to_db_err()?;

        let Some(row) = result_rows.next().to_db_err()? else {
            return Ok(None);
        };

        let value: String = row.get::<_, String>(0).unwrap_or_else(|_| {
            row.get::<_, i64>(0)
                .map(|n| n.to_string())
                .unwrap_or_else(|_| {
                    row.get::<_, f64>(0)
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| String::new())
                })
        });

        if value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    async fn merge_into(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()> {
        // DuckDB lacks native MERGE; emulate via DELETE + INSERT through temp table
        let temp_table = format!("__ff_merge_source_{}", unique_id());

        let create_temp = format!("CREATE TEMP TABLE {} AS {}", temp_table, source_sql);
        self.execute_sync(&create_temp)?;

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("{}.{} = {}.{}", target_table, k, temp_table, k))
            .collect();
        let join_clause = join_conditions.join(" AND ");

        let delete_sql = format!(
            "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {})",
            target_table, temp_table, join_clause
        );
        self.execute_sync(&delete_sql)?;

        let insert_sql = format!("INSERT INTO {} SELECT * FROM {}", target_table, temp_table);
        self.execute_sync(&insert_sql)?;

        let drop_temp = format!("DROP TABLE {}", temp_table);
        self.execute_sync(&drop_temp)?;

        Ok(())
    }

    async fn delete_insert(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()> {
        let temp_table = format!("__ff_delete_insert_source_{}", unique_id());

        let create_temp = format!("CREATE TEMP TABLE {} AS {}", temp_table, source_sql);
        self.execute_sync(&create_temp)?;

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("{}.{} = {}.{}", target_table, k, temp_table, k))
            .collect();
        let join_clause = join_conditions.join(" AND ");

        let delete_sql = format!(
            "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {})",
            target_table, temp_table, join_clause
        );
        self.execute_sync(&delete_sql)?;

        let insert_sql = format!("INSERT INTO {} SELECT * FROM {}", target_table, temp_table);
        self.execute_sync(&insert_sql)?;

        let drop_temp = format!("DROP TABLE {}", temp_table);
        self.execute_sync(&drop_temp)?;

        Ok(())
    }

    async fn get_table_schema(&self, table: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();

        let (schema, table_name) = if let Some(pos) = table.rfind('.') {
            (&table[..pos], &table[pos + 1..])
        } else {
            ("main", table)
        };

        let sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}' ORDER BY ordinal_position",
            schema, table_name
        );

        let mut stmt = conn.prepare(&sql).to_db_err()?;
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut rows = stmt.query([]).to_db_err()?;

        while let Some(row) = rows.next().to_db_err()? {
            let column_name: String = row.get(0).to_db_err()?;
            let column_type: String = row.get(1).to_db_err()?;
            columns.push((column_name, column_type));
        }

        Ok(columns)
    }

    async fn describe_query(&self, sql: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        let describe_sql = format!("DESCRIBE SELECT * FROM ({}) AS subq", sql);

        let mut stmt = conn.prepare(&describe_sql).to_db_err()?;
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut rows = stmt.query([]).to_db_err()?;

        while let Some(row) = rows.next().to_db_err()? {
            let column_name: String = row.get(0).to_db_err()?;
            let column_type: String = row.get(1).to_db_err()?;
            columns.push((column_name, column_type));
        }

        Ok(columns)
    }

    async fn add_columns(&self, table: &str, columns: &[(String, String)]) -> DbResult<()> {
        for (name, col_type) in columns {
            let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", table, name, col_type);
            self.execute_sync(&sql)?;
        }
        Ok(())
    }

    // ===== Snapshot Operations =====

    async fn execute_snapshot(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
        invalidate_hard_deletes: bool,
    ) -> DbResult<SnapshotResult> {
        let snapshot_exists = self.relation_exists(snapshot_table).await?;

        if !snapshot_exists {
            let source_schema = self.get_table_schema(source_table).await?;

            let mut columns: Vec<String> = source_schema
                .iter()
                .map(|(name, dtype)| format!("{} {}", name, dtype))
                .collect();

            columns.push("dbt_scd_id VARCHAR".to_string());
            columns.push("dbt_updated_at TIMESTAMP".to_string());
            columns.push("dbt_valid_from TIMESTAMP".to_string());
            columns.push("dbt_valid_to TIMESTAMP".to_string());

            let create_sql = format!("CREATE TABLE {} ({})", snapshot_table, columns.join(", "));
            self.execute_sync(&create_sql)?;

            let source_cols: Vec<&str> = source_schema.iter().map(|(n, _)| n.as_str()).collect();

            let updated_at_expr = if let Some(col) = updated_at_column {
                format!("CAST({} AS TIMESTAMP)", col)
            } else {
                "CURRENT_TIMESTAMP".to_string()
            };

            let key_concat: Vec<String> = unique_keys
                .iter()
                .map(|k| format!("COALESCE(CAST({} AS VARCHAR), '')", k))
                .collect();
            let scd_id_expr = format!(
                "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
                key_concat.join(" || '|' || ")
            );

            let insert_sql = format!(
                "INSERT INTO {} ({}, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to) \
                 SELECT {}, {}, {}, CURRENT_TIMESTAMP, NULL \
                 FROM {}",
                snapshot_table,
                source_cols.join(", "),
                source_cols.join(", "),
                scd_id_expr,
                updated_at_expr,
                source_table
            );
            self.execute_sync(&insert_sql)?;

            let new_count = self
                .query_count(&format!("SELECT * FROM {}", snapshot_table))
                .await?;

            return Ok(SnapshotResult {
                new_records: new_count,
                updated_records: 0,
                deleted_records: 0,
            });
        }

        let new_records = self
            .snapshot_insert_new(snapshot_table, source_table, unique_keys)
            .await?;

        let updated_records = self
            .snapshot_update_changed(
                snapshot_table,
                source_table,
                unique_keys,
                updated_at_column,
                check_cols,
            )
            .await?;

        let deleted_records = if invalidate_hard_deletes {
            self.snapshot_invalidate_deleted(snapshot_table, source_table, unique_keys)
                .await?
        } else {
            0
        };

        Ok(SnapshotResult {
            new_records,
            updated_records,
            deleted_records,
        })
    }

    async fn snapshot_insert_new(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize> {
        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("s.{} = snap.{}", k, k))
            .collect();

        let source_schema = self.get_table_schema(source_table).await?;
        let source_cols: Vec<&str> = source_schema.iter().map(|(n, _)| n.as_str()).collect();

        let key_concat: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("COALESCE(CAST(s.{} AS VARCHAR), '')", k))
            .collect();
        let scd_id_expr = format!(
            "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
            key_concat.join(" || '|' || ")
        );

        let prefixed_cols: Vec<String> = source_cols.iter().map(|c| format!("s.{}", c)).collect();
        let first_key = unique_keys.first().map(String::as_str).unwrap_or("id");

        let new_records_sql = format!(
            "SELECT s.* FROM {source} s \
             LEFT JOIN (SELECT * FROM {snapshot} WHERE dbt_valid_to IS NULL) snap \
               ON {join_conditions} \
             WHERE snap.{first_key} IS NULL",
            source = source_table,
            snapshot = snapshot_table,
            join_conditions = join_conditions.join(" AND "),
            first_key = first_key,
        );

        let new_count = self.query_count(&new_records_sql).await?;

        if new_count > 0 {
            let insert_sql = format!(
                "INSERT INTO {snapshot} ({cols}, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to) \
                 SELECT {prefixed_cols}, {scd_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL \
                 FROM {source} s \
                 LEFT JOIN (SELECT * FROM {snapshot} WHERE dbt_valid_to IS NULL) snap \
                   ON {join_conditions} \
                 WHERE snap.{first_key} IS NULL",
                snapshot = snapshot_table,
                cols = source_cols.join(", "),
                prefixed_cols = prefixed_cols.join(", "),
                scd_id = scd_id_expr,
                source = source_table,
                join_conditions = join_conditions.join(" AND "),
                first_key = first_key,
            );
            self.execute_sync(&insert_sql)?;
        }

        Ok(new_count)
    }

    async fn snapshot_update_changed(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
    ) -> DbResult<usize> {
        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("s.{} = snap.{}", k, k))
            .collect();

        let change_condition = if let Some(updated_at) = updated_at_column {
            format!("s.{} > snap.dbt_updated_at", updated_at)
        } else if let Some(cols) = check_cols {
            let comparisons: Vec<String> = cols
                .iter()
                .map(|c| format!("(s.{c} IS DISTINCT FROM snap.{c})", c = c))
                .collect();
            comparisons.join(" OR ")
        } else {
            return Ok(0);
        };

        let changed_records_sql = format!(
            "SELECT s.* FROM {source} s \
             INNER JOIN (SELECT * FROM {snapshot} WHERE dbt_valid_to IS NULL) snap \
               ON {join_conditions} \
             WHERE {change_condition}",
            source = source_table,
            snapshot = snapshot_table,
            join_conditions = join_conditions.join(" AND "),
            change_condition = change_condition,
        );

        let changed_count = self.query_count(&changed_records_sql).await?;

        if changed_count == 0 {
            return Ok(0);
        }

        let source_schema = self.get_table_schema(source_table).await?;
        let source_cols: Vec<&str> = source_schema.iter().map(|(n, _)| n.as_str()).collect();
        let prefixed_cols: Vec<String> = source_cols.iter().map(|c| format!("s.{}", c)).collect();

        let key_concat: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("COALESCE(CAST(s.{} AS VARCHAR), '')", k))
            .collect();
        let scd_id_expr = format!(
            "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
            key_concat.join(" || '|' || ")
        );

        let updated_at_expr = if let Some(col) = updated_at_column {
            format!("s.{}", col)
        } else {
            "CURRENT_TIMESTAMP".to_string()
        };

        // Insert new versions before invalidating old ones to capture changing records
        let insert_sql = format!(
            "INSERT INTO {snapshot} ({cols}, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to) \
             SELECT {prefixed_cols}, {scd_id}, {updated_at_expr}, CURRENT_TIMESTAMP, NULL \
             FROM {source} s \
             INNER JOIN (SELECT * FROM {snapshot} WHERE dbt_valid_to IS NULL) snap \
               ON {join_conditions} \
             WHERE {change_condition}",
            snapshot = snapshot_table,
            cols = source_cols.join(", "),
            prefixed_cols = prefixed_cols.join(", "),
            scd_id = scd_id_expr,
            updated_at_expr = updated_at_expr,
            source = source_table,
            join_conditions = join_conditions.join(" AND "),
            change_condition = change_condition,
        );
        self.execute_sync(&insert_sql)?;

        let key_match_for_newer: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("old.{k} = newer.{k}", k = k))
            .collect();

        let update_sql = format!(
            "UPDATE {snapshot} AS old SET dbt_valid_to = CURRENT_TIMESTAMP \
             WHERE old.dbt_valid_to IS NULL \
             AND EXISTS ( \
                 SELECT 1 FROM {snapshot} newer \
                 WHERE {key_match} \
                 AND newer.dbt_valid_from > old.dbt_valid_from \
                 AND newer.dbt_valid_to IS NULL \
             )",
            snapshot = snapshot_table,
            key_match = key_match_for_newer.join(" AND "),
        );
        self.execute_sync(&update_sql)?;

        Ok(changed_count)
    }

    async fn snapshot_invalidate_deleted(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize> {
        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("snap.{} = s.{}", k, k))
            .collect();

        let first_key = unique_keys.first().map(String::as_str).unwrap_or("id");

        let deleted_records_sql = format!(
            "SELECT snap.* FROM {snapshot} snap \
             LEFT JOIN {source} s ON {join_conditions} \
             WHERE snap.dbt_valid_to IS NULL AND s.{first_key} IS NULL",
            snapshot = snapshot_table,
            source = source_table,
            join_conditions = join_conditions.join(" AND "),
            first_key = first_key,
        );

        let deleted_count = self.query_count(&deleted_records_sql).await?;

        if deleted_count == 0 {
            return Ok(0);
        }

        let update_sql = format!(
            "UPDATE {snapshot} SET dbt_valid_to = CURRENT_TIMESTAMP \
             WHERE dbt_valid_to IS NULL \
             AND NOT EXISTS ( \
                 SELECT 1 FROM {source} s \
                 WHERE {key_match} \
             )",
            snapshot = snapshot_table,
            source = source_table,
            key_match = unique_keys
                .iter()
                .map(|k| format!("{snapshot}.{k} = s.{k}", snapshot = snapshot_table, k = k))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
        self.execute_sync(&update_sql)?;

        Ok(deleted_count)
    }
}

/// Generate a unique identifier for temp table names.
///
/// Uses timestamp plus atomic counter to avoid collisions in concurrent operations.
fn unique_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", now.as_secs(), now.subsec_nanos(), count)
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

    #[tokio::test]
    async fn test_merge_into() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create target table with initial data
        db.execute_batch(
            "CREATE TABLE users (id INT, name VARCHAR, updated_at VARCHAR);
             INSERT INTO users VALUES (1, 'Alice', '2024-01-01'), (2, 'Bob', '2024-01-01');",
        )
        .await
        .unwrap();

        // Merge in new/updated data
        let source_sql =
            "SELECT 2 AS id, 'Bobby' AS name, '2024-01-02' AS updated_at UNION ALL SELECT 3, 'Charlie', '2024-01-02'";

        db.merge_into("users", source_sql, &["id".to_string()])
            .await
            .unwrap();

        // Verify: id=1 unchanged, id=2 updated, id=3 inserted
        let count = db.query_count("SELECT * FROM users").await.unwrap();
        assert_eq!(count, 3);

        // Verify Bob was updated to Bobby
        let name = db
            .query_one("SELECT name FROM users WHERE id = 2")
            .await
            .unwrap();
        assert_eq!(name, Some("Bobby".to_string()));

        // Verify Alice unchanged
        let name = db
            .query_one("SELECT name FROM users WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(name, Some("Alice".to_string()));

        // Verify Charlie was inserted
        let name = db
            .query_one("SELECT name FROM users WHERE id = 3")
            .await
            .unwrap();
        assert_eq!(name, Some("Charlie".to_string()));
    }

    #[tokio::test]
    async fn test_delete_insert() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create target table with initial data
        db.execute_batch(
            "CREATE TABLE orders (order_id INT, customer_id INT, amount INT);
             INSERT INTO orders VALUES (1, 100, 50), (2, 100, 75), (3, 200, 100);",
        )
        .await
        .unwrap();

        // Delete+insert: delete matching order_ids and insert new versions
        // Source has order_id 1 (updated) and order_id 4 (new)
        let source_sql =
            "SELECT 1 AS order_id, 100 AS customer_id, 60 AS amount UNION ALL SELECT 4, 100, 80";

        db.delete_insert("orders", source_sql, &["order_id".to_string()])
            .await
            .unwrap();

        // Verify: order 1 replaced (only matched row deleted), order 2 unchanged, order 3 unchanged, order 4 inserted
        let count = db.query_count("SELECT * FROM orders").await.unwrap();
        assert_eq!(count, 4); // orders 1, 2, 3, 4

        // Verify order 1 amount updated
        let amount = db
            .query_one("SELECT amount FROM orders WHERE order_id = 1")
            .await
            .unwrap();
        assert_eq!(amount, Some("60".to_string()));

        // Verify order 2 unchanged (not in source, so not deleted)
        let amount = db
            .query_one("SELECT amount FROM orders WHERE order_id = 2")
            .await
            .unwrap();
        assert_eq!(amount, Some("75".to_string()));

        // Verify order 3 unchanged
        let amount = db
            .query_one("SELECT amount FROM orders WHERE order_id = 3")
            .await
            .unwrap();
        assert_eq!(amount, Some("100".to_string()));

        // Verify order 4 inserted
        let amount = db
            .query_one("SELECT amount FROM orders WHERE order_id = 4")
            .await
            .unwrap();
        assert_eq!(amount, Some("80".to_string()));
    }

    #[tokio::test]
    async fn test_merge_into_composite_key() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create target table with composite key
        db.execute_batch(
            "CREATE TABLE inventory (warehouse VARCHAR, product VARCHAR, qty INT);
             INSERT INTO inventory VALUES ('A', 'widget', 10), ('A', 'gadget', 5), ('B', 'widget', 20);",
        )
        .await
        .unwrap();

        // Merge with composite key
        let source_sql =
            "SELECT 'A' AS warehouse, 'widget' AS product, 15 AS qty UNION ALL SELECT 'C', 'widget', 30";

        db.merge_into(
            "inventory",
            source_sql,
            &["warehouse".to_string(), "product".to_string()],
        )
        .await
        .unwrap();

        // Verify counts
        let count = db.query_count("SELECT * FROM inventory").await.unwrap();
        assert_eq!(count, 4); // A-widget updated, A-gadget unchanged, B-widget unchanged, C-widget inserted

        // Verify A-widget updated
        let qty = db
            .query_one("SELECT qty FROM inventory WHERE warehouse = 'A' AND product = 'widget'")
            .await
            .unwrap();
        assert_eq!(qty, Some("15".to_string()));
    }

    #[tokio::test]
    async fn test_get_table_schema() {
        let db = DuckDbBackend::in_memory().unwrap();

        db.execute_batch(
            "CREATE TABLE test_schema (id INT, name VARCHAR, amount DOUBLE, created_at TIMESTAMP)",
        )
        .await
        .unwrap();

        let schema = db.get_table_schema("test_schema").await.unwrap();

        assert_eq!(schema.len(), 4);
        assert_eq!(schema[0].0, "id");
        assert_eq!(schema[1].0, "name");
        assert_eq!(schema[2].0, "amount");
        assert_eq!(schema[3].0, "created_at");
    }

    #[tokio::test]
    async fn test_describe_query() {
        let db = DuckDbBackend::in_memory().unwrap();

        let schema = db
            .describe_query("SELECT 1 AS id, 'hello' AS name, 3.14 AS value")
            .await
            .unwrap();

        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].0, "id");
        assert_eq!(schema[1].0, "name");
        assert_eq!(schema[2].0, "value");
    }

    #[tokio::test]
    async fn test_add_columns() {
        let db = DuckDbBackend::in_memory().unwrap();

        db.execute_batch("CREATE TABLE test_add (id INT)")
            .await
            .unwrap();

        // Add new columns
        db.add_columns(
            "test_add",
            &[
                ("name".to_string(), "VARCHAR".to_string()),
                ("created_at".to_string(), "TIMESTAMP".to_string()),
            ],
        )
        .await
        .unwrap();

        // Verify columns exist
        let schema = db.get_table_schema("test_add").await.unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[1].0, "name");
        assert_eq!(schema[2].0, "created_at");
    }

    // ===== Snapshot Tests =====

    #[tokio::test]
    async fn test_snapshot_initial_load() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create source table
        db.execute_batch(
            "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
             INSERT INTO customers VALUES
               (1, 'Alice', '2024-01-01'::TIMESTAMP),
               (2, 'Bob', '2024-01-01'::TIMESTAMP);",
        )
        .await
        .unwrap();

        // Execute initial snapshot
        let result = db
            .execute_snapshot(
                "customers_snapshot",
                "customers",
                &["id".to_string()],
                Some("updated_at"),
                None,
                false,
            )
            .await
            .unwrap();

        // Verify initial load
        assert_eq!(result.new_records, 2);
        assert_eq!(result.updated_records, 0);
        assert_eq!(result.deleted_records, 0);

        // Verify snapshot table has SCD columns
        let schema = db.get_table_schema("customers_snapshot").await.unwrap();
        let col_names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
        assert!(col_names.contains(&"dbt_scd_id"));
        assert!(col_names.contains(&"dbt_updated_at"));
        assert!(col_names.contains(&"dbt_valid_from"));
        assert!(col_names.contains(&"dbt_valid_to"));

        // All records should have dbt_valid_to = NULL (active)
        let active_count = db
            .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(active_count, 2);
    }

    #[tokio::test]
    async fn test_snapshot_insert_new_records() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create source table
        db.execute_batch(
            "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
             INSERT INTO customers VALUES (1, 'Alice', '2024-01-01'::TIMESTAMP);",
        )
        .await
        .unwrap();

        // Initial snapshot
        let initial_result = db
            .execute_snapshot(
                "customers_snapshot",
                "customers",
                &["id".to_string()],
                Some("updated_at"),
                None,
                false,
            )
            .await
            .unwrap();

        // Check initial snapshot loaded correctly
        assert_eq!(initial_result.new_records, 1);

        // Add new record to source
        db.execute_batch("INSERT INTO customers VALUES (2, 'Bob', '2024-01-02'::TIMESTAMP);")
            .await
            .unwrap();

        // Execute snapshot again
        let result = db
            .execute_snapshot(
                "customers_snapshot",
                "customers",
                &["id".to_string()],
                Some("updated_at"),
                None,
                false,
            )
            .await
            .unwrap();

        // Only new record should be inserted
        assert_eq!(result.new_records, 1);
        assert_eq!(result.updated_records, 0);
        assert_eq!(result.deleted_records, 0);

        // Total active records should be 2
        let active_count = db
            .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(active_count, 2);
    }

    #[tokio::test]
    async fn test_snapshot_update_changed_timestamp() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create source table
        db.execute_batch(
            "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
             INSERT INTO customers VALUES (1, 'Alice', '2024-01-01 00:00:00'::TIMESTAMP);",
        )
        .await
        .unwrap();

        // Initial snapshot
        db.execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

        // Update record in source with newer timestamp
        db.execute_batch(
            "UPDATE customers SET name = 'Alice Smith', updated_at = '2024-01-02 00:00:00'::TIMESTAMP WHERE id = 1;",
        )
        .await
        .unwrap();

        // Execute snapshot again
        let result = db
            .execute_snapshot(
                "customers_snapshot",
                "customers",
                &["id".to_string()],
                Some("updated_at"),
                None,
                false,
            )
            .await
            .unwrap();

        // Record should be updated (old version invalidated, new version inserted)
        assert_eq!(result.new_records, 0);
        assert_eq!(result.updated_records, 1);
        assert_eq!(result.deleted_records, 0);

        // Total records should be 2 (one active, one historical)
        let total_count = db
            .query_count("SELECT * FROM customers_snapshot")
            .await
            .unwrap();
        assert_eq!(total_count, 2);

        // Active records should be 1
        let active_count = db
            .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(active_count, 1);

        // Active record should have new name
        let name = db
            .query_one("SELECT name FROM customers_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(name, Some("Alice Smith".to_string()));
    }

    #[tokio::test]
    async fn test_snapshot_hard_deletes() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create source table
        db.execute_batch(
            "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
             INSERT INTO customers VALUES
               (1, 'Alice', '2024-01-01'::TIMESTAMP),
               (2, 'Bob', '2024-01-01'::TIMESTAMP);",
        )
        .await
        .unwrap();

        // Initial snapshot
        db.execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            true, // invalidate_hard_deletes = true
        )
        .await
        .unwrap();

        // Delete Bob from source
        db.execute_batch("DELETE FROM customers WHERE id = 2;")
            .await
            .unwrap();

        // Execute snapshot with hard delete tracking
        let result = db
            .execute_snapshot(
                "customers_snapshot",
                "customers",
                &["id".to_string()],
                Some("updated_at"),
                None,
                true,
            )
            .await
            .unwrap();

        // Bob should be invalidated
        assert_eq!(result.new_records, 0);
        assert_eq!(result.updated_records, 0);
        assert_eq!(result.deleted_records, 1);

        // Only Alice should be active
        let active_count = db
            .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(active_count, 1);

        let name = db
            .query_one("SELECT name FROM customers_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(name, Some("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_snapshot_check_strategy() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create source table (no timestamp column)
        db.execute_batch(
            "CREATE TABLE products (id INT, name VARCHAR, price DECIMAL);
             INSERT INTO products VALUES (1, 'Widget', 10.00);",
        )
        .await
        .unwrap();

        // Initial snapshot with check strategy
        db.execute_snapshot(
            "products_snapshot",
            "products",
            &["id".to_string()],
            None,
            Some(&["name".to_string(), "price".to_string()]),
            false,
        )
        .await
        .unwrap();

        // Update price
        db.execute_batch("UPDATE products SET price = 15.00 WHERE id = 1;")
            .await
            .unwrap();

        // Execute snapshot again
        let result = db
            .execute_snapshot(
                "products_snapshot",
                "products",
                &["id".to_string()],
                None,
                Some(&["name".to_string(), "price".to_string()]),
                false,
            )
            .await
            .unwrap();

        // Record should be updated due to price change
        assert_eq!(result.updated_records, 1);

        // Active record should have new price
        let price = db
            .query_one("SELECT price FROM products_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        // DuckDB formats DECIMAL differently, just check it starts with 15
        assert!(price.unwrap().starts_with("15"));
    }

    #[tokio::test]
    async fn test_snapshot_composite_key() {
        let db = DuckDbBackend::in_memory().unwrap();

        // Create source table with composite key
        db.execute_batch(
            "CREATE TABLE inventory (warehouse VARCHAR, product VARCHAR, qty INT, updated_at TIMESTAMP);
             INSERT INTO inventory VALUES
               ('A', 'widget', 10, '2024-01-01'::TIMESTAMP),
               ('A', 'gadget', 5, '2024-01-01'::TIMESTAMP);",
        )
        .await
        .unwrap();

        // Initial snapshot
        let result = db
            .execute_snapshot(
                "inventory_snapshot",
                "inventory",
                &["warehouse".to_string(), "product".to_string()],
                Some("updated_at"),
                None,
                false,
            )
            .await
            .unwrap();

        assert_eq!(result.new_records, 2);

        // Add new product
        db.execute_batch(
            "INSERT INTO inventory VALUES ('B', 'widget', 20, '2024-01-02'::TIMESTAMP);",
        )
        .await
        .unwrap();

        let result = db
            .execute_snapshot(
                "inventory_snapshot",
                "inventory",
                &["warehouse".to_string(), "product".to_string()],
                Some("updated_at"),
                None,
                false,
            )
            .await
            .unwrap();

        assert_eq!(result.new_records, 1);

        let active_count = db
            .query_count("SELECT * FROM inventory_snapshot WHERE dbt_valid_to IS NULL")
            .await
            .unwrap();
        assert_eq!(active_count, 3);
    }
}
