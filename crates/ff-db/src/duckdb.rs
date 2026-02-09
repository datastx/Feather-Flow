//! DuckDB database backend implementation

use crate::error::{DbError, DbResult};
use crate::traits::{
    CsvLoadOptions, DatabaseCore, DatabaseCsv, DatabaseIncremental, DatabaseSchema,
    DatabaseSnapshot, SnapshotResult,
};
use async_trait::async_trait;
use duckdb::Connection;
use ff_core::sql_utils::{escape_sql_string, quote_ident, quote_qualified};
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

    /// Acquire the database connection lock, returning a descriptive error on poison.
    fn lock_conn(&self) -> DbResult<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|e| DbError::MutexPoisoned(e.to_string()))
    }

    /// Execute SQL synchronously
    fn execute_sync(&self, sql: &str) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        conn.execute(sql, []).map_err(|e| {
            let truncated = if sql.len() > 200 {
                let end = sql
                    .char_indices()
                    .map(|(i, _)| i)
                    .find(|&i| i >= 200)
                    .unwrap_or(sql.len());
                format!("{}...", &sql[..end])
            } else {
                sql.to_string()
            };
            DbError::ExecutionError(format!("{}: {}", e, truncated))
        })
    }

    /// Execute batch SQL synchronously
    fn execute_batch_sync(&self, sql: &str) -> DbResult<()> {
        let conn = self.lock_conn()?;
        conn.execute_batch(sql).to_db_err()
    }

    /// Query count synchronously
    fn query_count_sync(&self, sql: &str) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM ({})", sql), [], |row| {
                row.get(0)
            })
            .to_db_err()?;
        Ok(usize::try_from(count).unwrap_or(0))
    }

    /// Check if relation exists synchronously
    fn relation_exists_sync(&self, name: &str) -> DbResult<bool> {
        let conn = self.lock_conn()?;

        let (schema, table) = if let Some(pos) = name.rfind('.') {
            (&name[..pos], &name[pos + 1..])
        } else {
            ("main", name)
        };

        let sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";

        let count: i64 = conn
            .query_row(sql, duckdb::params![schema, table], |row| row.get(0))
            .to_db_err()?;

        Ok(count > 0)
    }
}

#[async_trait]
impl DatabaseCore for DuckDbBackend {
    async fn execute(&self, sql: &str) -> DbResult<usize> {
        self.execute_sync(sql)
    }

    async fn execute_batch(&self, sql: &str) -> DbResult<()> {
        self.execute_batch_sync(sql)
    }

    async fn relation_exists(&self, name: &str) -> DbResult<bool> {
        self.relation_exists_sync(name)
    }

    async fn query_count(&self, sql: &str) -> DbResult<usize> {
        self.query_count_sync(sql)
    }

    async fn query_sample_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<String>> {
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;

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

    fn db_type(&self) -> &'static str {
        "duckdb"
    }
}

#[async_trait]
impl DatabaseSchema for DuckDbBackend {
    async fn create_table_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()> {
        let quoted = quote_qualified(name);
        let sql = if replace {
            format!("CREATE OR REPLACE TABLE {} AS {}", quoted, select)
        } else {
            format!("CREATE TABLE {} AS {}", quoted, select)
        };
        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn create_view_as(&self, name: &str, select: &str, replace: bool) -> DbResult<()> {
        let quoted = quote_qualified(name);
        let sql = if replace {
            format!("CREATE OR REPLACE VIEW {} AS {}", quoted, select)
        } else {
            format!("CREATE VIEW {} AS {}", quoted, select)
        };
        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn drop_if_exists(&self, name: &str) -> DbResult<()> {
        let quoted = quote_qualified(name);
        // Try VIEW first, then TABLE. DuckDB returns a CatalogError when the
        // relation exists but is the wrong type, so we ignore type-mismatch
        // errors and propagate everything else.
        match self.execute_sync(&format!("DROP VIEW IF EXISTS {}", quoted)) {
            Ok(_) => {}
            Err(DbError::ExecutionError(msg)) if msg.contains("trying to drop type") => {}
            Err(e) => return Err(e),
        }
        match self.execute_sync(&format!("DROP TABLE IF EXISTS {}", quoted)) {
            Ok(_) => {}
            Err(DbError::ExecutionError(msg)) if msg.contains("trying to drop type") => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    async fn create_schema_if_not_exists(&self, schema: &str) -> DbResult<()> {
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema));
        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn get_table_schema(&self, table: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.lock_conn()?;

        let (schema, table_name) = if let Some(pos) = table.rfind('.') {
            (&table[..pos], &table[pos + 1..])
        } else {
            ("main", table)
        };

        let sql = "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";

        let mut stmt = conn.prepare(sql).to_db_err()?;
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut rows = stmt
            .query(duckdb::params![schema, table_name])
            .to_db_err()?;

        while let Some(row) = rows.next().to_db_err()? {
            let column_name: String = row.get(0).to_db_err()?;
            let column_type: String = row.get(1).to_db_err()?;
            columns.push((column_name, column_type));
        }

        Ok(columns)
    }

    async fn describe_query(&self, sql: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.lock_conn()?;
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
        let quoted_table = quote_qualified(table);
        for (name, col_type) in columns {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                quoted_table,
                quote_ident(name),
                col_type
            );
            self.execute_sync(&sql)?;
        }
        Ok(())
    }
}

#[async_trait]
impl DatabaseCsv for DuckDbBackend {
    async fn load_csv(&self, table: &str, path: &str) -> DbResult<()> {
        let sql = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_csv_auto('{}')",
            quote_qualified(table),
            path.replace('\'', "''")
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
            csv_options.push(format!("delim = '{}'", escape_sql_string(&delim_str)));
        }

        if options.quote_columns() {
            csv_options.push("quote = '\"'".to_string());
        }

        let type_casts: Vec<String> = options
            .column_types()
            .iter()
            .map(|(col, typ)| {
                format!(
                    "CAST({} AS {}) AS {}",
                    quote_ident(col),
                    typ,
                    quote_ident(col)
                )
            })
            .collect();

        let csv_opts_str = if csv_options.is_empty() {
            String::new()
        } else {
            format!(", {}", csv_options.join(", "))
        };

        let target_table = if let Some(schema) = options.schema() {
            self.create_schema_if_not_exists(schema).await?;
            format!("{}.{}", quote_ident(schema), quote_ident(table))
        } else {
            quote_qualified(table)
        };

        let escaped_path = path.replace('\'', "''");
        let sql = if type_casts.is_empty() {
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_csv_auto('{}'{})",
                target_table, escaped_path, csv_opts_str
            )
        } else {
            let typed_columns = type_casts.join(", ");
            let exclude_cols: Vec<String> = options
                .column_types()
                .keys()
                .map(|c| quote_ident(c))
                .collect();
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT {}, * EXCLUDE ({}) FROM read_csv_auto('{}'{})",
                target_table,
                typed_columns,
                exclude_cols.join(", "),
                escaped_path,
                csv_opts_str
            )
        };

        self.execute_sync(&sql)?;
        Ok(())
    }

    async fn infer_csv_schema(&self, path: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.lock_conn()?;
        let sql = format!(
            "DESCRIBE SELECT * FROM read_csv_auto('{}')",
            path.replace('\'', "''")
        );

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
}

#[async_trait]
impl DatabaseIncremental for DuckDbBackend {
    async fn merge_into(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()> {
        // DuckDB lacks native MERGE; emulate via DELETE + INSERT through temp table
        let temp_name = format!("__ff_merge_source_{}", unique_id());
        let quoted_target = quote_qualified(target_table);
        let quoted_temp = quote_ident(&temp_name);

        let create_temp = format!("CREATE TEMP TABLE {} AS {}", quoted_temp, source_sql);
        self.execute_sync(&create_temp)?;

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("{}.{} = {}.{}", quoted_target, qk, quoted_temp, qk)
            })
            .collect();
        let join_clause = join_conditions.join(" AND ");

        let delete_sql = format!(
            "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {})",
            quoted_target, quoted_temp, join_clause
        );
        self.execute_sync(&delete_sql)?;

        let insert_sql = format!(
            "INSERT INTO {} SELECT * FROM {}",
            quoted_target, quoted_temp
        );
        self.execute_sync(&insert_sql)?;

        let drop_temp = format!("DROP TABLE {}", quoted_temp);
        self.execute_sync(&drop_temp)?;

        Ok(())
    }

    async fn delete_insert(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()> {
        let temp_name = format!("__ff_delete_insert_source_{}", unique_id());
        let quoted_target = quote_qualified(target_table);
        let quoted_temp = quote_ident(&temp_name);

        let create_temp = format!("CREATE TEMP TABLE {} AS {}", quoted_temp, source_sql);
        self.execute_sync(&create_temp)?;

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("{}.{} = {}.{}", quoted_target, qk, quoted_temp, qk)
            })
            .collect();
        let join_clause = join_conditions.join(" AND ");

        let delete_sql = format!(
            "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {})",
            quoted_target, quoted_temp, join_clause
        );
        self.execute_sync(&delete_sql)?;

        let insert_sql = format!(
            "INSERT INTO {} SELECT * FROM {}",
            quoted_target, quoted_temp
        );
        self.execute_sync(&insert_sql)?;

        let drop_temp = format!("DROP TABLE {}", quoted_temp);
        self.execute_sync(&drop_temp)?;

        Ok(())
    }
}

#[async_trait]
impl DatabaseSnapshot for DuckDbBackend {
    async fn execute_snapshot(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
        invalidate_hard_deletes: bool,
    ) -> DbResult<SnapshotResult> {
        // Validate inputs
        if unique_keys.is_empty() {
            return Err(DbError::ExecutionError(
                "Snapshot requires at least one unique_key".to_string(),
            ));
        }
        if snapshot_table == source_table {
            return Err(DbError::ExecutionError(
                "Snapshot table and source table must be different".to_string(),
            ));
        }

        let snapshot_exists = self.relation_exists(snapshot_table).await?;

        if !snapshot_exists {
            let source_schema = self.get_table_schema(source_table).await?;
            let quoted_snap = quote_qualified(snapshot_table);
            let quoted_src = quote_qualified(source_table);

            let mut columns: Vec<String> = source_schema
                .iter()
                .map(|(name, dtype)| format!("{} {}", quote_ident(name), dtype))
                .collect();

            columns.push("dbt_scd_id VARCHAR".to_string());
            columns.push("dbt_updated_at TIMESTAMP".to_string());
            columns.push("dbt_valid_from TIMESTAMP".to_string());
            columns.push("dbt_valid_to TIMESTAMP".to_string());

            let create_sql = format!("CREATE TABLE {} ({})", quoted_snap, columns.join(", "));
            self.execute_sync(&create_sql)?;

            let source_cols: Vec<String> =
                source_schema.iter().map(|(n, _)| quote_ident(n)).collect();

            let updated_at_expr = if let Some(col) = updated_at_column {
                format!("CAST({} AS TIMESTAMP)", quote_ident(col))
            } else {
                "CURRENT_TIMESTAMP".to_string()
            };

            let key_concat: Vec<String> = unique_keys
                .iter()
                .map(|k| format!("COALESCE(CAST({} AS VARCHAR), '')", quote_ident(k)))
                .collect();
            let scd_id_expr = format!(
                "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
                key_concat.join(" || '|' || ")
            );

            let insert_sql = format!(
                "INSERT INTO {} ({}, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to) \
                 SELECT {}, {}, {}, CURRENT_TIMESTAMP, NULL \
                 FROM {}",
                quoted_snap,
                source_cols.join(", "),
                source_cols.join(", "),
                scd_id_expr,
                updated_at_expr,
                quoted_src
            );
            self.execute_sync(&insert_sql)?;

            let new_count = self
                .query_count(&format!("SELECT * FROM {}", quoted_snap))
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
        let quoted_snap = quote_qualified(snapshot_table);
        let quoted_src = quote_qualified(source_table);

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("s.{} = snap.{}", qk, qk)
            })
            .collect();

        let source_schema = self.get_table_schema(source_table).await?;
        let source_cols: Vec<String> = source_schema.iter().map(|(n, _)| quote_ident(n)).collect();

        let key_concat: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("COALESCE(CAST(s.{} AS VARCHAR), '')", quote_ident(k)))
            .collect();
        let scd_id_expr = format!(
            "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
            key_concat.join(" || '|' || ")
        );

        let prefixed_cols: Vec<String> = source_cols.iter().map(|c| format!("s.{}", c)).collect();
        let first_key = unique_keys
            .first()
            .map(|k| quote_ident(k))
            .unwrap_or_else(|| quote_ident("id"));

        let new_records_sql = format!(
            "SELECT s.* FROM {} s \
             LEFT JOIN (SELECT * FROM {} WHERE dbt_valid_to IS NULL) snap \
               ON {} \
             WHERE snap.{} IS NULL",
            quoted_src,
            quoted_snap,
            join_conditions.join(" AND "),
            first_key,
        );

        let new_count = self.query_count(&new_records_sql).await?;

        if new_count > 0 {
            let insert_sql = format!(
                "INSERT INTO {} ({}, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to) \
                 SELECT {}, {}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL \
                 FROM {} s \
                 LEFT JOIN (SELECT * FROM {} WHERE dbt_valid_to IS NULL) snap \
                   ON {} \
                 WHERE snap.{} IS NULL",
                quoted_snap,
                source_cols.join(", "),
                prefixed_cols.join(", "),
                scd_id_expr,
                quoted_src,
                quoted_snap,
                join_conditions.join(" AND "),
                first_key,
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
        let quoted_snap = quote_qualified(snapshot_table);
        let quoted_src = quote_qualified(source_table);

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("s.{} = snap.{}", qk, qk)
            })
            .collect();

        let change_condition = if let Some(updated_at) = updated_at_column {
            format!("s.{} > snap.dbt_updated_at", quote_ident(updated_at))
        } else if let Some(cols) = check_cols {
            let comparisons: Vec<String> = cols
                .iter()
                .map(|c| {
                    let qc = quote_ident(c);
                    format!("(s.{} IS DISTINCT FROM snap.{})", qc, qc)
                })
                .collect();
            comparisons.join(" OR ")
        } else {
            return Ok(0);
        };

        let changed_records_sql = format!(
            "SELECT s.* FROM {} s \
             INNER JOIN (SELECT * FROM {} WHERE dbt_valid_to IS NULL) snap \
               ON {} \
             WHERE {}",
            quoted_src,
            quoted_snap,
            join_conditions.join(" AND "),
            change_condition,
        );

        let changed_count = self.query_count(&changed_records_sql).await?;

        if changed_count == 0 {
            return Ok(0);
        }

        let source_schema = self.get_table_schema(source_table).await?;
        let source_cols: Vec<String> = source_schema.iter().map(|(n, _)| quote_ident(n)).collect();
        let prefixed_cols: Vec<String> = source_cols.iter().map(|c| format!("s.{}", c)).collect();

        let key_concat: Vec<String> = unique_keys
            .iter()
            .map(|k| format!("COALESCE(CAST(s.{} AS VARCHAR), '')", quote_ident(k)))
            .collect();
        let scd_id_expr = format!(
            "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
            key_concat.join(" || '|' || ")
        );

        let updated_at_expr = if let Some(col) = updated_at_column {
            format!("s.{}", quote_ident(col))
        } else {
            "CURRENT_TIMESTAMP".to_string()
        };

        // Insert new versions before invalidating old ones to capture changing records
        let insert_sql = format!(
            "INSERT INTO {} ({}, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to) \
             SELECT {}, {}, {}, CURRENT_TIMESTAMP, NULL \
             FROM {} s \
             INNER JOIN (SELECT * FROM {} WHERE dbt_valid_to IS NULL) snap \
               ON {} \
             WHERE {}",
            quoted_snap,
            source_cols.join(", "),
            prefixed_cols.join(", "),
            scd_id_expr,
            updated_at_expr,
            quoted_src,
            quoted_snap,
            join_conditions.join(" AND "),
            change_condition,
        );
        self.execute_sync(&insert_sql)?;

        let key_match_for_newer: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("old.{} = newer.{}", qk, qk)
            })
            .collect();

        let update_sql = format!(
            "UPDATE {} AS old SET dbt_valid_to = CURRENT_TIMESTAMP \
             WHERE old.dbt_valid_to IS NULL \
             AND EXISTS ( \
                 SELECT 1 FROM {} newer \
                 WHERE {} \
                 AND newer.dbt_valid_from > old.dbt_valid_from \
                 AND newer.dbt_valid_to IS NULL \
             )",
            quoted_snap,
            quoted_snap,
            key_match_for_newer.join(" AND "),
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
        let quoted_snap = quote_qualified(snapshot_table);
        let quoted_src = quote_qualified(source_table);

        let join_conditions: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("snap.{} = s.{}", qk, qk)
            })
            .collect();

        let first_key = unique_keys
            .first()
            .map(|k| quote_ident(k))
            .unwrap_or_else(|| quote_ident("id"));

        let deleted_records_sql = format!(
            "SELECT snap.* FROM {} snap \
             LEFT JOIN {} s ON {} \
             WHERE snap.dbt_valid_to IS NULL AND s.{} IS NULL",
            quoted_snap,
            quoted_src,
            join_conditions.join(" AND "),
            first_key,
        );

        let deleted_count = self.query_count(&deleted_records_sql).await?;

        if deleted_count == 0 {
            return Ok(0);
        }

        let key_match: Vec<String> = unique_keys
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("{}.{} = s.{}", quoted_snap, qk, qk)
            })
            .collect();

        let update_sql = format!(
            "UPDATE {} SET dbt_valid_to = CURRENT_TIMESTAMP \
             WHERE dbt_valid_to IS NULL \
             AND NOT EXISTS ( \
                 SELECT 1 FROM {} s \
                 WHERE {} \
             )",
            quoted_snap,
            quoted_src,
            key_match.join(" AND "),
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
