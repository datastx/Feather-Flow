//! DuckDB database backend implementation

use crate::error::{DbError, DbResult};
use crate::traits::{
    CsvLoadOptions, DatabaseCore, DatabaseCsv, DatabaseFunction, DatabaseIncremental,
    DatabaseSchema, DatabaseSnapshot, SnapshotResult,
};
use async_trait::async_trait;
use duckdb::Connection;
use ff_core::sql_utils::{escape_sql_string, quote_ident, quote_qualified};
use std::path::Path;
use std::sync::Mutex;

use ff_core::snapshot::{SCD_ID, SCD_UPDATED_AT, SCD_VALID_FROM, SCD_VALID_TO};

/// Check whether a SQL string contains a semicolon outside of single-quoted literals.
///
/// Walks the string tracking quote depth so that values like `'foo;bar'` are
/// not treated as statement terminators. Escaped quotes (`''`) inside literals
/// are handled correctly.
fn contains_unquoted_semicolon(sql: &str) -> bool {
    let mut in_quote = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if in_quote => {
                // Two consecutive quotes inside a literal = escaped quote
                if chars.peek() == Some(&'\'') {
                    chars.next(); // skip the second quote
                } else {
                    in_quote = false;
                }
            }
            '\'' => in_quote = true,
            ';' if !in_quote => return true,
            _ => {}
        }
    }
    false
}

/// Build the MD5-based SCD ID expression from unique key columns.
///
/// Concatenates the key values (coalesced to empty string) with `|` separators
/// and the current timestamp, then hashes via MD5 to produce a deterministic
/// but time-unique surrogate key.
///
/// When `table_alias` is `Some("s")`, columns are prefixed as `s.col`.
fn build_scd_id_expr(unique_keys: &[String], table_alias: Option<&str>) -> String {
    let key_concat: Vec<String> = unique_keys
        .iter()
        .map(|k| {
            let col = match table_alias {
                Some(alias) => format!("{}.{}", alias, quote_ident(k)),
                None => quote_ident(k),
            };
            format!("COALESCE(CAST({} AS VARCHAR), '')", col)
        })
        .collect();
    format!(
        "MD5({} || '|' || CAST(CURRENT_TIMESTAMP AS VARCHAR))",
        key_concat.join(" || '|' || ")
    )
}

/// Execute SQL on an already-locked connection, returning affected row count.
///
/// Used by transaction-scoped operations that hold the Mutex for their
/// entire duration, avoiding per-statement lock/unlock overhead.
fn run_sql(conn: &Connection, sql: &str) -> DbResult<usize> {
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

/// Query row count on an already-locked connection.
fn count_rows(conn: &Connection, sql: &str) -> DbResult<usize> {
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM ({})", sql), [], |row| {
        row.get(0)
    })?;
    usize::try_from(count).map_err(|_| {
        DbError::ExecutionError(format!(
            "COUNT(*) returned non-representable value: {}",
            count
        ))
    })
}

/// Execute `body` within a BEGIN / COMMIT transaction, rolling back on error.
fn with_transaction<F, T>(conn: &Connection, body: F) -> DbResult<T>
where
    F: FnOnce(&Connection) -> DbResult<T>,
{
    run_sql(conn, "BEGIN TRANSACTION")?;
    let result = body(conn);
    match &result {
        Ok(_) => {
            if let Err(commit_err) = run_sql(conn, "COMMIT") {
                let _ = run_sql(conn, "ROLLBACK");
                return Err(commit_err);
            }
        }
        Err(_) => {
            let _ = run_sql(conn, "ROLLBACK");
        }
    }
    result
}

/// Get table column names and types on an already-locked connection.
fn table_columns(conn: &Connection, table: &str) -> DbResult<Vec<(String, String)>> {
    let (schema, table_name) = ff_core::sql_utils::split_qualified_name(table);
    let sql = "SELECT column_name, data_type FROM information_schema.columns \
               WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
    let mut stmt = conn.prepare(sql)?;
    let mut columns: Vec<(String, String)> = Vec::new();
    let mut rows = stmt.query(duckdb::params![schema, table_name])?;
    while let Some(row) = rows.next()? {
        let column_name: String = row.get(0)?;
        let column_type: String = row.get(1)?;
        columns.push((column_name, column_type));
    }
    Ok(columns)
}

/// Check if a relation exists on an already-locked connection.
fn relation_exists_on(conn: &Connection, name: &str) -> DbResult<bool> {
    let (schema, table) = ff_core::sql_utils::split_qualified_name(name);
    let sql =
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
    let count: i64 = conn.query_row(sql, duckdb::params![schema, table], |row| row.get(0))?;
    Ok(count > 0)
}

/// Extract a cell value as a string, trying multiple types.
///
/// DuckDB-rs requires the caller to specify the Rust type for extraction.
/// We try common types in order: String (covers VARCHAR, DATE, TIMESTAMP,
/// etc. via DuckDB's implicit cast), i64, f64, bool.  If all fail, the
/// value is treated as SQL NULL.
fn extract_cell_as_string(row: &duckdb::Row<'_>, idx: usize) -> String {
    if let Ok(s) = row.get::<_, String>(idx) {
        return s;
    }
    if let Ok(n) = row.get::<_, i64>(idx) {
        return n.to_string();
    }
    if let Ok(f) = row.get::<_, f64>(idx) {
        return f.to_string();
    }
    if let Ok(b) = row.get::<_, bool>(idx) {
        return b.to_string();
    }
    "NULL".to_string()
}

/// Allowlist of known SQL base type tokens for interpolation safety.
///
/// These are the base keywords that may appear before parenthesized parameters
/// (e.g. `VARCHAR(255)`, `DECIMAL(10,2)`). Anything not in this list is rejected
/// by [`validate_sql_type`].
const ALLOWED_SQL_TYPES: &[&str] = &[
    "BIGINT",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "BOOL",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "FLOAT",
    "HUGEINT",
    "INT",
    "INT1",
    "INT2",
    "INT4",
    "INT8",
    "INTEGER",
    "INTERVAL",
    "JSON",
    "LONG",
    "NUMERIC",
    "REAL",
    "SHORT",
    "SIGNED",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "UUID",
    "VARCHAR",
    "STRUCT",
    "MAP",
    "LIST",
    "UNION",
    "ENUM",
];

/// Validate that a SQL type string is safe for direct interpolation.
///
/// Rejects strings containing semicolons, comments (`--`, `/*`), or whose base
/// type token is not in the allowlist.
fn validate_sql_type(type_str: &str) -> DbResult<()> {
    let s = type_str.trim();
    if s.is_empty() {
        return Err(DbError::ExecutionError("empty SQL type string".to_string()));
    }
    // Reject obvious injection markers
    if s.contains(';') || s.contains("--") || s.contains("/*") {
        return Err(DbError::ExecutionError(format!(
            "invalid SQL type '{}': contains disallowed characters",
            s
        )));
    }
    // Extract the base type token (before any parenthesis)
    let base = s
        .split(|c: char| c == '(' || c.is_ascii_whitespace())
        .next()
        .unwrap_or("")
        .to_uppercase();
    if !ALLOWED_SQL_TYPES.contains(&base.as_str()) {
        return Err(DbError::ExecutionError(format!(
            "invalid SQL type '{}': base type '{}' is not in the allowlist",
            s, base
        )));
    }
    Ok(())
}

/// DuckDB database backend
///
/// Uses `std::sync::Mutex` (not `tokio::sync::Mutex`) because the DuckDB
/// connection is synchronous and the guard is never held across `.await`
/// points. All database operations acquire the lock, execute synchronously,
/// and release before any await.
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

    /// Shared implementation for temp-table-based upsert strategies.
    ///
    /// Both `merge_into` and `delete_insert` follow the same pattern: load
    /// source data into a temp table, delete matching rows from the target,
    /// insert all rows from the temp table, then drop the temp table.  The
    /// only difference is the temp table name prefix, which callers supply
    /// via `temp_prefix`.
    fn upsert_via_temp_table(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
        temp_prefix: &str,
    ) -> DbResult<()> {
        if unique_keys.is_empty() {
            return Err(DbError::ExecutionError(
                "upsert_via_temp_table requires at least one unique key".to_string(),
            ));
        }

        // Validate source_sql is a single statement to prevent injection.
        // Only reject unquoted semicolons so that string literals like
        // WHERE note = 'foo;bar' are not falsely rejected.
        if contains_unquoted_semicolon(source_sql) {
            return Err(DbError::ExecutionError(
                "source_sql must be a single statement".to_string(),
            ));
        }

        let conn = self.lock_conn()?;
        with_transaction(&conn, |conn| {
            let temp_name = format!("{}_{}", temp_prefix, unique_id());
            let quoted_target = quote_qualified(target_table);
            let quoted_temp = quote_ident(&temp_name);

            let create_temp = format!("CREATE TEMP TABLE {} AS {}", quoted_temp, source_sql);
            run_sql(conn, &create_temp)?;

            let join_clause = build_join_condition(unique_keys, &quoted_target, &quoted_temp)?;

            let delete_sql = format!(
                "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {})",
                quoted_target, quoted_temp, join_clause
            );
            run_sql(conn, &delete_sql)?;

            let insert_sql = format!(
                "INSERT INTO {} SELECT * FROM {}",
                quoted_target, quoted_temp
            );
            run_sql(conn, &insert_sql)?;

            let drop_temp = format!("DROP TABLE {}", quoted_temp);
            run_sql(conn, &drop_temp)?;

            Ok(())
        })
    }

    /// Execute SQL synchronously
    fn execute_sync(&self, sql: &str) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        run_sql(&conn, sql)
    }

    /// Execute batch SQL synchronously
    fn execute_batch_sync(&self, sql: &str) -> DbResult<()> {
        let conn = self.lock_conn()?;
        conn.execute_batch(sql)?;
        Ok(())
    }

    /// Query count synchronously
    fn query_count_sync(&self, sql: &str) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        count_rows(&conn, sql)
    }

    /// Check if relation exists synchronously
    fn relation_exists_sync(&self, name: &str) -> DbResult<bool> {
        let conn = self.lock_conn()?;
        relation_exists_on(&conn, name)
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

        let mut stmt = conn.prepare(&limited_sql)?;
        let mut rows: Vec<String> = Vec::new();
        let mut result_rows = stmt.query([])?;
        let column_count = result_rows.as_ref().map_or(0, |r| r.column_count());

        while let Some(row) = result_rows.next()? {
            let mut values: Vec<String> = Vec::new();
            for i in 0..column_count {
                let str_val = extract_cell_as_string(row, i);
                values.push(str_val);
            }
            rows.push(values.join(", "));
        }

        Ok(rows)
    }

    async fn query_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<Vec<String>>> {
        let conn = self.lock_conn()?;
        let limited_sql = format!("SELECT * FROM ({}) AS subq LIMIT {}", sql, limit);

        let mut stmt = conn.prepare(&limited_sql)?;
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut result_rows = stmt.query([])?;
        let column_count = result_rows.as_ref().map_or(0, |r| r.column_count());

        while let Some(row) = result_rows.next()? {
            let mut values: Vec<String> = Vec::with_capacity(column_count);
            for i in 0..column_count {
                values.push(extract_cell_as_string(row, i));
            }
            rows.push(values);
        }

        Ok(rows)
    }

    async fn query_one(&self, sql: &str) -> DbResult<Option<String>> {
        let conn = self.lock_conn()?;

        let mut stmt = conn.prepare(sql)?;
        let mut result_rows = stmt.query([])?;

        let Some(row) = result_rows.next()? else {
            return Ok(None);
        };

        // Try extracting as String first, then numeric types.
        // DuckDB returns an error for NULL values, so we treat "all types fail"
        // as a SQL NULL and return None.
        if let Ok(s) = row.get::<_, String>(0) {
            return Ok(Some(s));
        }
        if let Ok(n) = row.get::<_, i64>(0) {
            return Ok(Some(n.to_string()));
        }
        if let Ok(f) = row.get::<_, f64>(0) {
            return Ok(Some(f.to_string()));
        }
        Ok(None)
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
        table_columns(&conn, table)
    }

    async fn describe_query(&self, sql: &str) -> DbResult<Vec<(String, String)>> {
        let conn = self.lock_conn()?;
        let describe_sql = format!("DESCRIBE SELECT * FROM ({}) AS subq", sql);

        let mut stmt = conn.prepare(&describe_sql)?;
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            let column_type: String = row.get(1)?;
            columns.push((column_name, column_type));
        }

        Ok(columns)
    }

    async fn add_columns(&self, table: &str, columns: &[(String, String)]) -> DbResult<()> {
        let quoted_table = quote_qualified(table);
        for (name, col_type) in columns {
            validate_sql_type(col_type)?;
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
            escape_sql_string(path)
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
                validate_sql_type(typ)?;
                Ok(format!(
                    "CAST({} AS {}) AS {}",
                    quote_ident(col),
                    typ,
                    quote_ident(col)
                ))
            })
            .collect::<DbResult<Vec<String>>>()?;

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

        let escaped_path = escape_sql_string(path);
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
            escape_sql_string(path)
        );

        let mut stmt = conn.prepare(&sql)?;
        let mut schema: Vec<(String, String)> = Vec::new();
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            let column_type: String = row.get(1)?;
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
        self.upsert_via_temp_table(target_table, source_sql, unique_keys, "__ff_merge_source")
    }

    async fn delete_insert(
        &self,
        target_table: &str,
        source_sql: &str,
        unique_keys: &[String],
    ) -> DbResult<()> {
        self.upsert_via_temp_table(
            target_table,
            source_sql,
            unique_keys,
            "__ff_delete_insert_source",
        )
    }
}

// ---------------------------------------------------------------------------
// Transaction-safe snapshot helpers
//
// These free functions perform snapshot sub-operations on a raw `&Connection`,
// allowing the caller to hold the Mutex lock for the entire transaction
// scope.  The public trait methods delegate to these after locking.
// ---------------------------------------------------------------------------

/// Insert new records (present in source but not in active snapshot rows).
fn snapshot_insert_new_on(
    conn: &Connection,
    snapshot_table: &str,
    source_table: &str,
    unique_keys: &[String],
) -> DbResult<usize> {
    let quoted_snap = quote_qualified(snapshot_table);
    let quoted_src = quote_qualified(source_table);

    let join_condition = build_join_condition(unique_keys, "s", "snap")?;

    let source_schema = table_columns(conn, source_table)?;
    let source_cols: Vec<String> = source_schema.iter().map(|(n, _)| quote_ident(n)).collect();

    let scd_id_expr = build_scd_id_expr(unique_keys, Some("s"));

    let prefixed_cols: Vec<String> = source_cols.iter().map(|c| format!("s.{}", c)).collect();
    let Some(raw_first_key) = unique_keys.first() else {
        return Err(DbError::ExecutionError(
            "Snapshot requires at least one unique_key".to_string(),
        ));
    };
    let first_key = quote_ident(raw_first_key);

    let active_snap = format!(
        "SELECT * FROM {} WHERE {} IS NULL",
        quoted_snap, SCD_VALID_TO
    );

    let new_records_sql = format!(
        "SELECT s.* FROM {} s \
         LEFT JOIN ({}) snap \
           ON {} \
         WHERE snap.{} IS NULL",
        quoted_src, active_snap, join_condition, first_key,
    );

    let new_count = count_rows(conn, &new_records_sql)?;

    if new_count > 0 {
        let insert_sql = format!(
            "INSERT INTO {} ({}, {}, {}, {}, {}) \
             SELECT {}, {}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL \
             FROM {} s \
             LEFT JOIN ({}) snap \
               ON {} \
             WHERE snap.{} IS NULL",
            quoted_snap,
            source_cols.join(", "),
            SCD_ID,
            SCD_UPDATED_AT,
            SCD_VALID_FROM,
            SCD_VALID_TO,
            prefixed_cols.join(", "),
            scd_id_expr,
            quoted_src,
            active_snap,
            join_condition,
            first_key,
        );
        run_sql(conn, &insert_sql)?;
    }

    Ok(new_count)
}

/// Update changed records (timestamp or check-column strategy).
fn snapshot_update_changed_on(
    conn: &Connection,
    snapshot_table: &str,
    source_table: &str,
    unique_keys: &[String],
    updated_at_column: Option<&str>,
    check_cols: Option<&[String]>,
) -> DbResult<usize> {
    let quoted_snap = quote_qualified(snapshot_table);
    let quoted_src = quote_qualified(source_table);

    let join_condition = build_join_condition(unique_keys, "s", "snap")?;

    let change_condition = if let Some(updated_at) = updated_at_column {
        format!("s.{} > snap.{}", quote_ident(updated_at), SCD_UPDATED_AT)
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

    let active_snap = format!(
        "SELECT * FROM {} WHERE {} IS NULL",
        quoted_snap, SCD_VALID_TO
    );

    let changed_records_sql = format!(
        "SELECT s.* FROM {} s \
         INNER JOIN ({}) snap \
           ON {} \
         WHERE {}",
        quoted_src, active_snap, join_condition, change_condition,
    );

    let changed_count = count_rows(conn, &changed_records_sql)?;

    if changed_count == 0 {
        return Ok(0);
    }

    let source_schema = table_columns(conn, source_table)?;
    let source_cols: Vec<String> = source_schema.iter().map(|(n, _)| quote_ident(n)).collect();
    let prefixed_cols: Vec<String> = source_cols.iter().map(|c| format!("s.{}", c)).collect();

    let scd_id_expr = build_scd_id_expr(unique_keys, Some("s"));

    let updated_at_expr = if let Some(col) = updated_at_column {
        format!("s.{}", quote_ident(col))
    } else {
        "CURRENT_TIMESTAMP".to_string()
    };

    // Insert new versions before invalidating old ones to capture changing records
    let insert_sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}) \
         SELECT {}, {}, {}, CURRENT_TIMESTAMP, NULL \
         FROM {} s \
         INNER JOIN ({}) snap \
           ON {} \
         WHERE {}",
        quoted_snap,
        source_cols.join(", "),
        SCD_ID,
        SCD_UPDATED_AT,
        SCD_VALID_FROM,
        SCD_VALID_TO,
        prefixed_cols.join(", "),
        scd_id_expr,
        updated_at_expr,
        quoted_src,
        active_snap,
        join_condition,
        change_condition,
    );
    run_sql(conn, &insert_sql)?;

    let key_match_for_newer = build_join_condition(unique_keys, "old", "newer")?;

    let update_sql = format!(
        "UPDATE {} AS old SET {} = CURRENT_TIMESTAMP \
         WHERE old.{} IS NULL \
         AND EXISTS ( \
             SELECT 1 FROM {} newer \
             WHERE {} \
             AND newer.{} > old.{} \
             AND newer.{} IS NULL \
         )",
        quoted_snap,
        SCD_VALID_TO,
        SCD_VALID_TO,
        quoted_snap,
        key_match_for_newer,
        SCD_VALID_FROM,
        SCD_VALID_FROM,
        SCD_VALID_TO,
    );
    run_sql(conn, &update_sql)?;

    Ok(changed_count)
}

/// Invalidate records that were deleted from source (hard deletes).
fn snapshot_invalidate_deleted_on(
    conn: &Connection,
    snapshot_table: &str,
    source_table: &str,
    unique_keys: &[String],
) -> DbResult<usize> {
    let quoted_snap = quote_qualified(snapshot_table);
    let quoted_src = quote_qualified(source_table);

    let join_condition = build_join_condition(unique_keys, "snap", "s")?;

    let Some(raw_first_key) = unique_keys.first() else {
        return Err(DbError::ExecutionError(
            "Snapshot requires at least one unique_key".to_string(),
        ));
    };
    let first_key = quote_ident(raw_first_key);

    let deleted_records_sql = format!(
        "SELECT snap.* FROM {} snap \
         LEFT JOIN {} s ON {} \
         WHERE snap.{} IS NULL AND s.{} IS NULL",
        quoted_snap, quoted_src, join_condition, SCD_VALID_TO, first_key,
    );

    let deleted_count = count_rows(conn, &deleted_records_sql)?;

    if deleted_count == 0 {
        return Ok(0);
    }

    let key_match = build_join_condition(unique_keys, &quoted_snap, "s")?;

    let update_sql = format!(
        "UPDATE {} SET {} = CURRENT_TIMESTAMP \
         WHERE {} IS NULL \
         AND NOT EXISTS ( \
             SELECT 1 FROM {} s \
             WHERE {} \
         )",
        quoted_snap, SCD_VALID_TO, SCD_VALID_TO, quoted_src, key_match,
    );
    run_sql(conn, &update_sql)?;

    Ok(deleted_count)
}

/// Initial snapshot load: create the snapshot table and copy all source rows.
fn snapshot_initial_load(
    conn: &Connection,
    snapshot_table: &str,
    source_table: &str,
    unique_keys: &[String],
    updated_at_column: Option<&str>,
) -> DbResult<SnapshotResult> {
    let source_schema = table_columns(conn, source_table)?;
    let quoted_snap = quote_qualified(snapshot_table);
    let quoted_src = quote_qualified(source_table);

    let mut columns: Vec<String> = source_schema
        .iter()
        .map(|(name, dtype)| format!("{} {}", quote_ident(name), dtype))
        .collect();

    columns.push(format!("{} VARCHAR", quote_ident(SCD_ID)));
    columns.push(format!("{} TIMESTAMP", quote_ident(SCD_UPDATED_AT)));
    columns.push(format!("{} TIMESTAMP", quote_ident(SCD_VALID_FROM)));
    columns.push(format!("{} TIMESTAMP", quote_ident(SCD_VALID_TO)));

    let create_sql = format!("CREATE TABLE {} ({})", quoted_snap, columns.join(", "));
    run_sql(conn, &create_sql)?;

    let source_cols: Vec<String> = source_schema.iter().map(|(n, _)| quote_ident(n)).collect();

    let updated_at_expr = if let Some(col) = updated_at_column {
        format!("CAST({} AS TIMESTAMP)", quote_ident(col))
    } else {
        "CURRENT_TIMESTAMP".to_string()
    };

    let scd_id_expr = build_scd_id_expr(unique_keys, None);

    let insert_sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}) \
         SELECT {}, {}, {}, CURRENT_TIMESTAMP, NULL \
         FROM {}",
        quoted_snap,
        source_cols.join(", "),
        quote_ident(SCD_ID),
        quote_ident(SCD_UPDATED_AT),
        quote_ident(SCD_VALID_FROM),
        quote_ident(SCD_VALID_TO),
        source_cols.join(", "),
        scd_id_expr,
        updated_at_expr,
        quoted_src
    );
    run_sql(conn, &insert_sql)?;

    let new_count = count_rows(conn, &format!("SELECT * FROM {}", quoted_snap))?;

    Ok(SnapshotResult {
        new_records: new_count,
        updated_records: 0,
        deleted_records: 0,
    })
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

        // Hold the lock for the ENTIRE transaction to prevent interleaved operations.
        let conn = self.lock_conn()?;

        let snapshot_exists = relation_exists_on(&conn, snapshot_table)?;

        with_transaction(&conn, |conn| {
            if !snapshot_exists {
                snapshot_initial_load(
                    conn,
                    snapshot_table,
                    source_table,
                    unique_keys,
                    updated_at_column,
                )
            } else {
                let new_records =
                    snapshot_insert_new_on(conn, snapshot_table, source_table, unique_keys)?;
                let updated_records = snapshot_update_changed_on(
                    conn,
                    snapshot_table,
                    source_table,
                    unique_keys,
                    updated_at_column,
                    check_cols,
                )?;
                let deleted_records = if invalidate_hard_deletes {
                    snapshot_invalidate_deleted_on(conn, snapshot_table, source_table, unique_keys)?
                } else {
                    0
                };
                Ok(SnapshotResult {
                    new_records,
                    updated_records,
                    deleted_records,
                })
            }
        })
    }

    async fn snapshot_insert_new(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        snapshot_insert_new_on(&conn, snapshot_table, source_table, unique_keys)
    }

    async fn snapshot_update_changed(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
        updated_at_column: Option<&str>,
        check_cols: Option<&[String]>,
    ) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        snapshot_update_changed_on(
            &conn,
            snapshot_table,
            source_table,
            unique_keys,
            updated_at_column,
            check_cols,
        )
    }

    async fn snapshot_invalidate_deleted(
        &self,
        snapshot_table: &str,
        source_table: &str,
        unique_keys: &[String],
    ) -> DbResult<usize> {
        let conn = self.lock_conn()?;
        snapshot_invalidate_deleted_on(&conn, snapshot_table, source_table, unique_keys)
    }
}

#[async_trait]
impl DatabaseFunction for DuckDbBackend {
    async fn deploy_function(&self, create_sql: &str) -> DbResult<()> {
        self.execute_sync(create_sql)?;
        Ok(())
    }

    async fn drop_function(&self, drop_sql: &str) -> DbResult<()> {
        self.execute_sync(drop_sql)?;
        Ok(())
    }

    async fn function_exists(&self, name: &str) -> DbResult<bool> {
        let conn = self.lock_conn()?;
        let sql = "SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = ? AND function_type = 'macro'";
        let count: i64 = conn.query_row(sql, duckdb::params![name], |row| row.get(0))?;
        Ok(count > 0)
    }

    async fn list_user_functions(&self) -> DbResult<Vec<String>> {
        let conn = self.lock_conn()?;
        let sql = "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'macro' ORDER BY function_name";
        let mut stmt = conn.prepare(sql)?;
        let mut names = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            names.push(name);
        }
        Ok(names)
    }
}

/// Build a SQL join condition from a list of key columns with table alias prefixes.
///
/// Produces clauses like `left.key1 = right.key1 AND left.key2 = right.key2`.
fn build_join_condition(keys: &[String], left_alias: &str, right_alias: &str) -> DbResult<String> {
    if keys.is_empty() {
        return Err(DbError::ExecutionError(
            "build_join_condition requires at least one key".to_string(),
        ));
    }
    Ok(keys
        .iter()
        .map(|k| {
            let qk = quote_ident(k);
            format!("{}.{} = {}.{}", left_alias, qk, right_alias, qk)
        })
        .collect::<Vec<_>>()
        .join(" AND "))
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
