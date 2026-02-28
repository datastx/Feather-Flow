//! DuckDB database backend implementation

use crate::error::{DbError, DbResult};
use crate::traits::{
    CsvLoadOptions, DatabaseCore, DatabaseCsv, DatabaseFunction, DatabaseIncremental,
    DatabaseSchema,
};
use async_trait::async_trait;
use duckdb::Connection;
use ff_core::sql_utils::{escape_sql_string, quote_ident, quote_qualified};
use std::path::Path;
use std::sync::Mutex;

/// Check whether a SQL string contains a semicolon outside of quoted contexts.
///
/// Walks the string tracking both single-quoted literals (`'foo;bar'`) and
/// double-quoted identifiers (`"my;table"`) so that semicolons inside either
/// are not treated as statement terminators. Escaped quotes (`''` / `""`)
/// inside their respective contexts are handled correctly.
fn contains_unquoted_semicolon(sql: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if in_single => {
                // Two consecutive single quotes inside a literal = escaped quote
                if chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    in_single = false;
                }
            }
            '\'' if !in_double => in_single = true,
            '"' if in_double => {
                // Two consecutive double quotes inside an identifier = escaped quote
                if chars.peek() == Some(&'"') {
                    chars.next();
                } else {
                    in_double = false;
                }
            }
            '"' if !in_single => in_double = true,
            ';' if !in_single && !in_double => return true,
            _ => {}
        }
    }
    false
}

/// Truncate a SQL string for inclusion in error messages.
///
/// Keeps at most 200 characters (respecting UTF-8 boundaries) and appends `...`.
fn truncate_sql_for_error(sql: &str) -> String {
    if sql.len() > 200 {
        let end = sql
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| i >= 200)
            .unwrap_or(sql.len());
        format!("{}...", &sql[..end])
    } else {
        sql.to_string()
    }
}

/// Execute SQL on an already-locked connection, returning affected row count.
///
/// Used by transaction-scoped operations that hold the Mutex for their
/// entire duration, avoiding per-statement lock/unlock overhead.
fn run_sql(conn: &Connection, sql: &str) -> DbResult<usize> {
    conn.execute(sql, []).map_err(|e| {
        let truncated = truncate_sql_for_error(sql);
        DbError::ExecutionFailed {
            context: truncated,
            source: e,
        }
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
    handle_commit_result(conn, &result)?;
    result
}

/// Commit or rollback depending on the body result.
fn handle_commit_result<T>(conn: &Connection, result: &DbResult<T>) -> DbResult<()> {
    match result {
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
    Ok(())
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
///
/// NOTE: Near-duplicate exists in `ff-meta/src/row_helpers.rs` (uses
/// `Option<T>` wrappers).  They live in separate crates because `ff-meta`
/// does not depend on `ff-db`, and `ff-core` should not depend on `duckdb`.
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
    "null".to_string()
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
    "UHUGEINT",
    "UUID",
    "VARCHAR",
    "BITSTRING",
    "TIMETZ",
    "TIMESTAMP_TZ",
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

impl std::fmt::Debug for DuckDbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbBackend")
            .field("conn", &"<Mutex<Connection>>")
            .finish()
    }
}

impl DuckDbBackend {
    /// Create a new in-memory DuckDB connection
    pub fn in_memory() -> DbResult<Self> {
        let conn = Connection::open_in_memory().map_err(|e| DbError::ConnectionFailed {
            message: "in-memory".to_string(),
            source: e,
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create a new DuckDB connection from a file path
    fn from_path(path: &Path) -> DbResult<Self> {
        let conn = Connection::open(path).map_err(|e| DbError::ConnectionFailed {
            message: path.display().to_string(),
            source: e,
        })?;
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

    /// Derive the DuckDB catalog name from a database path.
    ///
    /// DuckDB names its default catalog after the file stem: `dev.duckdb` →
    /// `"dev"`, `:memory:` → `"memory"`. This helper mirrors that logic so
    /// the qualification map (compile time) and the live connection (runtime)
    /// agree on the catalog name.
    pub fn catalog_name_for_path(path: &str) -> String {
        if path == ":memory:" {
            "memory".to_string()
        } else {
            Path::new(path)
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "main".to_string())
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
        conn.execute_batch(sql)
            .map_err(|e| DbError::ExecutionFailed {
                context: truncate_sql_for_error(sql),
                source: e,
            })?;
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

    /// Shared implementation for `query_sample_rows` and `query_rows`
    fn query_rows_raw(&self, sql: &str, limit: usize) -> DbResult<Vec<Vec<String>>> {
        let conn = self.lock_conn()?;
        let limited_sql = format!("SELECT * FROM ({}) AS subq LIMIT {}", sql, limit);

        let mut stmt = conn
            .prepare(&limited_sql)
            .map_err(|e| DbError::ExecutionFailed {
                context: truncate_sql_for_error(&limited_sql),
                source: e,
            })?;
        let mut rows = Vec::new();
        let mut result_rows = stmt.query([])?;
        let column_count = result_rows.as_ref().map_or(0, |r| r.column_count());

        while let Some(row) = result_rows.next()? {
            let values: Vec<String> = (0..column_count)
                .map(|i| extract_cell_as_string(row, i))
                .collect();
            rows.push(values);
        }

        Ok(rows)
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
        let raw = self.query_rows_raw(sql, limit)?;
        Ok(raw.into_iter().map(|row| row.join(", ")).collect())
    }

    async fn query_rows(&self, sql: &str, limit: usize) -> DbResult<Vec<Vec<String>>> {
        self.query_rows_raw(sql, limit)
    }

    async fn query_one(&self, sql: &str) -> DbResult<Option<String>> {
        let conn = self.lock_conn()?;

        let mut stmt = conn.prepare(sql).map_err(|e| DbError::ExecutionFailed {
            context: truncate_sql_for_error(sql),
            source: e,
        })?;
        let mut result_rows = stmt.query([]).map_err(|e| DbError::ExecutionFailed {
            context: truncate_sql_for_error(sql),
            source: e,
        })?;

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
            Err(e) if e.is_wrong_relation_type() => {}
            Err(e) => return Err(e),
        }
        match self.execute_sync(&format!("DROP TABLE IF EXISTS {}", quoted)) {
            Ok(_) => {}
            Err(e) if e.is_wrong_relation_type() => {}
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

#[async_trait]
impl DatabaseFunction for DuckDbBackend {
    async fn deploy_function(&self, create_sql: &str) -> DbResult<()> {
        let trimmed = create_sql.trim_start();
        if !trimmed.to_uppercase().starts_with("CREATE") {
            return Err(DbError::ExecutionError(
                "deploy_function: SQL must start with CREATE".to_string(),
            ));
        }
        if contains_unquoted_semicolon(create_sql) {
            return Err(DbError::ExecutionError(
                "deploy_function: SQL must not contain multiple statements".to_string(),
            ));
        }
        self.execute_sync(create_sql)?;
        Ok(())
    }

    async fn drop_function(&self, drop_sql: &str) -> DbResult<()> {
        let trimmed = drop_sql.trim_start();
        if !trimmed.to_uppercase().starts_with("DROP") {
            return Err(DbError::ExecutionError(
                "drop_function: SQL must start with DROP".to_string(),
            ));
        }
        if contains_unquoted_semicolon(drop_sql) {
            return Err(DbError::ExecutionError(
                "drop_function: SQL must not contain multiple statements".to_string(),
            ));
        }
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
#[path = "duckdb_test.rs"]
mod tests;
