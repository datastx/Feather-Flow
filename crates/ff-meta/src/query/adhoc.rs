//! Ad-hoc query execution and table introspection helpers.
//!
//! Returns plain Rust types so callers don't need a direct `duckdb` dependency.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;

/// Result of executing an ad-hoc SQL query against the meta database.
pub struct QueryResult {
    /// Column names from the result set.
    pub columns: Vec<String>,
    /// Rows of string-coerced values.
    pub rows: Vec<Vec<String>>,
}

/// Execute an ad-hoc SQL query and return all results as strings.
///
/// DuckDB 1.4 panics on `stmt.column_count()` before execution, so we
/// collect all rows via `query_map` first, then read column metadata.
pub fn execute_query(conn: &Connection, sql: &str) -> MetaResult<QueryResult> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| MetaError::QueryError(format!("prepare failed: {e}")))?;

    let raw_rows: Vec<Vec<String>> = match stmt.query_map([], |row| {
        let col_count = row.as_ref().column_count();
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            vals.push(get_column_as_string(row, i));
        }
        Ok(vals)
    }) {
        Ok(mapped) => {
            let mut collected = Vec::new();
            for row_result in mapped {
                let row =
                    row_result.map_err(|e| MetaError::QueryError(format!("row error: {e}")))?;
                collected.push(row);
            }
            collected
        }
        Err(e) => {
            return Err(MetaError::QueryError(format!("query failed: {e}")));
        }
    };

    let column_count = stmt.column_count();
    let column_names: Vec<String> = (0..column_count)
        .map(|i| {
            stmt.column_name(i)
                .map_or("?".to_string(), |v| v.to_string())
        })
        .collect();

    Ok(QueryResult {
        columns: column_names,
        rows: raw_rows,
    })
}

/// List all tables in the `ff_meta` schema.
pub fn list_tables(conn: &Connection) -> MetaResult<Vec<String>> {
    let result = execute_query(
        conn,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'ff_meta' \
         ORDER BY table_name",
    )?;
    Ok(result.rows.into_iter().map(|r| r[0].clone()).collect())
}

/// Get the row count for a table in the `ff_meta` schema.
pub fn table_row_count(conn: &Connection, table_name: &str) -> MetaResult<i64> {
    let count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM ff_meta.{table_name}"),
            [],
            |row| row.get(0),
        )
        .map_err(|e| MetaError::QueryError(format!("count failed for {table_name}: {e}")))?;
    Ok(count)
}

/// Read a column value as a String, trying multiple DuckDB types.
///
/// DuckDB integer columns return `None` for `Option<String>`, so we try
/// String → i64 → f64 → bool → "null".
pub fn get_column_as_string(row: &duckdb::Row<'_>, idx: usize) -> String {
    if let Ok(Some(s)) = row.get::<_, Option<String>>(idx) {
        return s;
    }
    if let Ok(Some(n)) = row.get::<_, Option<i64>>(idx) {
        return n.to_string();
    }
    if let Ok(Some(f)) = row.get::<_, Option<f64>>(idx) {
        return f.to_string();
    }
    if let Ok(Some(b)) = row.get::<_, Option<bool>>(idx) {
        return b.to_string();
    }
    "null".to_string()
}

#[cfg(test)]
#[path = "adhoc_test.rs"]
mod tests;
