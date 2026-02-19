//! Shared helpers for reading DuckDB row columns as strings.

use crate::error::{MetaError, MetaResult};

/// Read a column value as a String, trying multiple DuckDB types.
///
/// DuckDB integer columns return `None` for `Option<String>`, so we try
/// String -> i64 -> f64 -> bool -> "null".
pub(crate) fn get_column_as_string(row: &duckdb::Row<'_>, idx: usize) -> String {
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

/// Execute a prepared statement and collect all results as strings.
///
/// DuckDB 1.4 panics on `stmt.column_count()` before execution, so we
/// collect all rows via `query_map` first, then read column metadata.
///
/// Returns `(column_names, rows)`.
pub(crate) fn execute_and_collect(
    stmt: &mut duckdb::Statement,
) -> MetaResult<(Vec<String>, Vec<Vec<String>>)> {
    let raw_rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let col_count = row.as_ref().column_count();
            Ok((0..col_count)
                .map(|i| get_column_as_string(row, i))
                .collect())
        })
        .map_err(|e| MetaError::QueryError(format!("query failed: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MetaError::QueryError(format!("row error: {e}")))?;

    let column_count = stmt.column_count();
    let column_names: Vec<String> = (0..column_count)
        .map(|i| {
            stmt.column_name(i)
                .map_or("?".to_string(), |v| v.to_string())
        })
        .collect();

    Ok((column_names, raw_rows))
}
