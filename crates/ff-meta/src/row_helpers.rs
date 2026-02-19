//! Shared helper for reading DuckDB row columns as strings.

/// Read a column value as a String, trying multiple DuckDB types.
///
/// DuckDB integer columns return `None` for `Option<String>`, so we try
/// String -> i64 -> f64 -> bool -> "null".
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
