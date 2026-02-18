//! Smart build queries: detect whether a model needs re-execution.
//!
//! These functions query the `model_latest_state` view and
//! `model_run_input_checksums` table to determine if a model's SQL,
//! schema, or upstream inputs have changed since its last successful run.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;
use std::collections::HashMap;

/// Check whether a model has been modified since its last successful run.
///
/// Returns `true` (needs rebuild) if:
/// - No previous successful run exists for this model
/// - The SQL checksum has changed
/// - The schema checksum has changed
/// - Any upstream input checksum has changed
/// - An upstream dependency was added or removed
pub fn is_model_modified(
    conn: &Connection,
    model_name: &str,
    current_sql_checksum: &str,
    current_schema_checksum: Option<&str>,
    current_input_checksums: &HashMap<String, String>,
) -> MetaResult<bool> {
    // Find the model's latest successful run state by joining through models table
    let state = conn.query_row(
        "SELECT mls.sql_checksum, mls.schema_checksum, mls.run_id, mls.model_id
             FROM ff_meta.model_latest_state mls
             JOIN ff_meta.models m ON m.model_id = mls.model_id
             WHERE m.name = ?",
        duckdb::params![model_name],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    );

    let (prev_sql_checksum, prev_schema_checksum, run_id, model_id) = match state {
        Ok(s) => s,
        Err(duckdb::Error::QueryReturnedNoRows) => return Ok(true),
        Err(e) => {
            return Err(MetaError::QueryError(format!(
                "query model_latest_state: {e}"
            )))
        }
    };

    // Check SQL checksum
    match prev_sql_checksum {
        Some(ref prev) if prev != current_sql_checksum => return Ok(true),
        None => return Ok(true),
        _ => {}
    }

    // Check schema checksum
    match (&prev_schema_checksum, current_schema_checksum) {
        (Some(old), Some(new)) if old != new => return Ok(true),
        (None, Some(_)) => return Ok(true),
        (Some(_), None) => return Ok(true),
        _ => {}
    }

    // Check input checksums from model_run_input_checksums
    let prev_inputs = load_input_checksums(conn, model_id, run_id)?;

    // Check for changed or new upstream checksums
    for (input_name, current_checksum) in current_input_checksums {
        match prev_inputs.get(input_name) {
            Some(old_checksum) if old_checksum != current_checksum => return Ok(true),
            None => return Ok(true),
            _ => {}
        }
    }

    // Check for removed upstream dependencies
    for input_name in prev_inputs.keys() {
        if !current_input_checksums.contains_key(input_name) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Load input checksums from a specific run, keyed by upstream model name.
fn load_input_checksums(
    conn: &Connection,
    model_id: i64,
    run_id: i64,
) -> MetaResult<HashMap<String, String>> {
    let mut stmt = conn
        .prepare(
            "SELECT m.name, mric.checksum
             FROM ff_meta.model_run_input_checksums mric
             JOIN ff_meta.models m ON m.model_id = mric.upstream_model_id
             WHERE mric.model_id = ? AND mric.run_id = ?",
        )
        .map_err(|e| MetaError::QueryError(format!("prepare input_checksums: {e}")))?;

    let rows: Vec<(String, String)> = stmt
        .query_map(duckdb::params![model_id, run_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| MetaError::QueryError(format!("query input_checksums: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MetaError::QueryError(format!("collect input_checksums: {e}")))?;

    Ok(rows.into_iter().collect())
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
