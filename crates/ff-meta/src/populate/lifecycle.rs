//! Compilation run lifecycle: begin, complete, and clear.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;

/// Begin a new population cycle.
///
/// Creates a compilation_runs row with status "running" and clears all
/// existing entity data (models, sources, functions, seeds, tests) for the
/// given project so that fresh data can be re-populated.
///
/// Returns the new `run_id`.
pub fn begin_population(
    conn: &Connection,
    project_id: i64,
    run_type: &str,
    node_selector: Option<&str>,
) -> MetaResult<i64> {
    conn.execute(
        "INSERT INTO ff_meta.compilation_runs (project_id, run_type, node_selector) VALUES (?, ?, ?)",
        duckdb::params![project_id, run_type, node_selector],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert compilation_runs: {e}")))?;

    let run_id: i64 = conn
        .query_row(
            "SELECT run_id FROM ff_meta.compilation_runs WHERE project_id = ? ORDER BY run_id DESC LIMIT 1",
            duckdb::params![project_id],
            |row| row.get(0),
        )
        .map_err(|e| MetaError::PopulationError(format!("select run_id: {e}")))?;

    clear_entity_data(conn, project_id)?;

    Ok(run_id)
}

/// Mark a compilation run as completed (success or error).
pub fn complete_population(conn: &Connection, run_id: i64, status: &str) -> MetaResult<()> {
    conn.execute(
        "UPDATE ff_meta.compilation_runs SET status = ?, completed_at = now() WHERE run_id = ?",
        duckdb::params![status, run_id],
    )
    .map_err(|e| MetaError::PopulationError(format!("update compilation_runs: {e}")))?;
    Ok(())
}

/// Clear all entity data for a project (models, sources, functions, seeds, tests)
/// while preserving compilation_runs and project_hooks/vars.
fn clear_entity_data(conn: &Connection, project_id: i64) -> MetaResult<()> {
    for stmt in crate::connection::ENTITY_DELETE_STMTS {
        conn.execute(stmt, duckdb::params![project_id])
            .map_err(|e| MetaError::PopulationError(format!("clear_entity_data: {e}")))?;
    }
    Ok(())
}
