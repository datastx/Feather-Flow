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
    let stmts = [
        // Leaf-level children of model_columns
        "DELETE FROM ff_meta.model_column_constraints WHERE column_id IN (SELECT column_id FROM ff_meta.model_columns WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?))",
        "DELETE FROM ff_meta.model_column_references WHERE column_id IN (SELECT column_id FROM ff_meta.model_columns WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?))",
        // Execution state children
        "DELETE FROM ff_meta.model_run_config WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_run_input_checksums WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_run_state WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        // Analysis data
        "DELETE FROM ff_meta.column_lineage WHERE target_model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.schema_mismatches WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.diagnostics WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        // Tests
        "DELETE FROM ff_meta.tests WHERE project_id = ?",
        "DELETE FROM ff_meta.singular_tests WHERE project_id = ?",
        // Model children
        "DELETE FROM ff_meta.model_columns WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_config WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_hooks WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_tags WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_meta WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_dependencies WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        "DELETE FROM ff_meta.model_external_dependencies WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
        // Source children
        "DELETE FROM ff_meta.source_columns WHERE source_table_id IN (SELECT source_table_id FROM ff_meta.source_tables WHERE source_id IN (SELECT source_id FROM ff_meta.sources WHERE project_id = ?))",
        "DELETE FROM ff_meta.source_tables WHERE source_id IN (SELECT source_id FROM ff_meta.sources WHERE project_id = ?)",
        "DELETE FROM ff_meta.source_tags WHERE source_id IN (SELECT source_id FROM ff_meta.sources WHERE project_id = ?)",
        // Function children
        "DELETE FROM ff_meta.function_args WHERE function_id IN (SELECT function_id FROM ff_meta.functions WHERE project_id = ?)",
        "DELETE FROM ff_meta.function_return_columns WHERE function_id IN (SELECT function_id FROM ff_meta.functions WHERE project_id = ?)",
        // Seed children
        "DELETE FROM ff_meta.seed_column_types WHERE seed_id IN (SELECT seed_id FROM ff_meta.seeds WHERE project_id = ?)",
        // Parent entity tables
        "DELETE FROM ff_meta.models WHERE project_id = ?",
        "DELETE FROM ff_meta.sources WHERE project_id = ?",
        "DELETE FROM ff_meta.functions WHERE project_id = ?",
        "DELETE FROM ff_meta.seeds WHERE project_id = ?",
    ];

    for stmt in &stmts {
        conn.execute(stmt, duckdb::params![project_id])
            .map_err(|e| MetaError::PopulationError(format!("clear_entity_data: {e}")))?;
    }
    Ok(())
}
