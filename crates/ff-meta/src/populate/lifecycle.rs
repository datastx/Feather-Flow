//! Compilation run lifecycle: begin, complete, and clear.

use crate::error::{MetaResult, MetaResultExt};
use duckdb::Connection;

/// The type of population run being performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunType {
    Compile,
    Validate,
    Run,
    Analyze,
    Rules,
}

impl RunType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Compile => "compile",
            Self::Validate => "validate",
            Self::Run => "run",
            Self::Analyze => "analyze",
            Self::Rules => "rules",
        }
    }
}

/// Terminal status of a population run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopulationStatus {
    Success,
    Error,
}

impl PopulationStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

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
    run_type: RunType,
    node_selector: Option<&str>,
) -> MetaResult<i64> {
    conn.execute(
        "INSERT INTO ff_meta.compilation_runs (project_id, run_type, node_selector) VALUES (?, ?, ?)",
        duckdb::params![project_id, run_type.as_str(), node_selector],
    )
    .populate_context("insert compilation_runs")?;

    let run_id: i64 = conn
        .query_row(
            "SELECT run_id FROM ff_meta.compilation_runs WHERE project_id = ? ORDER BY run_id DESC LIMIT 1",
            duckdb::params![project_id],
            |row| row.get(0),
        )
        .populate_context("select run_id")?;

    clear_entity_data(conn, project_id)?;

    Ok(run_id)
}

/// Mark a compilation run as completed (success or error).
pub fn complete_population(
    conn: &Connection,
    run_id: i64,
    status: PopulationStatus,
) -> MetaResult<()> {
    conn.execute(
        "UPDATE ff_meta.compilation_runs SET status = ?, completed_at = now() WHERE run_id = ?",
        duckdb::params![status.as_str(), run_id],
    )
    .populate_context("update compilation_runs")?;
    Ok(())
}

/// Clear all entity data for a project (models, sources, functions, seeds, tests)
/// while preserving compilation_runs and project_hooks/vars.
fn clear_entity_data(conn: &Connection, project_id: i64) -> MetaResult<()> {
    for stmt in crate::connection::ENTITY_DELETE_STMTS {
        conn.execute(stmt, duckdb::params![project_id])
            .populate_context("clear_entity_data")?;
    }
    Ok(())
}
