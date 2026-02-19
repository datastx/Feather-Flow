//! Meta database connection wrapper.
//!
//! [`MetaDb`] owns a DuckDB [`Connection`] and provides helpers for opening,
//! migrating, and transacting against the meta database.

use crate::error::{MetaError, MetaResult};
use crate::migration::run_migrations;
use duckdb::Connection;
use std::path::Path;

/// DELETE statements that clear all entity data (models, sources, functions,
/// seeds, tests) for a given project_id.  Shared between [`MetaDb::clear_models`],
/// [`MetaDb::clear_project_data`], and [`crate::populate::lifecycle::clear_entity_data`].
///
/// Ordered deepest children first to respect FK dependencies.
pub(crate) const ENTITY_DELETE_STMTS: &[&str] = &[
    "DELETE FROM ff_meta.model_column_constraints WHERE column_id IN (SELECT column_id FROM ff_meta.model_columns WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?))",
    "DELETE FROM ff_meta.model_column_references WHERE column_id IN (SELECT column_id FROM ff_meta.model_columns WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?))",
    "DELETE FROM ff_meta.model_run_config WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_run_input_checksums WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_run_state WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.column_lineage WHERE target_model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.schema_mismatches WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.diagnostics WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.tests WHERE project_id = ?",
    "DELETE FROM ff_meta.singular_tests WHERE project_id = ?",
    "DELETE FROM ff_meta.model_columns WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_config WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_hooks WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_tags WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_meta WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_dependencies WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.model_external_dependencies WHERE model_id IN (SELECT model_id FROM ff_meta.models WHERE project_id = ?)",
    "DELETE FROM ff_meta.source_columns WHERE source_table_id IN (SELECT source_table_id FROM ff_meta.source_tables WHERE source_id IN (SELECT source_id FROM ff_meta.sources WHERE project_id = ?))",
    "DELETE FROM ff_meta.source_tables WHERE source_id IN (SELECT source_id FROM ff_meta.sources WHERE project_id = ?)",
    "DELETE FROM ff_meta.source_tags WHERE source_id IN (SELECT source_id FROM ff_meta.sources WHERE project_id = ?)",
    "DELETE FROM ff_meta.function_args WHERE function_id IN (SELECT function_id FROM ff_meta.functions WHERE project_id = ?)",
    "DELETE FROM ff_meta.function_return_columns WHERE function_id IN (SELECT function_id FROM ff_meta.functions WHERE project_id = ?)",
    "DELETE FROM ff_meta.seed_column_types WHERE seed_id IN (SELECT seed_id FROM ff_meta.seeds WHERE project_id = ?)",
    "DELETE FROM ff_meta.models WHERE project_id = ?",
    "DELETE FROM ff_meta.sources WHERE project_id = ?",
    "DELETE FROM ff_meta.functions WHERE project_id = ?",
    "DELETE FROM ff_meta.seeds WHERE project_id = ?",
];

/// Wrapper around a DuckDB connection to `target/meta.duckdb`.
///
/// Single-threaded — no `Mutex` needed because meta population is sequential.
pub struct MetaDb {
    conn: Connection,
}

impl MetaDb {
    /// Open (or create) the meta database at `path` and run pending migrations.
    pub fn open(path: &Path) -> MetaResult<Self> {
        let conn = Connection::open(path)
            .map_err(|e| MetaError::ConnectionError(format!("{e}: {}", path.display())))?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    /// Create an in-memory meta database with all migrations applied.
    ///
    /// Useful for unit tests that don't need persistence.
    pub fn open_memory() -> MetaResult<Self> {
        let conn =
            Connection::open_in_memory().map_err(|e| MetaError::ConnectionError(e.to_string()))?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    /// Borrow the underlying DuckDB connection.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Execute `body` within a `BEGIN` / `COMMIT` transaction, rolling back on
    /// error.
    ///
    /// Mirrors the `with_transaction` pattern in `ff-db`.
    pub fn transaction<F, T>(&self, body: F) -> MetaResult<T>
    where
        F: FnOnce(&Connection) -> MetaResult<T>,
    {
        self.conn
            .execute_batch("BEGIN TRANSACTION")
            .map_err(|e| MetaError::TransactionError(format!("BEGIN failed: {e}")))?;

        let result = body(&self.conn);

        match &result {
            Ok(_) => {
                if let Err(commit_err) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetaError::TransactionError(format!(
                        "COMMIT failed: {commit_err}"
                    )));
                }
            }
            Err(_) => {
                let _ = self.conn.execute_batch("ROLLBACK");
            }
        }
        result
    }

    /// Delete all data for a project, respecting FK ordering.
    ///
    /// DuckDB does not support `ON DELETE CASCADE`, so we delete child tables
    /// first in reverse-dependency order.
    pub fn clear_project_data(&self, project_id: i64) -> MetaResult<()> {
        for stmt in ENTITY_DELETE_STMTS {
            self.conn
                .execute(stmt, duckdb::params![project_id])
                .map_err(|e| MetaError::QueryError(format!("clear_project_data failed: {e}")))?;
        }

        let project_stmts = [
            "DELETE FROM ff_meta.rule_violations WHERE run_id IN (SELECT run_id FROM ff_meta.compilation_runs WHERE project_id = ?)",
            "DELETE FROM ff_meta.diagnostics WHERE run_id IN (SELECT run_id FROM ff_meta.compilation_runs WHERE project_id = ?)",
            "DELETE FROM ff_meta.schema_mismatches WHERE run_id IN (SELECT run_id FROM ff_meta.compilation_runs WHERE project_id = ?)",
            "DELETE FROM ff_meta.compilation_runs WHERE project_id = ?",
            "DELETE FROM ff_meta.project_hooks WHERE project_id = ?",
            "DELETE FROM ff_meta.project_vars WHERE project_id = ?",
        ];
        for stmt in &project_stmts {
            self.conn
                .execute(stmt, duckdb::params![project_id])
                .map_err(|e| MetaError::QueryError(format!("clear_project_data failed: {e}")))?;
        }
        Ok(())
    }

    /// Delete all models and their child data for a project, preserving the
    /// project row and compilation_runs history.
    ///
    /// Used in the "clear-and-repopulate" pattern for phases 1-3.
    pub fn clear_models(&self, project_id: i64) -> MetaResult<()> {
        for stmt in ENTITY_DELETE_STMTS {
            self.conn
                .execute(stmt, duckdb::params![project_id])
                .map_err(|e| MetaError::QueryError(format!("clear_models failed: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "connection_test.rs"]
mod tests;
