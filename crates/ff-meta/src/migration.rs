//! Schema migration runner for the meta database.
//!
//! Tracks applied migration versions in `ff_meta.schema_version` and runs any
//! unapplied migrations on each open.

use crate::ddl::MIGRATIONS;
use crate::error::{MetaError, MetaResult};
use duckdb::Connection;

/// Ensure the `ff_meta` schema and `schema_version` table exist.
fn ensure_version_table(conn: &Connection) -> MetaResult<()> {
    conn.execute_batch(
        "CREATE SCHEMA IF NOT EXISTS ff_meta;
         CREATE TABLE IF NOT EXISTS ff_meta.schema_version (
             version    INTEGER NOT NULL,
             applied_at TIMESTAMP NOT NULL DEFAULT now()
         );",
    )
    .map_err(|e| {
        MetaError::MigrationError(format!("failed to create schema_version table: {e}"))
    })?;
    Ok(())
}

/// Return the highest applied migration version, or 0 if none.
fn current_version(conn: &Connection) -> MetaResult<i32> {
    let version: i32 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM ff_meta.schema_version",
            [],
            |row| row.get(0),
        )
        .map_err(|e| MetaError::MigrationError(format!("failed to read schema version: {e}")))?;
    Ok(version)
}

/// Run all unapplied migrations against `conn`.
///
/// Each migration runs inside its own implicit transaction (DuckDB auto-commit
/// for `execute_batch`). The version number is recorded in `schema_version`
/// after successful execution.
pub fn run_migrations(conn: &Connection) -> MetaResult<()> {
    ensure_version_table(conn)?;
    let current = current_version(conn)?;

    for migration in MIGRATIONS {
        if migration.version <= current {
            continue;
        }
        log::debug!("Applying meta migration v{:03}", migration.version);

        conn.execute_batch(migration.sql).map_err(|e| {
            MetaError::MigrationError(format!("migration v{:03} failed: {e}", migration.version))
        })?;

        conn.execute(
            "INSERT INTO ff_meta.schema_version (version) VALUES (?)",
            duckdb::params![migration.version],
        )
        .map_err(|e| {
            MetaError::MigrationError(format!(
                "failed to record migration v{:03}: {e}",
                migration.version
            ))
        })?;
    }
    Ok(())
}
