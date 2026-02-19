//! Populate compilation-phase data: compiled SQL, dependencies.

use crate::error::{MetaResult, MetaResultExt};
use duckdb::Connection;

/// Update a model with its compiled SQL output.
pub fn update_model_compiled(
    conn: &Connection,
    model_id: i64,
    compiled_sql: &str,
    compiled_path: &str,
    checksum: &str,
) -> MetaResult<()> {
    conn.execute(
        "UPDATE ff_meta.models SET compiled_sql = ?, compiled_path = ?, sql_checksum = ? WHERE model_id = ?",
        duckdb::params![compiled_sql, compiled_path, checksum, model_id],
    )
    .populate_context("update model compiled")?;
    Ok(())
}

/// Insert model-to-model dependencies discovered during compilation.
pub fn populate_dependencies(
    conn: &Connection,
    model_id: i64,
    depends_on_model_ids: &[i64],
) -> MetaResult<()> {
    for &dep_id in depends_on_model_ids {
        conn.execute(
            "INSERT INTO ff_meta.model_dependencies (model_id, depends_on_model_id) VALUES (?, ?)",
            duckdb::params![model_id, dep_id],
        )
        .populate_context("insert model_dependencies")?;
    }
    Ok(())
}

/// Insert external table dependencies discovered during compilation.
pub fn populate_external_dependencies(
    conn: &Connection,
    model_id: i64,
    table_names: &[&str],
) -> MetaResult<()> {
    for &table_name in table_names {
        conn.execute(
            "INSERT INTO ff_meta.model_external_dependencies (model_id, table_name) VALUES (?, ?)",
            duckdb::params![model_id, table_name],
        )
        .populate_context("insert model_external_dependencies")?;
    }
    Ok(())
}

/// Clear and repopulate dependencies for a model (used on recompilation).
pub fn clear_model_dependencies(conn: &Connection, model_id: i64) -> MetaResult<()> {
    conn.execute(
        "DELETE FROM ff_meta.model_dependencies WHERE model_id = ?",
        duckdb::params![model_id],
    )
    .populate_context("delete model_dependencies")?;
    conn.execute(
        "DELETE FROM ff_meta.model_external_dependencies WHERE model_id = ?",
        duckdb::params![model_id],
    )
    .populate_context("delete model_external_dependencies")?;
    Ok(())
}
