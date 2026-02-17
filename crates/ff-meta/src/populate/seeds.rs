//! Populate the `seeds` and `seed_column_types` tables.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;
use ff_core::Seed;

/// Insert all seed definitions and their column type overrides.
pub fn populate_seeds(conn: &Connection, project_id: i64, seeds: &[Seed]) -> MetaResult<()> {
    for seed in seeds {
        insert_seed(conn, project_id, seed)?;
    }
    Ok(())
}

fn insert_seed(conn: &Connection, project_id: i64, seed: &Seed) -> MetaResult<()> {
    let description = seed.description.clone();
    let schema_name = seed.target_schema().map(|s| s.to_string());
    let delimiter = seed.delimiter().to_string();
    let enabled = seed.is_enabled();

    conn.execute(
        "INSERT INTO ff_meta.seeds (project_id, name, path, description, schema_name, delimiter, enabled)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            project_id,
            seed.name,
            seed.path.display().to_string(),
            description,
            schema_name,
            delimiter,
            enabled,
        ],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert seeds ({}): {e}", seed.name)))?;

    let seed_id: i64 = conn
        .query_row(
            "SELECT seed_id FROM ff_meta.seeds WHERE project_id = ? AND name = ?",
            duckdb::params![project_id, seed.name],
            |row| row.get(0),
        )
        .map_err(|e| MetaError::PopulationError(format!("select seed_id: {e}")))?;

    for (col_name, data_type) in seed.column_types() {
        conn.execute(
            "INSERT INTO ff_meta.seed_column_types (seed_id, column_name, data_type) VALUES (?, ?, ?)",
            duckdb::params![seed_id, col_name, data_type],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert seed_column_types: {e}")))?;
    }

    Ok(())
}
