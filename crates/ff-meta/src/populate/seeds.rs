//! Populate the `seeds` and `seed_column_types` tables.

use crate::error::{MetaResult, MetaResultExt};
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
    let schema_name = seed.target_schema().map(|s| s.to_string());
    let delimiter = seed.delimiter().to_string();
    let enabled = seed.is_enabled();

    conn.execute(
        "INSERT INTO ff_meta.seeds (project_id, name, path, description, schema_name, delimiter, enabled)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            project_id,
            seed.name.as_str(),
            seed.path.display().to_string(),
            seed.description.as_deref(),
            schema_name,
            delimiter,
            enabled,
        ],
    )
    .populate_context(&format!("insert seeds ({})", seed.name))?;

    let seed_id: i64 = conn
        .query_row(
            "SELECT seed_id FROM ff_meta.seeds WHERE project_id = ? AND name = ?",
            duckdb::params![project_id, seed.name.as_str()],
            |row| row.get(0),
        )
        .populate_context("select seed_id")?;

    for (col_name, data_type) in seed.column_types() {
        conn.execute(
            "INSERT INTO ff_meta.seed_column_types (seed_id, column_name, data_type) VALUES (?, ?, ?)",
            duckdb::params![seed_id, col_name, data_type],
        )
        .populate_context("insert seed_column_types")?;
    }

    Ok(())
}
