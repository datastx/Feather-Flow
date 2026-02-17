//! Populate the `sources`, `source_tags`, `source_tables`, and `source_columns` tables.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;
use ff_core::SourceFile;

/// Insert all source definitions and their children.
pub fn populate_sources(
    conn: &Connection,
    project_id: i64,
    sources: &[SourceFile],
) -> MetaResult<()> {
    for source in sources {
        let source_id = insert_source(conn, project_id, source)?;
        insert_source_tags(conn, source_id, source)?;
        insert_source_tables(conn, source_id, source)?;
    }
    Ok(())
}

fn insert_source(conn: &Connection, project_id: i64, source: &SourceFile) -> MetaResult<i64> {
    conn.execute(
        "INSERT INTO ff_meta.sources (project_id, name, description, database_name, schema_name, owner)
         VALUES (?, ?, ?, ?, ?, ?)",
        duckdb::params![
            project_id,
            source.name,
            source.description,
            source.database,
            source.schema,
            source.owner,
        ],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert sources ({}): {e}", source.name)))?;

    let source_id: i64 = conn
        .query_row(
            "SELECT source_id FROM ff_meta.sources WHERE project_id = ? AND name = ?",
            duckdb::params![project_id, source.name],
            |row| row.get(0),
        )
        .map_err(|e| MetaError::PopulationError(format!("select source_id: {e}")))?;

    Ok(source_id)
}

fn insert_source_tags(conn: &Connection, source_id: i64, source: &SourceFile) -> MetaResult<()> {
    for tag in &source.tags {
        conn.execute(
            "INSERT INTO ff_meta.source_tags (source_id, tag) VALUES (?, ?)",
            duckdb::params![source_id, tag],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert source_tags: {e}")))?;
    }
    Ok(())
}

fn insert_source_tables(conn: &Connection, source_id: i64, source: &SourceFile) -> MetaResult<()> {
    for table in &source.tables {
        conn.execute(
            "INSERT INTO ff_meta.source_tables (source_id, name, identifier, description) VALUES (?, ?, ?, ?)",
            duckdb::params![source_id, table.name, table.identifier, table.description],
        )
        .map_err(|e| {
            MetaError::PopulationError(format!("insert source_tables ({}): {e}", table.name))
        })?;

        let table_id: i64 = conn
            .query_row(
                "SELECT source_table_id FROM ff_meta.source_tables WHERE source_id = ? AND name = ?",
                duckdb::params![source_id, table.name],
                |row| row.get(0),
            )
            .map_err(|e| MetaError::PopulationError(format!("select source_table_id: {e}")))?;

        for (i, col) in table.columns.iter().enumerate() {
            conn.execute(
                "INSERT INTO ff_meta.source_columns (source_table_id, name, data_type, description, ordinal_position)
                 VALUES (?, ?, ?, ?, ?)",
                duckdb::params![table_id, col.name, col.data_type, col.description, (i + 1) as i32],
            )
            .map_err(|e| MetaError::PopulationError(format!("insert source_columns: {e}")))?;
        }
    }
    Ok(())
}
