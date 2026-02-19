//! Populate the `sources`, `source_tags`, `source_tables`, and `source_columns` tables.

use crate::error::{MetaResult, MetaResultExt};
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
            source.name.as_str(),
            source.description,
            source.database,
            source.schema,
            source.owner,
        ],
    )
    .populate_context(&format!("insert sources ({})", source.name))?;

    let source_id: i64 = conn
        .query_row(
            "SELECT source_id FROM ff_meta.sources WHERE project_id = ? AND name = ?",
            duckdb::params![project_id, source.name.as_str()],
            |row| row.get(0),
        )
        .populate_context("select source_id")?;

    Ok(source_id)
}

fn insert_source_tags(conn: &Connection, source_id: i64, source: &SourceFile) -> MetaResult<()> {
    for tag in &source.tags {
        conn.execute(
            "INSERT INTO ff_meta.source_tags (source_id, tag) VALUES (?, ?)",
            duckdb::params![source_id, tag],
        )
        .populate_context("insert source_tags")?;
    }
    Ok(())
}

fn insert_source_tables(conn: &Connection, source_id: i64, source: &SourceFile) -> MetaResult<()> {
    for table in &source.tables {
        insert_single_source_table(conn, source_id, table)?;
    }
    Ok(())
}

/// Insert a single source table and its columns.
fn insert_single_source_table(
    conn: &Connection,
    source_id: i64,
    table: &ff_core::SourceTable,
) -> MetaResult<()> {
    conn.execute(
        "INSERT INTO ff_meta.source_tables (source_id, name, identifier, description) VALUES (?, ?, ?, ?)",
        duckdb::params![source_id, table.name, table.identifier, table.description],
    )
    .populate_context(&format!("insert source_tables ({})", table.name))?;

    let table_id: i64 = conn
        .query_row(
            "SELECT source_table_id FROM ff_meta.source_tables WHERE source_id = ? AND name = ?",
            duckdb::params![source_id, table.name],
            |row| row.get(0),
        )
        .populate_context("select source_table_id")?;

    for (i, col) in table.columns.iter().enumerate() {
        conn.execute(
            "INSERT INTO ff_meta.source_columns (source_table_id, name, data_type, description, ordinal_position)
             VALUES (?, ?, ?, ?, ?)",
            duckdb::params![table_id, col.name, col.data_type, col.description, (i + 1) as i32],
        )
        .populate_context("insert source_columns")?;
    }

    Ok(())
}
