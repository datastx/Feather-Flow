//! Meta database query and export commands.

use crate::cli::{GlobalArgs, MetaArgs, MetaCommands};
use crate::commands::common::{self, load_project};
use anyhow::{Context, Result};

/// Execute the meta command.
pub async fn execute(args: &MetaArgs, global: &GlobalArgs) -> Result<()> {
    match &args.command {
        MetaCommands::Query(query_args) => execute_query(query_args, global).await,
        MetaCommands::Export(export_args) => execute_export(export_args, global).await,
        MetaCommands::Tables => execute_tables(global).await,
    }
}

async fn execute_query(args: &crate::cli::MetaQueryArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let Some(meta_db) = common::open_meta_db(&project) else {
        anyhow::bail!("Meta database not found. Run `ff compile` or `ff run` first.");
    };

    let result = ff_meta::query::execute_query(meta_db.conn(), &args.sql)
        .context("Failed to execute query")?;

    if args.json {
        print_json_output(&result.columns, &result.rows)?;
    } else {
        print_table_output(&result.columns, &result.rows);
    }

    Ok(())
}

async fn execute_export(args: &crate::cli::MetaExportArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let Some(meta_db) = common::open_meta_db(&project) else {
        anyhow::bail!("Meta database not found. Run `ff compile` or `ff run` first.");
    };

    let conn = meta_db.conn();
    let tables =
        ff_meta::query::list_tables(conn).context("Failed to list meta database tables")?;

    let mut export = serde_json::Map::new();

    for table in &tables {
        let query = format!("SELECT * FROM ff_meta.{table}");
        let result = match ff_meta::query::execute_query(conn, &query) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Warning: failed to query {table}: {e}");
                continue;
            }
        };

        let json_rows: Vec<serde_json::Value> = result
            .rows
            .iter()
            .map(|row| {
                let map: serde_json::Map<String, serde_json::Value> = result
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, val)| {
                        let json_val = if val == "null" {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(val.clone())
                        };
                        (col.clone(), json_val)
                    })
                    .collect();
                serde_json::Value::Object(map)
            })
            .collect();

        export.insert(table.clone(), serde_json::Value::Array(json_rows));
    }

    if let Some(ref path) = args.output {
        let json = serde_json::to_string_pretty(&serde_json::Value::Object(export))
            .context("Failed to serialize export")?;
        std::fs::write(path, &json).with_context(|| format!("Failed to write export to {path}"))?;
        println!("Exported {} tables to {path}", tables.len());
        return Ok(());
    }

    let output = serde_json::to_string_pretty(&serde_json::Value::Object(export))
        .context("Failed to serialize export")?;
    println!("{output}");
    Ok(())
}

async fn execute_tables(global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let Some(meta_db) = common::open_meta_db(&project) else {
        anyhow::bail!("Meta database not found. Run `ff compile` or `ff run` first.");
    };

    let conn = meta_db.conn();
    let tables =
        ff_meta::query::list_tables(conn).context("Failed to list meta database tables")?;

    println!("Meta database tables ({}):\n", tables.len());
    for table in &tables {
        let count = ff_meta::query::table_row_count(conn, table).unwrap_or(0);
        println!("  {:<40} {:>6} rows", table, count);
    }

    Ok(())
}

fn print_table_output(columns: &[String], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    let headers: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
    common::print_table(&headers, rows);
    println!("\n({} rows)", rows.len());
}

fn print_json_output(columns: &[String], rows: &[Vec<String>]) -> Result<()> {
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let map: serde_json::Map<String, serde_json::Value> = columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| {
                    let json_val = if val == "null" {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(val.clone())
                    };
                    (col.clone(), json_val)
                })
                .collect();
            serde_json::Value::Object(map)
        })
        .collect();

    let output =
        serde_json::to_string_pretty(&json_rows).context("Failed to serialize JSON output")?;
    println!("{output}");
    Ok(())
}
