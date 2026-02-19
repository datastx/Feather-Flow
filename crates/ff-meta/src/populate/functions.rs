//! Populate the `functions`, `function_args`, and `function_return_columns` tables.

use crate::error::{MetaResult, MetaResultExt};
use duckdb::Connection;
use ff_core::FunctionDef;

/// Insert all function definitions and their children.
pub fn populate_functions(
    conn: &Connection,
    project_id: i64,
    functions: &[FunctionDef],
) -> MetaResult<()> {
    for func in functions {
        insert_function(conn, project_id, func)?;
    }
    Ok(())
}

fn insert_function(conn: &Connection, project_id: i64, func: &FunctionDef) -> MetaResult<()> {
    let function_type = match func.function_type {
        ff_core::function::FunctionType::Scalar => "scalar",
        ff_core::function::FunctionType::Table => "table",
    };

    let return_type = match &func.returns {
        ff_core::function::FunctionReturn::Scalar { data_type } => Some(data_type.as_str()),
        ff_core::function::FunctionReturn::Table { .. } => None,
    };

    conn.execute(
        "INSERT INTO ff_meta.functions (project_id, name, function_type, description, sql_body, sql_path, yaml_path, schema_name, deterministic, return_type)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            project_id,
            func.name.as_ref(),
            function_type,
            func.description,
            func.sql_body,
            func.sql_path.display().to_string(),
            func.yaml_path.display().to_string(),
            func.config.schema,
            func.config.deterministic,
            return_type,
        ],
    )
    .populate_context(&format!("insert functions ({})", func.name))?;

    let function_id: i64 = conn
        .query_row(
            "SELECT function_id FROM ff_meta.functions WHERE project_id = ? AND name = ?",
            duckdb::params![project_id, func.name.as_ref()],
            |row| row.get(0),
        )
        .populate_context("select function_id")?;

    for (i, arg) in func.args.iter().enumerate() {
        conn.execute(
            "INSERT INTO ff_meta.function_args (function_id, name, data_type, default_value, description, ordinal_position)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![
                function_id,
                arg.name,
                arg.data_type,
                arg.default,
                arg.description,
                (i + 1) as i32,
            ],
        )
        .populate_context("insert function_args")?;
    }

    if let ff_core::function::FunctionReturn::Table { columns } = &func.returns {
        for (i, col) in columns.iter().enumerate() {
            conn.execute(
                "INSERT INTO ff_meta.function_return_columns (function_id, name, data_type, ordinal_position)
                 VALUES (?, ?, ?, ?)",
                duckdb::params![function_id, col.name, col.data_type, (i + 1) as i32],
            )
            .populate_context("insert function_return_columns")?;
        }
    }

    Ok(())
}
