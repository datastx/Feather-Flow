//! Populate the `projects`, `project_hooks`, and `project_vars` tables.

use crate::error::{MetaResult, MetaResultExt};
use duckdb::Connection;
use ff_core::Config;
use std::path::Path;

/// Insert a project row and its hooks/vars. Returns the generated `project_id`.
pub fn populate_project(conn: &Connection, config: &Config, root: &Path) -> MetaResult<i64> {
    let materialization = if config.materialization.is_ephemeral() {
        "view"
    } else {
        config.materialization.as_str()
    };

    let db = config.get_database_config(None).map_err(|e| {
        crate::error::MetaError::QueryError(format!("default database config: {}", e))
    })?;

    let dialect = match db.db_type {
        ff_core::DbType::DuckDb => "duckdb",
        ff_core::DbType::Snowflake => "snowflake",
    };

    let target_path = config.target_path_absolute(root);

    conn.execute(
        "INSERT INTO ff_meta.projects (name, version, root_path, schema_name, materialization, wap_schema, dialect, db_path, db_name, target_path)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            config.name,
            config.version,
            root.display().to_string(),
            config.get_schema(None),
            materialization,
            config.get_wap_schema(None),
            dialect,
            db.path,
            db.name,
            target_path.display().to_string(),
        ],
    )
    .populate_context("insert projects")?;

    let project_id: i64 = conn
        .query_row(
            "SELECT project_id FROM ff_meta.projects WHERE name = ?",
            duckdb::params![config.name],
            |row| row.get(0),
        )
        .populate_context("select project_id")?;

    populate_hooks(conn, project_id, config)?;
    populate_vars(conn, project_id, config)?;

    Ok(project_id)
}

fn populate_hooks(conn: &Connection, project_id: i64, config: &Config) -> MetaResult<()> {
    for (i, sql) in config.on_run_start.iter().enumerate() {
        conn.execute(
            "INSERT INTO ff_meta.project_hooks (project_id, hook_type, sql_text, ordinal_position) VALUES (?, 'on_run_start', ?, ?)",
            duckdb::params![project_id, sql, (i + 1) as i32],
        )
        .populate_context("insert project_hooks")?;
    }

    for (i, sql) in config.on_run_end.iter().enumerate() {
        conn.execute(
            "INSERT INTO ff_meta.project_hooks (project_id, hook_type, sql_text, ordinal_position) VALUES (?, 'on_run_end', ?, ?)",
            duckdb::params![project_id, sql, (i + 1) as i32],
        )
        .populate_context("insert project_hooks")?;
    }

    Ok(())
}

fn populate_vars(conn: &Connection, project_id: i64, config: &Config) -> MetaResult<()> {
    for (key, value) in &config.vars {
        let (serialized, value_type) = serialize_yaml_value(value);
        conn.execute(
            "INSERT INTO ff_meta.project_vars (project_id, key, value, value_type) VALUES (?, ?, ?, ?)",
            duckdb::params![project_id, key, serialized, value_type],
        )
        .populate_context("insert project_vars")?;
    }
    Ok(())
}

/// Serialize a serde_yaml::Value into a (string, type_name) pair for storage.
pub(crate) fn serialize_yaml_value(value: &serde_yaml::Value) -> (String, &'static str) {
    match value {
        serde_yaml::Value::String(s) => (s.clone(), "string"),
        serde_yaml::Value::Number(n) => (n.to_string(), "number"),
        serde_yaml::Value::Bool(b) => (b.to_string(), "bool"),
        serde_yaml::Value::Null => ("null".to_string(), "null"),
        serde_yaml::Value::Sequence(_) | serde_yaml::Value::Mapping(_) => {
            let json = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            (json, "json")
        }
        serde_yaml::Value::Tagged(t) => serialize_yaml_value(&t.value),
    }
}
