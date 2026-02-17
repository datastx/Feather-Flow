//! Populate the `models` and related child tables.

use crate::error::{MetaError, MetaResult};
use crate::populate::project::serialize_yaml_value;
use duckdb::Connection;
use ff_core::{Config, Model, ModelName};
use std::collections::HashMap;

/// Insert all models and their children. Returns a map of `ModelName → model_id`.
pub fn populate_models(
    conn: &Connection,
    project_id: i64,
    models: &HashMap<ModelName, Model>,
    config: &Config,
) -> MetaResult<HashMap<String, i64>> {
    let mut id_map = HashMap::with_capacity(models.len());

    for model in models.values() {
        let model_id = insert_model(conn, project_id, model, config)?;
        id_map.insert(model.name.as_ref().to_string(), model_id);
        insert_model_config(conn, model_id, model)?;
        insert_model_hooks(conn, model_id, model)?;
        insert_model_tags(conn, model_id, model)?;
        insert_model_meta(conn, model_id, model)?;
        insert_model_columns(conn, model_id, model)?;
    }

    Ok(id_map)
}

/// Retrieve a mapping of model name → model_id for a given project.
pub fn get_model_id_map(conn: &Connection, project_id: i64) -> MetaResult<HashMap<String, i64>> {
    let mut stmt = conn
        .prepare("SELECT model_id, name FROM ff_meta.models WHERE project_id = ?")
        .map_err(|e| MetaError::PopulationError(format!("prepare get_model_id_map: {e}")))?;

    let rows = stmt
        .query_map(duckdb::params![project_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| MetaError::PopulationError(format!("query get_model_id_map: {e}")))?;

    let mut map = HashMap::new();
    for row in rows {
        let (model_id, name) =
            row.map_err(|e| MetaError::PopulationError(format!("row get_model_id_map: {e}")))?;
        map.insert(name, model_id);
    }
    Ok(map)
}

fn insert_model(
    conn: &Connection,
    project_id: i64,
    model: &Model,
    config: &Config,
) -> MetaResult<i64> {
    let materialization = match model.materialization(config.materialization) {
        ff_core::config::Materialization::View => "view",
        ff_core::config::Materialization::Table => "table",
        ff_core::config::Materialization::Incremental => "incremental",
        ff_core::config::Materialization::Ephemeral => "view",
    };

    let schema_name = model.target_schema(config.schema.as_deref());
    let description = model.schema.as_ref().and_then(|s| s.description.clone());
    let owner = model.get_owner();
    let deprecated = model.schema.as_ref().map(|s| s.deprecated).unwrap_or(false);
    let deprecation_message = model.get_deprecation_message().map(|s| s.to_string());
    let contract_enforced = model
        .schema
        .as_ref()
        .and_then(|s| s.contract.as_ref())
        .map(|c| c.enforced)
        .unwrap_or(false);
    let checksum = model.sql_checksum();

    conn.execute(
        "INSERT INTO ff_meta.models (project_id, name, source_path, materialization, schema_name, description, owner, deprecated, deprecation_message, base_name, version_number, contract_enforced, raw_sql, sql_checksum)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            project_id,
            model.name.as_ref(),
            model.path.display().to_string(),
            materialization,
            schema_name,
            description,
            owner,
            deprecated,
            deprecation_message,
            model.base_name,
            model.version.map(|v| v as i32),
            contract_enforced,
            model.raw_sql,
            checksum,
        ],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert models ({}): {e}", model.name)))?;

    let model_id: i64 = conn
        .query_row(
            "SELECT model_id FROM ff_meta.models WHERE project_id = ? AND name = ?",
            duckdb::params![project_id, model.name.as_ref()],
            |row| row.get(0),
        )
        .map_err(|e| MetaError::PopulationError(format!("select model_id: {e}")))?;

    Ok(model_id)
}

fn insert_model_config(conn: &Connection, model_id: i64, model: &Model) -> MetaResult<()> {
    let unique_key = model.config.unique_key.as_ref().map(|k| k.to_string());
    let strategy = model
        .config
        .incremental_strategy
        .as_ref()
        .map(|s| format!("{s:?}").to_lowercase());
    let on_schema_change = model
        .config
        .on_schema_change
        .as_ref()
        .map(|s| format!("{s:?}").to_lowercase());
    let wap = model.config.wap;

    conn.execute(
        "INSERT INTO ff_meta.model_config (model_id, unique_key, incremental_strategy, on_schema_change, wap_enabled)
         VALUES (?, ?, ?, ?, ?)",
        duckdb::params![model_id, unique_key, strategy, on_schema_change, wap],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert model_config: {e}")))?;

    Ok(())
}

fn insert_model_hooks(conn: &Connection, model_id: i64, model: &Model) -> MetaResult<()> {
    for (i, sql) in model.config.pre_hook.iter().enumerate() {
        conn.execute(
            "INSERT INTO ff_meta.model_hooks (model_id, hook_type, sql_text, ordinal_position) VALUES (?, 'pre_hook', ?, ?)",
            duckdb::params![model_id, sql, (i + 1) as i32],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert model_hooks: {e}")))?;
    }

    for (i, sql) in model.config.post_hook.iter().enumerate() {
        conn.execute(
            "INSERT INTO ff_meta.model_hooks (model_id, hook_type, sql_text, ordinal_position) VALUES (?, 'post_hook', ?, ?)",
            duckdb::params![model_id, sql, (i + 1) as i32],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert model_hooks: {e}")))?;
    }

    Ok(())
}

fn insert_model_tags(conn: &Connection, model_id: i64, model: &Model) -> MetaResult<()> {
    let mut seen = std::collections::HashSet::new();

    let schema_tags = model
        .schema
        .as_ref()
        .map(|s| s.tags.as_slice())
        .unwrap_or(&[]);
    let config_tags = &model.config.tags;

    for tag in schema_tags.iter().chain(config_tags.iter()) {
        if !seen.insert(tag.as_str()) {
            continue;
        }
        conn.execute(
            "INSERT INTO ff_meta.model_tags (model_id, tag) VALUES (?, ?)",
            duckdb::params![model_id, tag],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert model_tags: {e}")))?;
    }

    Ok(())
}

fn insert_model_meta(conn: &Connection, model_id: i64, model: &Model) -> MetaResult<()> {
    let Some(schema) = &model.schema else {
        return Ok(());
    };

    for (key, value) in &schema.meta {
        let (serialized, _type) = serialize_yaml_value(value);
        conn.execute(
            "INSERT INTO ff_meta.model_meta (model_id, key, value) VALUES (?, ?, ?)",
            duckdb::params![model_id, key, serialized],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert model_meta: {e}")))?;
    }

    Ok(())
}

fn insert_model_columns(conn: &Connection, model_id: i64, model: &Model) -> MetaResult<()> {
    let Some(schema) = &model.schema else {
        return Ok(());
    };

    for (i, col) in schema.columns.iter().enumerate() {
        let nullability = col
            .constraints
            .iter()
            .any(|c| matches!(c, ff_core::ColumnConstraint::NotNull))
            .then_some("not_null");

        let classification = col.classification.as_ref().map(|c| match c {
            ff_core::DataClassification::Pii => "pii",
            ff_core::DataClassification::Sensitive => "sensitive",
            ff_core::DataClassification::Internal => "internal",
            ff_core::DataClassification::Public => "public",
        });

        conn.execute(
            "INSERT INTO ff_meta.model_columns (model_id, name, declared_type, nullability_declared, description, is_primary_key, classification, ordinal_position)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                model_id,
                col.name,
                col.data_type,
                nullability,
                col.description,
                col.primary_key,
                classification,
                (i + 1) as i32,
            ],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert model_columns: {e}")))?;

        let column_id: i64 = conn
            .query_row(
                "SELECT column_id FROM ff_meta.model_columns WHERE model_id = ? AND name = ?",
                duckdb::params![model_id, col.name],
                |row| row.get(0),
            )
            .map_err(|e| MetaError::PopulationError(format!("select column_id: {e}")))?;

        for constraint in &col.constraints {
            let ct = match constraint {
                ff_core::ColumnConstraint::NotNull => "not_null",
                ff_core::ColumnConstraint::PrimaryKey => "primary_key",
                ff_core::ColumnConstraint::Unique => "unique",
            };
            conn.execute(
                "INSERT INTO ff_meta.model_column_constraints (column_id, constraint_type) VALUES (?, ?)",
                duckdb::params![column_id, ct],
            )
            .map_err(|e| MetaError::PopulationError(format!("insert column_constraints: {e}")))?;
        }

        if let Some(ref_info) = &col.references {
            conn.execute(
                "INSERT INTO ff_meta.model_column_references (column_id, referenced_model_name, referenced_column_name) VALUES (?, ?, ?)",
                duckdb::params![column_id, ref_info.model.as_ref(), ref_info.column],
            )
            .map_err(|e| MetaError::PopulationError(format!("insert column_references: {e}")))?;
        }
    }

    Ok(())
}
