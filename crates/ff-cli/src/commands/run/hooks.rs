//! Pre/post-hook execution, schema creation, and database connection.

use anyhow::{Context, Result};
use ff_core::config::Config;
use ff_core::Project;
use ff_db::Database;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::cli::GlobalArgs;
use crate::commands::common;

use super::compile::CompiledModel;

/// Create database connection from project config, optionally using target override.
///
/// If --target is specified (or FF_TARGET env var is set), uses the database config
/// from that target. Otherwise, uses the base database config.
pub(crate) fn create_database_connection(
    project: &Project,
    global: &GlobalArgs,
) -> Result<Arc<dyn Database>> {
    if global.verbose {
        let database = Config::resolve_database(global.database.as_deref());
        if let Some(ref db_name) = database {
            eprintln!("[verbose] Using database connection '{}'", db_name);
        } else {
            eprintln!("[verbose] Using default database");
        }
    }

    common::create_database_connection(&project.config, global.database.as_deref())
}

/// Create all required schemas before running models
pub(crate) async fn create_schemas(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    global: &GlobalArgs,
) -> Result<()> {
    let schemas_to_create: HashSet<String> = compiled_models
        .values()
        .filter_map(|m| m.schema.clone())
        .collect();

    for schema in &schemas_to_create {
        if global.verbose {
            eprintln!("[verbose] Creating schema if not exists: {}", schema);
        }
        db.create_schema_if_not_exists(schema)
            .await
            .with_context(|| format!("Failed to create schema: {}", schema))?;
    }

    Ok(())
}

/// Set the DuckDB search path to include all project schemas.
///
/// Without this, unqualified table references in SQL (e.g. `FROM raw_customers`)
/// only resolve against the default `main` schema. By including all model schemas
/// and the project's default schema, cross-schema references work naturally.
pub(crate) async fn set_search_path(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    project: &Project,
    wap_schema: Option<&str>,
    global: &GlobalArgs,
) -> Result<()> {
    let mut schemas: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let push_unique =
        |s: String, schemas: &mut Vec<String>, seen: &mut std::collections::HashSet<String>| {
            if seen.insert(s.clone()) {
                schemas.push(s);
            }
        };

    if let Some(default_schema) = project.config.get_schema(None) {
        push_unique(default_schema.to_string(), &mut schemas, &mut seen);
    }

    for model in compiled_models.values() {
        if let Some(ref s) = model.schema {
            push_unique(s.clone(), &mut schemas, &mut seen);
        }
    }

    if let Some(ws) = wap_schema {
        push_unique(ws.to_string(), &mut schemas, &mut seen);
    }

    push_unique("main".to_string(), &mut schemas, &mut seen);

    if global.verbose {
        eprintln!("[verbose] Setting search_path to: {}", schemas.join(","));
    }

    let path = schemas.join(",");
    db.execute(&format!("SET search_path = '{path}'"))
        .await
        .context("Failed to set search_path")?;

    Ok(())
}

/// Execute pre/post-hook SQL statements for a model.
///
/// Replaces `{{ this }}` (or `{{this}}`) with the qualified table name.
/// Uses simple string replacement rather than full Jinja rendering because
/// hooks only support the `this` variable and the cost of a full template
/// engine round-trip is unnecessary here.
///
/// Hooks that contain only SQL comments (no executable statements) are
/// silently skipped, since DuckDB rejects comment-only SQL.
pub(super) async fn execute_hooks(
    db: &Arc<dyn Database>,
    hooks: &[String],
    qualified_name: &str,
) -> ff_db::error::DbResult<()> {
    for hook in hooks {
        let sql = hook
            .replace("{{ this }}", qualified_name)
            .replace("{{this}}", qualified_name);
        if is_comment_only(&sql) {
            continue;
        }
        db.execute(&sql).await?;
    }
    Ok(())
}

/// Returns `true` if `sql` contains only line comments (`--`) and whitespace.
fn is_comment_only(sql: &str) -> bool {
    sql.lines()
        .map(str::trim)
        .all(|line| line.is_empty() || line.starts_with("--"))
}
