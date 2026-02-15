//! Pre/post-hook execution, schema creation, database connection, and contract validation.

use anyhow::{Context, Result};
use ff_core::config::Config;
use ff_core::contract::{validate_contract, ContractValidationResult, ViolationType};
use ff_core::model::ModelSchema;
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
        let target = Config::resolve_target(global.target.as_deref());
        if let Some(ref target_name) = target {
            eprintln!("[verbose] Using target '{}' database", target_name);
        } else {
            eprintln!("[verbose] Using default database");
        }
    }

    common::create_database_connection(&project.config, global.target.as_deref())
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

    if let Some(default_schema) = &project.config.schema {
        schemas.push(default_schema.clone());
    }

    for model in compiled_models.values() {
        if let Some(ref s) = model.schema {
            if !schemas.contains(s) {
                schemas.push(s.clone());
            }
        }
    }

    if let Some(ws) = wap_schema {
        if !schemas.iter().any(|s| s == ws) {
            schemas.push(ws.to_string());
        }
    }

    if !schemas.iter().any(|s| s == "main") {
        schemas.push("main".to_string());
    }

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
    for line in sql.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            return false;
        }
    }
    true
}

/// Validate schema contract for a model after execution.
///
/// Returns Ok(Some(result)) if contract validation was performed,
/// Ok(None) if no contract was defined,
/// Err if contract was enforced and violations were found.
pub(super) async fn validate_model_contract(
    db: &Arc<dyn Database>,
    model_name: &str,
    qualified_name: &str,
    model_schema: Option<&ModelSchema>,
    verbose: bool,
) -> Result<Option<ContractValidationResult>> {
    // Check if model has a schema with contract
    let schema = match model_schema {
        Some(s) if s.contract.is_some() => s,
        _ => return Ok(None), // No contract to validate
    };

    if verbose {
        eprintln!("[verbose] Validating contract for model: {}", model_name);
    }

    // Get actual table schema from database
    let actual_columns = db
        .get_table_schema(qualified_name)
        .await
        .context("Failed to get schema for contract validation")?;

    // Validate the contract
    let result = validate_contract(model_name, schema, &actual_columns);

    // Log violations
    for violation in &result.violations {
        let severity = if result.enforced { "ERROR" } else { "WARN" };
        match &violation.violation_type {
            ViolationType::MissingColumn { column } => {
                eprintln!(
                    "    [{}] Contract violation: missing column '{}'",
                    severity, column
                );
            }
            ViolationType::TypeMismatch {
                column,
                expected,
                actual,
            } => {
                eprintln!(
                    "    [{}] Contract violation: column '{}' type mismatch (expected {}, got {})",
                    severity, column, expected, actual
                );
            }
            ViolationType::ExtraColumn { column } => {
                if verbose {
                    eprintln!("    [INFO] Extra column '{}' not in contract", column);
                }
            }
            ViolationType::ConstraintNotMet { column, constraint } => {
                eprintln!(
                    "    [{}] Contract violation: column '{}' constraint {:?} not met",
                    severity, column, constraint
                );
            }
        }
    }

    // If contract is enforced and has violations (excluding extra columns), fail
    if result.enforced && !result.passed {
        let violation_count = result
            .violations
            .iter()
            .filter(|v| !matches!(v.violation_type, ViolationType::ExtraColumn { .. }))
            .count();
        anyhow::bail!(
            "Contract enforcement failed: {} violation(s)",
            violation_count
        );
    }

    Ok(Some(result))
}
