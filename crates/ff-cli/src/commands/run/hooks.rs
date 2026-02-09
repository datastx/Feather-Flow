//! Pre/post-hook execution, schema creation, database connection, and contract validation.

use anyhow::{Context, Result};
use ff_core::contract::{validate_contract, ContractValidationResult, ViolationType};
use ff_core::model::ModelSchema;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::cli::GlobalArgs;

use super::compile::CompiledModel;

/// Create database connection from project config, optionally using target override.
///
/// If --target is specified (or FF_TARGET env var is set), uses the database config
/// from that target. Otherwise, uses the base database config.
pub(super) fn create_database_connection(
    project: &Project,
    global: &GlobalArgs,
) -> Result<Arc<dyn Database>> {
    use ff_core::config::Config;

    // Resolve target from CLI flag or FF_TARGET env var
    let target = Config::resolve_target(global.target.as_deref());

    // Get database config, applying target overrides if specified
    let db_config = project
        .config
        .get_database_config(target.as_deref())
        .context("Failed to get database configuration")?;

    if global.verbose {
        if let Some(ref target_name) = target {
            eprintln!(
                "[verbose] Using target '{}' with database: {}",
                target_name, db_config.path
            );
        } else {
            eprintln!("[verbose] Using default database: {}", db_config.path);
        }
    }

    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(&db_config.path).context("Failed to connect to database")?);
    Ok(db)
}

/// Create all required schemas before running models
pub(super) async fn create_schemas(
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
            .context(format!("Failed to create schema: {}", schema))?;
    }

    Ok(())
}

/// Execute pre/post-hook SQL statements for a model.
///
/// Replaces `{{ this }}` (or `{{this}}`) with the qualified table name.
/// Uses simple string replacement rather than full Jinja rendering because
/// hooks only support the `this` variable and the cost of a full template
/// engine round-trip is unnecessary here.
pub(crate) async fn execute_hooks(
    db: &Arc<dyn Database>,
    hooks: &[String],
    qualified_name: &str,
) -> ff_db::error::DbResult<()> {
    for hook in hooks {
        let sql = hook
            .replace("{{ this }}", qualified_name)
            .replace("{{this}}", qualified_name);
        db.execute(&sql).await?;
    }
    Ok(())
}

/// Validate schema contract for a model after execution.
///
/// Returns Ok(Some(result)) if contract validation was performed,
/// Ok(None) if no contract was defined,
/// Err if contract was enforced and violations were found.
pub(crate) async fn validate_model_contract(
    db: &Arc<dyn Database>,
    model_name: &str,
    qualified_name: &str,
    model_schema: Option<&ModelSchema>,
    verbose: bool,
) -> Result<Option<ContractValidationResult>, String> {
    // Check if model has a schema with contract
    let schema = match model_schema {
        Some(s) if s.contract.is_some() => s,
        _ => return Ok(None), // No contract to validate
    };

    if verbose {
        eprintln!("[verbose] Validating contract for model: {}", model_name);
    }

    // Get actual table schema from database
    let actual_columns = match db.get_table_schema(qualified_name).await {
        Ok(cols) => cols,
        Err(e) => {
            return Err(format!(
                "Failed to get schema for contract validation: {}",
                e
            ));
        }
    };

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
        return Err(format!(
            "Contract enforcement failed: {} violation(s)",
            violation_count
        ));
    }

    Ok(Some(result))
}
