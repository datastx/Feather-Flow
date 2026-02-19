//! Python model execution via `uv run`.
//!
//! Python models are executed as standalone scripts using `uv run`. The script
//! receives database connection info and table names via environment variables,
//! reads from upstream tables, and writes its output table. After execution,
//! Feather-Flow validates the output schema matches the declared columns.

use anyhow::{Context, Result};
use ff_core::sql_utils::quote_qualified;
use ff_db::Database;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::commands::common::RunStatus;

use super::compile::CompiledModel;
use super::state::ModelRunResult;

/// Check that `uv` is available on the system PATH.
pub(crate) fn check_uv_available() -> Result<(), ff_core::error::CoreError> {
    match std::process::Command::new("uv")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(status) if status.success() => Ok(()),
        _ => Err(ff_core::error::CoreError::UvNotFound),
    }
}

/// Build the qualified output table name for a Python model.
fn build_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", s, name),
        None => name.to_string(),
    }
}

/// Build environment variables for the Python script.
///
/// The following variables are set:
/// - `FF_DATABASE_PATH` — DuckDB file path (or `:memory:`)
/// - `FF_INPUT_TABLES` — JSON array of qualified upstream table names
/// - `FF_OUTPUT_TABLE` — qualified output table name
/// - `FF_SCHEMA` — JSON array of `{name, type}` objects describing expected output columns
/// - `FF_MODEL_NAME` — the model name
fn build_env_vars(
    name: &str,
    compiled: &CompiledModel,
    compiled_models: &HashMap<String, CompiledModel>,
    db_path: &str,
) -> HashMap<String, String> {
    let mut env = HashMap::new();

    env.insert("FF_DATABASE_PATH".to_string(), db_path.to_string());
    env.insert("FF_MODEL_NAME".to_string(), name.to_string());

    // Build input tables list from dependencies
    let input_tables: Vec<String> = compiled
        .dependencies
        .iter()
        .map(|dep| {
            let dep_schema = compiled_models.get(dep).and_then(|m| m.schema.as_deref());
            build_qualified_name(dep_schema, dep)
        })
        .collect();
    env.insert(
        "FF_INPUT_TABLES".to_string(),
        serde_json::to_string(&input_tables).unwrap_or_else(|_| "[]".to_string()),
    );

    // Build output table name
    let output_table = build_qualified_name(compiled.schema.as_deref(), name);
    env.insert("FF_OUTPUT_TABLE".to_string(), output_table);

    // Build expected output schema from model_schema columns
    if let Some(ref model_schema) = compiled.model_schema {
        let columns: Vec<serde_json::Value> = model_schema
            .columns
            .iter()
            .map(|col| {
                serde_json::json!({
                    "name": col.name,
                    "type": col.data_type,
                })
            })
            .collect();
        env.insert(
            "FF_SCHEMA".to_string(),
            serde_json::to_string(&columns).unwrap_or_else(|_| "[]".to_string()),
        );
    }

    env
}

/// Execute a Python model via `uv run` and validate its output.
pub(crate) async fn run_python_model(
    db: &Arc<dyn Database>,
    name: &str,
    compiled: &CompiledModel,
    compiled_models: &HashMap<String, CompiledModel>,
    db_path: &str,
) -> ModelRunResult {
    let model_start = Instant::now();

    let script_path = match &compiled.script_path {
        Some(p) => p.clone(),
        None => {
            return ModelRunResult {
                model: name.to_string(),
                status: RunStatus::Error,
                materialization: "python".to_string(),
                duration_secs: model_start.elapsed().as_secs_f64(),
                error: Some("Python model missing script_path".to_string()),
            };
        }
    };

    // Check uv is available
    if let Err(e) = check_uv_available() {
        return ModelRunResult {
            model: name.to_string(),
            status: RunStatus::Error,
            materialization: "python".to_string(),
            duration_secs: model_start.elapsed().as_secs_f64(),
            error: Some(e.to_string()),
        };
    }

    let env_vars = build_env_vars(name, compiled, compiled_models, db_path);

    // Run the Python script via uv
    match execute_uv_run(&script_path, &env_vars).await {
        Ok(output) => {
            if !output.success {
                return ModelRunResult {
                    model: name.to_string(),
                    status: RunStatus::Error,
                    materialization: "python".to_string(),
                    duration_secs: model_start.elapsed().as_secs_f64(),
                    error: Some(format!(
                        "uv run failed (exit {}):\n{}",
                        output.exit_code, output.stderr
                    )),
                };
            }

            // Validate output table exists and schema matches
            let qualified_name = build_qualified_name(compiled.schema.as_deref(), name);
            if let Err(e) = validate_python_output(db, name, &qualified_name, compiled).await {
                return ModelRunResult {
                    model: name.to_string(),
                    status: RunStatus::Error,
                    materialization: "python".to_string(),
                    duration_secs: model_start.elapsed().as_secs_f64(),
                    error: Some(e.to_string()),
                };
            }

            ModelRunResult {
                model: name.to_string(),
                status: RunStatus::Success,
                materialization: "python".to_string(),
                duration_secs: model_start.elapsed().as_secs_f64(),
                error: None,
            }
        }
        Err(e) => ModelRunResult {
            model: name.to_string(),
            status: RunStatus::Error,
            materialization: "python".to_string(),
            duration_secs: model_start.elapsed().as_secs_f64(),
            error: Some(format!("Failed to execute uv run: {}", e)),
        },
    }
}

/// Output from a `uv run` invocation.
struct UvRunOutput {
    success: bool,
    exit_code: i32,
    #[allow(dead_code)]
    stdout: String,
    stderr: String,
}

/// Execute `uv run <script>` with the given environment variables.
async fn execute_uv_run(
    script_path: &Path,
    env_vars: &HashMap<String, String>,
) -> Result<UvRunOutput> {
    let output = tokio::process::Command::new("uv")
        .arg("run")
        .arg(script_path)
        .envs(env_vars)
        .output()
        .await
        .context("Failed to execute 'uv run' — is uv installed?")?;

    Ok(UvRunOutput {
        success: output.status.success(),
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

/// Validate that the Python model produced the expected output table with correct columns.
async fn validate_python_output(
    db: &Arc<dyn Database>,
    name: &str,
    qualified_name: &str,
    compiled: &CompiledModel,
) -> Result<(), ff_core::error::CoreError> {
    // Check the table exists
    let quoted = quote_qualified(qualified_name);
    let exists = db.relation_exists(qualified_name).await.map_err(|e| {
        ff_core::error::CoreError::PythonSchemaViolation {
            model: name.to_string(),
            message: format!("failed to check if output table exists: {}", e),
        }
    })?;

    if !exists {
        return Err(ff_core::error::CoreError::PythonSchemaViolation {
            model: name.to_string(),
            message: format!(
                "Python script did not create the expected output table '{}'",
                qualified_name
            ),
        });
    }

    // If schema has columns defined, validate they exist in the output
    if let Some(ref model_schema) = compiled.model_schema {
        if !model_schema.columns.is_empty() {
            let actual_schema = db.get_table_schema(&quoted).await.map_err(|e| {
                ff_core::error::CoreError::PythonSchemaViolation {
                    model: name.to_string(),
                    message: format!("failed to read output table schema: {}", e),
                }
            })?;

            // Build a case-insensitive map of actual column names
            let actual_columns: std::collections::HashSet<String> = actual_schema
                .iter()
                .map(|(col_name, _)| col_name.to_lowercase())
                .collect();

            // Check each declared column exists in the output
            let mut missing: Vec<String> = Vec::new();
            for expected_col in &model_schema.columns {
                if !actual_columns.contains(&expected_col.name.to_lowercase()) {
                    missing.push(expected_col.name.clone());
                }
            }

            if !missing.is_empty() {
                return Err(ff_core::error::CoreError::PythonSchemaViolation {
                    model: name.to_string(),
                    message: format!(
                        "output table is missing declared columns: {}",
                        missing.join(", ")
                    ),
                });
            }
        }
    }

    Ok(())
}
