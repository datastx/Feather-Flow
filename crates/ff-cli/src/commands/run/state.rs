//! Run state tracking: results, checksums, smart builds, and resume support.

use anyhow::{Context, Result};
use chrono::Utc;
use ff_core::run_state::RunState;
use ff_core::state::{compute_checksum, ModelState, ModelStateConfig, StateFile};
use ff_core::Project;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{GlobalArgs, RunArgs};
use crate::commands::common::{self, CommandResults, RunStatus};

use super::compile::CompiledModel;

/// Run result for a single model
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelRunResult {
    pub(crate) model: String,
    pub(crate) status: RunStatus,
    pub(crate) materialization: String,
    pub(crate) duration_secs: f64,
    pub(crate) error: Option<String>,
}

/// Run results output file format
pub(super) type RunResults = CommandResults<ModelRunResult>;

/// Compute a hash of the project configuration for resume validation
pub(super) fn compute_config_hash(project: &Project) -> String {
    let config_str = format!(
        "{}:{}:{}",
        project.config.name,
        project.config.database.path,
        project.config.schema.as_deref().unwrap_or("default")
    );
    compute_checksum(&config_str)
}

/// Handle resume mode - load previous state and determine what to run
pub(super) fn handle_resume_mode(
    run_state_path: &Path,
    compiled_models: &HashMap<String, CompiledModel>,
    args: &RunArgs,
    global: &GlobalArgs,
    config_hash: &str,
) -> Result<(Vec<String>, Option<RunState>)> {
    let previous_state = RunState::load(run_state_path)
        .context("Failed to load run state")?
        .ok_or_else(|| anyhow::anyhow!("No run state found. Run 'ff run' first."))?;

    // Warn if config has changed
    if previous_state.config_hash != config_hash {
        eprintln!("Warning: Project configuration has changed since last run");
    }

    // Determine which models to run
    let models_to_run = if args.retry_failed {
        // Only retry failed models
        previous_state.failed_model_names()
    } else {
        // Retry failed + run pending
        previous_state.models_to_run()
    };

    // Filter to only models that exist in compiled_models
    let execution_order: Vec<String> = models_to_run
        .into_iter()
        .filter(|m| compiled_models.contains_key(m.as_str()))
        .map(|m| m.to_string())
        .collect();

    // Log what we're skipping
    for completed in &previous_state.completed_models {
        if global.verbose {
            eprintln!(
                "[verbose] Skipping {} (completed in previous run)",
                completed.name
            );
        }
    }

    Ok((execution_order, Some(previous_state)))
}

/// Compute which models can be skipped in smart build mode
pub(super) fn compute_smart_skips(
    project: &Project,
    compiled_models: &HashMap<String, CompiledModel>,
    global: &GlobalArgs,
) -> Result<HashSet<String>> {
    let state_path = project.target_dir().join("state.json");
    let state_file = match StateFile::load(&state_path) {
        Ok(sf) => sf,
        Err(e) => {
            eprintln!(
                "[warn] Failed to load state file at {}, smart build will rebuild all models: {}",
                state_path.display(),
                e
            );
            StateFile::default()
        }
    };

    let mut skipped = HashSet::new();

    for (name, compiled) in compiled_models {
        let sql_checksum = compute_checksum(&compiled.sql);
        let schema_checksum = compute_schema_checksum(name, compiled_models);
        let input_checksums = compute_input_checksums(name, compiled_models);

        if !state_file.is_model_or_inputs_modified(
            name,
            &sql_checksum,
            schema_checksum.as_deref(),
            &input_checksums,
        ) {
            if global.verbose {
                eprintln!("[verbose] Smart build: skipping unchanged model '{}'", name);
            }
            skipped.insert(name.clone());
        }
    }

    Ok(skipped)
}

/// Compute schema checksum for a model (from its YAML schema)
pub(super) fn compute_schema_checksum(
    name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
) -> Option<String> {
    compiled_models
        .get(name)
        .and_then(|c| c.model_schema.as_ref())
        .and_then(|schema| {
            serde_json::to_string(schema)
                .ok()
                .map(|yaml| compute_checksum(&yaml))
        })
}

/// Compute input checksums for a model (upstream model SQL checksums)
pub(super) fn compute_input_checksums(
    name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
) -> HashMap<String, String> {
    compiled_models
        .get(name)
        .map(|compiled| {
            compiled
                .dependencies
                .iter()
                .filter_map(|dep| {
                    compiled_models
                        .get(dep)
                        .map(|dep_compiled| (dep.clone(), compute_checksum(&dep_compiled.sql)))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Update state file entry for a successfully-run model
pub(super) fn update_state_for_model(
    state_file: &mut StateFile,
    name: &str,
    compiled: &CompiledModel,
    compiled_models: &HashMap<String, CompiledModel>,
    row_count: Option<usize>,
) -> anyhow::Result<()> {
    let state_config = ModelStateConfig::new(
        compiled.materialization,
        compiled.schema.clone(),
        compiled.unique_key.clone(),
        compiled.incremental_strategy,
        compiled.on_schema_change,
    );
    let schema_checksum = compute_schema_checksum(name, compiled_models);
    let input_checksums = compute_input_checksums(name, compiled_models);
    let model_name = ff_core::ModelName::try_new(name)
        .ok_or_else(|| anyhow::anyhow!("Empty model name in state update"))?;
    let model_state = ModelState::new_with_checksums(
        model_name,
        &compiled.sql,
        row_count,
        state_config,
        schema_checksum,
        input_checksums,
    );
    state_file.upsert_model(model_state);
    Ok(())
}

/// Write run results to JSON file
pub(super) fn write_run_results(
    project: &Project,
    run_results: &[ModelRunResult],
    start_time: std::time::Instant,
    success_count: usize,
    failure_count: usize,
) -> Result<()> {
    let results = RunResults {
        timestamp: Utc::now(),
        elapsed_secs: start_time.elapsed().as_secs_f64(),
        success_count,
        failure_count,
        results: run_results.to_vec(),
    };

    let results_path = project.target_dir().join("run_results.json");
    common::write_json_results(&results_path, &results)
}
