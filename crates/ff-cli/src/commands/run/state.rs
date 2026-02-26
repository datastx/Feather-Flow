//! Run state tracking: results, checksums, smart builds, and resume support.

use anyhow::{Context, Result};
use chrono::Utc;
use ff_core::compute_checksum;
use ff_core::run_state::RunState;
use ff_core::Project;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{GlobalArgs, RunArgs};
use crate::commands::common::{self, CommandResults, RunStatus};

use super::compile::CompiledModel;

/// Run result for a single model.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelRunResult {
    /// Model name
    pub(crate) model: String,
    /// Execution outcome
    pub(crate) status: RunStatus,
    /// Materialization strategy used (view, table, incremental, ephemeral)
    pub(crate) materialization: String,
    /// Wall-clock execution time in seconds
    pub(crate) duration_secs: f64,
    /// Error message if the model failed, `None` on success
    pub(crate) error: Option<String>,
}

/// Run results output file format
pub(super) type RunResults = CommandResults<ModelRunResult>;

/// Compute a hash of the project configuration for resume validation
pub(super) fn compute_config_hash(project: &Project) -> String {
    let config_str = format!(
        "{}:{}:{}",
        project.config.name,
        project
            .config
            .get_database_config(None)
            .map(|c| c.path.as_str())
            .unwrap_or(""),
        project.config.get_schema(None).unwrap_or("default")
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

/// Compute which models can be skipped in smart build mode.
///
/// Queries the meta database for previous run state. If meta DB is unavailable,
/// falls back to rebuilding all models (returns empty skip set).
pub(super) fn compute_smart_skips(
    compiled_models: &HashMap<String, CompiledModel>,
    global: &GlobalArgs,
    meta_db: Option<&ff_meta::MetaDb>,
) -> Result<HashSet<String>> {
    let Some(meta_db) = meta_db else {
        eprintln!("[warn] Meta database unavailable, smart build will rebuild all models");
        return Ok(HashSet::new());
    };

    let mut skipped = HashSet::new();

    for (name, compiled) in compiled_models {
        let sql_checksum = compute_checksum(&compiled.sql);
        let schema_checksum = compute_schema_checksum(name, compiled_models);
        let input_checksums = compute_input_checksums(name, compiled_models);

        match ff_meta::query::state::is_model_modified(
            meta_db.conn(),
            name,
            &sql_checksum,
            schema_checksum.as_deref(),
            &input_checksums,
        ) {
            Ok(false) => {
                if global.verbose {
                    eprintln!("[verbose] Smart build: skipping unchanged model '{}'", name);
                }
                skipped.insert(name.clone());
            }
            Ok(true) => {} // Model is modified, don't skip
            Err(e) => {
                if global.verbose {
                    eprintln!(
                        "[verbose] Smart build: error checking '{}', will rebuild: {}",
                        name, e
                    );
                }
            }
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
