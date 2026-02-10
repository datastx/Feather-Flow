//! Model execution: sequential and parallel runners, single-model execution.

use ff_core::config::Materialization;
use ff_core::run_state::RunState;
use ff_core::sql_utils::quote_qualified;
use ff_core::state::StateFile;
use ff_db::Database;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;

use crate::cli::{OutputFormat, RunArgs};
use crate::commands::common::RunStatus;

use super::compile::CompiledModel;
use super::hooks::{execute_hooks, validate_model_contract};
use super::incremental::{execute_incremental, execute_wap};
use super::state::{update_state_for_model, ModelRunResult};

/// Shared context for model execution that groups related parameters
pub(super) struct ExecutionContext<'a> {
    pub(super) db: &'a Arc<dyn Database>,
    pub(super) compiled_models: &'a HashMap<String, CompiledModel>,
    pub(super) execution_order: &'a [String],
    pub(super) args: &'a RunArgs,
    pub(super) wap_schema: Option<&'a str>,
}

impl std::fmt::Debug for ExecutionContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("execution_order", &self.execution_order)
            .field("wap_schema", &self.wap_schema)
            .finish_non_exhaustive()
    }
}

/// Execute a single model: pre-hooks -> materialize -> post-hooks -> contract validation.
///
/// Returns a `ModelRunResult` with the outcome. Callers handle state-file updates
/// because the sequential and parallel paths have different timing requirements.
async fn run_single_model(
    db: &Arc<dyn Database>,
    name: &str,
    compiled: &CompiledModel,
    full_refresh: bool,
    wap_schema: Option<&str>,
) -> ModelRunResult {
    let qualified_name = match &compiled.schema {
        Some(s) => quote_qualified(&format!("{}.{}", s, name)),
        None => quote_qualified(name),
    };

    let model_start = Instant::now();

    // Execute pre-hooks
    if let Err(e) = execute_hooks(db, &compiled.pre_hook, &qualified_name).await {
        let duration = model_start.elapsed();
        println!(
            "  \u{2717} {} (pre-hook) - {} [{}ms]",
            name,
            e,
            duration.as_millis()
        );
        return ModelRunResult {
            model: name.to_string(),
            status: RunStatus::Error,
            materialization: compiled.materialization.to_string(),
            duration_secs: duration.as_secs_f64(),
            error: Some(format!("pre-hook failed: {}", e)),
        };
    }

    if full_refresh {
        if let Err(e) = db.drop_if_exists(&qualified_name).await {
            eprintln!(
                "[warn] Failed to drop {} during full refresh: {}",
                qualified_name, e
            );
        }
    }

    // Append query comment to SQL for execution (compiled.sql stays clean for checksums)
    let exec_sql = match &compiled.query_comment {
        Some(comment) => ff_core::query_comment::append_query_comment(&compiled.sql, comment),
        None => compiled.sql.clone(),
    };

    // Determine if this model should use WAP flow
    let is_wap = compiled.wap
        && wap_schema.is_some()
        && matches!(
            compiled.materialization,
            Materialization::Table | Materialization::Incremental
        );

    let result = if is_wap {
        let Some(ws) = wap_schema else {
            unreachable!("is_wap is only true when wap_schema.is_some()")
        };
        execute_wap(
            db,
            name,
            &qualified_name,
            ws,
            compiled,
            full_refresh,
            &exec_sql,
        )
        .await
    } else {
        match compiled.materialization {
            Materialization::View => db.create_view_as(&qualified_name, &exec_sql, true).await,
            Materialization::Table => db.create_table_as(&qualified_name, &exec_sql, true).await,
            Materialization::Incremental => {
                execute_incremental(db, &qualified_name, compiled, full_refresh, &exec_sql).await
            }
            Materialization::Ephemeral => Ok(()),
        }
    };

    match result {
        Ok(_) => {
            // Execute post-hooks
            if let Err(e) = execute_hooks(db, &compiled.post_hook, &qualified_name).await {
                let duration = model_start.elapsed();
                println!(
                    "  \u{2717} {} (post-hook) - {} [{}ms]",
                    name,
                    e,
                    duration.as_millis()
                );
                return ModelRunResult {
                    model: name.to_string(),
                    status: RunStatus::Error,
                    materialization: compiled.materialization.to_string(),
                    duration_secs: duration.as_secs_f64(),
                    error: Some(format!("post-hook failed: {}", e)),
                };
            }

            // Validate schema contract if defined
            if let Err(contract_error) = validate_model_contract(
                db,
                name,
                &qualified_name,
                compiled.model_schema.as_ref(),
                false,
            )
            .await
            {
                let duration = model_start.elapsed();
                println!(
                    "  \u{2717} {} (contract) - {} [{}ms]",
                    name,
                    contract_error,
                    duration.as_millis()
                );
                return ModelRunResult {
                    model: name.to_string(),
                    status: RunStatus::Error,
                    materialization: compiled.materialization.to_string(),
                    duration_secs: duration.as_secs_f64(),
                    error: Some(contract_error.to_string()),
                };
            }

            let duration = model_start.elapsed();
            println!(
                "  \u{2713} {} ({}) [{}ms]",
                name,
                compiled.materialization,
                duration.as_millis()
            );
            ModelRunResult {
                model: name.to_string(),
                status: RunStatus::Success,
                materialization: compiled.materialization.to_string(),
                duration_secs: duration.as_secs_f64(),
                error: None,
            }
        }
        Err(e) => {
            let duration = model_start.elapsed();
            println!("  \u{2717} {} - {} [{}ms]", name, e, duration.as_millis());
            ModelRunResult {
                model: name.to_string(),
                status: RunStatus::Error,
                materialization: compiled.materialization.to_string(),
                duration_secs: duration.as_secs_f64(),
                error: Some(e.to_string()),
            }
        }
    }
}

/// Execute all models in order with optional parallelism
async fn execute_models(
    ctx: &ExecutionContext<'_>,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    // For single thread, use the simple sequential execution
    if ctx.args.threads <= 1 {
        return execute_models_sequential(ctx, state_file).await;
    }

    // Parallel execution using DAG levels
    execute_models_parallel(ctx, state_file).await
}

/// Execute all models in order with run state tracking for resume capability
pub(super) async fn execute_models_with_state(
    ctx: &ExecutionContext<'_>,
    state_file: &mut StateFile,
    run_state: &mut RunState,
    run_state_path: &Path,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    let (run_results, success_count, failure_count, stopped_early) =
        execute_models(ctx, state_file).await;

    // Update run state based on results
    for result in &run_results {
        let duration_ms = (result.duration_secs * 1000.0) as u64;
        if matches!(result.status, RunStatus::Success) {
            run_state.mark_completed(&result.model, duration_ms);
        } else {
            run_state.mark_failed(
                &result.model,
                result.error.as_deref().unwrap_or("unknown error"),
            );
        }
    }

    // Save run state after execution
    if let Err(e) = run_state.save(run_state_path) {
        eprintln!("Warning: Failed to save run state: {}", e);
    }

    (run_results, success_count, failure_count, stopped_early)
}

/// Get all transitive dependents of a model
fn get_transitive_dependents(
    model_name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
) -> HashSet<String> {
    let mut dependents = HashSet::new();
    let mut to_check = vec![model_name.to_string()];

    while let Some(current) = to_check.pop() {
        for (name, compiled) in compiled_models {
            if compiled.dependencies.contains(&current) && !dependents.contains(name) {
                dependents.insert(name.clone());
                to_check.push(name.clone());
            }
        }
    }

    dependents
}

/// Execute models sequentially (original behavior for --threads=1)
async fn execute_models_sequential(
    ctx: &ExecutionContext<'_>,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    let mut success_count = 0;
    let mut failure_count = 0;
    let mut run_results: Vec<ModelRunResult> = Vec::new();
    let mut stopped_early = false;
    let mut failed_models: HashSet<String> = HashSet::new();

    // Count non-ephemeral models for progress bar
    let executable_models: Vec<&String> = ctx
        .execution_order
        .iter()
        .filter(|name| {
            ctx.compiled_models
                .get(*name)
                .map(|m| m.materialization != Materialization::Ephemeral)
                .unwrap_or(true)
        })
        .collect();
    let executable_count = executable_models.len();

    // Create progress bar if not in quiet mode
    let progress = if !ctx.args.quiet && ctx.args.output == OutputFormat::Text {
        let pb = ProgressBar::new(executable_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    let mut executable_idx = 0;
    for name in ctx.execution_order.iter() {
        let Some(compiled) = ctx.compiled_models.get(name) else {
            eprintln!(
                "[warn] Model '{}' missing from compiled_models, skipping",
                name
            );
            continue;
        };

        // Skip models whose upstream WAP failed
        if failed_models.contains(name) {
            failure_count += 1;
            println!("  - {} (skipped: upstream WAP failure)", name);
            run_results.push(ModelRunResult {
                model: name.clone(),
                status: RunStatus::Skipped,
                materialization: compiled.materialization.to_string(),
                duration_secs: 0.0,
                error: Some("skipped: upstream WAP failure".to_string()),
            });
            continue;
        }

        // Skip ephemeral models (they're inlined during compilation)
        if compiled.materialization == Materialization::Ephemeral {
            success_count += 1;
            run_results.push(ModelRunResult {
                model: name.clone(),
                status: RunStatus::Success,
                materialization: "ephemeral".to_string(),
                duration_secs: 0.0,
                error: None,
            });
            continue;
        }

        // Update progress bar
        if let Some(ref pb) = progress {
            pb.set_message(format!("Running: {}", name));
            pb.set_position(executable_idx as u64);
        }
        executable_idx += 1;

        // Run the model (pre-hooks -> materialize -> post-hooks -> contract)
        let model_result = run_single_model(
            ctx.db,
            name,
            compiled,
            ctx.args.full_refresh,
            ctx.wap_schema,
        )
        .await;

        let is_error = matches!(model_result.status, RunStatus::Error);
        let is_wap = compiled.wap
            && ctx.wap_schema.is_some()
            && matches!(
                compiled.materialization,
                Materialization::Table | Materialization::Incremental
            );

        if is_error {
            failure_count += 1;

            // If WAP model failed, skip all transitive dependents
            if is_wap {
                let dependents = get_transitive_dependents(name, ctx.compiled_models);
                for dep in &dependents {
                    failed_models.insert(dep.clone());
                }
            }

            run_results.push(model_result);
            if ctx.args.fail_fast {
                stopped_early = true;
                println!("\n  Stopping due to --fail-fast");
                break;
            }
        } else {
            success_count += 1;

            // Try to get row count for state tracking (non-blocking)
            let qualified_name = match &compiled.schema {
                Some(s) => format!("{}.{}", s, name),
                None => name.clone(),
            };
            let row_count = match ctx
                .db
                .query_count(&format!(
                    "SELECT * FROM {}",
                    quote_qualified(&qualified_name)
                ))
                .await
            {
                Ok(count) => Some(count),
                Err(e) => {
                    eprintln!(
                        "[warn] Failed to get row count for {}: {}",
                        qualified_name, e
                    );
                    None
                }
            };

            // Update state for this model (with checksums for smart builds)
            update_state_for_model(state_file, name, compiled, ctx.compiled_models, row_count);

            run_results.push(model_result);
        }
    }

    // Finish progress bar
    if let Some(pb) = progress {
        pb.finish_with_message("Complete");
    }

    (run_results, success_count, failure_count, stopped_early)
}

/// Execute models in parallel using DAG-aware scheduling
async fn execute_models_parallel(
    ctx: &ExecutionContext<'_>,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));
    let run_results = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(AtomicBool::new(false));

    // Create a semaphore to limit concurrent execution
    let semaphore = Arc::new(Semaphore::new(ctx.args.threads));

    // Track completed models
    let completed = Arc::new(Mutex::new(HashSet::new()));

    // Group models by their dependency level
    let levels = compute_execution_levels(ctx.execution_order, ctx.compiled_models);

    // Count non-ephemeral models for progress bar
    let executable_count = ctx
        .execution_order
        .iter()
        .filter(|name| {
            ctx.compiled_models
                .get(*name)
                .map(|m| m.materialization != Materialization::Ephemeral)
                .unwrap_or(true)
        })
        .count();

    // Create progress bar if not in quiet mode
    let progress = if !ctx.args.quiet && ctx.args.output == OutputFormat::Text {
        let pb = ProgressBar::new(executable_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("#>-"),
        );
        Some(Arc::new(pb))
    } else {
        None
    };

    println!(
        "  [parallel mode: {} threads, {} levels]",
        ctx.args.threads,
        levels.len()
    );

    for level_models in &levels {
        // Check if we should stop
        if stopped.load(Ordering::SeqCst) {
            break;
        }

        // Spawn tasks for all models in this level
        let mut handles = Vec::new();

        for name in level_models {
            // Check if we should stop before starting a new model
            if stopped.load(Ordering::SeqCst) && ctx.args.fail_fast {
                break;
            }

            let name = name.clone();
            let db = Arc::clone(ctx.db);
            let Some(compiled) = ctx.compiled_models.get(&name) else {
                eprintln!(
                    "[warn] Model '{}' missing from compiled_models, skipping",
                    name
                );
                continue;
            };

            // Skip ephemeral models (they're inlined during compilation)
            if compiled.materialization == Materialization::Ephemeral {
                success_count.fetch_add(1, Ordering::SeqCst);
                run_results
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(ModelRunResult {
                        model: name.clone(),
                        status: RunStatus::Success,
                        materialization: "ephemeral".to_string(),
                        duration_secs: 0.0,
                        error: None,
                    });
                completed
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(name);
                continue;
            }
            // Clone fields needed inside the spawned task
            let compiled_owned = compiled.clone();
            let full_refresh = ctx.args.full_refresh;
            let fail_fast = ctx.args.fail_fast;
            let wap_schema_owned = ctx.wap_schema.map(String::from);

            let semaphore = Arc::clone(&semaphore);
            let success_count = Arc::clone(&success_count);
            let failure_count = Arc::clone(&failure_count);
            let run_results = Arc::clone(&run_results);
            let stopped = Arc::clone(&stopped);
            let completed = Arc::clone(&completed);
            let progress = progress.clone();

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        // Semaphore was closed -- treat as cancellation
                        return (name, None);
                    }
                };

                // Check if we should stop
                if stopped.load(Ordering::SeqCst) && fail_fast {
                    return (name, None);
                }

                let model_result = run_single_model(
                    &db,
                    &name,
                    &compiled_owned,
                    full_refresh,
                    wap_schema_owned.as_deref(),
                )
                .await;

                let is_error = matches!(model_result.status, RunStatus::Error);
                if is_error {
                    failure_count.fetch_add(1, Ordering::SeqCst);
                    if fail_fast {
                        stopped.store(true, Ordering::SeqCst);
                        println!("\n  Stopping due to --fail-fast");
                    }
                } else {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }

                run_results
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(model_result);
                completed
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(name.clone());

                // Update progress bar
                if let Some(ref pb) = progress {
                    pb.inc(1);
                }

                (name, Some(()))
            });

            handles.push(handle);
        }

        // Wait for all models in this level to complete
        for handle in handles {
            if let Err(e) = handle.await {
                eprintln!("[warn] Task join error: {}", e);
            }
        }
    }

    // Finish progress bar
    if let Some(pb) = progress {
        pb.finish_with_message("Complete");
    }

    let final_results = run_results
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let final_success = success_count.load(Ordering::SeqCst);
    let final_failure = failure_count.load(Ordering::SeqCst);
    let final_stopped = stopped.load(Ordering::SeqCst);

    // Update state file with results (need to do this after parallel execution)
    for result in &final_results {
        if matches!(result.status, RunStatus::Success) {
            if let Some(compiled) = ctx.compiled_models.get(&result.model) {
                // Query row count (same as sequential path)
                let qualified_name = match &compiled.schema {
                    Some(s) => format!("{}.{}", s, result.model),
                    None => result.model.clone(),
                };
                let row_count = ctx
                    .db
                    .query_count(&format!(
                        "SELECT * FROM {}",
                        quote_qualified(&qualified_name)
                    ))
                    .await
                    .ok();
                update_state_for_model(
                    state_file,
                    &result.model,
                    compiled,
                    ctx.compiled_models,
                    row_count,
                );
            }
        }
    }

    (final_results, final_success, final_failure, final_stopped)
}

/// Compute execution levels - models at the same level have no dependencies on each other
fn compute_execution_levels(
    execution_order: &[String],
    compiled_models: &HashMap<String, CompiledModel>,
) -> Vec<Vec<String>> {
    let mut levels: Vec<Vec<String>> = Vec::new();
    let mut completed: HashSet<String> = HashSet::new();

    let order_set: HashSet<String> = execution_order.iter().cloned().collect();

    // Process models in topological order, grouping by when they become ready
    let mut remaining: Vec<String> = execution_order.to_vec();

    while !remaining.is_empty() {
        let mut current_level = Vec::new();

        for name in &remaining {
            if let Some(compiled) = compiled_models.get(name) {
                // Check if all dependencies are completed or not in our execution set
                let deps_satisfied = compiled
                    .dependencies
                    .iter()
                    .all(|dep| completed.contains(dep) || !order_set.contains(dep));

                if deps_satisfied {
                    current_level.push(name.clone());
                }
            }
        }

        // Mark current level as completed
        for name in &current_level {
            completed.insert(name.clone());
        }

        // Remove current level from remaining (use HashSet for O(1) lookup)
        let current_set: HashSet<&String> = current_level.iter().collect();
        remaining.retain(|name| !current_set.contains(name));

        if !current_level.is_empty() {
            levels.push(current_level);
        } else if !remaining.is_empty() {
            // Safety: if we can't make progress, just add all remaining as single level
            levels.push(remaining.clone());
            break;
        }
    }

    levels
}
