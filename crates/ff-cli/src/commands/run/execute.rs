//! Model execution: sequential and parallel runners, single-model execution.

use ff_core::config::Materialization;
use ff_core::run_state::RunState;
use ff_core::sql_utils::quote_qualified;
use ff_core::state::StateFile;
use ff_db::Database;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::cli::{OutputFormat, RunArgs};
use crate::commands::common::RunStatus;

use super::compile::CompiledModel;
use super::hooks::{execute_hooks, validate_model_contract};
use super::incremental::{execute_incremental, execute_wap, WapParams};
use super::state::{update_state_for_model, ModelRunResult};

/// Create an optional progress bar for model execution.
fn create_progress_bar(count: usize, quiet: bool, output: &OutputFormat) -> Option<ProgressBar> {
    if !quiet && *output == OutputFormat::Text {
        let pb = ProgressBar::new(count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .expect("static progress bar template is valid")
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    }
}

/// Build a schema-qualified name from an optional schema and a model name.
fn build_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", s, name),
        None => name.to_string(),
    }
}

/// Acquire a mutex lock, recovering from a poisoned state if necessary.
///
/// If a previous thread panicked while holding the lock the mutex becomes
/// poisoned.  This helper logs a warning and recovers the inner data so
/// execution can continue.
fn recover_mutex<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|p| {
        eprintln!("[warn] mutex poisoned, recovering");
        p.into_inner()
    })
}

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

/// Execute the materialization strategy for a non-WAP model.
async fn execute_materialization(
    db: &Arc<dyn Database>,
    qualified_name: &str,
    compiled: &CompiledModel,
    full_refresh: bool,
    exec_sql: &str,
) -> ff_db::error::DbResult<()> {
    match compiled.materialization {
        Materialization::View => db.create_view_as(qualified_name, exec_sql, true).await,
        Materialization::Table => db.create_table_as(qualified_name, exec_sql, true).await,
        Materialization::Incremental => {
            execute_incremental(db, qualified_name, compiled, full_refresh, exec_sql).await
        }
        Materialization::Ephemeral => Ok(()),
    }
}

/// Execute a single model: pre-hooks -> materialize -> post-hooks -> contract validation.
///
/// Returns a `ModelRunResult` with the outcome. Callers handle state-file updates
/// because the sequential and parallel paths have different timing requirements.
pub(crate) async fn run_single_model(
    db: &Arc<dyn Database>,
    name: &str,
    compiled: &CompiledModel,
    full_refresh: bool,
    wap_schema: Option<&str>,
) -> ModelRunResult {
    let qualified_name = build_qualified_name(compiled.schema.as_deref(), name);
    let quoted_name = quote_qualified(&qualified_name);

    let model_start = Instant::now();

    if let Err(e) = execute_hooks(db, &compiled.pre_hook, &quoted_name).await {
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

    let is_wap = compiled.wap
        && wap_schema.is_some()
        && matches!(
            compiled.materialization,
            Materialization::Table | Materialization::Incremental
        );

    let result = if is_wap {
        let Some(ws) = wap_schema else {
            // Defensive: is_wap guard above checks is_some(), but avoid panic in production
            let duration = model_start.elapsed();
            return ModelRunResult {
                model: name.to_string(),
                status: RunStatus::Error,
                materialization: compiled.materialization.to_string(),
                duration_secs: duration.as_secs_f64(),
                error: Some("WAP schema unexpectedly missing".to_string()),
            };
        };
        execute_wap(&WapParams {
            db,
            name,
            qualified_name: &qualified_name,
            wap_schema: ws,
            compiled,
            full_refresh,
            exec_sql: &exec_sql,
        })
        .await
    } else {
        execute_materialization(db, &qualified_name, compiled, full_refresh, &exec_sql).await
    };

    match result {
        Ok(_) => {
            if let Err(e) = execute_hooks(db, &compiled.post_hook, &quoted_name).await {
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
    if ctx.args.threads <= 1 {
        return execute_models_sequential(ctx, state_file).await;
    }

    execute_models_parallel(ctx, state_file).await
}

/// Execute all models in order with run state tracking for resume capability
pub(super) async fn execute_models_with_state(
    ctx: &ExecutionContext<'_>,
    state_file: &mut StateFile,
    run_state: &mut RunState,
    run_state_path: &Path,
) -> anyhow::Result<(Vec<ModelRunResult>, usize, usize, bool)> {
    let (run_results, success_count, failure_count, stopped_early) =
        execute_models(ctx, state_file).await;

    for result in &run_results {
        let duration_ms = (result.duration_secs * 1000.0) as u64;
        if matches!(result.status, RunStatus::Success) {
            run_state.mark_completed(&result.model, duration_ms)?;
        } else {
            run_state.mark_failed(
                &result.model,
                result.error.as_deref().unwrap_or("unknown error"),
            )?;
        }
    }

    if let Err(e) = run_state.save(run_state_path) {
        eprintln!("Warning: Failed to save run state: {}", e);
    }

    Ok((run_results, success_count, failure_count, stopped_early))
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

    let progress = create_progress_bar(executable_count, ctx.args.quiet, &ctx.args.output);

    let mut executable_idx = 0;
    for name in ctx.execution_order.iter() {
        let Some(compiled) = ctx.compiled_models.get(name) else {
            eprintln!(
                "[warn] Model '{}' missing from compiled_models, skipping",
                name
            );
            continue;
        };

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

        if let Some(ref pb) = progress {
            pb.set_message(format!("Running: {}", name));
            pb.set_position(executable_idx as u64);
        }
        executable_idx += 1;

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
            let qualified_name = build_qualified_name(compiled.schema.as_deref(), name);
            let row_count = match ctx
                .db
                .query_count(&format!(
                    "SELECT 1 FROM {}",
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

            if let Err(e) =
                update_state_for_model(state_file, name, compiled, ctx.compiled_models, row_count)
            {
                eprintln!("[warn] Failed to update state for '{}': {}", name, e);
            }

            run_results.push(model_result);
        }
    }

    if let Some(pb) = progress {
        pb.finish_with_message("Complete");
    }

    (run_results, success_count, failure_count, stopped_early)
}

/// Async task body for executing a single model in parallel mode.
#[allow(clippy::too_many_arguments)]
async fn execute_model_task(
    db: Arc<dyn Database>,
    name: String,
    compiled: CompiledModel,
    full_refresh: bool,
    wap_schema: Option<String>,
    fail_fast: bool,
    semaphore: Arc<Semaphore>,
    success_count: Arc<AtomicUsize>,
    failure_count: Arc<AtomicUsize>,
    run_results: Arc<Mutex<Vec<ModelRunResult>>>,
    stopped: Arc<AtomicBool>,
    completed: Arc<Mutex<HashSet<String>>>,
    progress: Option<Arc<ProgressBar>>,
) {
    // Semaphore was closed -- treat as cancellation
    let Ok(_permit) = semaphore.acquire().await else {
        return;
    };

    if stopped.load(Ordering::SeqCst) && fail_fast {
        return;
    }

    let model_result =
        run_single_model(&db, &name, &compiled, full_refresh, wap_schema.as_deref()).await;

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

    recover_mutex(&run_results).push(model_result);
    recover_mutex(&completed).insert(name);

    if let Some(ref pb) = progress {
        pb.inc(1);
    }
}

/// Execute models in parallel using DAG-aware scheduling
async fn execute_models_parallel(
    ctx: &ExecutionContext<'_>,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));
    let run_results = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(AtomicBool::new(false));
    let semaphore = Arc::new(Semaphore::new(ctx.args.threads));
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let levels = compute_execution_levels(ctx.execution_order, ctx.compiled_models);

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

    let progress = if !ctx.args.quiet && ctx.args.output == OutputFormat::Text {
        let pb = ProgressBar::new(executable_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})",
                )
                .expect("static progress bar template is valid")
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
        if stopped.load(Ordering::SeqCst) {
            break;
        }

        let mut set = JoinSet::new();

        for name in level_models {
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

            if compiled.materialization == Materialization::Ephemeral {
                success_count.fetch_add(1, Ordering::SeqCst);
                recover_mutex(&run_results).push(ModelRunResult {
                    model: name.clone(),
                    status: RunStatus::Success,
                    materialization: "ephemeral".to_string(),
                    duration_secs: 0.0,
                    error: None,
                });
                recover_mutex(&completed).insert(name);
                continue;
            }

            set.spawn(execute_model_task(
                db,
                name,
                compiled.clone(),
                ctx.args.full_refresh,
                ctx.wap_schema.map(String::from),
                ctx.args.fail_fast,
                Arc::clone(&semaphore),
                Arc::clone(&success_count),
                Arc::clone(&failure_count),
                Arc::clone(&run_results),
                Arc::clone(&stopped),
                Arc::clone(&completed),
                progress.clone(),
            ));
        }

        while let Some(res) = set.join_next().await {
            if let Err(e) = res {
                eprintln!("[warn] Task join error: {}", e);
            }
        }
    }

    if let Some(pb) = progress {
        pb.finish_with_message("Complete");
    }

    let final_results = recover_mutex(&run_results).clone();
    let final_success = success_count.load(Ordering::SeqCst);
    let final_failure = failure_count.load(Ordering::SeqCst);
    let final_stopped = stopped.load(Ordering::SeqCst);

    // Update state file with results (need to do this after parallel execution)
    for result in &final_results {
        if matches!(result.status, RunStatus::Success) {
            if let Some(compiled) = ctx.compiled_models.get(&result.model) {
                let qualified_name =
                    build_qualified_name(compiled.schema.as_deref(), &result.model);
                let row_count = match ctx
                    .db
                    .query_count(&format!(
                        "SELECT 1 FROM {}",
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
                if let Err(e) = update_state_for_model(
                    state_file,
                    &result.model,
                    compiled,
                    ctx.compiled_models,
                    row_count,
                ) {
                    eprintln!(
                        "[warn] Failed to update state for '{}': {}",
                        result.model, e
                    );
                }
            }
        }
    }

    (final_results, final_success, final_failure, final_stopped)
}

/// Check if all dependencies of a model are satisfied (completed or not in the execution set).
fn deps_satisfied(
    name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
    completed: &HashSet<String>,
    order_set: &HashSet<String>,
) -> bool {
    compiled_models.get(name).is_some_and(|compiled| {
        compiled
            .dependencies
            .iter()
            .all(|dep| completed.contains(dep) || !order_set.contains(dep))
    })
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
            if deps_satisfied(name, compiled_models, &completed, &order_set) {
                current_level.push(name.clone());
            } else if !compiled_models.contains_key(name) {
                eprintln!(
                    "[warn] Model '{}' in execution order but not in compiled models",
                    name
                );
            }
        }

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
