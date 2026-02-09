//! Run command implementation
//!
//! Split into submodules:
//! - `compile` — model compilation, caching, and DAG resolution
//! - `execute` — sequential and parallel model execution
//! - `incremental` — incremental strategies and Write-Audit-Publish (WAP)
//! - `hooks` — pre/post hooks, schema creation, DB connection, contract validation
//! - `state` — run results, state tracking, smart builds, resume support

mod compile;
mod execute;
mod hooks;
mod incremental;
mod state;

use anyhow::{Context, Result};
use chrono::Utc;
use ff_core::config::Materialization;
use ff_core::run_state::RunState;
use ff_core::source::build_source_lookup;
use ff_core::state::StateFile;
use ff_core::ModelName;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use crate::cli::{GlobalArgs, OutputFormat, RunArgs};
use crate::commands::common::{build_schema_catalog, load_project};

use compile::{determine_execution_order, load_or_compile_models};
use execute::{execute_models_with_state, ExecutionContext};
use hooks::{create_database_connection, create_schemas};
use state::{
    compute_config_hash, compute_smart_skips, find_affected_exposures, write_run_results,
    RunResults,
};

/// Execute the run command
pub async fn execute(args: &RunArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project = load_project(global)?;

    let json_mode = args.output == OutputFormat::Json;

    let db = create_database_connection(&project, global)?;

    // Create query comment context if enabled
    let comment_ctx = if project.config.query_comment.enabled {
        let target = ff_core::config::Config::resolve_target(global.target.as_deref());
        Some(ff_core::query_comment::QueryCommentContext::new(
            &project.config.name,
            target.as_deref(),
        ))
    } else {
        None
    };

    let compiled_models = load_or_compile_models(&project, args, global, comment_ctx.as_ref())?;

    // Static analysis gate: validate SQL models before execution
    if !args.skip_static_analysis {
        let has_errors = run_pre_execution_analysis(&project, &compiled_models, global, json_mode)?;
        if has_errors {
            if !json_mode {
                eprintln!("Static analysis found errors. Use --skip-static-analysis to bypass.");
            }
            return Err(crate::commands::common::ExitCode(1).into());
        }
    }

    // Smart build: filter out unchanged models
    let smart_skipped: HashSet<String> = if args.smart {
        compute_smart_skips(&project, &compiled_models, global)?
    } else {
        HashSet::new()
    };

    // Compute config hash for run state validation
    let config_hash = compute_config_hash(&project);

    // Determine run state path
    let run_state_path = args
        .state_file
        .as_ref()
        .map(|s| Path::new(s).to_path_buf())
        .unwrap_or_else(|| project.target_dir().join("run_state.json"));

    // Handle resume mode
    let (execution_order, previous_run_state) = if args.resume {
        state::handle_resume_mode(
            &run_state_path,
            &compiled_models,
            args,
            global,
            &config_hash,
        )?
    } else {
        let order = determine_execution_order(&compiled_models, &project, args, global)?;
        (order, None)
    };

    // Apply smart build filtering
    let execution_order: Vec<String> = if !smart_skipped.is_empty() {
        let before = execution_order.len();
        let filtered: Vec<String> = execution_order
            .into_iter()
            .filter(|m| !smart_skipped.contains(m))
            .collect();
        if !json_mode {
            println!(
                "Smart build: skipping {} unchanged model(s)\n",
                before - filtered.len()
            );
        }
        filtered
    } else {
        execution_order
    };

    if execution_order.is_empty() {
        if json_mode {
            let empty_result = RunResults {
                timestamp: Utc::now(),
                elapsed_secs: 0.0,
                success_count: 0,
                failure_count: 0,
                results: vec![],
            };
            println!("{}", serde_json::to_string_pretty(&empty_result)?);
        } else {
            println!("No models to run.");
        }
        return Ok(());
    }

    if global.verbose {
        eprintln!(
            "[verbose] Running {} models in order: {:?}",
            execution_order.len(),
            execution_order
        );
    }

    // Count non-ephemeral models (ephemeral models are inlined, not executed)
    let ephemeral_count = execution_order
        .iter()
        .filter(|name| {
            compiled_models
                .get(*name)
                .map(|m| m.materialization == Materialization::Ephemeral)
                .unwrap_or(false)
        })
        .count();
    let executable_count = execution_order.len() - ephemeral_count;

    // Show resume summary if applicable (text mode only)
    if !json_mode {
        if let Some(ref prev_state) = previous_run_state {
            let summary = prev_state.summary();
            println!(
                "Resuming: {} skipped, {} to retry, {} pending\n",
                summary.completed, summary.failed, summary.pending
            );
        } else if ephemeral_count > 0 {
            println!(
                "Running {} models ({} ephemeral inlined)...\n",
                executable_count, ephemeral_count
            );
        } else {
            println!("Running {} models...\n", execution_order.len());
        }

        // Show affected exposures
        let affected_exposures = find_affected_exposures(&project, &execution_order);
        if !affected_exposures.is_empty() {
            println!(
                "  {} downstream exposure(s) may be affected:",
                affected_exposures.len()
            );
            for (exposure_name, exposure_type, depends_on) in &affected_exposures {
                let models_affected: Vec<&str> = depends_on
                    .iter()
                    .filter(|d| execution_order.contains(&d.to_string()))
                    .map(|s| s.as_str())
                    .collect();
                println!(
                    "    - {} ({}) via {}",
                    exposure_name,
                    exposure_type,
                    models_affected.join(", ")
                );
            }
            println!();
        }
    }

    // Resolve WAP schema from config
    let target = ff_core::config::Config::resolve_target(global.target.as_deref());
    let wap_schema = project.config.get_wap_schema(target.as_deref());

    create_schemas(&db, &compiled_models, global).await?;

    // Create WAP schema if configured
    if let Some(ref ws) = wap_schema {
        db.create_schema_if_not_exists(ws)
            .await
            .context(format!("Failed to create WAP schema: {}", ws))?;
    }

    // Execute on-run-start hooks (skip if resuming)
    if previous_run_state.is_none() && !project.config.on_run_start.is_empty() {
        if global.verbose {
            eprintln!(
                "[verbose] Executing {} on-run-start hooks",
                project.config.on_run_start.len()
            );
        }
        for hook in &project.config.on_run_start {
            if let Err(e) = db.execute(hook).await {
                println!("  \u{2717} on-run-start hook failed: {}", e);
                return Err(anyhow::anyhow!("on-run-start hook failed: {}", e));
            }
        }
    }

    // Load state for incremental tracking
    let state_path = project.target_dir().join("state.json");
    let mut state_file = StateFile::load(&state_path).unwrap_or_default();

    // Create run state for tracking this execution
    let selection_str = args.select.clone().or(args.models.clone());
    let mut run_state = RunState::new(
        execution_order
            .iter()
            .map(|s| ModelName::new(s.clone()))
            .collect(),
        selection_str,
        config_hash,
    );

    // Save initial run state
    if let Err(e) = run_state.save(&run_state_path) {
        eprintln!("Warning: Failed to save initial run state: {}", e);
    }

    let exec_ctx = ExecutionContext {
        db: &db,
        compiled_models: &compiled_models,
        execution_order: &execution_order,
        args,
        wap_schema: wap_schema.as_deref(),
    };

    let (run_results, success_count, failure_count, stopped_early) =
        execute_models_with_state(&exec_ctx, &mut state_file, &mut run_state, &run_state_path)
            .await;

    // Mark run as completed
    run_state.mark_run_completed();
    if let Err(e) = run_state.save(&run_state_path) {
        eprintln!("Warning: Failed to save final run state: {}", e);
    }

    // Save updated incremental state
    if let Err(e) = state_file.save(&state_path) {
        eprintln!("Warning: Failed to save state file: {}", e);
    }

    // Execute on-run-end hooks (even if models failed, unless we stopped early with --fail-fast)
    if !project.config.on_run_end.is_empty() && !stopped_early {
        if global.verbose {
            eprintln!(
                "[verbose] Executing {} on-run-end hooks",
                project.config.on_run_end.len()
            );
        }
        for hook in &project.config.on_run_end {
            if let Err(e) = db.execute(hook).await {
                println!("  \u{2717} on-run-end hook failed: {}", e);
                // Don't fail the entire run for on-run-end hook failures, just warn
                eprintln!("Warning: on-run-end hook failed: {}", e);
            }
        }
    }

    write_run_results(
        &project,
        &run_results,
        start_time,
        success_count,
        failure_count,
    )?;

    // Output results based on format
    if json_mode {
        let results = RunResults {
            timestamp: Utc::now(),
            elapsed_secs: start_time.elapsed().as_secs_f64(),
            success_count,
            failure_count,
            results: run_results,
        };
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        if stopped_early {
            let remaining = execution_order.len() - run_results.len();
            if remaining > 0 {
                println!("  {} model(s) skipped due to early termination", remaining);
            }
        }

        println!();
        println!(
            "Completed: {} succeeded, {} failed",
            success_count, failure_count
        );
        println!("Total time: {}ms", start_time.elapsed().as_millis());
    }

    if failure_count > 0 {
        return Err(crate::commands::common::ExitCode(4).into());
    }

    Ok(())
}

/// Run DataFusion-based static analysis before execution.
///
/// Returns `true` if there are schema errors that should block execution.
fn run_pre_execution_analysis(
    project: &ff_core::Project,
    compiled_models: &HashMap<String, compile::CompiledModel>,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<bool> {
    use ff_analysis::propagate_schemas;

    if global.verbose {
        eprintln!("[verbose] Running pre-execution static analysis...");
    }

    // Build schema catalog from YAML definitions and external tables
    let source_tables = build_source_lookup(&project.sources);
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    external_tables.extend(source_tables);
    let (schema_catalog, yaml_schemas) = build_schema_catalog(project, &external_tables);

    // Build dependency map and topological order
    let dependencies: HashMap<String, Vec<String>> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.dependencies.clone()))
        .collect();

    let dag =
        ff_core::dag::ModelDag::build(&dependencies).context("Failed to build dependency DAG")?;
    let topo_order = dag
        .topological_order()
        .context("Failed to get topological order")?;

    // Build SQL sources from compiled models
    let sql_sources: HashMap<String, String> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.sql.clone()))
        .collect();

    // Filter topo order to models we have SQL for
    let filtered_order: Vec<String> = topo_order
        .into_iter()
        .filter(|n| sql_sources.contains_key(n))
        .collect();

    if filtered_order.is_empty() {
        return Ok(false);
    }

    // Run schema propagation
    let result = propagate_schemas(
        &filtered_order,
        &sql_sources,
        &yaml_schemas,
        &schema_catalog,
    );

    // Check for schema errors (all mismatches are errors that block execution)
    let mut has_errors = false;
    for (model_name, plan_result) in &result.model_plans {
        for mismatch in &plan_result.mismatches {
            has_errors = true;
            if !json_mode {
                eprintln!(
                    "  [error] {model_name}: {mismatch}",
                    model_name = model_name,
                    mismatch = mismatch
                );
            }
        }
    }

    if global.verbose {
        let plan_count = result.model_plans.len();
        let failure_count = result.failures.len();
        eprintln!(
            "[verbose] Static analysis: {} models planned, {} failures",
            plan_count, failure_count
        );
        for (model, err) in &result.failures {
            eprintln!("[verbose] Static analysis failed for '{}': {}", model, err);
        }
    }

    Ok(has_errors)
}
