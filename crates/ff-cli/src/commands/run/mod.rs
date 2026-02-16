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
use ff_core::state::StateFile;
use ff_core::ModelName;
use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use crate::cli::{GlobalArgs, OutputFormat, RunArgs};
use crate::commands::common::{self, load_project};

pub(crate) use compile::{determine_execution_order, load_or_compile_models, CompiledModel};
pub(crate) use execute::run_single_model;
use execute::{execute_models_with_state, ExecutionContext};
pub(crate) use hooks::{create_database_connection, create_schemas, set_search_path};
use state::{compute_config_hash, compute_smart_skips, write_run_results, RunResults};

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

    let mut compiled_models = load_or_compile_models(&project, args, global, comment_ctx.as_ref())?;

    // Qualify table references: rewrite bare names to 3-part database.schema.table
    {
        let compiled_schemas: std::collections::HashMap<String, Option<String>> = compiled_models
            .iter()
            .map(|(name, cm)| (name.clone(), cm.schema.clone()))
            .collect();
        let qualification_map = common::build_qualification_map(&project, &compiled_schemas);

        for (name, compiled) in &mut compiled_models {
            match ff_sql::qualify_table_references(&compiled.sql, &qualification_map) {
                Ok(qualified) => compiled.sql = qualified,
                Err(e) => {
                    if global.verbose {
                        eprintln!("[verbose] Failed to qualify references in {}: {}", name, e);
                    }
                }
            }
        }
    }

    // Static analysis gate: validate SQL models before execution
    if !args.skip_static_analysis {
        let has_errors =
            common::run_pre_execution_analysis(&project, &compiled_models, global, json_mode)?;
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
    }

    // Resolve WAP schema from config
    let target = ff_core::config::Config::resolve_target(global.target.as_deref());
    let wap_schema = project.config.get_wap_schema(target.as_deref());

    create_schemas(&db, &compiled_models, global).await?;

    // Create WAP schema if configured
    if let Some(ws) = wap_schema {
        db.create_schema_if_not_exists(ws)
            .await
            .with_context(|| format!("Failed to create WAP schema: {}", ws))?;
    }

    set_search_path(&db, &compiled_models, &project, wap_schema, global).await?;

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

    let state_path = project.target_dir().join("state.json");
    let mut state_file = StateFile::load(&state_path).unwrap_or_default();

    let selection_str = args.nodes.clone();
    let mut run_state = RunState::new(
        execution_order
            .iter()
            .filter_map(|s| ModelName::try_new(s.clone()))
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
        wap_schema,
    };

    let (run_results, success_count, failure_count, stopped_early) =
        execute_models_with_state(&exec_ctx, &mut state_file, &mut run_state, &run_state_path)
            .await?;

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
