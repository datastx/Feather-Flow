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
pub(crate) mod python;
mod state;

use anyhow::{Context, Result};
use chrono::Utc;
use ff_core::config::{Materialization, RunMode};
use ff_core::run_state::RunState;
use ff_core::ModelName;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{CliRunMode, GlobalArgs, OutputFormat, RunArgs};
use crate::commands::common::{self, load_project};

pub(crate) use compile::{determine_execution_order, load_or_compile_models, CompiledModel};
pub(crate) use execute::run_single_model;
use execute::{execute_models_with_state, ExecutionContext};
pub(crate) use hooks::{create_database_connection, create_schemas, set_search_path};
use state::{compute_config_hash, compute_smart_skips, write_run_results, RunResults};

/// Qualify bare table names in compiled SQL to fully-qualified references.
fn qualify_sql_references(
    compiled_models: &mut HashMap<String, CompiledModel>,
    project: &ff_core::Project,
    global: &GlobalArgs,
) {
    let compiled_schemas: std::collections::HashMap<String, Option<String>> = compiled_models
        .iter()
        .map(|(name, cm)| (name.clone(), cm.schema.clone()))
        .collect();
    let qualification_map = common::build_qualification_map(project, &compiled_schemas);
    for (name, compiled) in compiled_models.iter_mut() {
        if compiled.is_python {
            continue;
        }
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

/// Map CLI run mode enum to core config enum.
fn resolve_mode(cli_mode: Option<CliRunMode>, config_mode: Option<&RunMode>) -> RunMode {
    if let Some(m) = cli_mode {
        return match m {
            CliRunMode::Models => RunMode::Models,
            CliRunMode::Test => RunMode::Test,
            CliRunMode::Build => RunMode::Build,
        };
    }
    config_mode.copied().unwrap_or(RunMode::Build)
}

/// Execute the run command
pub(crate) async fn execute(args: &RunArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project = load_project(global)?;

    let mode = resolve_mode(
        args.mode,
        project.config.run.as_ref().map(|r| &r.default_mode),
    );

    match mode {
        RunMode::Models => execute_models_mode(args, global, &project, start_time).await,
        RunMode::Test => execute_test_mode(args, global, &project).await,
        RunMode::Build => execute_build_mode(args, global, &project, start_time).await,
    }
}

/// Execute `--mode test`: run schema tests against existing tables.
async fn execute_test_mode(
    args: &RunArgs,
    global: &GlobalArgs,
    _project: &ff_core::Project,
) -> Result<()> {
    // Delegate to the test module with equivalent args
    let test_args = crate::cli::TestArgs {
        nodes: args.nodes.clone(),
        fail_fast: args.fail_fast,
        store_failures: args.store_failures,
        warn_only: args.warn_only,
        threads: args.threads,
        output: args.output,
        quiet: args.quiet,
    };
    crate::commands::test::execute(&test_args, global).await
}

/// Execute `--mode build`: seeds + functions, then interleaved model+test.
async fn execute_build_mode(
    args: &RunArgs,
    global: &GlobalArgs,
    _project: &ff_core::Project,
    _start_time: Instant,
) -> Result<()> {
    // Delegate to the build module with equivalent args
    let build_args = crate::cli::BuildArgs {
        nodes: args.nodes.clone(),
        exclude: args.exclude.clone(),
        full_refresh: args.full_refresh,
        fail_fast: args.fail_fast,
        threads: args.threads,
        store_failures: args.store_failures,
        skip_static_analysis: args.skip_static_analysis,
        output: args.output,
        quiet: args.quiet,
    };
    crate::commands::build::execute(&build_args, global).await
}

/// Execute `--mode models`: the original run behavior (models only, no tests).
async fn execute_models_mode(
    args: &RunArgs,
    global: &GlobalArgs,
    project: &ff_core::Project,
    start_time: Instant,
) -> Result<()> {
    let json_mode = args.output == OutputFormat::Json;

    let db = create_database_connection(project, global)?;

    let comment_ctx =
        common::build_query_comment_context(&project.config, global.database.as_deref());

    let mut compiled_models = load_or_compile_models(project, args, global, comment_ctx.as_ref())?;
    qualify_sql_references(&mut compiled_models, project, global);

    let compiled_models = Arc::new(compiled_models);

    common::run_static_analysis_gate(
        project,
        &compiled_models,
        global,
        args.skip_static_analysis,
        json_mode,
    )?;

    // Open meta database early (needed for smart build and execution state)
    let meta_db = common::open_meta_db(project);

    let smart_skipped: HashSet<String> = if args.smart {
        compute_smart_skips(&compiled_models, global, meta_db.as_ref())?
    } else {
        HashSet::new()
    };

    let config_hash = compute_config_hash(project);

    let run_state_path = args
        .state_file
        .as_ref()
        .map(|s| Path::new(s).to_path_buf())
        .unwrap_or_else(|| project.target_dir().join("run_state.json"));

    let (execution_order, previous_run_state) = if args.resume {
        state::handle_resume_mode(
            &run_state_path,
            &compiled_models,
            args,
            global,
            &config_hash,
        )?
    } else {
        let order = determine_execution_order(&compiled_models, project, args, global)?;
        (order, None)
    };

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

    let database = ff_core::config::Config::resolve_database(global.database.as_deref());
    let wap_schema = project.config.get_wap_schema(database.as_deref());

    create_schemas(&db, &compiled_models, global).await?;

    if let Some(ws) = wap_schema {
        db.create_schema_if_not_exists(ws)
            .await
            .with_context(|| format!("Failed to create WAP schema: {}", ws))?;
    }

    set_search_path(&db, &compiled_models, project, wap_schema, global).await?;

    if previous_run_state.is_none() {
        common::execute_hooks(
            db.as_ref(),
            &project.config.on_run_start,
            "on-run-start",
            global.verbose,
            false,
        )
        .await?;
    }

    let selection_str = args.nodes.clone();
    let mut run_state = RunState::new(
        execution_order
            .iter()
            .filter_map(|s| ModelName::try_new(s.clone()))
            .collect(),
        selection_str,
        config_hash,
    );

    if let Err(e) = run_state.save(&run_state_path) {
        eprintln!("Warning: Failed to save initial run state: {}", e);
    }

    let meta_ids = meta_db
        .as_ref()
        .and_then(|db| common::populate_meta_phase1(db, project, "run", args.nodes.as_deref()));
    let (meta_run_id, meta_model_id_map) = match &meta_ids {
        Some((_project_id, run_id, model_id_map)) => (Some(*run_id), Some(model_id_map)),
        None => (None, None),
    };

    let db_path_str = project
        .config
        .get_database_config(None)
        .map(|c| c.path.clone())
        .unwrap_or_default();
    let db_path_ref = db_path_str.as_str();

    let exec_ctx = ExecutionContext {
        db: &db,
        compiled_models: Arc::clone(&compiled_models),
        execution_order: &execution_order,
        args,
        wap_schema,
        meta_db: meta_db.as_ref(),
        meta_run_id,
        meta_model_id_map,
        db_path: Some(db_path_ref),
    };

    let (run_results, success_count, failure_count, stopped_early) =
        execute_models_with_state(&exec_ctx, &mut run_state, &run_state_path).await?;

    run_state.mark_run_completed();
    if let Err(e) = run_state.save(&run_state_path) {
        eprintln!("Warning: Failed to save final run state: {}", e);
    }

    if let (Some(ref meta_db), Some(run_id)) = (&meta_db, meta_run_id) {
        let status = if failure_count > 0 {
            "error"
        } else {
            "success"
        };
        common::complete_meta_run(meta_db, run_id, status);
    }

    if !stopped_early {
        if let Err(e) = common::execute_hooks(
            db.as_ref(),
            &project.config.on_run_end,
            "on-run-end",
            global.verbose,
            false,
        )
        .await
        {
            eprintln!("Warning: {}", e);
        }
    }

    write_run_results(
        project,
        &run_results,
        start_time,
        success_count,
        failure_count,
    )?;

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
