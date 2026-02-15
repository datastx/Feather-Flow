//! Build command implementation
//!
//! Orchestrates seed → per-model (run + test) in topological order.
//! After seeding, each model is materialized and then tested before
//! proceeding to the next model. Any model-run failure or test failure
//! stops the build immediately.

use anyhow::{Context, Result};
use ff_core::config::Materialization;
use ff_jinja::{CustomTestRegistry, JinjaEnvironment};
use ff_test::TestRunner;
use std::collections::HashMap;
use std::time::Instant;

use crate::cli::{BuildArgs, GlobalArgs, OutputFormat, RunArgs, SeedArgs};
use crate::commands::common::{self, load_project, ExitCode, RunStatus};
use crate::commands::run::{
    create_database_connection, create_schemas, determine_execution_order, load_or_compile_models,
    run_single_model, set_search_path, CompiledModel,
};
use crate::commands::{seed, test};

/// Extract a structured exit code from an anyhow error.
///
/// Returns `Ok(None)` on success, `Ok(Some(code))` for structured `ExitCode`
/// failures, and propagates real errors via `Err`.
fn classify_phase_result(result: Result<()>) -> Result<Option<i32>> {
    match result {
        Ok(()) => Ok(None),
        Err(err) => match err.downcast_ref::<ExitCode>() {
            Some(ec) => Ok(Some(ec.0)),
            None => Err(err),
        },
    }
}

/// Run DataFusion-based static analysis before execution.
///
/// Returns `true` if there are schema errors that should block execution.
fn run_pre_execution_analysis(
    project: &ff_core::Project,
    compiled_models: &HashMap<String, CompiledModel>,
    global: &GlobalArgs,
    quiet: bool,
) -> Result<bool> {
    if global.verbose {
        eprintln!("[verbose] Running pre-execution static analysis...");
    }

    let external_tables = common::build_external_tables_lookup(project);

    let dependencies: HashMap<String, Vec<String>> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.dependencies.clone()))
        .collect();

    let dag =
        ff_core::dag::ModelDag::build(&dependencies).context("Failed to build dependency DAG")?;
    let topo_order = dag
        .topological_order()
        .context("Failed to get topological order")?;

    let sql_sources: HashMap<String, String> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.sql.clone()))
        .collect();

    if sql_sources.is_empty() {
        return Ok(false);
    }

    let output =
        common::run_static_analysis_pipeline(project, &sql_sources, &topo_order, &external_tables)?;
    let result = &output.result;

    let (_, plan_count, failure_count) = common::report_static_analysis_results(
        result,
        |model_name, mismatch| {
            if !quiet {
                let label = if mismatch.is_error() { "error" } else { "warn" };
                eprintln!("  [{label}] {model_name}: {mismatch}");
            }
        },
        |model, err| {
            if !quiet {
                eprintln!("  [error] {model}: planning failed: {err}");
            }
        },
    );
    if global.verbose {
        eprintln!(
            "[verbose] Static analysis: {} models planned, {} failures",
            plan_count, failure_count
        );
    }

    Ok(output.has_errors)
}

/// Execute the build command: seed → per-model (materialize + test).
pub async fn execute(args: &BuildArgs, global: &GlobalArgs) -> Result<()> {
    let quiet = args.quiet || args.output == OutputFormat::Json;
    let start_time = Instant::now();

    if !quiet {
        println!("Starting build...\n");
    }

    // ── Phase 1: Seed ───────────────────────────────────────────────────
    if !quiet {
        println!("=== Phase 1: Seed ===\n");
    }
    let seed_args = SeedArgs {
        seeds: None,
        full_refresh: args.full_refresh,
        show_columns: false,
    };
    if let Some(code) = classify_phase_result(seed::execute(&seed_args, global).await)? {
        if !quiet {
            println!("\nBuild stopped: seed phase failed.");
        }
        return Err(ExitCode(code).into());
    }

    // ── Phase 2: Compile + per-model run/test ───────────────────────────
    if !quiet {
        println!("\n=== Phase 2: Run + Test (per model) ===\n");
    }

    let project = load_project(global)?;
    let db = create_database_connection(&project, global)?;

    // Build a RunArgs for compile & execution-order helpers
    let run_args = RunArgs {
        nodes: args.nodes.clone(),
        exclude: args.exclude.clone(),
        full_refresh: args.full_refresh,
        fail_fast: true, // build always fails fast
        no_cache: false,
        defer: None,
        state: None,
        threads: args.threads,
        resume: false,
        retry_failed: false,
        state_file: None,
        output: args.output,
        quiet: args.quiet,
        smart: false,
        skip_static_analysis: args.skip_static_analysis,
    };

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

    let compiled_models =
        load_or_compile_models(&project, &run_args, global, comment_ctx.as_ref())?;

    // Static analysis gate
    if !args.skip_static_analysis {
        let has_errors = run_pre_execution_analysis(&project, &compiled_models, global, quiet)?;
        if has_errors {
            if !quiet {
                eprintln!("Static analysis found errors. Use --skip-static-analysis to bypass.");
            }
            return Err(ExitCode(1).into());
        }
    }

    let execution_order = determine_execution_order(&compiled_models, &project, &run_args, global)?;

    if execution_order.is_empty() {
        if !quiet {
            println!("No models to run.");
        }
        return Ok(());
    }

    // Resolve WAP schema from config
    let target = ff_core::config::Config::resolve_target(global.target.as_deref());
    let wap_schema = project.config.get_wap_schema(target.as_deref());

    create_schemas(&db, &compiled_models, global).await?;

    if let Some(ws) = wap_schema {
        db.create_schema_if_not_exists(ws)
            .await
            .with_context(|| format!("Failed to create WAP schema: {}", ws))?;
    }

    set_search_path(&db, &compiled_models, &project, wap_schema, global).await?;

    // Execute on-run-start hooks
    if !project.config.on_run_start.is_empty() {
        if global.verbose {
            eprintln!(
                "[verbose] Executing {} on-run-start hooks",
                project.config.on_run_start.len()
            );
        }
        for hook in &project.config.on_run_start {
            if let Err(e) = db.execute(hook).await {
                if !quiet {
                    println!("  \u{2717} on-run-start hook failed: {}", e);
                }
                return Err(anyhow::anyhow!("on-run-start hook failed: {}", e));
            }
        }
    }

    // Set up test infrastructure
    let merged_vars = project.config.get_merged_vars(target.as_deref());
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&merged_vars, &macro_paths);

    let mut custom_test_registry = CustomTestRegistry::new();
    custom_test_registry
        .discover(&macro_paths)
        .context("Failed to discover custom test macros")?;

    // Build model → qualified-name map (same logic as test command)
    let mut model_qualified_names: HashMap<String, String> = HashMap::new();
    for (name, model) in &project.models {
        let schema = if let Ok((_, config_values)) = jinja.render_with_config(&model.raw_sql) {
            config_values
                .get("schema")
                .and_then(|v| v.as_str())
                .map(String::from)
                .or_else(|| project.config.schema.clone())
        } else {
            project.config.schema.clone()
        };
        let qualified_name = match schema {
            Some(s) => format!("{}.{}", s, name),
            None => name.to_string(),
        };
        model_qualified_names.insert(name.to_string(), qualified_name);
    }

    let runner = TestRunner::new(db.as_ref());

    let executable_count = execution_order
        .iter()
        .filter(|name| {
            compiled_models
                .get(*name)
                .map(|m| m.materialization != Materialization::Ephemeral)
                .unwrap_or(true)
        })
        .count();

    if !quiet {
        println!("Running {} model(s)...\n", executable_count);
    }

    let mut models_succeeded = 0usize;
    let mut total_tests_passed = 0usize;
    let mut total_tests_failed = 0usize;

    // ── Per-model loop ──────────────────────────────────────────────────
    for name in &execution_order {
        let Some(compiled) = compiled_models.get(name) else {
            eprintln!(
                "[warn] Model '{}' missing from compiled_models, skipping",
                name
            );
            continue;
        };

        // Skip ephemeral models (inlined during compilation)
        if compiled.materialization == Materialization::Ephemeral {
            models_succeeded += 1;
            continue;
        }

        // (a) Materialize the model
        let model_result =
            run_single_model(&db, name, compiled, args.full_refresh, wap_schema).await;

        if matches!(model_result.status, RunStatus::Error) {
            if !quiet {
                println!("\nBuild stopped: model '{}' failed to materialize.", name);
            }
            return Err(ExitCode(4).into());
        }

        // (b) Collect and run tests for this model
        let model_tests: Vec<_> = project.tests.iter().filter(|t| t.model == *name).collect();

        let test_count = model_tests.len();
        let mut model_tests_passed = 0usize;
        let mut model_tests_failed = 0usize;

        let qualified_name = model_qualified_names
            .get(name)
            .map(|s| s.as_str())
            .unwrap_or(name);

        for schema_test in &model_tests {
            let generated = test::generate_test_with_custom_support(
                schema_test,
                qualified_name,
                &custom_test_registry,
                &macro_paths,
            );
            let result = runner.run_test(&generated).await;

            if result.passed {
                model_tests_passed += 1;
                if !quiet {
                    println!(
                        "    \u{2713} {} [{}ms]",
                        result.name,
                        result.duration.as_millis()
                    );
                }
            } else if let Some(error) = &result.error {
                model_tests_failed += 1;
                if !quiet {
                    println!(
                        "    \u{2717} {} - {} [{}ms]",
                        result.name,
                        error,
                        result.duration.as_millis()
                    );
                }
            } else {
                model_tests_failed += 1;
                if !quiet {
                    println!(
                        "    \u{2717} {} ({} failures) [{}ms]",
                        result.name,
                        result.failure_count,
                        result.duration.as_millis()
                    );
                }
            }

            if model_tests_failed > 0 {
                break; // stop testing this model on first failure
            }
        }

        total_tests_passed += model_tests_passed;
        total_tests_failed += model_tests_failed;

        if model_tests_failed > 0 {
            if !quiet {
                println!("\nBuild stopped: test failure for model '{}'.", name);
            }
            return Err(ExitCode(2).into());
        }

        models_succeeded += 1;

        if !quiet && test_count > 0 {
            println!(
                "  \u{2713} {} ({}) \u{2014} {}/{} tests passed",
                name, compiled.materialization, model_tests_passed, test_count
            );
        }
    }

    // Execute on-run-end hooks
    if !project.config.on_run_end.is_empty() {
        if global.verbose {
            eprintln!(
                "[verbose] Executing {} on-run-end hooks",
                project.config.on_run_end.len()
            );
        }
        for hook in &project.config.on_run_end {
            if let Err(e) = db.execute(hook).await {
                if !quiet {
                    println!("  \u{2717} on-run-end hook failed: {}", e);
                }
                eprintln!("Warning: on-run-end hook failed: {}", e);
            }
        }
    }

    // ── Summary ─────────────────────────────────────────────────────────
    if !quiet {
        println!();
        println!(
            "Build completed successfully: {} model(s) materialized, {}/{} tests passed",
            models_succeeded,
            total_tests_passed,
            total_tests_passed + total_tests_failed,
        );
        println!("Total time: {}ms", start_time.elapsed().as_millis());
    }

    Ok(())
}
