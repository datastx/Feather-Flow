//! Build command implementation
//!
//! Orchestrates seed → per-model (run + test) in topological order.
//! After seeding, each model is materialized and then tested before
//! proceeding to the next model. Any model-run failure or test failure
//! stops the build immediately.

use anyhow::{Context, Result};
use ff_core::config::Materialization;
use ff_jinja::CustomTestRegistry;
use ff_test::TestRunner;
use std::collections::HashMap;
use std::time::Instant;

use crate::cli::{BuildArgs, GlobalArgs, OutputFormat, RunArgs, SeedArgs};
use crate::commands::common::{self, load_project, ExitCode, RunStatus};
use crate::commands::run::{
    create_database_connection, create_schemas, determine_execution_order, load_or_compile_models,
    run_single_model, set_search_path,
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

/// Run schema tests for a single model, stopping on first failure.
///
/// Returns `(passed_count, failed_count)`.
async fn run_model_tests(
    runner: &TestRunner<'_>,
    model_tests: &[&ff_core::model::SchemaTest],
    qualified_name: &str,
    custom_test_registry: &CustomTestRegistry,
    custom_test_env: &minijinja::Environment<'static>,
    quiet: bool,
) -> (usize, usize) {
    let mut passed = 0usize;
    let mut failed = 0usize;

    for schema_test in model_tests {
        let generated = test::generate_test_with_custom_support(
            schema_test,
            qualified_name,
            custom_test_registry,
            custom_test_env,
        );
        let result = runner.run_test(&generated).await;

        if result.passed {
            passed += 1;
            if !quiet {
                println!(
                    "    \u{2713} {} [{}ms]",
                    result.name,
                    result.duration.as_millis()
                );
            }
        } else if let Some(error) = &result.error {
            failed += 1;
            if !quiet {
                println!(
                    "    \u{2717} {} - {} [{}ms]",
                    result.name,
                    error,
                    result.duration.as_millis()
                );
            }
        } else {
            failed += 1;
            if !quiet {
                println!(
                    "    \u{2717} {} ({} failures) [{}ms]",
                    result.name,
                    result.failure_count,
                    result.duration.as_millis()
                );
            }
        }

        if failed > 0 {
            break;
        }
    }

    (passed, failed)
}

/// Execute the build command: seed → per-model (materialize + test).
pub(crate) async fn execute(args: &BuildArgs, global: &GlobalArgs) -> Result<()> {
    let quiet = args.quiet || args.output == OutputFormat::Json;
    let start_time = Instant::now();

    if !quiet {
        println!("Starting build...\n");
    }

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

    if !quiet {
        println!("\n=== Phase 2: Run + Test (per model) ===\n");
    }

    let project = load_project(global)?;
    let db = create_database_connection(&project, global)?;

    let run_args = RunArgs {
        mode: None,
        nodes: args.nodes.clone(),
        exclude: args.exclude.clone(),
        full_refresh: args.full_refresh,
        fail_fast: true,
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
        store_failures: args.store_failures,
        warn_only: false,
        telemetry: false,
    };

    let comment_ctx =
        common::build_query_comment_context(&project.config, global.database.as_deref());

    let compiled_models = load_or_compile_models(&project, &run_args, global, comment_ctx.as_ref())
        .context("Failed to compile models")?;

    common::run_static_analysis_gate(
        &project,
        &compiled_models,
        global,
        args.skip_static_analysis,
        quiet,
    )?;

    let execution_order = determine_execution_order(&compiled_models, &project, &run_args, global)?;

    if execution_order.is_empty() {
        if !quiet {
            println!("No models to run.");
        }
        return Ok(());
    }

    let database = ff_core::config::Config::resolve_database(global.database.as_deref());
    let wap_schema = project.config.get_wap_schema(database.as_deref());

    create_schemas(&db, &compiled_models, global).await?;

    if let Some(ws) = wap_schema {
        db.create_schema_if_not_exists(ws)
            .await
            .with_context(|| format!("Failed to create WAP schema: {}", ws))?;
    }

    set_search_path(&db, &compiled_models, &project, wap_schema, global).await?;

    common::execute_hooks(
        db.as_ref(),
        &project.config.on_run_start,
        "on-run-start",
        global.verbose,
        quiet,
    )
    .await?;

    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let mut custom_test_registry = CustomTestRegistry::new();
    custom_test_registry
        .discover(&macro_paths)
        .context("Failed to discover custom test macros")?;
    let custom_test_env = test::build_custom_test_env(&macro_paths);

    let mut model_qualified_names: HashMap<String, String> =
        HashMap::with_capacity(project.models.len());
    for (name, model) in &project.models {
        let schema = model
            .config
            .schema
            .clone()
            .or_else(|| project.config.get_schema(None).map(|s| s.to_string()));
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

    for name in &execution_order {
        let Some(compiled) = compiled_models.get(name) else {
            eprintln!(
                "[warn] Model '{}' missing from compiled_models, skipping",
                name
            );
            continue;
        };

        if compiled.materialization == Materialization::Ephemeral {
            models_succeeded += 1;
            continue;
        }

        let model_result =
            run_single_model(&db, name, compiled, args.full_refresh, wap_schema).await;

        if matches!(model_result.status, RunStatus::Error) {
            if !quiet {
                println!("\nBuild stopped: model '{}' failed to materialize.", name);
            }
            return Err(ExitCode(4).into());
        }

        let model_tests: Vec<_> = project.tests.iter().filter(|t| t.model == *name).collect();

        let test_count = model_tests.len();

        let qualified_name = model_qualified_names
            .get(name)
            .map(|s| s.as_str())
            .unwrap_or(name);

        let (model_tests_passed, model_tests_failed) = run_model_tests(
            &runner,
            &model_tests,
            qualified_name,
            &custom_test_registry,
            &custom_test_env,
            quiet,
        )
        .await;

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

    if let Err(e) = common::execute_hooks(
        db.as_ref(),
        &project.config.on_run_end,
        "on-run-end",
        global.verbose,
        quiet,
    )
    .await
    {
        eprintln!("Warning: {}", e);
    }

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
