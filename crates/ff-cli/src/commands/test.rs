//! Test command implementation

use anyhow::{Context, Result};
use ff_core::model::SchemaTest;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_test::{generator::GeneratedTest, TestRunner};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{GlobalArgs, TestArgs};

/// Execute the test command
pub async fn execute(args: &TestArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Create database connection (use --target override if provided)
    let db_path = global
        .target
        .as_ref()
        .unwrap_or(&project.config.database.path);
    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(db_path).context("Failed to connect to database")?);

    // Build a map of model name -> qualified name (with schema if specified)
    let jinja = JinjaEnvironment::new(&project.config.vars);
    let mut model_qualified_names: HashMap<String, String> = HashMap::new();

    for (name, model) in &project.models {
        // Get schema from rendered config
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
            None => name.clone(),
        };
        model_qualified_names.insert(name.clone(), qualified_name);
    }

    // Filter tests based on --models argument
    let model_filter: Option<Vec<String>> = args.models.as_ref().map(|m| {
        m.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });

    // Get tests to run
    let tests_to_run: Vec<&SchemaTest> = project
        .tests
        .iter()
        .filter(|t| {
            model_filter
                .as_ref()
                .map(|f| f.contains(&t.model))
                .unwrap_or(true)
        })
        .collect();

    if tests_to_run.is_empty() {
        println!("No tests to run.");
        return Ok(());
    }

    println!("Running {} tests...\n", tests_to_run.len());

    let runner = TestRunner::new(db.as_ref());
    let mut passed = 0;
    let mut failed = 0;
    let mut errors = 0;

    for schema_test in tests_to_run {
        // Get the qualified name for this model
        let qualified_name = model_qualified_names
            .get(&schema_test.model)
            .map(|s| s.as_str())
            .unwrap_or(&schema_test.model);
        let generated = GeneratedTest::from_schema_test_qualified(schema_test, qualified_name);
        let result = runner.run_test(&generated).await;

        if result.passed {
            passed += 1;
            println!(
                "  PASS {} [{:.2}s]",
                result.name,
                result.duration.as_secs_f64()
            );
        } else if let Some(error) = &result.error {
            errors += 1;
            println!(
                "  ERROR {} - {} [{:.2}s]",
                result.name,
                error,
                result.duration.as_secs_f64()
            );
        } else {
            failed += 1;
            println!(
                "  FAIL {} ({} failures) [{:.2}s]",
                result.name,
                result.failure_count,
                result.duration.as_secs_f64()
            );

            // Show sample failing rows
            if global.verbose && result.failure_count > 0 {
                eprintln!("    Failing rows: {} found", result.failure_count);
            }
        }

        // Fail fast if requested
        if args.fail_fast && (!result.passed) {
            break;
        }
    }

    let total_duration = start_time.elapsed();

    println!();
    println!(
        "Test Results: {} passed, {} failed, {} errors in {:.2}s",
        passed,
        failed,
        errors,
        total_duration.as_secs_f64()
    );

    if failed > 0 || errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}
