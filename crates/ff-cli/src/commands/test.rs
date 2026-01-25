//! Test command implementation

use anyhow::{Context, Result};
use ff_core::model::SchemaTest;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_test::{generator::GeneratedTest, TestRunner};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{GlobalArgs, TestArgs};

/// Execute the test command
pub async fn execute(args: &TestArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Create database connection
    let db: Arc<dyn Database> = Arc::new(
        DuckDbBackend::new(&project.config.database.path)
            .context("Failed to connect to database")?,
    );

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
        let generated = GeneratedTest::from_schema_test(schema_test);
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
