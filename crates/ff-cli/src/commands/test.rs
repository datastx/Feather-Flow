//! Test command implementation

use anyhow::{Context, Result};
use ff_core::model::{parse_test_definition, SchemaTest};
use ff_core::source::SourceFile;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_test::{generator::GeneratedTest, TestRunner};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::cli::{GlobalArgs, TestArgs};

/// Generate tests from source files
fn generate_source_tests(sources: &[SourceFile]) -> Vec<SchemaTest> {
    let mut tests = Vec::new();

    for source in sources {
        for table in &source.tables {
            for column in &table.columns {
                for test_def in &column.tests {
                    // Use the same parsing logic as model tests
                    if let Some(test_type) = parse_test_definition(test_def) {
                        tests.push(SchemaTest {
                            model: format!("{}.{}", source.schema, table.name),
                            column: column.name.clone(),
                            test_type,
                        });
                    }
                }
            }
        }
    }

    tests
}

/// Execute the test command
pub async fn execute(args: &TestArgs, global: &GlobalArgs) -> Result<()> {
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

    // Generate tests from source files
    let source_tests = generate_source_tests(&project.sources);

    // Combine model tests and source tests
    let all_tests: Vec<SchemaTest> = project
        .tests
        .iter()
        .cloned()
        .chain(source_tests.into_iter())
        .collect();

    // Get set of models that have tests defined
    let models_with_tests: std::collections::HashSet<_> =
        all_tests.iter().map(|t| t.model.as_str()).collect();

    // Get tests to run
    let tests_to_run: Vec<&SchemaTest> = all_tests
        .iter()
        .filter(|t| {
            model_filter
                .as_ref()
                .map(|f| f.contains(&t.model))
                .unwrap_or(true)
        })
        .collect();

    // Report models without tests when filtering by model
    if let Some(filter) = &model_filter {
        let models_without_tests: Vec<&str> = filter
            .iter()
            .filter(|m| !models_with_tests.contains(m.as_str()))
            .map(|s| s.as_str())
            .collect();

        if !models_without_tests.is_empty() {
            println!(
                "Skipping {} model(s) without tests: {}\n",
                models_without_tests.len(),
                models_without_tests.join(", ")
            );
        }
    }

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
        // For source tests, the model field already contains schema.table
        let qualified_name = model_qualified_names
            .get(&schema_test.model)
            .map(|s| s.as_str())
            .unwrap_or(&schema_test.model);
        let generated = GeneratedTest::from_schema_test_qualified(schema_test, qualified_name);
        let result = runner.run_test(&generated).await;

        if result.passed {
            passed += 1;
            println!("  ✓ {} [{}ms]", result.name, result.duration.as_millis());
        } else if let Some(error) = &result.error {
            errors += 1;
            println!(
                "  ✗ {} - {} [{}ms]",
                result.name,
                error,
                result.duration.as_millis()
            );
        } else {
            failed += 1;
            println!(
                "  ✗ {} ({} failures) [{}ms]",
                result.name,
                result.failure_count,
                result.duration.as_millis()
            );

            // Show sample failing rows (always show up to 5)
            if !result.sample_failures.is_empty() {
                println!("    Sample failing rows:");
                for (i, row) in result.sample_failures.iter().enumerate() {
                    println!("      {}. {}", i + 1, row);
                }
                if result.failure_count > result.sample_failures.len() {
                    println!(
                        "      ... and {} more",
                        result.failure_count - result.sample_failures.len()
                    );
                }
            }
        }

        // Fail fast if requested
        if args.fail_fast && (!result.passed) {
            break;
        }
    }

    println!();
    println!("Passed: {}, Failed: {}", passed, failed + errors);

    if failed > 0 || errors > 0 {
        // Exit code 2 = Test failures (per spec)
        std::process::exit(2);
    }

    Ok(())
}
