//! Test command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::model::{parse_test_definition, SchemaTest, SingularTest, TestSeverity, TestType};
use ff_core::source::SourceFile;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::{CustomTestRegistry, JinjaEnvironment};
use ff_test::{generator::GeneratedTest, TestRunner};
use futures::stream::{self, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::cli::{GlobalArgs, OutputFormat, TestArgs};

/// Test result for JSON output
#[derive(Debug, Clone, Serialize)]
struct TestResultOutput {
    name: String,
    status: String, // "pass", "fail", "warn", "error"
    test_type: String,
    model: Option<String>,
    column: Option<String>,
    failure_count: usize,
    duration_secs: f64,
    error: Option<String>,
}

/// Test results summary for JSON output
#[derive(Debug, Serialize)]
struct TestResults {
    timestamp: DateTime<Utc>,
    elapsed_secs: f64,
    total_tests: usize,
    passed: usize,
    failed: usize,
    warned: usize,
    errors: usize,
    results: Vec<TestResultOutput>,
}

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
                            config: ff_core::model::TestConfig::default(),
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
    use ff_core::config::Config;

    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Resolve target from CLI flag or FF_TARGET env var
    let target = Config::resolve_target(global.target.as_deref());

    // Get database config, applying target overrides if specified
    let db_config = project
        .config
        .get_database_config(target.as_deref())
        .context("Failed to get database configuration")?;

    if global.verbose {
        if let Some(ref target_name) = target {
            eprintln!(
                "[verbose] Using target '{}' with database: {}",
                target_name, db_config.path
            );
        }
    }

    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(&db_config.path).context("Failed to connect to database")?);

    // Get merged vars with target overrides
    let merged_vars = project.config.get_merged_vars(target.as_deref());

    // Get macro paths and set up Jinja environment for custom tests
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&merged_vars, &macro_paths);

    // Discover custom test macros
    let mut custom_test_registry = CustomTestRegistry::new();
    custom_test_registry.discover(&macro_paths);

    if global.verbose && !custom_test_registry.is_empty() {
        eprintln!(
            "[verbose] Discovered {} custom test macro(s): {}",
            custom_test_registry.len(),
            custom_test_registry
                .list()
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Build a map of model name -> qualified name (with schema if specified)
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

    let total_schema_tests = tests_to_run.len();
    let total_singular_tests = project.singular_tests.len();
    let json_mode = args.output == OutputFormat::Json;
    let start_time = Instant::now();

    if total_schema_tests == 0 && total_singular_tests == 0 {
        if json_mode {
            let empty_result = TestResults {
                timestamp: Utc::now(),
                elapsed_secs: 0.0,
                total_tests: 0,
                passed: 0,
                failed: 0,
                warned: 0,
                errors: 0,
                results: vec![],
            };
            println!("{}", serde_json::to_string_pretty(&empty_result)?);
        } else {
            println!("No tests to run.");
        }
        return Ok(());
    }

    let total_tests = total_schema_tests + total_singular_tests;
    let thread_count = args.threads.max(1);

    if !json_mode {
        if thread_count > 1 {
            println!(
                "Running {} tests with {} threads...\n",
                total_tests, thread_count
            );
        } else {
            println!("Running {} tests...\n", total_tests);
        }
    }

    // Create target/test_failures directory if --store-failures is set
    let failures_dir = if args.store_failures {
        let dir = project.target_dir().join("test_failures");
        std::fs::create_dir_all(&dir).ok();
        Some(Arc::new(dir))
    } else {
        None
    };

    // Shared counters for execution
    let passed = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let warned = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let early_stop = Arc::new(AtomicBool::new(false));
    let output_lock = Arc::new(Mutex::new(()));
    let test_results: Arc<Mutex<Vec<TestResultOutput>>> = Arc::new(Mutex::new(Vec::new()));

    // Wrap Jinja environment and custom registry in Arc for sharing
    let jinja_env = Arc::new(jinja);
    let custom_registry = Arc::new(custom_test_registry);

    // Run tests based on thread count
    if thread_count > 1 {
        // Parallel execution
        run_tests_parallel(
            &db,
            &tests_to_run,
            &model_qualified_names,
            &project.singular_tests,
            args,
            &failures_dir,
            thread_count,
            &passed,
            &failed,
            &warned,
            &errors,
            &early_stop,
            &output_lock,
            &test_results,
            json_mode,
            &jinja_env,
            &custom_registry,
            &macro_paths,
        )
        .await;
    } else {
        // Sequential execution (original behavior)
        run_tests_sequential(
            &db,
            &tests_to_run,
            &model_qualified_names,
            &project.singular_tests,
            args,
            &failures_dir,
            &passed,
            &failed,
            &warned,
            &errors,
            &early_stop,
            &output_lock,
            &test_results,
            json_mode,
            &jinja_env,
            &custom_registry,
            &macro_paths,
        )
        .await;
    }

    let final_passed = passed.load(Ordering::SeqCst);
    let final_failed = failed.load(Ordering::SeqCst);
    let final_warned = warned.load(Ordering::SeqCst);
    let final_errors = errors.load(Ordering::SeqCst);

    if json_mode {
        let results = test_results.lock().await.clone();
        let output = TestResults {
            timestamp: Utc::now(),
            elapsed_secs: start_time.elapsed().as_secs_f64(),
            total_tests,
            passed: final_passed,
            failed: final_failed + final_errors,
            warned: final_warned,
            errors: final_errors,
            results,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!();
        if final_warned > 0 {
            println!(
                "Passed: {}, Failed: {}, Warned: {}",
                final_passed,
                final_failed + final_errors,
                final_warned
            );
        } else {
            println!(
                "Passed: {}, Failed: {}",
                final_passed,
                final_failed + final_errors
            );
        }
    }

    if (final_failed > 0 || final_errors > 0) && !args.warn_only {
        // Exit code 2 = Test failures (per spec)
        std::process::exit(2);
    }

    Ok(())
}

/// Run tests sequentially (original behavior)
#[allow(clippy::too_many_arguments)]
async fn run_tests_sequential(
    db: &Arc<dyn Database>,
    schema_tests: &[&SchemaTest],
    model_qualified_names: &HashMap<String, String>,
    singular_tests: &[SingularTest],
    args: &TestArgs,
    failures_dir: &Option<Arc<std::path::PathBuf>>,
    passed: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    warned: &Arc<AtomicUsize>,
    errors: &Arc<AtomicUsize>,
    early_stop: &Arc<AtomicBool>,
    _output_lock: &Arc<Mutex<()>>,
    test_results: &Arc<Mutex<Vec<TestResultOutput>>>,
    json_mode: bool,
    jinja_env: &Arc<JinjaEnvironment<'_>>,
    custom_registry: &Arc<CustomTestRegistry>,
    macro_paths: &[std::path::PathBuf],
) {
    let runner = TestRunner::new(db.as_ref());

    // Run schema tests
    for schema_test in schema_tests {
        if early_stop.load(Ordering::SeqCst) {
            break;
        }

        let qualified_name = model_qualified_names
            .get(&schema_test.model)
            .map(|s| s.as_str())
            .unwrap_or(&schema_test.model);

        // Generate test SQL, handling custom tests specially
        let generated = generate_test_with_custom_support(
            schema_test,
            qualified_name,
            jinja_env,
            custom_registry,
            macro_paths,
        );
        let result = runner.run_test(&generated).await;

        process_schema_test_result(
            &result,
            schema_test,
            &generated,
            db.as_ref(),
            failures_dir,
            passed,
            failed,
            warned,
            errors,
            test_results,
            json_mode,
        )
        .await;

        // Fail fast if requested (but not for warning-severity tests)
        if args.fail_fast && !result.passed && schema_test.config.severity == TestSeverity::Error {
            early_stop.store(true, Ordering::SeqCst);
        }
    }

    // Run singular tests
    for singular_test in singular_tests {
        if early_stop.load(Ordering::SeqCst) {
            break;
        }

        let result = run_singular_test(db.as_ref(), singular_test).await;

        process_singular_test_result(
            &result,
            singular_test,
            db.as_ref(),
            failures_dir,
            passed,
            failed,
            errors,
            test_results,
            json_mode,
        )
        .await;

        // Fail fast if requested
        if args.fail_fast && !result.passed {
            early_stop.store(true, Ordering::SeqCst);
        }
    }
}

/// Run tests in parallel
#[allow(clippy::too_many_arguments)]
async fn run_tests_parallel(
    db: &Arc<dyn Database>,
    schema_tests: &[&SchemaTest],
    model_qualified_names: &HashMap<String, String>,
    singular_tests: &[SingularTest],
    args: &TestArgs,
    failures_dir: &Option<Arc<std::path::PathBuf>>,
    thread_count: usize,
    passed: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    warned: &Arc<AtomicUsize>,
    errors: &Arc<AtomicUsize>,
    early_stop: &Arc<AtomicBool>,
    output_lock: &Arc<Mutex<()>>,
    test_results: &Arc<Mutex<Vec<TestResultOutput>>>,
    json_mode: bool,
    jinja_env: &Arc<JinjaEnvironment<'_>>,
    custom_registry: &Arc<CustomTestRegistry>,
    macro_paths: &[std::path::PathBuf],
) {
    // Pre-generate all test SQL (including custom tests) before parallel execution
    // This is done synchronously since Jinja rendering isn't async
    let generated_tests: Vec<(SchemaTest, String, GeneratedTest)> = schema_tests
        .iter()
        .map(|schema_test| {
            let qualified_name = model_qualified_names
                .get(&schema_test.model)
                .cloned()
                .unwrap_or_else(|| schema_test.model.clone());
            let generated = generate_test_with_custom_support(
                schema_test,
                &qualified_name,
                jinja_env,
                custom_registry,
                macro_paths,
            );
            ((*schema_test).clone(), qualified_name, generated)
        })
        .collect();

    // Prepare schema test tasks
    let schema_test_futures: Vec<_> = generated_tests
        .into_iter()
        .map(|(schema_test, _qualified_name, generated)| {
            let db = db.clone();
            let failures_dir = failures_dir.clone();
            let passed = passed.clone();
            let failed = failed.clone();
            let warned = warned.clone();
            let errors = errors.clone();
            let early_stop = early_stop.clone();
            let output_lock = output_lock.clone();
            let test_results = test_results.clone();
            let fail_fast = args.fail_fast;

            async move {
                if early_stop.load(Ordering::SeqCst) {
                    return;
                }

                let runner = TestRunner::new(db.as_ref());
                let result = runner.run_test(&generated).await;

                // Lock for output
                let _lock = output_lock.lock().await;

                process_schema_test_result(
                    &result,
                    &schema_test,
                    &generated,
                    db.as_ref(),
                    &failures_dir,
                    &passed,
                    &failed,
                    &warned,
                    &errors,
                    &test_results,
                    json_mode,
                )
                .await;

                // Fail fast if requested (but not for warning-severity tests)
                if fail_fast && !result.passed && schema_test.config.severity == TestSeverity::Error
                {
                    early_stop.store(true, Ordering::SeqCst);
                }
            }
        })
        .collect();

    // Run schema tests in parallel with buffer
    stream::iter(schema_test_futures)
        .buffer_unordered(thread_count)
        .collect::<Vec<_>>()
        .await;

    // Prepare singular test tasks
    let singular_test_futures: Vec<_> = singular_tests
        .iter()
        .map(|singular_test| {
            let db = db.clone();
            let singular_test = singular_test.clone();
            let failures_dir = failures_dir.clone();
            let passed = passed.clone();
            let failed = failed.clone();
            let errors = errors.clone();
            let early_stop = early_stop.clone();
            let output_lock = output_lock.clone();
            let test_results = test_results.clone();
            let fail_fast = args.fail_fast;

            async move {
                if early_stop.load(Ordering::SeqCst) {
                    return;
                }

                let result = run_singular_test(db.as_ref(), &singular_test).await;

                // Lock for output
                let _lock = output_lock.lock().await;

                process_singular_test_result(
                    &result,
                    &singular_test,
                    db.as_ref(),
                    &failures_dir,
                    &passed,
                    &failed,
                    &errors,
                    &test_results,
                    json_mode,
                )
                .await;

                // Fail fast if requested
                if fail_fast && !result.passed {
                    early_stop.store(true, Ordering::SeqCst);
                }
            }
        })
        .collect();

    // Run singular tests in parallel with buffer
    stream::iter(singular_test_futures)
        .buffer_unordered(thread_count)
        .collect::<Vec<_>>()
        .await;
}

/// Process and print schema test result
#[allow(clippy::too_many_arguments)]
async fn process_schema_test_result(
    result: &ff_test::runner::TestResult,
    schema_test: &SchemaTest,
    generated: &GeneratedTest,
    db: &dyn Database,
    failures_dir: &Option<Arc<std::path::PathBuf>>,
    passed: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    warned: &Arc<AtomicUsize>,
    errors: &Arc<AtomicUsize>,
    test_results: &Arc<Mutex<Vec<TestResultOutput>>>,
    json_mode: bool,
) {
    let (status, error_msg) = if result.passed {
        passed.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!("  ✓ {} [{}ms]", result.name, result.duration.as_millis());
        }
        ("pass".to_string(), None)
    } else if let Some(error) = &result.error {
        errors.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  ✗ {} - {} [{}ms]",
                result.name,
                error,
                result.duration.as_millis()
            );
        }
        ("error".to_string(), Some(error.clone()))
    } else {
        let is_warning = schema_test.config.severity == TestSeverity::Warn;
        if is_warning {
            warned.fetch_add(1, Ordering::SeqCst);
            if !json_mode {
                println!(
                    "  ⚠ {} ({} failures, warn) [{}ms]",
                    result.name,
                    result.failure_count,
                    result.duration.as_millis()
                );
            }
            ("warn".to_string(), None)
        } else {
            failed.fetch_add(1, Ordering::SeqCst);
            if !json_mode {
                println!(
                    "  ✗ {} ({} failures) [{}ms]",
                    result.name,
                    result.failure_count,
                    result.duration.as_millis()
                );
            }
            ("fail".to_string(), None)
        }
    };

    // Add to results for JSON output
    let test_output = TestResultOutput {
        name: result.name.clone(),
        status,
        test_type: format!("{:?}", schema_test.test_type),
        model: Some(schema_test.model.clone()),
        column: Some(schema_test.column.clone()),
        failure_count: result.failure_count,
        duration_secs: result.duration.as_secs_f64(),
        error: error_msg,
    };
    test_results.lock().await.push(test_output);

    // Show sample failing rows (text mode only)
    if !json_mode && !result.passed && result.error.is_none() && !result.sample_failures.is_empty()
    {
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

        // Store failures if requested
        if let Some(ref dir) = failures_dir {
            store_test_failures(db, &result.name, &generated.sql, dir).await;
        }
    }
}

/// Process and print singular test result
#[allow(clippy::too_many_arguments)]
async fn process_singular_test_result(
    result: &SingularTestResult,
    singular_test: &SingularTest,
    db: &dyn Database,
    failures_dir: &Option<Arc<std::path::PathBuf>>,
    passed: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    errors: &Arc<AtomicUsize>,
    test_results: &Arc<Mutex<Vec<TestResultOutput>>>,
    json_mode: bool,
) {
    let (status, error_msg) = if result.passed {
        passed.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  ✓ {} (singular) [{}ms]",
                result.name,
                result.duration.as_millis()
            );
        }
        ("pass".to_string(), None)
    } else if let Some(error) = &result.error {
        errors.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  ✗ {} (singular) - {} [{}ms]",
                result.name,
                error,
                result.duration.as_millis()
            );
        }
        ("error".to_string(), Some(error.clone()))
    } else {
        failed.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  ✗ {} (singular) ({} failures) [{}ms]",
                result.name,
                result.failure_count,
                result.duration.as_millis()
            );
        }
        ("fail".to_string(), None)
    };

    // Add to results for JSON output
    let test_output = TestResultOutput {
        name: result.name.clone(),
        status,
        test_type: "singular".to_string(),
        model: None,
        column: None,
        failure_count: result.failure_count,
        duration_secs: result.duration.as_secs_f64(),
        error: error_msg,
    };
    test_results.lock().await.push(test_output);

    // Show sample failing rows (text mode only)
    if !json_mode && !result.passed && result.error.is_none() && !result.sample_failures.is_empty()
    {
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

        // Store failures if requested
        if let Some(ref dir) = failures_dir {
            store_test_failures(db, &result.name, &singular_test.sql, dir).await;
        }
    }
}

/// Result of running a singular test
#[derive(Clone)]
struct SingularTestResult {
    name: String,
    passed: bool,
    failure_count: usize,
    sample_failures: Vec<String>,
    error: Option<String>,
    duration: std::time::Duration,
}

/// Run a singular test (SQL that should return 0 rows)
async fn run_singular_test(db: &dyn Database, test: &SingularTest) -> SingularTestResult {
    let start = Instant::now();

    // Execute the SQL and check if it returns any rows
    match db.query_count(&test.sql).await {
        Ok(count) => {
            if count == 0 {
                SingularTestResult {
                    name: test.name.clone(),
                    passed: true,
                    failure_count: 0,
                    sample_failures: Vec::new(),
                    error: None,
                    duration: start.elapsed(),
                }
            } else {
                // Test failed - get sample failing rows
                let sample_failures = db.query_sample_rows(&test.sql, 5).await.unwrap_or_default();

                SingularTestResult {
                    name: test.name.clone(),
                    passed: false,
                    failure_count: count,
                    sample_failures,
                    error: None,
                    duration: start.elapsed(),
                }
            }
        }
        Err(e) => SingularTestResult {
            name: test.name.clone(),
            passed: false,
            failure_count: 0,
            sample_failures: Vec::new(),
            error: Some(e.to_string()),
            duration: start.elapsed(),
        },
    }
}

/// Store failing rows to a table in target/test_failures/
async fn store_test_failures(
    _db: &dyn Database,
    test_name: &str,
    sql: &str,
    failures_dir: &std::path::Path,
) {
    // Create a table name from the test name (sanitize it)
    let table_name = test_name
        .replace(|c: char| !c.is_alphanumeric() && c != '_', "_")
        .to_lowercase();

    // Create a DuckDB file for the failures
    let db_path = failures_dir.join(format!("{}.duckdb", table_name));

    if let Ok(failures_db) = DuckDbBackend::new(db_path.to_str().unwrap_or(":memory:")) {
        // Create table with failing rows
        let create_sql = format!("CREATE TABLE IF NOT EXISTS failures AS {}", sql);
        let _ = failures_db.execute(&create_sql).await;
    }
}

/// Generate test SQL with support for custom test macros
///
/// For built-in test types, uses the standard generator.
/// For custom test types, looks up the macro in the registry and renders it.
fn generate_test_with_custom_support(
    schema_test: &SchemaTest,
    qualified_name: &str,
    _jinja_env: &JinjaEnvironment<'_>,
    custom_registry: &CustomTestRegistry,
    macro_paths: &[std::path::PathBuf],
) -> GeneratedTest {
    match &schema_test.test_type {
        TestType::Custom { name, kwargs } => {
            // Look up the custom test macro
            if let Some(macro_info) = custom_registry.get(name) {
                // Create a minijinja Environment with the macro paths loaded
                let mut env = minijinja::Environment::new();
                for macro_path in macro_paths {
                    if macro_path.exists() && macro_path.is_dir() {
                        env.set_loader(minijinja::path_loader(macro_path.clone()));
                        break;
                    }
                }

                // Try to render the custom test SQL
                match ff_jinja::generate_custom_test_sql(
                    &env,
                    &macro_info.source_file,
                    &macro_info.macro_name,
                    qualified_name,
                    &schema_test.column,
                    kwargs,
                ) {
                    Ok(sql) => GeneratedTest::with_custom_sql(schema_test, sql),
                    Err(e) => {
                        // Return a test that will fail with the error message
                        let error_sql = format!(
                            "-- Error rendering custom test '{}': {}\nSELECT 1 WHERE FALSE",
                            name, e
                        );
                        GeneratedTest::with_custom_sql(schema_test, error_sql)
                    }
                }
            } else {
                // Custom test not found - return a placeholder that shows an error
                let error_sql = format!(
                    "-- Custom test '{}' not found in registered macros\nSELECT 1 AS error_custom_test_not_found",
                    name
                );
                GeneratedTest::with_custom_sql(schema_test, error_sql)
            }
        }
        _ => {
            // Built-in test types use the standard generator
            GeneratedTest::from_schema_test_qualified(schema_test, qualified_name)
        }
    }
}
