//! Test command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::model::{parse_test_definition, SchemaTest, SingularTest, TestSeverity, TestType};
use ff_core::source::SourceFile;
use ff_db::{Database, DatabaseCore, DuckDbBackend};
use ff_jinja::{CustomTestRegistry, JinjaEnvironment};
use ff_test::{generator::GeneratedTest, TestRunner};
use futures::stream::{self, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::cli::{GlobalArgs, OutputFormat, TestArgs};
use crate::commands::common::{self, load_project, TestStatus};

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

/// Shared atomic counters for tracking test execution outcomes
#[derive(Debug)]
struct TestCounters {
    passed: AtomicUsize,
    failed: AtomicUsize,
    warned: AtomicUsize,
    errors: AtomicUsize,
    early_stop: AtomicBool,
    test_results: Mutex<Vec<TestResultOutput>>,
}

impl TestCounters {
    fn new() -> Self {
        Self {
            passed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            warned: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            early_stop: AtomicBool::new(false),
            test_results: Mutex::new(Vec::new()),
        }
    }
}

/// Shared context for test execution that groups related parameters
struct TestRunContext<'a> {
    db: &'a Arc<dyn Database>,
    model_qualified_names: &'a HashMap<String, String>,
    singular_tests: &'a [SingularTest],
    args: &'a TestArgs,
    failures_dir: &'a Option<Arc<std::path::PathBuf>>,
    json_mode: bool,
    custom_registry: &'a Arc<CustomTestRegistry>,
    custom_test_env: &'a minijinja::Environment<'static>,
}

impl std::fmt::Debug for TestRunContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestRunContext")
            .field("json_mode", &self.json_mode)
            .field("singular_tests_count", &self.singular_tests.len())
            .finish_non_exhaustive()
    }
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
                            model: ff_core::model_name::ModelName::new(format!(
                                "{}.{}",
                                source.schema, table.name
                            )),
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
pub(crate) async fn execute(args: &TestArgs, global: &GlobalArgs) -> Result<()> {
    use ff_core::config::Config;

    let project = load_project(global)?;

    let target = Config::resolve_target(global.target.as_deref());

    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    let merged_vars = project.config.get_merged_vars(target.as_deref());

    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&merged_vars, &macro_paths);

    let mut custom_test_registry = CustomTestRegistry::new();
    custom_test_registry
        .discover(&macro_paths)
        .context("Failed to discover custom test macros")?;

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

    let model_filter: Option<std::collections::HashSet<String>> = if args.nodes.is_some() {
        let (_, dag) = common::build_project_dag(&project)?;
        let resolved = common::resolve_nodes(&project, &dag, &args.nodes)?;
        Some(resolved.into_iter().collect())
    } else {
        None
    };

    let source_tests = generate_source_tests(&project.sources);

    let all_tests: Vec<SchemaTest> = project
        .tests
        .iter()
        .cloned()
        .chain(source_tests.into_iter())
        .collect();

    let models_with_tests: std::collections::HashSet<_> =
        all_tests.iter().map(|t| t.model.as_str()).collect();

    let tests_to_run: Vec<&SchemaTest> = all_tests
        .iter()
        .filter(|t| {
            model_filter
                .as_ref()
                .map(|f| f.contains(t.model.as_str()))
                .unwrap_or(true)
        })
        .collect();

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
            println!(
                "{}",
                serde_json::to_string_pretty(&empty_result)
                    .context("Failed to serialize test results")?
            );
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
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!(
                "[warn] Failed to create test_failures directory at {}: {}",
                dir.display(),
                e
            );
        }
        Some(Arc::new(dir))
    } else {
        None
    };

    let custom_registry = Arc::new(custom_test_registry);

    let counters = Arc::new(TestCounters::new());
    let output_lock = Arc::new(Mutex::new(()));
    let custom_test_env = build_custom_test_env(&macro_paths);

    let ctx = TestRunContext {
        db: &db,
        model_qualified_names: &model_qualified_names,
        singular_tests: &project.singular_tests,
        args,
        failures_dir: &failures_dir,
        json_mode,
        custom_registry: &custom_registry,
        custom_test_env: &custom_test_env,
    };

    if thread_count > 1 {
        run_tests_parallel(&ctx, &tests_to_run, &counters, &output_lock, thread_count).await;
    } else {
        run_tests_sequential(&ctx, &tests_to_run, &counters).await;
    }

    let final_passed = counters.passed.load(Ordering::SeqCst);
    let final_failed = counters.failed.load(Ordering::SeqCst);
    let final_warned = counters.warned.load(Ordering::SeqCst);
    let final_errors = counters.errors.load(Ordering::SeqCst);

    if json_mode {
        let results = counters.test_results.lock().await.clone();
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
        println!(
            "{}",
            serde_json::to_string_pretty(&output).context("Failed to serialize test results")?
        );
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
        return Err(crate::commands::common::ExitCode(2).into());
    }

    Ok(())
}

/// Run tests sequentially (original behavior)
async fn run_tests_sequential(
    ctx: &TestRunContext<'_>,
    schema_tests: &[&SchemaTest],
    counters: &Arc<TestCounters>,
) {
    let runner = TestRunner::new(ctx.db.as_ref());

    for schema_test in schema_tests {
        if counters.early_stop.load(Ordering::SeqCst) {
            break;
        }

        let qualified_name = ctx
            .model_qualified_names
            .get(schema_test.model.as_str())
            .map(|s| s.as_str())
            .unwrap_or(schema_test.model.as_str());

        let generated = generate_test_with_custom_support(
            schema_test,
            qualified_name,
            ctx.custom_registry,
            ctx.custom_test_env,
        );
        let result = runner.run_test(&generated).await;

        process_schema_test_result(
            &result,
            schema_test,
            &generated,
            ctx.failures_dir,
            counters,
            ctx.json_mode,
        )
        .await;

        if ctx.args.fail_fast
            && !result.passed
            && schema_test.config.severity == TestSeverity::Error
        {
            counters.early_stop.store(true, Ordering::SeqCst);
        }
    }

    for singular_test in ctx.singular_tests {
        if counters.early_stop.load(Ordering::SeqCst) {
            break;
        }

        let result = run_singular_test(ctx.db.as_ref(), singular_test).await;

        process_singular_test_result(
            &result,
            singular_test,
            ctx.failures_dir,
            counters,
            ctx.json_mode,
        )
        .await;

        if ctx.args.fail_fast && !result.passed {
            counters.early_stop.store(true, Ordering::SeqCst);
        }
    }
}

/// Run tests in parallel
async fn run_tests_parallel(
    ctx: &TestRunContext<'_>,
    schema_tests: &[&SchemaTest],
    counters: &Arc<TestCounters>,
    output_lock: &Arc<Mutex<()>>,
    thread_count: usize,
) {
    // Pre-generate all test SQL (including custom tests) before parallel execution
    // This is done synchronously since Jinja rendering isn't async
    let generated_tests: Vec<(SchemaTest, String, GeneratedTest)> = schema_tests
        .iter()
        .map(|schema_test| {
            let qualified_name = ctx
                .model_qualified_names
                .get(schema_test.model.as_str())
                .cloned()
                .unwrap_or_else(|| schema_test.model.to_string());
            let generated = generate_test_with_custom_support(
                schema_test,
                &qualified_name,
                ctx.custom_registry,
                ctx.custom_test_env,
            );
            ((*schema_test).clone(), qualified_name, generated)
        })
        .collect();

    let schema_test_futures: Vec<_> = generated_tests
        .into_iter()
        .map(|(schema_test, _, generated)| {
            let db = ctx.db.clone();
            let failures_dir = ctx.failures_dir.clone();
            let counters = Arc::clone(counters);
            let output_lock = output_lock.clone();
            let fail_fast = ctx.args.fail_fast;
            let json_mode = ctx.json_mode;

            async move {
                if counters.early_stop.load(Ordering::SeqCst) {
                    return;
                }

                let runner = TestRunner::new(db.as_ref());
                let result = runner.run_test(&generated).await;

                let _lock = output_lock.lock().await;

                process_schema_test_result(
                    &result,
                    &schema_test,
                    &generated,
                    &failures_dir,
                    &counters,
                    json_mode,
                )
                .await;

                if fail_fast && !result.passed && schema_test.config.severity == TestSeverity::Error
                {
                    counters.early_stop.store(true, Ordering::SeqCst);
                }
            }
        })
        .collect();

    stream::iter(schema_test_futures)
        .buffer_unordered(thread_count)
        .collect::<Vec<_>>()
        .await;

    let singular_test_futures: Vec<_> = ctx
        .singular_tests
        .iter()
        .map(|singular_test| {
            let db = ctx.db.clone();
            let singular_test = singular_test.clone();
            let failures_dir = ctx.failures_dir.clone();
            let counters = Arc::clone(counters);
            let output_lock = output_lock.clone();
            let fail_fast = ctx.args.fail_fast;
            let json_mode = ctx.json_mode;

            async move {
                if counters.early_stop.load(Ordering::SeqCst) {
                    return;
                }

                let result = run_singular_test(db.as_ref(), &singular_test).await;

                let _lock = output_lock.lock().await;

                process_singular_test_result(
                    &result,
                    &singular_test,
                    &failures_dir,
                    &counters,
                    json_mode,
                )
                .await;

                if fail_fast && !result.passed {
                    counters.early_stop.store(true, Ordering::SeqCst);
                }
            }
        })
        .collect();

    stream::iter(singular_test_futures)
        .buffer_unordered(thread_count)
        .collect::<Vec<_>>()
        .await;
}

/// Process and print schema test result
async fn process_schema_test_result(
    result: &ff_test::runner::TestResult,
    schema_test: &SchemaTest,
    generated: &GeneratedTest,
    failures_dir: &Option<Arc<std::path::PathBuf>>,
    counters: &TestCounters,
    json_mode: bool,
) {
    let (status, error_msg) = if result.passed {
        counters.passed.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  \u{2713} {} [{}ms]",
                result.name,
                result.duration.as_millis()
            );
        }
        (TestStatus::Pass.to_string(), None)
    } else if let Some(error) = &result.error {
        counters.errors.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  \u{2717} {} - {} [{}ms]",
                result.name,
                error,
                result.duration.as_millis()
            );
        }
        (TestStatus::Error.to_string(), Some(error.clone()))
    } else {
        let is_warning = schema_test.config.severity == TestSeverity::Warn;
        if is_warning {
            counters.warned.fetch_add(1, Ordering::SeqCst);
            if !json_mode {
                println!(
                    "  \u{26a0} {} ({} failures, warn) [{}ms]",
                    result.name,
                    result.failure_count,
                    result.duration.as_millis()
                );
            }
            ("warn".to_string(), None)
        } else {
            counters.failed.fetch_add(1, Ordering::SeqCst);
            if !json_mode {
                println!(
                    "  \u{2717} {} ({} failures) [{}ms]",
                    result.name,
                    result.failure_count,
                    result.duration.as_millis()
                );
            }
            (TestStatus::Fail.to_string(), None)
        }
    };

    let test_output = TestResultOutput {
        name: result.name.clone(),
        status,
        test_type: format!("{:?}", schema_test.test_type),
        model: Some(schema_test.model.to_string()),
        column: Some(schema_test.column.clone()),
        failure_count: result.failure_count,
        duration_secs: result.duration.as_secs_f64(),
        error: error_msg,
    };
    counters.test_results.lock().await.push(test_output);

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

        if let Some(ref dir) = failures_dir {
            store_test_failures(&result.name, &generated.sql, dir).await;
        }
    }
}

/// Process and print singular test result
async fn process_singular_test_result(
    result: &SingularTestResult,
    singular_test: &SingularTest,
    failures_dir: &Option<Arc<std::path::PathBuf>>,
    counters: &TestCounters,
    json_mode: bool,
) {
    let (status, error_msg) = if result.passed {
        counters.passed.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  \u{2713} {} (singular) [{}ms]",
                result.name,
                result.duration.as_millis()
            );
        }
        (TestStatus::Pass.to_string(), None)
    } else if let Some(error) = &result.error {
        counters.errors.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  \u{2717} {} (singular) - {} [{}ms]",
                result.name,
                error,
                result.duration.as_millis()
            );
        }
        (TestStatus::Error.to_string(), Some(error.clone()))
    } else {
        counters.failed.fetch_add(1, Ordering::SeqCst);
        if !json_mode {
            println!(
                "  \u{2717} {} (singular) ({} failures) [{}ms]",
                result.name,
                result.failure_count,
                result.duration.as_millis()
            );
        }
        (TestStatus::Fail.to_string(), None)
    };

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
    counters.test_results.lock().await.push(test_output);

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

        if let Some(ref dir) = failures_dir {
            store_test_failures(&result.name, &singular_test.sql, dir).await;
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
                let sample_failures = match db.query_sample_rows(&test.sql, 5).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        eprintln!(
                            "[warn] Failed to fetch sample failing rows for {}: {}",
                            test.name, e
                        );
                        Vec::new()
                    }
                };

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
async fn store_test_failures(test_name: &str, sql: &str, failures_dir: &std::path::Path) {
    let table_name = test_name
        .replace(|c: char| !c.is_alphanumeric() && c != '_', "_")
        .to_lowercase();

    let db_path = failures_dir.join(format!("{}.duckdb", table_name));

    let db_path_str = match db_path.to_str() {
        Some(s) => s,
        None => {
            eprintln!(
                "[warn] Failed to store test failures for {}: database path is not valid UTF-8",
                table_name
            );
            return;
        }
    };
    match DuckDbBackend::new(db_path_str) {
        Ok(failures_db) => {
            let create_sql = format!("CREATE TABLE IF NOT EXISTS failures AS {}", sql);
            if let Err(e) = failures_db.execute(&create_sql).await {
                eprintln!(
                    "[warn] Failed to store test failures for {}: {}",
                    table_name, e
                );
            }
        }
        Err(e) => {
            eprintln!(
                "[warn] Failed to create failures database for {}: {}",
                table_name, e
            );
        }
    }
}

/// Build a minijinja Environment configured with the first valid macro path.
pub(crate) fn build_custom_test_env(
    macro_paths: &[std::path::PathBuf],
) -> minijinja::Environment<'static> {
    let mut env = minijinja::Environment::new();
    for macro_path in macro_paths {
        if macro_path.exists() && macro_path.is_dir() {
            env.set_loader(minijinja::path_loader(macro_path.clone()));
            break;
        }
    }
    env
}

/// Render a custom test macro, returning the generated SQL.
fn render_custom_test_macro(
    env: &minijinja::Environment<'_>,
    macro_info: &ff_jinja::CustomTestMacro,
    qualified_name: &str,
    column: &str,
    name: &str,
    kwargs: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    match ff_jinja::generate_custom_test_sql(
        env,
        &macro_info.source_file,
        &macro_info.macro_name,
        qualified_name,
        column,
        kwargs,
    ) {
        Ok(sql) => sql,
        Err(e) => format!(
            "-- Error rendering custom test '{}': {}\nSELECT 1 WHERE FALSE",
            name, e
        ),
    }
}

/// Generate test SQL with support for custom test macros
///
/// For built-in test types, uses the standard generator.
/// For custom test types, looks up the macro in the registry and renders it.
pub(crate) fn generate_test_with_custom_support(
    schema_test: &SchemaTest,
    qualified_name: &str,
    custom_registry: &CustomTestRegistry,
    env: &minijinja::Environment<'_>,
) -> GeneratedTest {
    match &schema_test.test_type {
        TestType::Custom { name, kwargs } => {
            if let Some(macro_info) = custom_registry.get(name) {
                let sql = render_custom_test_macro(
                    env,
                    macro_info,
                    qualified_name,
                    &schema_test.column,
                    name,
                    kwargs,
                );
                GeneratedTest::with_custom_sql(schema_test, sql)
            } else {
                let error_sql = format!(
                    "-- Custom test '{}' not found in registered macros\nSELECT 1 AS error_custom_test_not_found",
                    name
                );
                GeneratedTest::with_custom_sql(schema_test, error_sql)
            }
        }
        _ => GeneratedTest::from_schema_test_qualified(schema_test, qualified_name),
    }
}
