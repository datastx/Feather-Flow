//! Shared utilities for CLI commands

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::{Config, IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::source::build_source_lookup;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use crate::cli::GlobalArgs;

/// Error type representing a non-zero process exit code.
///
/// Use `return Err(ExitCode(N).into())` instead of `std::process::exit(N)`
/// so that RAII destructors run and cleanup happens properly.
#[derive(Debug)]
pub(crate) struct ExitCode(pub(crate) i32);

impl fmt::Display for ExitCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Intentionally empty: ExitCode is a control-flow mechanism, not a
        // user-facing error. If anyhow's Display chain ever reaches this
        // (e.g. downcast_ref fails in main.rs), we don't want "exit code N"
        // leaking into stderr.
        write!(f, "")
    }
}

impl std::error::Error for ExitCode {}

/// Status for model run / compile / snapshot operations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Success,
    Error,
    Skipped,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunStatus::Success => write!(f, "success"),
            RunStatus::Error => write!(f, "error"),
            RunStatus::Skipped => write!(f, "skipped"),
        }
    }
}

/// Status for schema test results.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TestStatus {
    Pass,
    Fail,
    Error,
}

impl fmt::Display for TestStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestStatus::Pass => write!(f, "pass"),
            TestStatus::Fail => write!(f, "fail"),
            TestStatus::Error => write!(f, "error"),
        }
    }
}

/// Parse hook SQL strings from captured Jinja config values.
///
/// Handles both single-string and array-of-strings representations.
pub(crate) fn parse_hooks_from_config(
    config_values: &HashMap<String, minijinja::Value>,
    key: &str,
) -> Vec<String> {
    config_values
        .get(key)
        .map(|v| {
            if let Some(s) = v.as_str() {
                // Single string hook
                vec![s.to_string()]
            } else if v.kind() == minijinja::value::ValueKind::Seq {
                // Array of hooks
                v.try_iter()
                    .map(|iter| {
                        iter.filter_map(|item| item.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        })
        .unwrap_or_default()
}

/// Resolve nodes from the project using selector-aware filtering with DAG support.
///
/// Parses each comma-separated token through `Selector::parse()`, applies them
/// against the DAG, and returns the union in topological order.
/// If `nodes_arg` is `None`, returns all models in topological order.
pub(crate) fn resolve_nodes(
    project: &Project,
    dag: &ff_core::dag::ModelDag,
    nodes_arg: &Option<String>,
) -> Result<Vec<String>> {
    use ff_core::selector::apply_selectors;

    Ok(apply_selectors(nodes_arg, &project.models, dag)?)
}

/// Build a lookup set of all external tables including sources.
pub(crate) fn build_external_tables_lookup(project: &Project) -> HashSet<String> {
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    let source_tables = build_source_lookup(&project.sources);
    external_tables.extend(source_tables);
    external_tables
}

/// Parse a materialization string from Jinja config values.
pub(crate) fn parse_materialization(s: &str) -> Materialization {
    match s {
        "table" => Materialization::Table,
        "incremental" => Materialization::Incremental,
        "ephemeral" => Materialization::Ephemeral,
        _ => Materialization::View,
    }
}

/// Parse an incremental strategy string from Jinja config values.
pub(crate) fn parse_incremental_strategy(s: &str) -> IncrementalStrategy {
    match s {
        "merge" => IncrementalStrategy::Merge,
        "delete+insert" | "delete_insert" => IncrementalStrategy::DeleteInsert,
        _ => IncrementalStrategy::Append,
    }
}

/// Parse an on_schema_change string from Jinja config values.
pub(crate) fn parse_on_schema_change(s: &str) -> OnSchemaChange {
    match s {
        "fail" => OnSchemaChange::Fail,
        "append_new_columns" => OnSchemaChange::AppendNewColumns,
        _ => OnSchemaChange::Ignore,
    }
}

/// Parse various timestamp formats into a UTC DateTime.
pub(crate) fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];

    for fmt in &formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
    }

    // Try parsing as RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try date-only format
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(datetime) = date.and_hms_opt(0, 0, 0) {
            return Some(DateTime::from_naive_utc_and_offset(datetime, Utc));
        }
    }

    None
}

/// Freshness status shared between source and model freshness commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessStatus {
    Pass,
    Warn,
    Error,
    RuntimeError,
}

impl fmt::Display for FreshnessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FreshnessStatus::Pass => write!(f, "pass"),
            FreshnessStatus::Warn => write!(f, "warn"),
            FreshnessStatus::Error => write!(f, "error"),
            FreshnessStatus::RuntimeError => write!(f, "runtime_error"),
        }
    }
}

/// Build a schema catalog from project model YAML definitions and external tables.
///
/// Iterates all project models, converts YAML column definitions to `TypedColumn`s
/// with parsed SQL types and nullability, and populates the schema catalog.
/// External tables are added with empty schemas.
///
/// Returns `(schema_catalog, yaml_schemas)`.
pub(crate) fn build_schema_catalog(
    project: &Project,
    external_tables: &HashSet<String>,
) -> (
    ff_analysis::SchemaCatalog,
    HashMap<ff_core::ModelName, ff_analysis::RelSchema>,
) {
    use ff_analysis::{parse_sql_type, Nullability, RelSchema, TypedColumn};

    let mut schema_catalog: ff_analysis::SchemaCatalog = HashMap::new();
    let mut yaml_schemas: HashMap<ff_core::ModelName, RelSchema> = HashMap::new();

    for (name, model) in &project.models {
        let Some(schema) = &model.schema else {
            continue;
        };
        let columns: Vec<TypedColumn> = schema
            .columns
            .iter()
            .map(|col| {
                let sql_type = parse_sql_type(&col.data_type);
                let has_not_null = col
                    .constraints
                    .iter()
                    .any(|c| matches!(c, ff_core::ColumnConstraint::NotNull));
                let nullability = if has_not_null {
                    Nullability::NotNull
                } else {
                    Nullability::Unknown
                };
                TypedColumn {
                    name: col.name.clone(),
                    source_table: None,
                    sql_type,
                    nullability,
                    provenance: vec![],
                }
            })
            .collect();
        let rel_schema = RelSchema::new(columns);
        schema_catalog.insert(name.to_string(), rel_schema.clone());
        yaml_schemas.insert(name.clone(), rel_schema);
    }

    // Add source tables to catalog — use column definitions if available,
    // fall back to empty schema for sources without column metadata
    for source_file in &project.sources {
        for table in &source_file.tables {
            if schema_catalog.contains_key(&table.name) {
                continue;
            }
            if table.columns.is_empty() {
                schema_catalog.insert(table.name.clone(), RelSchema::empty());
            } else {
                let columns: Vec<TypedColumn> = table
                    .columns
                    .iter()
                    .map(|col| {
                        let sql_type = parse_sql_type(&col.data_type);
                        TypedColumn {
                            name: col.name.clone(),
                            source_table: None,
                            sql_type,
                            nullability: Nullability::Unknown,
                            provenance: vec![],
                        }
                    })
                    .collect();
                schema_catalog.insert(table.name.clone(), RelSchema::new(columns));
            }
        }
    }

    // Add remaining external tables with empty schemas
    for ext in external_tables {
        if !schema_catalog.contains_key(ext) {
            schema_catalog.insert(ext.clone(), RelSchema::empty());
        }
    }

    (schema_catalog, yaml_schemas)
}

/// Load a project from the directory specified in global CLI arguments.
pub(crate) fn load_project(global: &GlobalArgs) -> Result<Project> {
    Project::load(&global.project_dir).context("Failed to load project")
}

/// Build a DAG from a project by rendering Jinja, parsing SQL, and extracting
/// dependencies for every model.
///
/// Models that fail Jinja rendering or SQL parsing are included in the DAG
/// with no dependencies (disconnected nodes) so that selectors that reference
/// them by name still work.
///
/// Returns `(dependencies_map, dag)`.
pub(crate) fn build_project_dag(
    project: &Project,
) -> Result<(HashMap<String, Vec<String>>, ff_core::dag::ModelDag)> {
    let jinja = ff_jinja::JinjaEnvironment::new(&project.config.vars);
    let parser = ff_sql::SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let external_tables = build_external_tables_lookup(project);
    let known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();

    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    for (name, model) in &project.models {
        let rendered = match jinja.render(&model.raw_sql) {
            Ok(sql) => sql,
            Err(_) => {
                dependencies.insert(name.to_string(), Vec::new());
                continue;
            }
        };
        let stmts = match parser.parse(&rendered) {
            Ok(s) => s,
            Err(_) => {
                dependencies.insert(name.to_string(), Vec::new());
                continue;
            }
        };
        let deps = ff_sql::extract_dependencies(&stmts);
        let (model_deps, _) =
            ff_sql::extractor::categorize_dependencies(deps, &known_models, &external_tables);
        dependencies.insert(name.to_string(), model_deps);
    }

    let dag =
        ff_core::dag::ModelDag::build(&dependencies).context("Failed to build dependency graph")?;
    Ok((dependencies, dag))
}

/// Build user function stubs for static analysis from project functions.
///
/// Converts each scalar `FunctionDef` into a `UserFunctionStub` that can
/// be registered in the DataFusion `FeatherFlowProvider`. Table functions
/// are skipped (they require output schema registration in the `SchemaCatalog`).
pub(crate) fn build_user_function_stubs(project: &Project) -> Vec<ff_analysis::UserFunctionStub> {
    use ff_core::function::FunctionReturn;

    let mut stubs = Vec::new();
    let mut skipped_table_fns = 0u32;

    for f in &project.functions {
        match &f.returns {
            FunctionReturn::Scalar { data_type } => {
                let sig = f.signature();
                if let Some(stub) = ff_analysis::UserFunctionStub::new(
                    sig.name.to_string(),
                    sig.arg_types,
                    data_type.clone(),
                ) {
                    stubs.push(stub);
                }
            }
            FunctionReturn::Table { .. } => {
                skipped_table_fns += 1;
            }
        }
    }

    if skipped_table_fns > 0 {
        eprintln!(
            "Note: {skipped_table_fns} table function(s) excluded from static analysis (not yet supported)"
        );
    }

    stubs
}

/// Result of a static analysis pipeline run.
///
/// Contains the propagation result from DataFusion plus the set of external
/// tables used to build the schema catalog.
pub(crate) struct StaticAnalysisOutput {
    /// The propagation result from DataFusion
    pub result: ff_analysis::PropagationResult,
    /// Whether any schema-mismatch errors were found
    pub has_errors: bool,
}

/// Run the shared static analysis pipeline (schema catalog + propagation).
///
/// This is the common core used by `compile`, `validate`, and `run` commands.
/// Callers are responsible for reporting results in their own format.
pub(crate) fn run_static_analysis_pipeline(
    project: &Project,
    sql_sources: &HashMap<String, String>,
    topo_order: &[String],
    external_tables: &HashSet<String>,
) -> Result<StaticAnalysisOutput> {
    use ff_analysis::propagate_schemas;

    let (schema_catalog, yaml_schemas) = build_schema_catalog(project, external_tables);

    let filtered_order: Vec<String> = topo_order
        .iter()
        .filter(|n| sql_sources.contains_key(n.as_str()))
        .cloned()
        .collect();

    let yaml_string_map: HashMap<String, ff_analysis::RelSchema> = yaml_schemas
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();

    let user_fn_stubs = build_user_function_stubs(project);
    let result = propagate_schemas(
        &filtered_order,
        sql_sources,
        &yaml_string_map,
        &schema_catalog,
        &user_fn_stubs,
    );

    let has_errors = result
        .model_plans
        .values()
        .any(|pr| !pr.mismatches.is_empty());

    Ok(StaticAnalysisOutput { result, has_errors })
}

/// Report static analysis results: mismatches and failures.
///
/// Iterates over mismatch diagnostics and failures from the propagation result.
/// Returns `(mismatch_count, plan_count, failure_count)`.
///
/// The `on_mismatch` callback is called for each schema mismatch with
/// `(model_name, mismatch_display)`. The `on_failure` callback is called
/// for each model that failed planning with `(model_name, error_message)`.
pub(crate) fn report_static_analysis_results(
    result: &ff_analysis::PropagationResult,
    mut on_mismatch: impl FnMut(&str, &str),
    mut on_failure: impl FnMut(&str, &str),
) -> (usize, usize, usize) {
    let mut mismatch_count = 0;

    // Sort model names for deterministic output ordering
    let mut model_names: Vec<&String> = result.model_plans.keys().collect();
    model_names.sort();

    for model_name in model_names {
        let plan_result = &result.model_plans[model_name];
        for mismatch in &plan_result.mismatches {
            on_mismatch(model_name, &mismatch.to_string());
            mismatch_count += 1;
        }
    }

    // Sort failure keys for deterministic output ordering
    let mut failure_names: Vec<&String> = result.failures.keys().collect();
    failure_names.sort();

    for model in failure_names {
        let err = &result.failures[model];
        on_failure(model, &err.to_string());
    }

    (
        mismatch_count,
        result.model_plans.len(),
        result.failures.len(),
    )
}

/// Generic wrapper for command results written to JSON.
///
/// Many commands (run, snapshot, etc.) produce a JSON file with the same
/// envelope: a timestamp, elapsed seconds, success/failure counts, and a
/// vec of per-item results.  `CommandResults<T>` captures that pattern so
/// each command only needs to define its per-item result type.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CommandResults<T: Serialize> {
    pub timestamp: DateTime<Utc>,
    pub elapsed_secs: f64,
    pub success_count: usize,
    pub failure_count: usize,
    pub results: Vec<T>,
}

/// Serialize `data` as pretty-printed JSON and write it to `path`.
///
/// Creates any missing parent directories before writing.  Returns an
/// `anyhow::Result` with context describing which step failed.
pub(crate) fn write_json_results<T: Serialize + ?Sized>(path: &Path, data: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("Failed to create target directory")?;
    }
    let json = serde_json::to_string_pretty(data).context("Failed to serialize results")?;
    std::fs::write(path, json).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Table-printing utilities
// ---------------------------------------------------------------------------

/// Calculate column widths for a table given headers and row data.
///
/// For each column, returns the maximum width across the header and all
/// row values so that data aligns when printed with left-padding.
pub(crate) fn calculate_column_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    widths
}

/// Print a formatted table to stdout.
///
/// Calculates column widths from `headers` and `rows`, then prints
/// a left-aligned header row, a separator line of dashes, and each
/// data row.  Columns are separated by two spaces.
///
/// # Examples
///
/// ```ignore
/// print_table(
///     &["NAME", "TYPE"],
///     &[vec!["orders".into(), "model".into()]],
/// );
/// // NAME    TYPE
/// // ------  -----
/// // orders  model
/// ```
pub(crate) fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let widths = calculate_column_widths(headers, rows);

    // Print header
    let header_parts: Vec<String> = headers
        .iter()
        .zip(&widths)
        .map(|(h, &w)| format!("{:<width$}", h, width = w))
        .collect();
    println!("{}", header_parts.join("  "));

    // Print separator
    let sep_parts: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep_parts.join("  "));

    // Print rows
    for row in rows {
        let row_parts: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, &w)| format!("{:<width$}", cell, width = w))
            .collect();
        println!("{}", row_parts.join("  "));
    }
}

/// Print just the header and separator lines for a table.
///
/// This is useful for commands that need to print rows individually
/// (e.g. to interleave extra output like error messages between rows).
/// Use [`calculate_column_widths`] to obtain the `widths` parameter.
pub(crate) fn print_table_header(headers: &[&str], widths: &[usize]) {
    let header_parts: Vec<String> = headers
        .iter()
        .zip(widths)
        .map(|(h, &w)| format!("{:<width$}", h, width = w))
        .collect();
    println!("{}", header_parts.join("  "));

    let sep_parts: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep_parts.join("  "));
}

/// Create a database connection from a config and optional target override.
///
/// Resolves the target via `Config::resolve_target`, gets the database
/// configuration with `Config::get_database_config`, and creates a
/// `DuckDbBackend` wrapped in an `Arc<dyn Database>`.
pub(crate) fn create_database_connection(
    config: &Config,
    target: Option<&str>,
) -> Result<Arc<dyn Database>> {
    let resolved_target = Config::resolve_target(target);
    let db_config = config
        .get_database_config(resolved_target.as_deref())
        .context("Failed to get database configuration")?;
    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(&db_config.path).context("Failed to connect to database")?);
    Ok(db)
}
