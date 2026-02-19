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

/// Status for model run / compile operations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RunStatus {
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
pub(crate) enum TestStatus {
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
                vec![s.to_string()]
            } else if v.kind() == minijinja::value::ValueKind::Seq {
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
    project
        .config
        .external_tables
        .iter()
        .cloned()
        .chain(build_source_lookup(&project.sources))
        .collect()
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
    HashMap<ff_core::ModelName, Arc<ff_analysis::RelSchema>>,
) {
    use ff_analysis::{parse_sql_type, Nullability, RelSchema, TypedColumn};

    let mut schema_catalog: ff_analysis::SchemaCatalog =
        HashMap::with_capacity(project.models.len());
    let mut yaml_schemas: HashMap<ff_core::ModelName, Arc<RelSchema>> =
        HashMap::with_capacity(project.models.len());

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
        let rel_schema = Arc::new(RelSchema::new(columns));
        schema_catalog.insert(name.to_string(), Arc::clone(&rel_schema));
        yaml_schemas.insert(name.clone(), rel_schema);
    }

    // Add source tables to catalog — use column definitions if available,
    // fall back to empty schema for sources without column metadata
    for source_file in &project.sources {
        for table in &source_file.tables {
            if schema_catalog.contains_key(&table.name) {
                continue;
            }
            let schema = build_source_table_schema(table);
            schema_catalog.insert(table.name.clone(), Arc::new(schema));
        }
    }

    // Add remaining external tables with empty schemas
    for ext in external_tables {
        if !schema_catalog.contains_key(ext) {
            schema_catalog.insert(ext.clone(), Arc::new(RelSchema::empty()));
        }
    }

    (schema_catalog, yaml_schemas)
}

/// Load a project from the directory specified in global CLI arguments.
pub(crate) fn load_project(global: &GlobalArgs) -> Result<Project> {
    Project::load(&global.project_dir).context("Failed to load project")
}

/// Build a [`RelSchema`](ff_analysis::RelSchema) from a source table's column definitions.
///
/// Returns an empty schema if the table has no columns defined.
fn build_source_table_schema(table: &ff_core::SourceTable) -> ff_analysis::RelSchema {
    use ff_analysis::{parse_sql_type, Nullability, RelSchema, TypedColumn};

    if table.columns.is_empty() {
        return RelSchema::empty();
    }
    let columns: Vec<TypedColumn> = table
        .columns
        .iter()
        .map(|col| {
            let sql_type = parse_sql_type(&col.data_type);
            let has_not_null = col
                .tests
                .iter()
                .any(|t| matches!(t, ff_core::model::TestDefinition::Simple(s) if s == "not_null"));
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
    RelSchema::new(columns)
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
    let jinja = build_jinja_env(project);
    let parser = ff_sql::SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let external_tables = build_external_tables_lookup(project);
    let known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();

    let mut dependencies: HashMap<String, Vec<String>> =
        HashMap::with_capacity(project.models.len());

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
        let (mut model_deps, _, unknown_deps) =
            ff_sql::extractor::categorize_dependencies_with_unknown(
                deps,
                &known_models,
                &external_tables,
            );

        // Resolve table function transitive dependencies
        let (func_model_deps, _) = resolve_function_dependencies(
            &unknown_deps,
            project,
            &parser,
            &known_models,
            &external_tables,
        );
        model_deps.extend(func_model_deps);
        model_deps.sort();
        model_deps.dedup();

        dependencies.insert(name.to_string(), model_deps);
    }

    let dag =
        ff_core::dag::ModelDag::build(&dependencies).context("Failed to build dependency graph")?;
    Ok((dependencies, dag))
}

/// Result of a static analysis pipeline run.
///
/// Contains the propagation result from DataFusion plus the set of external
/// tables used to build the schema catalog.
pub(crate) struct StaticAnalysisOutput {
    /// The propagation result from DataFusion
    pub result: ff_analysis::PropagationResult,
    /// Whether any schema-mismatch errors were found (after applying overrides)
    pub has_errors: bool,
    /// User-configured severity overrides for SA codes
    pub overrides: ff_analysis::SeverityOverrides,
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
    use ff_analysis::{propagate_schemas, OverriddenSeverity, SeverityOverrides};

    let overrides = SeverityOverrides::from_config(&project.config.analysis.severity_overrides);
    let (schema_catalog, yaml_schemas) = build_schema_catalog(project, external_tables);

    let filtered_order: Vec<ff_core::ModelName> = topo_order
        .iter()
        .filter(|n| sql_sources.contains_key(n.as_str()))
        .map(|n| ff_core::ModelName::new(n.clone()))
        .collect();

    let sql_model_sources: HashMap<ff_core::ModelName, String> = sql_sources
        .iter()
        .map(|(k, v)| (ff_core::ModelName::new(k.clone()), v.clone()))
        .collect();

    let yaml_model_map: HashMap<ff_core::ModelName, Arc<ff_analysis::RelSchema>> = yaml_schemas
        .iter()
        .map(|(k, v)| (k.clone(), Arc::clone(v)))
        .collect();

    let (user_fn_stubs, user_table_fn_stubs) = ff_analysis::build_user_function_stubs(project);
    let result = propagate_schemas(
        &filtered_order,
        &sql_model_sources,
        &yaml_model_map,
        schema_catalog,
        &user_fn_stubs,
        &user_table_fn_stubs,
    );

    // Compute has_errors respecting severity overrides for SA codes
    let has_errors = !result.failures.is_empty()
        || result.model_plans.values().any(|pr| {
            pr.mismatches.iter().any(|m| {
                let sa_code = m.code();
                match overrides.get_for_sa(sa_code) {
                    Some(OverriddenSeverity::Off) => false,
                    Some(OverriddenSeverity::Level(ff_analysis::Severity::Error)) => true,
                    Some(OverriddenSeverity::Level(_)) => false,
                    None => m.is_error(),
                }
            })
        });

    Ok(StaticAnalysisOutput {
        result,
        has_errors,
        overrides,
    })
}

/// Report static analysis results: mismatches and failures.
///
/// Iterates over mismatch diagnostics and failures from the propagation result.
/// Returns `(mismatch_count, plan_count, failure_count)`.
///
/// The `on_mismatch` callback is called for each schema mismatch with
/// `(model_name, &SchemaMismatch, is_error)`. The `is_error` flag reflects
/// severity overrides when provided. The `on_failure` callback is called
/// for each model that failed planning with `(model_name, error_message)`.
pub(crate) fn report_static_analysis_results(
    result: &ff_analysis::PropagationResult,
    overrides: &ff_analysis::SeverityOverrides,
    mut on_mismatch: impl FnMut(&str, &ff_analysis::SchemaMismatch, bool),
    mut on_failure: impl FnMut(&str, &str),
) -> (usize, usize, usize) {
    use ff_analysis::OverriddenSeverity;

    let mut mismatch_count = 0;

    let mut model_names: Vec<&ff_core::ModelName> = result.model_plans.keys().collect();
    model_names.sort();

    for model_name in model_names {
        let plan_result = &result.model_plans[model_name.as_str()];
        for mismatch in &plan_result.mismatches {
            let sa_code = mismatch.code();
            match overrides.get_for_sa(sa_code) {
                Some(OverriddenSeverity::Off) => {
                    continue;
                }
                Some(OverriddenSeverity::Level(ff_analysis::Severity::Error)) => {
                    on_mismatch(model_name, mismatch, true);
                }
                Some(OverriddenSeverity::Level(_)) => {
                    on_mismatch(model_name, mismatch, false);
                }
                None => {
                    on_mismatch(model_name, mismatch, mismatch.is_error());
                }
            }
            mismatch_count += 1;
        }
    }

    let mut failure_names: Vec<&ff_core::ModelName> = result.failures.keys().collect();
    failure_names.sort();

    for model in failure_names {
        let err = &result.failures[model.as_str()];
        on_failure(model, &err.to_string());
    }

    (
        mismatch_count,
        result.model_plans.len(),
        result.failures.len(),
    )
}

/// Run DataFusion-based static analysis on compiled models before execution.
///
/// Builds the dependency DAG and runs schema propagation. When `quiet` is
/// `true`, mismatch and failure messages are suppressed.
/// Returns `true` when schema errors that should block execution are found.
pub(crate) fn run_pre_execution_analysis(
    project: &Project,
    compiled_models: &HashMap<String, super::run::CompiledModel>,
    global: &GlobalArgs,
    quiet: bool,
) -> Result<bool> {
    if global.verbose {
        eprintln!("[verbose] Running pre-execution static analysis...");
    }

    let external_tables = build_external_tables_lookup(project);

    let dependencies: HashMap<String, Vec<String>> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.dependencies.clone()))
        .collect();

    let dag =
        ff_core::dag::ModelDag::build(&dependencies).context("Failed to build dependency DAG")?;
    let topo_order = dag
        .topological_order()
        .context("Failed to get topological order")?;

    // Exclude Python models from static analysis (no SQL to analyze)
    let sql_sources: HashMap<String, String> = compiled_models
        .iter()
        .filter(|(_, model)| !model.is_python)
        .map(|(name, model)| (name.clone(), model.sql.clone()))
        .collect();

    if sql_sources.is_empty() {
        return Ok(false);
    }

    let output =
        run_static_analysis_pipeline(project, &sql_sources, &topo_order, &external_tables)?;
    let result = &output.result;

    let (_, plan_count, failure_count) = report_static_analysis_results(
        result,
        &output.overrides,
        |model_name, mismatch, is_error| {
            if !quiet {
                let label = if is_error { "error" } else { "warn" };
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

/// Generic wrapper for command results written to JSON.
///
/// Many commands (run, etc.) produce a JSON file with the same
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
        for (w, cell) in widths.iter_mut().zip(row.iter()) {
            *w = (*w).max(cell.len());
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

    let header_parts: Vec<String> = headers
        .iter()
        .zip(&widths)
        .map(|(h, &w)| format!("{:<width$}", h, width = w))
        .collect();
    println!("{}", header_parts.join("  "));

    let sep_parts: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep_parts.join("  "));

    for row in rows {
        let row_parts: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, &w)| format!("{:<width$}", cell, width = w))
            .collect();
        println!("{}", row_parts.join("  "));
    }
}

/// Build a `JinjaEnvironment` using the project's vars and macro paths.
///
/// Use this for commands that don't need template context variables
/// (`{{ project_name }}`, `{{ target }}`, etc.).
pub(crate) fn build_jinja_env(project: &Project) -> ff_jinja::JinjaEnvironment<'static> {
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    ff_jinja::JinjaEnvironment::with_macros(&project.config.vars, &macro_paths)
}

/// Build a `JinjaEnvironment` with template context variables.
///
/// Includes `{{ project_name }}`, `{{ target }}`, `{{ run_id }}`,
/// `{{ executing }}`, etc. Set `executing` to `true` for `ff run`,
/// `false` for compile/validate/analyze.
pub(crate) fn build_jinja_env_with_context(
    project: &Project,
    target: Option<&str>,
    executing: bool,
) -> ff_jinja::JinjaEnvironment<'static> {
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let template_ctx = build_template_context(project, target, executing);
    ff_jinja::JinjaEnvironment::with_context(&project.config.vars, &macro_paths, &template_ctx)
}

/// Build a `TemplateContext` from project and target info.
///
/// Populates `project_name`, `target` (name, schema, database_type),
/// and `executing` flag. `run_id` and `run_started_at` are generated
/// inside `TemplateContext::new()`.
pub(crate) fn build_template_context(
    project: &Project,
    target: Option<&str>,
    executing: bool,
) -> ff_jinja::TemplateContext {
    let resolved_target = ff_core::config::Config::resolve_target(target);
    let target_name = resolved_target.as_deref().unwrap_or("default").to_string();

    let schema = project.config.schema.clone();
    let database_type = project.config.database.db_type.to_string();

    ff_jinja::TemplateContext::new(
        project.config.name.clone(),
        ff_jinja::TargetContext {
            name: target_name,
            schema,
            database_type,
        },
        executing,
    )
}

/// Build a `ModelContext` for a given model.
///
/// Reads the model's config (materialized, schema, tags) and path,
/// computing a relative path from the project root.
pub(crate) fn build_model_context(
    model: &ff_core::model::Model,
    project: &Project,
) -> ff_jinja::ModelContext {
    let materialized = model
        .config
        .materialized
        .unwrap_or(project.config.materialization)
        .to_string();

    let schema = model
        .config
        .schema
        .clone()
        .or_else(|| project.config.schema.clone());

    let rel_path = model
        .path
        .strip_prefix(&project.root)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| model.path.display().to_string());

    ff_jinja::ModelContext {
        name: model.name.to_string(),
        schema,
        materialized,
        tags: model.config.tags.clone(),
        path: rel_path,
    }
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

/// Set the DuckDB search path using the project's configured schema.
///
/// This allows unqualified table references in SQL to resolve against
/// the project's default schema (where seeds and models typically live)
/// in addition to the default `main` schema.
pub(crate) async fn set_project_search_path(
    db: &Arc<dyn Database>,
    project: &Project,
) -> Result<()> {
    let mut schemas: Vec<String> = project.config.schema.iter().cloned().collect();
    if !schemas.iter().any(|s| s == "main") {
        schemas.push("main".to_string());
    }

    let path = schemas.join(",");
    db.execute(&format!("SET search_path = '{path}'"))
        .await
        .context("Failed to set search_path")?;

    Ok(())
}

/// Resolve table function references in a model's dependency list.
///
/// When a dependency matches a known table function, parse the function's
/// SQL body and extract its model dependencies (transitively). Returns
/// `(additional_model_deps, remaining_unknown)` — the additional model deps
/// discovered through functions, plus any truly-unknown dependencies that
/// are not a model, source, or function.
pub(crate) fn resolve_function_dependencies(
    unknown_deps: &[String],
    project: &Project,
    parser: &ff_sql::SqlParser,
    known_models: &HashSet<&str>,
    external_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut additional_model_deps = Vec::new();
    let mut remaining_unknown = Vec::new();
    let mut visited_functions: HashSet<String> = HashSet::new();

    for dep in unknown_deps {
        let dep_lower = dep.to_lowercase();
        if let Some(func) = project.get_function(&dep_lower) {
            if func.function_type == ff_core::function::FunctionType::Table {
                extract_function_deps(
                    func,
                    project,
                    parser,
                    known_models,
                    external_tables,
                    &mut additional_model_deps,
                    &mut visited_functions,
                );
            }
            // Scalar or table — either way it's a known function, not unknown
        } else {
            remaining_unknown.push(dep.clone());
        }
    }

    additional_model_deps.sort();
    additional_model_deps.dedup();
    (additional_model_deps, remaining_unknown)
}

/// Recursively extract model dependencies from a function's SQL body.
fn extract_function_deps(
    func: &ff_core::function::FunctionDef,
    project: &Project,
    parser: &ff_sql::SqlParser,
    known_models: &HashSet<&str>,
    external_tables: &HashSet<String>,
    additional_deps: &mut Vec<String>,
    visited: &mut HashSet<String>,
) {
    let func_name = func.name.to_string();
    if !visited.insert(func_name) {
        return; // Already processed (prevents infinite recursion)
    }

    let Ok(stmts) = parser.parse(&func.sql_body) else {
        return; // Can't parse function SQL — skip silently
    };

    let deps = ff_sql::extract_dependencies(&stmts);
    let (model_deps, _, nested_unknown) = ff_sql::extractor::categorize_dependencies_with_unknown(
        deps,
        known_models,
        external_tables,
    );

    additional_deps.extend(model_deps);

    // Recurse into any nested table function references
    for unknown in &nested_unknown {
        let unknown_lower = unknown.to_lowercase();
        if let Some(nested_func) = project.get_function(&unknown_lower) {
            if nested_func.function_type == ff_core::function::FunctionType::Table {
                extract_function_deps(
                    nested_func,
                    project,
                    parser,
                    known_models,
                    external_tables,
                    additional_deps,
                    visited,
                );
            }
        }
    }
}

/// Build a map from bare table names to qualified references.
///
/// Models in the default database produce 2-part names (`schema.table`).
/// Source tables with an explicit database different from the project default
/// produce 3-part names (`database.schema.table`).
pub(crate) fn build_qualification_map(
    project: &Project,
    compiled_schemas: &HashMap<String, Option<String>>,
) -> HashMap<String, ff_sql::qualify::QualifiedRef> {
    use ff_sql::qualify::QualifiedRef;

    let db_name = &project.config.database.name;
    let default_schema = project.config.schema.as_deref().unwrap_or("main");
    let mut map = HashMap::new();

    // Models: bare_name → schema.name (default database, 2-part)
    for (name, schema) in compiled_schemas {
        let schema = schema.as_deref().unwrap_or(default_schema);
        map.insert(
            name.to_lowercase(),
            QualifiedRef {
                database: None,
                schema: schema.to_string(),
                table: name.clone(),
            },
        );
    }

    // Source tables: use 3-part only when the source targets a different database
    for source in &project.sources {
        let source_db = source.database.as_deref().unwrap_or(db_name);
        let database = if source_db != db_name {
            Some(source_db.to_string())
        } else {
            None
        };
        for table in &source.tables {
            let actual_name = table.identifier.as_ref().unwrap_or(&table.name);
            map.insert(
                table.name.to_lowercase(),
                QualifiedRef {
                    database: database.clone(),
                    schema: source.schema.clone(),
                    table: actual_name.clone(),
                },
            );
            // Also register by identifier if different from logical name
            if let Some(ref ident) = table.identifier {
                if ident != &table.name {
                    map.insert(
                        ident.to_lowercase(),
                        QualifiedRef {
                            database: database.clone(),
                            schema: source.schema.clone(),
                            table: ident.clone(),
                        },
                    );
                }
            }
        }
    }

    map
}

/// Build a [`ff_core::query_comment::QueryCommentContext`] when query comments
/// are enabled in the project config, returning `None` otherwise.
pub(crate) fn build_query_comment_context(
    config: &Config,
    target: Option<&str>,
) -> Option<ff_core::query_comment::QueryCommentContext> {
    if !config.query_comment.enabled {
        return None;
    }
    let resolved_target = Config::resolve_target(target);
    Some(ff_core::query_comment::QueryCommentContext::new(
        &config.name,
        resolved_target.as_deref(),
        config.query_comment.clone(),
    ))
}

/// Open the meta database, returning `None` with a warning on failure.
///
/// During the dual-write phase, meta database errors are non-fatal.
/// The meta database is stored at `<target_dir>/meta.duckdb`.
pub(crate) fn open_meta_db(project: &Project) -> Option<ff_meta::MetaDb> {
    let meta_path = project.target_dir().join("meta.duckdb");
    if let Some(parent) = meta_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            log::warn!("Meta database: failed to create directory: {e}");
            return None;
        }
    }
    match ff_meta::MetaDb::open(&meta_path) {
        Ok(db) => Some(db),
        Err(e) => {
            log::warn!("Meta database: failed to open: {e}");
            None
        }
    }
}

/// Populate the meta database with phase-1 data (project load).
///
/// Opens the meta database, begins a population cycle, inserts project
/// data, and completes the run. Returns `None` with a warning on any
/// failure — meta errors are non-fatal during the dual-write phase.
pub(crate) fn populate_meta_phase1(
    meta_db: &ff_meta::MetaDb,
    project: &Project,
    run_type: &str,
    node_selector: Option<&str>,
) -> Option<(i64, i64, std::collections::HashMap<String, i64>)> {
    let result = meta_db.transaction(|conn| {
        let project_id = ff_meta::populate::populate_project_load(conn, project)?;
        let run_id = ff_meta::populate::lifecycle::begin_population(
            conn,
            project_id,
            run_type,
            node_selector,
        )?;
        let model_id_map = ff_meta::populate::models::get_model_id_map(conn, project_id)?;
        Ok((project_id, run_id, model_id_map))
    });
    match result {
        Ok(ids) => Some(ids),
        Err(e) => {
            log::warn!("Meta database population failed: {e}. JSON output is unaffected.");
            None
        }
    }
}

/// Complete a meta database population run.
pub(crate) fn complete_meta_run(meta_db: &ff_meta::MetaDb, run_id: i64, status: &str) {
    if let Err(e) = meta_db
        .transaction(|conn| ff_meta::populate::lifecycle::complete_population(conn, run_id, status))
    {
        log::warn!("Meta database: failed to complete run: {e}");
    }
}

/// Run the static analysis gate before model execution.
///
/// Returns `Ok(())` if analysis passes or is skipped, or an `ExitCode(1)`
/// error if blocking diagnostics are found.
pub(crate) fn run_static_analysis_gate(
    project: &Project,
    compiled_models: &HashMap<String, super::run::CompiledModel>,
    global: &GlobalArgs,
    skip: bool,
    quiet: bool,
) -> Result<()> {
    if skip {
        return Ok(());
    }
    let has_errors = run_pre_execution_analysis(project, compiled_models, global, quiet)?;
    if has_errors {
        if !quiet {
            eprintln!("Static analysis found errors. Use --skip-static-analysis to bypass.");
        }
        return Err(ExitCode(1).into());
    }
    Ok(())
}

/// Execute a list of SQL hooks against the database.
///
/// Logs verbose output and returns an error on the first hook failure.
pub(crate) async fn execute_hooks(
    db: &dyn Database,
    hooks: &[String],
    hook_type: &str,
    verbose: bool,
    quiet: bool,
) -> Result<()> {
    if hooks.is_empty() {
        return Ok(());
    }
    if verbose {
        eprintln!("[verbose] Executing {} {} hooks", hooks.len(), hook_type);
    }
    for hook in hooks {
        if let Err(e) = db.execute(hook).await {
            if !quiet {
                println!("  \u{2717} {} hook failed: {}", hook_type, e);
            }
            return Err(anyhow::anyhow!("{} hook failed: {}", hook_type, e));
        }
    }
    Ok(())
}
