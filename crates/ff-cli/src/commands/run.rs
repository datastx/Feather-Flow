//! Run command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::selector::Selector;
use ff_core::state::{ModelState, ModelStateConfig, StateFile};
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;

use crate::cli::{GlobalArgs, RunArgs};

/// Parse hooks from config() values (minijinja::Value)
/// Hooks can be specified as a single string or an array of strings
fn parse_hooks_from_config(
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

/// Run result for a single model
#[derive(Debug, Clone, Serialize)]
struct ModelRunResult {
    model: String,
    status: String,
    materialization: String,
    duration_secs: f64,
    error: Option<String>,
}

/// Run results output file format
#[derive(Debug, Serialize)]
struct RunResults {
    timestamp: DateTime<Utc>,
    elapsed_secs: f64,
    success_count: usize,
    failure_count: usize,
    results: Vec<ModelRunResult>,
}

/// Compiled model data needed for execution
struct CompiledModel {
    sql: String,
    materialization: Materialization,
    schema: Option<String>,
    dependencies: Vec<String>,
    /// Unique key(s) for incremental merge/delete_insert strategies
    unique_key: Option<Vec<String>>,
    /// Incremental strategy (append, merge, delete_insert)
    incremental_strategy: Option<IncrementalStrategy>,
    /// How to handle schema changes for incremental models
    on_schema_change: Option<OnSchemaChange>,
    /// SQL statements to execute before the model runs
    pre_hook: Vec<String>,
    /// SQL statements to execute after the model runs
    post_hook: Vec<String>,
}

/// Check if manifest cache is valid (newer than all source files)
fn is_cache_valid(project: &Project) -> bool {
    let manifest_path = project.manifest_path();
    let Ok(manifest_meta) = std::fs::metadata(&manifest_path) else {
        return false;
    };
    let Ok(manifest_mtime) = manifest_meta.modified() else {
        return false;
    };

    // Check all model files are older than manifest
    for model in project.models.values() {
        if let Ok(meta) = std::fs::metadata(&model.path) {
            if let Ok(mtime) = meta.modified() {
                if mtime > manifest_mtime {
                    return false;
                }
            }
        }
    }

    // Check config file
    let config_path = project.root.join("featherflow.yml");
    if let Ok(meta) = std::fs::metadata(config_path) {
        if let Ok(mtime) = meta.modified() {
            if mtime > manifest_mtime {
                return false;
            }
        }
    }

    true
}

/// Execute the run command
pub async fn execute(args: &RunArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    let db = create_database_connection(&project, global)?;

    let compiled_models = load_or_compile_models(&project, args, global)?;

    let execution_order = determine_execution_order(&compiled_models, &project, args, global)?;

    if global.verbose {
        eprintln!(
            "[verbose] Running {} models in order: {:?}",
            execution_order.len(),
            execution_order
        );
    }

    println!("Running {} models...\n", execution_order.len());

    create_schemas(&db, &compiled_models, global).await?;

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
                println!("  ✗ on-run-start hook failed: {}", e);
                return Err(anyhow::anyhow!("on-run-start hook failed: {}", e));
            }
        }
    }

    // Load state for incremental tracking
    let state_path = project.target_dir().join("state.json");
    let mut state_file = StateFile::load(&state_path).unwrap_or_default();

    let (run_results, success_count, failure_count, stopped_early) = execute_models(
        &db,
        &compiled_models,
        &execution_order,
        args,
        &mut state_file,
    )
    .await;

    // Save updated state
    if let Err(e) = state_file.save(&state_path) {
        eprintln!("Warning: Failed to save state file: {}", e);
    }

    // Execute on-run-end hooks (even if models failed, unless we stopped early with --fail-fast)
    if !project.config.on_run_end.is_empty() && !stopped_early {
        if global.verbose {
            eprintln!(
                "[verbose] Executing {} on-run-end hooks",
                project.config.on_run_end.len()
            );
        }
        for hook in &project.config.on_run_end {
            if let Err(e) = db.execute(hook).await {
                println!("  ✗ on-run-end hook failed: {}", e);
                // Don't fail the entire run for on-run-end hook failures, just warn
                eprintln!("Warning: on-run-end hook failed: {}", e);
            }
        }
    }

    write_run_results(
        &project,
        &run_results,
        start_time,
        success_count,
        failure_count,
    )?;

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

    if failure_count > 0 {
        std::process::exit(4);
    }

    Ok(())
}

/// Create database connection from project config or CLI override
fn create_database_connection(project: &Project, global: &GlobalArgs) -> Result<Arc<dyn Database>> {
    let db_path = global
        .target
        .as_ref()
        .unwrap_or(&project.config.database.path);
    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(db_path).context("Failed to connect to database")?);
    Ok(db)
}

/// Load models from cache or compile them fresh
fn load_or_compile_models(
    project: &Project,
    args: &RunArgs,
    global: &GlobalArgs,
) -> Result<HashMap<String, CompiledModel>> {
    let all_model_names: Vec<String> = project
        .model_names()
        .into_iter()
        .map(String::from)
        .collect();

    let use_cache = !args.no_cache && is_cache_valid(project);
    let cached_manifest = if use_cache {
        Manifest::load(&project.manifest_path()).ok()
    } else {
        None
    };

    if let Some(ref manifest) = cached_manifest {
        if global.verbose {
            eprintln!("[verbose] Using cached manifest");
        }
        load_from_manifest(project, manifest, &all_model_names)
    } else {
        if global.verbose && !args.no_cache {
            eprintln!("[verbose] Cache invalid or missing, recompiling");
        }
        compile_all_models(project, &all_model_names)
    }
}

/// Load compiled models from cached manifest
fn load_from_manifest(
    project: &Project,
    manifest: &Manifest,
    model_names: &[String],
) -> Result<HashMap<String, CompiledModel>> {
    let mut compiled_models = HashMap::new();
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    for name in model_names {
        if let Some(manifest_model) = manifest.get_model(name) {
            let compiled_path = project.root.join(&manifest_model.compiled_path);
            let sql = std::fs::read_to_string(&compiled_path).unwrap_or_else(|_| {
                // Fall back to recompiling this model
                let model = project.get_model(name).unwrap();
                jinja
                    .render_with_config(&model.raw_sql)
                    .map(|(rendered, _)| rendered)
                    .unwrap_or_default()
            });

            compiled_models.insert(
                name.clone(),
                CompiledModel {
                    sql,
                    materialization: manifest_model.materialized,
                    schema: manifest_model.schema.clone(),
                    dependencies: manifest_model.depends_on.clone(),
                    unique_key: manifest_model.unique_key.clone(),
                    incremental_strategy: manifest_model.incremental_strategy,
                    on_schema_change: manifest_model.on_schema_change,
                    pre_hook: manifest_model.pre_hook.clone(),
                    post_hook: manifest_model.post_hook.clone(),
                },
            );
        }
    }

    Ok(compiled_models)
}

/// Compile all models fresh
fn compile_all_models(
    project: &Project,
    model_names: &[String],
) -> Result<HashMap<String, CompiledModel>> {
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    let external_tables: HashSet<String> = project.config.external_tables.iter().cloned().collect();
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    let mut compiled_models = HashMap::new();

    for name in model_names {
        let model = project
            .get_model(name)
            .context(format!("Model not found: {}", name))?;

        let (rendered, config_values) = jinja
            .render_with_config(&model.raw_sql)
            .context(format!("Failed to render template for model: {}", name))?;

        let statements = parser
            .parse(&rendered)
            .context(format!("Failed to parse SQL for model: {}", name))?;

        let deps = extract_dependencies(&statements);
        let (model_deps, _) =
            ff_sql::extractor::categorize_dependencies(deps, &known_models, &external_tables);

        let mat = config_values
            .get("materialized")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "table" => Materialization::Table,
                "incremental" => Materialization::Incremental,
                _ => Materialization::View,
            })
            .unwrap_or(project.config.materialization);

        let schema = config_values
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| project.config.schema.clone());

        // Parse incremental config if model is incremental
        let (unique_key, incremental_strategy, on_schema_change) =
            if mat == Materialization::Incremental {
                let unique_key = config_values
                    .get("unique_key")
                    .and_then(|v| v.as_str())
                    .map(|s| {
                        s.split(',')
                            .map(|k| k.trim().to_string())
                            .filter(|k| !k.is_empty())
                            .collect::<Vec<_>>()
                    });

                let strategy = config_values
                    .get("incremental_strategy")
                    .and_then(|v| v.as_str())
                    .map(|s| match s {
                        "merge" => IncrementalStrategy::Merge,
                        "delete+insert" | "delete_insert" => IncrementalStrategy::DeleteInsert,
                        _ => IncrementalStrategy::Append,
                    });

                let on_change = config_values
                    .get("on_schema_change")
                    .and_then(|v| v.as_str())
                    .map(|s| match s {
                        "fail" => OnSchemaChange::Fail,
                        "append_new_columns" => OnSchemaChange::AppendNewColumns,
                        _ => OnSchemaChange::Ignore,
                    });

                (unique_key, strategy, on_change)
            } else {
                (None, None, None)
            };

        // Parse hooks from config
        let pre_hook = parse_hooks_from_config(&config_values, "pre_hook");
        let post_hook = parse_hooks_from_config(&config_values, "post_hook");

        compiled_models.insert(
            name.to_string(),
            CompiledModel {
                sql: rendered,
                materialization: mat,
                schema,
                dependencies: model_deps,
                unique_key,
                incremental_strategy,
                on_schema_change,
                pre_hook,
                post_hook,
            },
        );
    }

    Ok(compiled_models)
}

/// Determine execution order based on DAG and CLI arguments
fn determine_execution_order(
    compiled_models: &HashMap<String, CompiledModel>,
    project: &Project,
    args: &RunArgs,
    global: &GlobalArgs,
) -> Result<Vec<String>> {
    let dependencies: HashMap<String, Vec<String>> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.dependencies.clone()))
        .collect();

    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;

    // Load reference manifest if --state is provided
    let reference_manifest: Option<Manifest> = if let Some(state_path) = &args.state {
        let path = Path::new(state_path);
        if !path.exists() {
            anyhow::bail!("State manifest not found: {}", state_path);
        }
        if global.verbose {
            eprintln!("[verbose] Loading reference manifest from: {}", state_path);
        }
        Some(Manifest::load(path).context("Failed to load reference manifest")?)
    } else {
        None
    };

    let models_to_run: Vec<String> = if let Some(select) = &args.select {
        // Parse the selector to check if it's a state selector
        let selector = Selector::parse(select).context("Invalid selector")?;

        if selector.requires_state() && reference_manifest.is_none() {
            anyhow::bail!("state: selector requires --state flag with path to reference manifest");
        }

        selector
            .apply_with_state(&project.models, &dag, reference_manifest.as_ref())
            .context("Failed to apply selector")?
    } else if let Some(models) = &args.models {
        models
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        dag.topological_order()
            .context("Failed to get execution order")?
    };

    // Apply exclusion filter if provided
    let models_after_exclusion: Vec<String> = if let Some(exclude) = &args.exclude {
        let excluded: HashSet<String> = exclude
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        models_to_run
            .into_iter()
            .filter(|m| !excluded.contains(m))
            .collect()
    } else {
        models_to_run
    };

    let execution_order: Vec<String> = dag
        .topological_order()?
        .into_iter()
        .filter(|m| models_after_exclusion.contains(m))
        .collect();

    // Log if using --defer
    if let Some(defer_path) = &args.defer {
        if global.verbose {
            eprintln!(
                "[verbose] Deferring to manifest at: {} for unselected models",
                defer_path
            );
        }
        // Note: --defer functionality would be used during execution
        // to resolve missing upstream dependencies from the deferred manifest.
        // For now, we just log the intent.
    }

    Ok(execution_order)
}

/// Create all required schemas before running models
async fn create_schemas(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    global: &GlobalArgs,
) -> Result<()> {
    let schemas_to_create: HashSet<String> = compiled_models
        .values()
        .filter_map(|m| m.schema.clone())
        .collect();

    for schema in &schemas_to_create {
        if global.verbose {
            eprintln!("[verbose] Creating schema if not exists: {}", schema);
        }
        db.create_schema_if_not_exists(schema)
            .await
            .context(format!("Failed to create schema: {}", schema))?;
    }

    Ok(())
}

/// Execute an incremental model with schema change handling
async fn execute_incremental(
    db: &Arc<dyn Database>,
    table_name: &str,
    compiled: &CompiledModel,
    full_refresh: bool,
) -> ff_db::error::DbResult<()> {
    // Check if table exists
    let exists = db.relation_exists(table_name).await.unwrap_or(false);

    if !exists || full_refresh {
        // First run or full refresh: create table from full query
        return db.create_table_as(table_name, &compiled.sql, true).await;
    }

    // Check for schema changes
    let on_schema_change = compiled.on_schema_change.unwrap_or(OnSchemaChange::Ignore);

    if on_schema_change != OnSchemaChange::Ignore {
        // Get existing table schema
        let existing_schema = db.get_table_schema(table_name).await?;
        let existing_columns: std::collections::HashSet<String> = existing_schema
            .iter()
            .map(|(name, _)| name.to_lowercase())
            .collect();

        // Get new query schema
        let new_schema = db.describe_query(&compiled.sql).await?;
        let new_columns: std::collections::HashMap<String, String> = new_schema
            .iter()
            .map(|(name, typ)| (name.to_lowercase(), typ.clone()))
            .collect();

        // Find columns that are in new but not in existing
        let added_columns: Vec<(String, String)> = new_schema
            .iter()
            .filter(|(name, _)| !existing_columns.contains(&name.to_lowercase()))
            .map(|(name, typ)| (name.clone(), typ.clone()))
            .collect();

        // Find columns that are in existing but not in new
        let removed_columns: Vec<String> = existing_schema
            .iter()
            .filter(|(name, _)| !new_columns.contains_key(&name.to_lowercase()))
            .map(|(name, _)| name.clone())
            .collect();

        let has_changes = !added_columns.is_empty() || !removed_columns.is_empty();

        if has_changes {
            match on_schema_change {
                OnSchemaChange::Fail => {
                    let mut msg = String::from("Schema change detected: ");
                    if !added_columns.is_empty() {
                        msg.push_str(&format!(
                            "new columns: {}; ",
                            added_columns
                                .iter()
                                .map(|(n, t)| format!("{} ({})", n, t))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ));
                    }
                    if !removed_columns.is_empty() {
                        msg.push_str(&format!("removed columns: {}", removed_columns.join(", ")));
                    }
                    return Err(ff_db::DbError::ExecutionError(msg));
                }
                OnSchemaChange::AppendNewColumns => {
                    // Add new columns to existing table
                    if !added_columns.is_empty() {
                        db.add_columns(table_name, &added_columns).await?;
                    }
                    // Note: removed columns are ignored in append_new_columns mode
                }
                OnSchemaChange::Ignore => {
                    // Do nothing - handled by outer condition
                }
            }
        }
    }

    // Execute the incremental strategy
    let strategy = compiled
        .incremental_strategy
        .unwrap_or(IncrementalStrategy::Append);

    match strategy {
        IncrementalStrategy::Append => {
            let insert_sql = format!("INSERT INTO {} {}", table_name, compiled.sql);
            db.execute(&insert_sql).await.map(|_| ())
        }
        IncrementalStrategy::Merge => {
            let unique_keys = compiled.unique_key.clone().unwrap_or_default();
            if unique_keys.is_empty() {
                Err(ff_db::DbError::ExecutionError(
                    "Merge strategy requires unique_key to be specified".to_string(),
                ))
            } else {
                db.merge_into(table_name, &compiled.sql, &unique_keys).await
            }
        }
        IncrementalStrategy::DeleteInsert => {
            let unique_keys = compiled.unique_key.clone().unwrap_or_default();
            if unique_keys.is_empty() {
                Err(ff_db::DbError::ExecutionError(
                    "Delete+insert strategy requires unique_key to be specified".to_string(),
                ))
            } else {
                db.delete_insert(table_name, &compiled.sql, &unique_keys)
                    .await
            }
        }
    }
}

/// Execute hooks (pre or post) for a model
/// Replaces `{{ this }}` with the qualified table name
async fn execute_hooks(
    db: &Arc<dyn Database>,
    hooks: &[String],
    qualified_name: &str,
) -> ff_db::error::DbResult<()> {
    for hook in hooks {
        // Replace {{ this }} with the actual table name
        let sql = hook
            .replace("{{ this }}", qualified_name)
            .replace("{{this}}", qualified_name);
        db.execute(&sql).await?;
    }
    Ok(())
}

/// Execute all models in order with optional parallelism
async fn execute_models(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    // For single thread, use the simple sequential execution
    if args.threads <= 1 {
        return execute_models_sequential(db, compiled_models, execution_order, args, state_file)
            .await;
    }

    // Parallel execution using DAG levels
    execute_models_parallel(db, compiled_models, execution_order, args, state_file).await
}

/// Execute models sequentially (original behavior for --threads=1)
async fn execute_models_sequential(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    let mut success_count = 0;
    let mut failure_count = 0;
    let mut run_results: Vec<ModelRunResult> = Vec::new();
    let mut stopped_early = false;

    for name in execution_order {
        let compiled = compiled_models.get(name).unwrap();
        let qualified_name = match &compiled.schema {
            Some(s) => format!("{}.{}", s, name),
            None => name.clone(),
        };

        let model_start = Instant::now();

        // Execute pre-hooks
        let pre_hook_result = execute_hooks(db, &compiled.pre_hook, &qualified_name).await;
        if let Err(e) = pre_hook_result {
            failure_count += 1;
            let duration = model_start.elapsed();
            println!(
                "  ✗ {} (pre-hook) - {} [{}ms]",
                name,
                e,
                duration.as_millis()
            );
            run_results.push(ModelRunResult {
                model: name.clone(),
                status: "failure".to_string(),
                materialization: compiled.materialization.to_string(),
                duration_secs: duration.as_secs_f64(),
                error: Some(format!("pre-hook failed: {}", e)),
            });
            if args.fail_fast {
                stopped_early = true;
                println!("\n  Stopping due to --fail-fast");
                break;
            }
            continue;
        }

        if args.full_refresh {
            let _ = db.drop_if_exists(&qualified_name).await;
        }

        let result = match compiled.materialization {
            Materialization::View => {
                db.create_view_as(&qualified_name, &compiled.sql, true)
                    .await
            }
            Materialization::Table => {
                db.create_table_as(&qualified_name, &compiled.sql, true)
                    .await
            }
            Materialization::Incremental => {
                execute_incremental(db, &qualified_name, compiled, args.full_refresh).await
            }
        };

        let duration = model_start.elapsed();

        match result {
            Ok(_) => {
                // Execute post-hooks
                let post_hook_result =
                    execute_hooks(db, &compiled.post_hook, &qualified_name).await;
                let final_duration = model_start.elapsed();

                if let Err(e) = post_hook_result {
                    failure_count += 1;
                    println!(
                        "  ✗ {} (post-hook) - {} [{}ms]",
                        name,
                        e,
                        final_duration.as_millis()
                    );
                    run_results.push(ModelRunResult {
                        model: name.clone(),
                        status: "failure".to_string(),
                        materialization: compiled.materialization.to_string(),
                        duration_secs: final_duration.as_secs_f64(),
                        error: Some(format!("post-hook failed: {}", e)),
                    });
                    if args.fail_fast {
                        stopped_early = true;
                        println!("\n  Stopping due to --fail-fast");
                        break;
                    }
                    continue;
                }

                success_count += 1;
                println!(
                    "  ✓ {} ({}) [{}ms]",
                    name,
                    compiled.materialization,
                    final_duration.as_millis()
                );

                // Try to get row count for state tracking (non-blocking)
                let row_count = db
                    .query_count(&format!("SELECT * FROM {}", qualified_name))
                    .await
                    .ok();

                // Update state for this model
                let state_config = ModelStateConfig::new(
                    compiled.materialization,
                    compiled.schema.clone(),
                    compiled.unique_key.clone(),
                    compiled.incremental_strategy,
                    compiled.on_schema_change,
                );
                let model_state =
                    ModelState::new(name.clone(), &compiled.sql, row_count, state_config);
                state_file.upsert_model(model_state);

                run_results.push(ModelRunResult {
                    model: name.clone(),
                    status: "success".to_string(),
                    materialization: compiled.materialization.to_string(),
                    duration_secs: final_duration.as_secs_f64(),
                    error: None,
                });
            }
            Err(e) => {
                failure_count += 1;
                println!("  ✗ {} - {} [{}ms]", name, e, duration.as_millis());
                run_results.push(ModelRunResult {
                    model: name.clone(),
                    status: "failure".to_string(),
                    materialization: compiled.materialization.to_string(),
                    duration_secs: duration.as_secs_f64(),
                    error: Some(e.to_string()),
                });

                // Stop on first failure if --fail-fast is set
                if args.fail_fast {
                    stopped_early = true;
                    println!("\n  Stopping due to --fail-fast");
                    break;
                }
            }
        }
    }

    (run_results, success_count, failure_count, stopped_early)
}

/// Execute models in parallel using DAG-aware scheduling
async fn execute_models_parallel(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));
    let run_results = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(AtomicBool::new(false));

    // Create a semaphore to limit concurrent execution
    let semaphore = Arc::new(Semaphore::new(args.threads));

    // Track completed models
    let completed = Arc::new(Mutex::new(HashSet::new()));

    // Group models by their dependency level
    let levels = compute_execution_levels(execution_order, compiled_models);

    println!(
        "  [parallel mode: {} threads, {} levels]",
        args.threads,
        levels.len()
    );

    for level_models in &levels {
        // Check if we should stop
        if stopped.load(Ordering::SeqCst) {
            break;
        }

        // Spawn tasks for all models in this level
        let mut handles = Vec::new();

        for name in level_models {
            // Check if we should stop before starting a new model
            if stopped.load(Ordering::SeqCst) && args.fail_fast {
                break;
            }

            let name = name.clone();
            let db = Arc::clone(db);
            let compiled = compiled_models.get(&name).unwrap();
            let sql = compiled.sql.clone();
            let materialization = compiled.materialization;
            let schema = compiled.schema.clone();
            let unique_key = compiled.unique_key.clone();
            let incremental_strategy = compiled.incremental_strategy;
            let on_schema_change = compiled.on_schema_change;
            let pre_hook = compiled.pre_hook.clone();
            let post_hook = compiled.post_hook.clone();
            let full_refresh = args.full_refresh;
            let fail_fast = args.fail_fast;

            let semaphore = Arc::clone(&semaphore);
            let success_count = Arc::clone(&success_count);
            let failure_count = Arc::clone(&failure_count);
            let run_results = Arc::clone(&run_results);
            let stopped = Arc::clone(&stopped);
            let completed = Arc::clone(&completed);

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.unwrap();

                // Check if we should stop
                if stopped.load(Ordering::SeqCst) && fail_fast {
                    return (name, None);
                }

                let qualified_name = match &schema {
                    Some(s) => format!("{}.{}", s, name),
                    None => name.clone(),
                };

                let model_start = Instant::now();

                // Execute pre-hooks
                if let Err(e) = execute_hooks(&db, &pre_hook, &qualified_name).await {
                    let duration = model_start.elapsed();
                    failure_count.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "  ✗ {} (pre-hook) - {} [{}ms]",
                        name,
                        e,
                        duration.as_millis()
                    );
                    if fail_fast {
                        stopped.store(true, Ordering::SeqCst);
                        println!("\n  Stopping due to --fail-fast");
                    }
                    let model_result = ModelRunResult {
                        model: name.clone(),
                        status: "failure".to_string(),
                        materialization: materialization.to_string(),
                        duration_secs: duration.as_secs_f64(),
                        error: Some(format!("pre-hook failed: {}", e)),
                    };
                    run_results.lock().unwrap().push(model_result);
                    completed.lock().unwrap().insert(name.clone());
                    return (name, None);
                }

                if full_refresh {
                    let _ = db.drop_if_exists(&qualified_name).await;
                }

                // Create a temporary CompiledModel for execute_incremental
                let compiled = CompiledModel {
                    sql: sql.clone(),
                    materialization,
                    schema: schema.clone(),
                    dependencies: vec![], // Not needed for execution
                    unique_key: unique_key.clone(),
                    incremental_strategy,
                    on_schema_change,
                    pre_hook: vec![],  // Already executed
                    post_hook: vec![], // Will execute separately
                };

                let result = match materialization {
                    Materialization::View => db.create_view_as(&qualified_name, &sql, true).await,
                    Materialization::Table => db.create_table_as(&qualified_name, &sql, true).await,
                    Materialization::Incremental => {
                        execute_incremental(&db, &qualified_name, &compiled, full_refresh).await
                    }
                };

                let model_result = match result {
                    Ok(_) => {
                        // Execute post-hooks
                        if let Err(e) = execute_hooks(&db, &post_hook, &qualified_name).await {
                            let duration = model_start.elapsed();
                            failure_count.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "  ✗ {} (post-hook) - {} [{}ms]",
                                name,
                                e,
                                duration.as_millis()
                            );
                            if fail_fast {
                                stopped.store(true, Ordering::SeqCst);
                                println!("\n  Stopping due to --fail-fast");
                            }
                            ModelRunResult {
                                model: name.clone(),
                                status: "failure".to_string(),
                                materialization: materialization.to_string(),
                                duration_secs: duration.as_secs_f64(),
                                error: Some(format!("post-hook failed: {}", e)),
                            }
                        } else {
                            let duration = model_start.elapsed();
                            success_count.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "  ✓ {} ({}) [{}ms]",
                                name,
                                materialization,
                                duration.as_millis()
                            );

                            // Row count retrieved later in state update
                            ModelRunResult {
                                model: name.clone(),
                                status: "success".to_string(),
                                materialization: materialization.to_string(),
                                duration_secs: duration.as_secs_f64(),
                                error: None,
                            }
                        }
                    }
                    Err(e) => {
                        let duration = model_start.elapsed();
                        failure_count.fetch_add(1, Ordering::SeqCst);
                        println!("  ✗ {} - {} [{}ms]", name, e, duration.as_millis());

                        if fail_fast {
                            stopped.store(true, Ordering::SeqCst);
                            println!("\n  Stopping due to --fail-fast");
                        }

                        ModelRunResult {
                            model: name.clone(),
                            status: "failure".to_string(),
                            materialization: materialization.to_string(),
                            duration_secs: duration.as_secs_f64(),
                            error: Some(e.to_string()),
                        }
                    }
                };

                run_results.lock().unwrap().push(model_result);
                completed.lock().unwrap().insert(name.clone());

                (name, Some(()))
            });

            handles.push(handle);
        }

        // Wait for all models in this level to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    let final_results = run_results.lock().unwrap().clone();
    let final_success = success_count.load(Ordering::SeqCst);
    let final_failure = failure_count.load(Ordering::SeqCst);
    let final_stopped = stopped.load(Ordering::SeqCst);

    // Update state file with results (need to do this after parallel execution)
    for result in &final_results {
        if result.status == "success" {
            if let Some(compiled) = compiled_models.get(&result.model) {
                let state_config = ModelStateConfig::new(
                    compiled.materialization,
                    compiled.schema.clone(),
                    compiled.unique_key.clone(),
                    compiled.incremental_strategy,
                    compiled.on_schema_change,
                );
                let model_state =
                    ModelState::new(result.model.clone(), &compiled.sql, None, state_config);
                state_file.upsert_model(model_state);
            }
        }
    }

    (final_results, final_success, final_failure, final_stopped)
}

/// Compute execution levels - models at the same level have no dependencies on each other
fn compute_execution_levels(
    execution_order: &[String],
    compiled_models: &HashMap<String, CompiledModel>,
) -> Vec<Vec<String>> {
    let mut levels: Vec<Vec<String>> = Vec::new();
    let mut completed: HashSet<String> = HashSet::new();

    let order_set: HashSet<String> = execution_order.iter().cloned().collect();

    // Process models in topological order, grouping by when they become ready
    let mut remaining: Vec<String> = execution_order.to_vec();

    while !remaining.is_empty() {
        let mut current_level = Vec::new();

        for name in &remaining {
            if let Some(compiled) = compiled_models.get(name) {
                // Check if all dependencies are completed or not in our execution set
                let deps_satisfied = compiled
                    .dependencies
                    .iter()
                    .all(|dep| completed.contains(dep) || !order_set.contains(dep));

                if deps_satisfied {
                    current_level.push(name.clone());
                }
            }
        }

        // Mark current level as completed
        for name in &current_level {
            completed.insert(name.clone());
        }

        // Remove current level from remaining
        remaining.retain(|name| !current_level.contains(name));

        if !current_level.is_empty() {
            levels.push(current_level);
        } else if !remaining.is_empty() {
            // Safety: if we can't make progress, just add all remaining as single level
            levels.push(remaining.clone());
            break;
        }
    }

    levels
}

/// Write run results to JSON file
fn write_run_results(
    project: &Project,
    run_results: &[ModelRunResult],
    start_time: Instant,
    success_count: usize,
    failure_count: usize,
) -> Result<()> {
    let results = RunResults {
        timestamp: Utc::now(),
        elapsed_secs: start_time.elapsed().as_secs_f64(),
        success_count,
        failure_count,
        results: run_results.to_vec(),
    };

    let target_dir = project.target_dir();
    std::fs::create_dir_all(&target_dir).context("Failed to create target directory")?;
    let results_path = target_dir.join("run_results.json");
    let results_json =
        serde_json::to_string_pretty(&results).context("Failed to serialize run results")?;
    std::fs::write(&results_path, results_json).context("Failed to write run_results.json")?;

    Ok(())
}
