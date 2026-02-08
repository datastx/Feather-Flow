//! Run command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::contract::{validate_contract, ContractValidationResult, ViolationType};
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::ModelSchema;
use ff_core::run_state::RunState;
use ff_core::selector::Selector;
use ff_core::state::{compute_checksum, ModelState, ModelStateConfig, StateFile};
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;

use crate::cli::{GlobalArgs, OutputFormat, RunArgs};

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
    /// Model schema for contract validation (from .yml file)
    model_schema: Option<ModelSchema>,
    /// Query comment to append when executing SQL
    query_comment: Option<String>,
    /// Whether this model uses Write-Audit-Publish pattern
    wap: bool,
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

    let json_mode = args.output == OutputFormat::Json;

    let db = create_database_connection(&project, global)?;

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

    let compiled_models = load_or_compile_models(&project, args, global, comment_ctx.as_ref())?;

    // Smart build: filter out unchanged models
    let smart_skipped: HashSet<String> = if args.smart {
        compute_smart_skips(&project, &compiled_models, global)?
    } else {
        HashSet::new()
    };

    // Compute config hash for run state validation
    let config_hash = compute_config_hash(&project);

    // Determine run state path
    let run_state_path = args
        .state_file
        .as_ref()
        .map(|s| Path::new(s).to_path_buf())
        .unwrap_or_else(|| project.target_dir().join("run_state.json"));

    // Handle resume mode
    let (execution_order, previous_run_state) = if args.resume {
        handle_resume_mode(
            &run_state_path,
            &compiled_models,
            &project,
            args,
            global,
            &config_hash,
        )?
    } else {
        let order = determine_execution_order(&compiled_models, &project, args, global)?;
        (order, None)
    };

    // Apply smart build filtering
    let execution_order: Vec<String> = if !smart_skipped.is_empty() {
        let before = execution_order.len();
        let filtered: Vec<String> = execution_order
            .into_iter()
            .filter(|m| !smart_skipped.contains(m))
            .collect();
        if !json_mode {
            println!(
                "Smart build: skipping {} unchanged model(s)\n",
                before - filtered.len()
            );
        }
        filtered
    } else {
        execution_order
    };

    if execution_order.is_empty() {
        if json_mode {
            let empty_result = RunResults {
                timestamp: Utc::now(),
                elapsed_secs: 0.0,
                success_count: 0,
                failure_count: 0,
                results: vec![],
            };
            println!("{}", serde_json::to_string_pretty(&empty_result)?);
        } else {
            println!("No models to run.");
        }
        return Ok(());
    }

    if global.verbose {
        eprintln!(
            "[verbose] Running {} models in order: {:?}",
            execution_order.len(),
            execution_order
        );
    }

    // Count non-ephemeral models (ephemeral models are inlined, not executed)
    let ephemeral_count = execution_order
        .iter()
        .filter(|name| {
            compiled_models
                .get(*name)
                .map(|m| m.materialization == Materialization::Ephemeral)
                .unwrap_or(false)
        })
        .count();
    let executable_count = execution_order.len() - ephemeral_count;

    // Show resume summary if applicable (text mode only)
    if !json_mode {
        if let Some(ref prev_state) = previous_run_state {
            let summary = prev_state.summary();
            println!(
                "Resuming: {} skipped, {} to retry, {} pending\n",
                summary.completed, summary.failed, summary.pending
            );
        } else if ephemeral_count > 0 {
            println!(
                "Running {} models ({} ephemeral inlined)...\n",
                executable_count, ephemeral_count
            );
        } else {
            println!("Running {} models...\n", execution_order.len());
        }

        // Show affected exposures
        let affected_exposures = find_affected_exposures(&project, &execution_order);
        if !affected_exposures.is_empty() {
            println!(
                "  {} downstream exposure(s) may be affected:",
                affected_exposures.len()
            );
            for (exposure_name, exposure_type, depends_on) in &affected_exposures {
                let models_affected: Vec<&str> = depends_on
                    .iter()
                    .filter(|d| execution_order.contains(&d.to_string()))
                    .map(|s| s.as_str())
                    .collect();
                println!(
                    "    - {} ({}) via {}",
                    exposure_name,
                    exposure_type,
                    models_affected.join(", ")
                );
            }
            println!();
        }
    }

    // Resolve WAP schema from config
    let target = ff_core::config::Config::resolve_target(global.target.as_deref());
    let wap_schema = project.config.get_wap_schema(target.as_deref());

    create_schemas(&db, &compiled_models, global).await?;

    // Create WAP schema if configured
    if let Some(ref ws) = wap_schema {
        db.create_schema_if_not_exists(ws)
            .await
            .context(format!("Failed to create WAP schema: {}", ws))?;
    }

    // Execute on-run-start hooks (skip if resuming)
    if previous_run_state.is_none() && !project.config.on_run_start.is_empty() {
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

    // Create run state for tracking this execution
    let selection_str = args.select.clone().or(args.models.clone());
    let mut run_state = RunState::new(execution_order.clone(), selection_str, config_hash);

    // Save initial run state
    if let Err(e) = run_state.save(&run_state_path) {
        eprintln!("Warning: Failed to save initial run state: {}", e);
    }

    let (run_results, success_count, failure_count, stopped_early) = execute_models_with_state(
        &db,
        &compiled_models,
        &execution_order,
        args,
        &mut state_file,
        &mut run_state,
        &run_state_path,
        wap_schema.as_deref(),
    )
    .await;

    // Mark run as completed
    run_state.mark_run_completed();
    if let Err(e) = run_state.save(&run_state_path) {
        eprintln!("Warning: Failed to save final run state: {}", e);
    }

    // Save updated incremental state
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

    // Output results based on format
    if json_mode {
        let results = RunResults {
            timestamp: Utc::now(),
            elapsed_secs: start_time.elapsed().as_secs_f64(),
            success_count,
            failure_count,
            results: run_results,
        };
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
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
    }

    if failure_count > 0 {
        std::process::exit(4);
    }

    Ok(())
}

/// Compute a hash of the project configuration for resume validation
fn compute_config_hash(project: &Project) -> String {
    let config_str = format!(
        "{}:{}:{}",
        project.config.name,
        project.config.database.path,
        project.config.schema.as_deref().unwrap_or("default")
    );
    compute_checksum(&config_str)
}

/// Handle resume mode - load previous state and determine what to run
fn handle_resume_mode(
    run_state_path: &Path,
    compiled_models: &HashMap<String, CompiledModel>,
    _project: &Project,
    args: &RunArgs,
    global: &GlobalArgs,
    config_hash: &str,
) -> Result<(Vec<String>, Option<RunState>)> {
    let previous_state = RunState::load(run_state_path)
        .context("Failed to load run state")?
        .ok_or_else(|| anyhow::anyhow!("No run state found. Run 'ff run' first."))?;

    // Warn if config has changed
    if previous_state.config_hash != config_hash {
        eprintln!("Warning: Project configuration has changed since last run");
    }

    // Determine which models to run
    let models_to_run = if args.retry_failed {
        // Only retry failed models
        previous_state.failed_model_names()
    } else {
        // Retry failed + run pending
        previous_state.models_to_run()
    };

    // Filter to only models that exist in compiled_models
    let execution_order: Vec<String> = models_to_run
        .into_iter()
        .filter(|m| compiled_models.contains_key(m))
        .collect();

    // Log what we're skipping
    for completed in &previous_state.completed_models {
        if global.verbose {
            eprintln!(
                "[verbose] Skipping {} (completed in previous run)",
                completed.name
            );
        }
    }

    Ok((execution_order, Some(previous_state)))
}

/// Create database connection from project config, optionally using target override
///
/// If --target is specified (or FF_TARGET env var is set), uses the database config
/// from that target. Otherwise, uses the base database config.
fn create_database_connection(project: &Project, global: &GlobalArgs) -> Result<Arc<dyn Database>> {
    use ff_core::config::Config;

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
        } else {
            eprintln!("[verbose] Using default database: {}", db_config.path);
        }
    }

    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(&db_config.path).context("Failed to connect to database")?);
    Ok(db)
}

/// Load models from cache or compile them fresh
fn load_or_compile_models(
    project: &Project,
    args: &RunArgs,
    global: &GlobalArgs,
    comment_ctx: Option<&ff_core::query_comment::QueryCommentContext>,
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
        load_from_manifest(project, manifest, &all_model_names, comment_ctx)
    } else {
        if global.verbose && !args.no_cache {
            eprintln!("[verbose] Cache invalid or missing, recompiling");
        }
        compile_all_models(project, &all_model_names, comment_ctx)
    }
}

/// Load compiled models from cached manifest
fn load_from_manifest(
    project: &Project,
    manifest: &Manifest,
    model_names: &[String],
    comment_ctx: Option<&ff_core::query_comment::QueryCommentContext>,
) -> Result<HashMap<String, CompiledModel>> {
    let mut compiled_models = HashMap::new();
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    for name in model_names {
        if let Some(manifest_model) = manifest.get_model(name) {
            let compiled_path = project.root.join(&manifest_model.compiled_path);
            let raw_sql = std::fs::read_to_string(&compiled_path).unwrap_or_else(|_| {
                // Fall back to recompiling this model
                project
                    .get_model(name)
                    .and_then(|model| {
                        jinja
                            .render_with_config(&model.raw_sql)
                            .map(|(rendered, _)| rendered)
                            .ok()
                    })
                    .unwrap_or_default()
            });

            // Strip any existing query comment from cached compiled SQL
            let sql = ff_core::query_comment::strip_query_comment(&raw_sql).to_string();

            // Regenerate query comment for this invocation
            let query_comment = comment_ctx.map(|ctx| {
                let metadata = ctx.build_metadata(name, &manifest_model.materialized.to_string());
                ff_core::query_comment::build_query_comment(&metadata)
            });

            // Get model schema from project if available
            let model_schema = project.get_model(name).and_then(|m| m.schema.clone());

            // Merge project-level hooks with manifest (model-level) hooks
            // Project pre_hooks run BEFORE model pre_hooks
            let mut pre_hook = project.config.pre_hook.clone();
            pre_hook.extend(manifest_model.pre_hook.clone());
            // Model post_hooks run BEFORE project post_hooks
            let mut post_hook = manifest_model.post_hook.clone();
            post_hook.extend(project.config.post_hook.clone());

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
                    pre_hook,
                    post_hook,
                    model_schema,
                    query_comment,
                    wap: manifest_model.wap.unwrap_or(false),
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
    comment_ctx: Option<&ff_core::query_comment::QueryCommentContext>,
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
                "ephemeral" => Materialization::Ephemeral,
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

        // Parse hooks from config and merge with project-level hooks
        // Project pre_hooks run BEFORE model pre_hooks
        let mut pre_hook = project.config.pre_hook.clone();
        pre_hook.extend(parse_hooks_from_config(&config_values, "pre_hook"));
        // Model post_hooks run BEFORE project post_hooks
        let mut post_hook = parse_hooks_from_config(&config_values, "post_hook");
        post_hook.extend(project.config.post_hook.clone());

        // Get model schema for contract validation
        let model_schema = model.schema.clone();

        // Resolve WAP from config() or YAML
        let wap = config_values
            .get("wap")
            .map(|v| {
                v.as_str()
                    .map(|s| s == "true")
                    .unwrap_or_else(|| v.is_true())
            })
            .unwrap_or_else(|| model.wap_enabled());

        // Build query comment for this model
        let query_comment = comment_ctx.map(|ctx| {
            let metadata = ctx.build_metadata(name, &mat.to_string());
            ff_core::query_comment::build_query_comment(&metadata)
        });

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
                model_schema,
                query_comment,
                wap,
            },
        );
    }

    Ok(compiled_models)
}

/// Load the deferred manifest from --defer path
/// Returns error if manifest doesn't exist or can't be parsed
fn load_deferred_manifest(defer_path: &str, global: &GlobalArgs) -> Result<Manifest> {
    let path = Path::new(defer_path);

    if !path.exists() {
        anyhow::bail!("Deferred manifest not found at: {}", defer_path);
    }

    if global.verbose {
        eprintln!("[verbose] Loading deferred manifest from: {}", defer_path);
    }

    Manifest::load(path).context(format!(
        "Failed to parse deferred manifest at: {}",
        defer_path
    ))
}

/// Resolve unselected upstream dependencies from deferred manifest
/// Returns the list of models that should be deferred (not executed)
fn resolve_deferred_dependencies(
    selected_models: &[String],
    compiled_models: &HashMap<String, CompiledModel>,
    deferred_manifest: &Manifest,
    global: &GlobalArgs,
) -> Result<HashSet<String>> {
    let selected_set: HashSet<String> = selected_models.iter().cloned().collect();
    let mut deferred_models: HashSet<String> = HashSet::new();

    // Find all upstream dependencies that are not in the selected set
    for model_name in selected_models {
        if let Some(compiled) = compiled_models.get(model_name) {
            for dep in &compiled.dependencies {
                if !selected_set.contains(dep) {
                    // This dependency is not selected - need to defer it
                    // Check if it exists in the deferred manifest
                    if deferred_manifest.get_model(dep).is_some() {
                        if !deferred_models.contains(dep) {
                            deferred_models.insert(dep.clone());
                            if global.verbose {
                                eprintln!("[verbose] Deferring {} to production manifest", dep);
                            }
                        }
                    } else {
                        anyhow::bail!(
                            "Model '{}' not found in deferred manifest. It is required by: {}",
                            dep,
                            model_name
                        );
                    }
                }
            }
        }
    }

    // Also check for transitive dependencies of deferred models
    let mut to_check: Vec<String> = deferred_models.iter().cloned().collect();
    while let Some(model_name) = to_check.pop() {
        if let Some(manifest_model) = deferred_manifest.get_model(&model_name) {
            for dep in &manifest_model.depends_on {
                if !selected_set.contains(dep)
                    && !deferred_models.contains(dep)
                    && deferred_manifest.get_model(dep).is_some()
                {
                    deferred_models.insert(dep.clone());
                    to_check.push(dep.clone());
                    if global.verbose {
                        eprintln!(
                            "[verbose] Deferring {} to production manifest (transitive)",
                            dep
                        );
                    }
                }
                // Note: Don't fail on transitive deps missing from manifest
                // They might be external tables or already executed
            }
        }
    }

    Ok(deferred_models)
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

    // Validate --defer usage
    if args.defer.is_some()
        && args.state.is_none()
        && args.select.is_none()
        && args.models.is_none()
    {
        anyhow::bail!(
            "The --defer flag requires either --state, --select, or --models to specify which models to run"
        );
    }

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

    // Handle --defer: load deferred manifest and validate dependencies
    if let Some(defer_path) = &args.defer {
        let deferred_manifest = load_deferred_manifest(defer_path, global)?;

        // Resolve which models can be deferred
        let deferred_models = resolve_deferred_dependencies(
            &models_after_exclusion,
            compiled_models,
            &deferred_manifest,
            global,
        )?;

        // Log deferred models summary
        if !deferred_models.is_empty() {
            println!(
                "Deferring {} model(s) to manifest at: {}",
                deferred_models.len(),
                defer_path
            );
            for model in &deferred_models {
                println!("  → {}", model);
            }
            println!();
        }
    }

    let execution_order: Vec<String> = dag
        .topological_order()?
        .into_iter()
        .filter(|m| models_after_exclusion.contains(m))
        .collect();

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
    exec_sql: &str,
) -> ff_db::error::DbResult<()> {
    // Check if table exists
    let exists = db.relation_exists(table_name).await.unwrap_or(false);

    if !exists || full_refresh {
        // First run or full refresh: create table from full query
        return db.create_table_as(table_name, exec_sql, true).await;
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

        // Get new query schema (use clean SQL without comment for describe)
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
            let insert_sql = format!("INSERT INTO {} {}", table_name, exec_sql);
            db.execute(&insert_sql).await.map(|_| ())
        }
        IncrementalStrategy::Merge => {
            let unique_keys = compiled.unique_key.clone().unwrap_or_default();
            if unique_keys.is_empty() {
                Err(ff_db::DbError::ExecutionError(
                    "Merge strategy requires unique_key to be specified".to_string(),
                ))
            } else {
                db.merge_into(table_name, exec_sql, &unique_keys).await
            }
        }
        IncrementalStrategy::DeleteInsert => {
            let unique_keys = compiled.unique_key.clone().unwrap_or_default();
            if unique_keys.is_empty() {
                Err(ff_db::DbError::ExecutionError(
                    "Delete+insert strategy requires unique_key to be specified".to_string(),
                ))
            } else {
                db.delete_insert(table_name, exec_sql, &unique_keys).await
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

/// Execute Write-Audit-Publish flow for a model
///
/// 1. Create WAP schema if needed
/// 2. For tables: CTAS into wap_schema
///    For incremental: copy prod to wap_schema, then apply incremental
/// 3. Run schema tests against wap_schema copy
/// 4. If tests pass: DROP prod + CTAS from wap to prod
/// 5. If tests fail: keep wap table, return error
#[allow(clippy::too_many_arguments)]
async fn execute_wap(
    db: &Arc<dyn Database>,
    name: &str,
    qualified_name: &str,
    wap_schema: &str,
    compiled: &CompiledModel,
    full_refresh: bool,
    exec_sql: &str,
) -> Result<(), ff_db::error::DbError> {
    let wap_qualified = format!("{}.{}", wap_schema, name);

    // 1. Create WAP schema
    db.create_schema_if_not_exists(wap_schema).await?;

    // 2. Materialize into WAP schema
    match compiled.materialization {
        Materialization::Table => {
            db.create_table_as(&wap_qualified, exec_sql, true).await?;
        }
        Materialization::Incremental => {
            // Copy existing prod table to WAP schema (if it exists and not full refresh)
            if !full_refresh {
                let exists = db.relation_exists(qualified_name).await.unwrap_or(false);
                if exists {
                    let copy_sql = format!(
                        "CREATE OR REPLACE TABLE {} AS FROM {}",
                        wap_qualified, qualified_name
                    );
                    db.execute(&copy_sql).await?;
                }
            }
            // Apply incremental logic against the WAP copy
            execute_incremental(db, &wap_qualified, compiled, full_refresh, exec_sql).await?;
        }
        _ => unreachable!("WAP only applies to table/incremental"),
    }

    // 3. Run schema tests against WAP copy
    let test_failures =
        run_wap_tests(db, name, &wap_qualified, compiled.model_schema.as_ref()).await;

    if test_failures > 0 {
        return Err(ff_db::error::DbError::ExecutionError(format!(
            "WAP audit failed: {} test(s) failed for '{}'. \
             Staging table preserved at '{}' for debugging. \
             Production table is untouched.",
            test_failures, name, wap_qualified
        )));
    }

    // 4. Tests passed — publish: DROP prod + CTAS from WAP
    db.drop_if_exists(qualified_name).await?;

    let publish_sql = format!("CREATE TABLE {} AS FROM {}", qualified_name, wap_qualified);
    db.execute(&publish_sql).await?;

    // Clean up WAP table after successful publish
    let _ = db.drop_if_exists(&wap_qualified).await;

    Ok(())
}

/// Run schema tests for a single model against a specific qualified name
///
/// Returns the number of test failures
async fn run_wap_tests(
    db: &Arc<dyn Database>,
    model_name: &str,
    wap_qualified_name: &str,
    model_schema: Option<&ModelSchema>,
) -> usize {
    let schema = match model_schema {
        Some(s) => s,
        None => return 0, // No schema = no tests = pass
    };

    let tests = schema.extract_tests(model_name);
    if tests.is_empty() {
        return 0;
    }

    let mut failures = 0;

    for test in &tests {
        let generated =
            ff_test::generator::GeneratedTest::from_schema_test_qualified(test, wap_qualified_name);

        match db.query_count(&generated.sql).await {
            Ok(count) if count > 0 => {
                println!(
                    "    WAP audit FAIL: {} on {}.{} ({} failures)",
                    test.test_type, model_name, test.column, count
                );
                failures += 1;
            }
            Err(e) => {
                println!(
                    "    WAP audit ERROR: {} on {}.{}: {}",
                    test.test_type, model_name, test.column, e
                );
                failures += 1;
            }
            _ => {} // pass
        }
    }

    failures
}

/// Get all transitive dependents of a model
fn get_transitive_dependents(
    model_name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
) -> HashSet<String> {
    let mut dependents = HashSet::new();
    let mut to_check = vec![model_name.to_string()];

    while let Some(current) = to_check.pop() {
        for (name, compiled) in compiled_models {
            if compiled.dependencies.contains(&current) && !dependents.contains(name) {
                dependents.insert(name.clone());
                to_check.push(name.clone());
            }
        }
    }

    dependents
}

/// Validate schema contract for a model after execution
///
/// Returns Ok(Some(result)) if contract validation was performed,
/// Ok(None) if no contract was defined,
/// Err if contract was enforced and violations were found
async fn validate_model_contract(
    db: &Arc<dyn Database>,
    model_name: &str,
    qualified_name: &str,
    model_schema: Option<&ModelSchema>,
    verbose: bool,
) -> Result<Option<ContractValidationResult>, String> {
    // Check if model has a schema with contract
    let schema = match model_schema {
        Some(s) if s.contract.is_some() => s,
        _ => return Ok(None), // No contract to validate
    };

    if verbose {
        eprintln!("[verbose] Validating contract for model: {}", model_name);
    }

    // Get actual table schema from database
    let actual_columns = match db.get_table_schema(qualified_name).await {
        Ok(cols) => cols,
        Err(e) => {
            return Err(format!(
                "Failed to get schema for contract validation: {}",
                e
            ));
        }
    };

    // Validate the contract
    let result = validate_contract(model_name, schema, &actual_columns);

    // Log violations
    for violation in &result.violations {
        let severity = if result.enforced { "ERROR" } else { "WARN" };
        match &violation.violation_type {
            ViolationType::MissingColumn { column } => {
                eprintln!(
                    "    [{}] Contract violation: missing column '{}'",
                    severity, column
                );
            }
            ViolationType::TypeMismatch {
                column,
                expected,
                actual,
            } => {
                eprintln!(
                    "    [{}] Contract violation: column '{}' type mismatch (expected {}, got {})",
                    severity, column, expected, actual
                );
            }
            ViolationType::ExtraColumn { column } => {
                if verbose {
                    eprintln!("    [INFO] Extra column '{}' not in contract", column);
                }
            }
            ViolationType::ConstraintNotMet { column, constraint } => {
                eprintln!(
                    "    [{}] Contract violation: column '{}' constraint {:?} not met",
                    severity, column, constraint
                );
            }
        }
    }

    // If contract is enforced and has violations (excluding extra columns), fail
    if result.enforced && !result.passed {
        let violation_count = result
            .violations
            .iter()
            .filter(|v| !matches!(v.violation_type, ViolationType::ExtraColumn { .. }))
            .count();
        return Err(format!(
            "Contract enforcement failed: {} violation(s)",
            violation_count
        ));
    }

    Ok(Some(result))
}

/// Execute all models in order with optional parallelism
async fn execute_models(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
    wap_schema: Option<&str>,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    // For single thread, use the simple sequential execution
    if args.threads <= 1 {
        return execute_models_sequential(
            db,
            compiled_models,
            execution_order,
            args,
            state_file,
            wap_schema,
        )
        .await;
    }

    // Parallel execution using DAG levels
    execute_models_parallel(
        db,
        compiled_models,
        execution_order,
        args,
        state_file,
        wap_schema,
    )
    .await
}

/// Execute models sequentially (original behavior for --threads=1)
#[allow(clippy::too_many_arguments)]
async fn execute_models_sequential(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
    wap_schema: Option<&str>,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    let mut success_count = 0;
    let mut failure_count = 0;
    let mut run_results: Vec<ModelRunResult> = Vec::new();
    let mut stopped_early = false;
    let mut failed_models: HashSet<String> = HashSet::new();

    // Count non-ephemeral models for progress bar
    let executable_models: Vec<&String> = execution_order
        .iter()
        .filter(|name| {
            compiled_models
                .get(*name)
                .map(|m| m.materialization != Materialization::Ephemeral)
                .unwrap_or(true)
        })
        .collect();
    let executable_count = executable_models.len();

    // Create progress bar if not in quiet mode
    let progress = if !args.quiet && args.output == OutputFormat::Text {
        let pb = ProgressBar::new(executable_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    let mut executable_idx = 0;
    for name in execution_order.iter() {
        let compiled = compiled_models.get(name).expect(
            "model exists in compiled_models - validated during execution order construction",
        );

        // Skip models whose upstream WAP failed
        if failed_models.contains(name) {
            failure_count += 1;
            println!("  - {} (skipped: upstream WAP failure)", name);
            run_results.push(ModelRunResult {
                model: name.clone(),
                status: "skipped".to_string(),
                materialization: compiled.materialization.to_string(),
                duration_secs: 0.0,
                error: Some("skipped: upstream WAP failure".to_string()),
            });
            continue;
        }

        // Skip ephemeral models (they're inlined during compilation)
        if compiled.materialization == Materialization::Ephemeral {
            success_count += 1;
            run_results.push(ModelRunResult {
                model: name.clone(),
                status: "success".to_string(),
                materialization: "ephemeral".to_string(),
                duration_secs: 0.0,
                error: None,
            });
            continue;
        }

        // Update progress bar
        if let Some(ref pb) = progress {
            pb.set_message(format!("Running: {}", name));
            pb.set_position(executable_idx as u64);
        }
        executable_idx += 1;
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

        // Append query comment to SQL for execution (but compiled.sql stays clean for checksums)
        let exec_sql = match &compiled.query_comment {
            Some(comment) => ff_core::query_comment::append_query_comment(&compiled.sql, comment),
            None => compiled.sql.clone(),
        };

        // Determine if this model should use WAP flow
        let is_wap = compiled.wap
            && wap_schema.is_some()
            && matches!(
                compiled.materialization,
                Materialization::Table | Materialization::Incremental
            );

        let result = if is_wap {
            execute_wap(
                db,
                name,
                &qualified_name,
                wap_schema.unwrap(),
                compiled,
                args.full_refresh,
                &exec_sql,
            )
            .await
        } else {
            match compiled.materialization {
                Materialization::View => db.create_view_as(&qualified_name, &exec_sql, true).await,
                Materialization::Table => {
                    db.create_table_as(&qualified_name, &exec_sql, true).await
                }
                Materialization::Incremental => {
                    execute_incremental(db, &qualified_name, compiled, args.full_refresh, &exec_sql)
                        .await
                }
                Materialization::Ephemeral => Ok(()),
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

                // Validate schema contract if defined
                let contract_result = validate_model_contract(
                    db,
                    name,
                    &qualified_name,
                    compiled.model_schema.as_ref(),
                    false, // verbose flag from args would go here
                )
                .await;

                if let Err(contract_error) = contract_result {
                    failure_count += 1;
                    let final_duration = model_start.elapsed();
                    println!(
                        "  ✗ {} (contract) - {} [{}ms]",
                        name,
                        contract_error,
                        final_duration.as_millis()
                    );
                    run_results.push(ModelRunResult {
                        model: name.clone(),
                        status: "failure".to_string(),
                        materialization: compiled.materialization.to_string(),
                        duration_secs: final_duration.as_secs_f64(),
                        error: Some(contract_error),
                    });
                    if args.fail_fast {
                        stopped_early = true;
                        println!("\n  Stopping due to --fail-fast");
                        break;
                    }
                    continue;
                }

                success_count += 1;
                let final_duration = model_start.elapsed();
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

                // Update state for this model (with checksums for smart builds)
                let state_config = ModelStateConfig::new(
                    compiled.materialization,
                    compiled.schema.clone(),
                    compiled.unique_key.clone(),
                    compiled.incremental_strategy,
                    compiled.on_schema_change,
                );
                let schema_checksum = compute_schema_checksum(name, compiled_models);
                let input_checksums = compute_input_checksums(name, compiled_models);
                let model_state = ModelState::new_with_checksums(
                    name.clone(),
                    &compiled.sql,
                    row_count,
                    state_config,
                    schema_checksum,
                    input_checksums,
                );
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

                // If WAP model failed, skip all transitive dependents
                if is_wap {
                    let dependents = get_transitive_dependents(name, compiled_models);
                    for dep in &dependents {
                        failed_models.insert(dep.clone());
                    }
                }

                // Stop on first failure if --fail-fast is set
                if args.fail_fast {
                    stopped_early = true;
                    println!("\n  Stopping due to --fail-fast");
                    break;
                }
            }
        }
    }

    // Finish progress bar
    if let Some(pb) = progress {
        pb.finish_with_message("Complete");
    }

    (run_results, success_count, failure_count, stopped_early)
}

/// Execute models in parallel using DAG-aware scheduling
#[allow(clippy::too_many_arguments)]
async fn execute_models_parallel(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
    wap_schema: Option<&str>,
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

    // Count non-ephemeral models for progress bar
    let executable_count = execution_order
        .iter()
        .filter(|name| {
            compiled_models
                .get(*name)
                .map(|m| m.materialization != Materialization::Ephemeral)
                .unwrap_or(true)
        })
        .count();

    // Create progress bar if not in quiet mode
    let progress = if !args.quiet && args.output == OutputFormat::Text {
        let pb = ProgressBar::new(executable_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})",
                )
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(Arc::new(pb))
    } else {
        None
    };

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
            let compiled = compiled_models.get(&name).expect(
                "model exists in compiled_models - validated during execution order construction",
            );

            // Skip ephemeral models (they're inlined during compilation)
            if compiled.materialization == Materialization::Ephemeral {
                success_count.fetch_add(1, Ordering::SeqCst);
                run_results.lock().unwrap().push(ModelRunResult {
                    model: name.clone(),
                    status: "success".to_string(),
                    materialization: "ephemeral".to_string(),
                    duration_secs: 0.0,
                    error: None,
                });
                completed.lock().unwrap().insert(name);
                continue;
            }
            let sql = compiled.sql.clone();
            let query_comment = compiled.query_comment.clone();
            let materialization = compiled.materialization;
            let schema = compiled.schema.clone();
            let unique_key = compiled.unique_key.clone();
            let incremental_strategy = compiled.incremental_strategy;
            let on_schema_change = compiled.on_schema_change;
            let pre_hook = compiled.pre_hook.clone();
            let post_hook = compiled.post_hook.clone();
            let model_schema = compiled.model_schema.clone();
            let model_wap = compiled.wap;
            let full_refresh = args.full_refresh;
            let fail_fast = args.fail_fast;
            let wap_schema_owned = wap_schema.map(String::from);

            let semaphore = Arc::clone(&semaphore);
            let success_count = Arc::clone(&success_count);
            let failure_count = Arc::clone(&failure_count);
            let run_results = Arc::clone(&run_results);
            let stopped = Arc::clone(&stopped);
            let completed = Arc::clone(&completed);
            let progress = progress.clone();

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("semaphore closed unexpectedly - this should not happen during normal execution");

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

                // Append query comment to SQL for execution
                let exec_sql = match &query_comment {
                    Some(comment) => ff_core::query_comment::append_query_comment(&sql, comment),
                    None => sql.clone(),
                };

                // Create a temporary CompiledModel for execute_incremental/WAP
                let compiled = CompiledModel {
                    sql: sql.clone(),
                    query_comment: query_comment.clone(),
                    materialization,
                    schema: schema.clone(),
                    dependencies: vec![], // Not needed for execution
                    unique_key: unique_key.clone(),
                    incremental_strategy,
                    on_schema_change,
                    pre_hook: vec![],  // Already executed
                    post_hook: vec![], // Will execute separately
                    model_schema: model_schema.clone(),
                    wap: model_wap,
                };

                // Determine if this model should use WAP flow
                let is_wap = model_wap
                    && wap_schema_owned.is_some()
                    && matches!(
                        materialization,
                        Materialization::Table | Materialization::Incremental
                    );

                let result = if is_wap {
                    execute_wap(
                        &db,
                        &name,
                        &qualified_name,
                        wap_schema_owned.as_ref().unwrap(),
                        &compiled,
                        full_refresh,
                        &exec_sql,
                    )
                    .await
                } else {
                    match materialization {
                        Materialization::View => {
                            db.create_view_as(&qualified_name, &exec_sql, true).await
                        }
                        Materialization::Table => {
                            db.create_table_as(&qualified_name, &exec_sql, true).await
                        }
                        Materialization::Incremental => {
                            execute_incremental(
                                &db,
                                &qualified_name,
                                &compiled,
                                full_refresh,
                                &exec_sql,
                            )
                            .await
                        }
                        Materialization::Ephemeral => Ok(()),
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
                            // Validate schema contract if defined
                            let contract_result = validate_model_contract(
                                &db,
                                &name,
                                &qualified_name,
                                model_schema.as_ref(),
                                false, // verbose
                            )
                            .await;

                            if let Err(contract_error) = contract_result {
                                let duration = model_start.elapsed();
                                failure_count.fetch_add(1, Ordering::SeqCst);
                                println!(
                                    "  ✗ {} (contract) - {} [{}ms]",
                                    name,
                                    contract_error,
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
                                    error: Some(contract_error),
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

                // Update progress bar
                if let Some(ref pb) = progress {
                    pb.inc(1);
                }

                (name, Some(()))
            });

            handles.push(handle);
        }

        // Wait for all models in this level to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    // Finish progress bar
    if let Some(pb) = progress {
        pb.finish_with_message("Complete");
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
                let schema_checksum = compute_schema_checksum(&result.model, compiled_models);
                let input_checksums = compute_input_checksums(&result.model, compiled_models);
                let model_state = ModelState::new_with_checksums(
                    result.model.clone(),
                    &compiled.sql,
                    None,
                    state_config,
                    schema_checksum,
                    input_checksums,
                );
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

/// Execute all models in order with run state tracking for resume capability
#[allow(clippy::too_many_arguments)]
async fn execute_models_with_state(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
    state_file: &mut StateFile,
    run_state: &mut RunState,
    run_state_path: &Path,
    wap_schema: Option<&str>,
) -> (Vec<ModelRunResult>, usize, usize, bool) {
    // Use the existing execute_models function and update run state after
    let (run_results, success_count, failure_count, stopped_early) = execute_models(
        db,
        compiled_models,
        execution_order,
        args,
        state_file,
        wap_schema,
    )
    .await;

    // Update run state based on results
    for result in &run_results {
        let duration_ms = (result.duration_secs * 1000.0) as u64;
        if result.status == "success" {
            run_state.mark_completed(&result.model, duration_ms);
        } else {
            run_state.mark_failed(
                &result.model,
                result.error.as_deref().unwrap_or("unknown error"),
            );
        }
    }

    // Save run state after execution
    if let Err(e) = run_state.save(run_state_path) {
        eprintln!("Warning: Failed to save run state: {}", e);
    }

    (run_results, success_count, failure_count, stopped_early)
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

/// Compute which models can be skipped in smart build mode
fn compute_smart_skips(
    project: &Project,
    compiled_models: &HashMap<String, CompiledModel>,
    global: &GlobalArgs,
) -> Result<HashSet<String>> {
    let state_path = project.target_dir().join("state.json");
    let state_file = StateFile::load(&state_path).unwrap_or_default();

    let mut skipped = HashSet::new();

    for (name, compiled) in compiled_models {
        let sql_checksum = compute_checksum(&compiled.sql);
        let schema_checksum = compute_schema_checksum(name, compiled_models);
        let input_checksums = compute_input_checksums(name, compiled_models);

        if !state_file.is_model_or_inputs_modified(
            name,
            &sql_checksum,
            schema_checksum.as_deref(),
            &input_checksums,
        ) {
            if global.verbose {
                eprintln!("[verbose] Smart build: skipping unchanged model '{}'", name);
            }
            skipped.insert(name.clone());
        }
    }

    Ok(skipped)
}

/// Compute schema checksum for a model (from its YAML schema)
fn compute_schema_checksum(
    name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
) -> Option<String> {
    compiled_models
        .get(name)
        .and_then(|c| c.model_schema.as_ref())
        .map(|schema| {
            let yaml = serde_json::to_string(schema).unwrap_or_default();
            compute_checksum(&yaml)
        })
}

/// Compute input checksums for a model (upstream model SQL checksums)
fn compute_input_checksums(
    name: &str,
    compiled_models: &HashMap<String, CompiledModel>,
) -> HashMap<String, String> {
    compiled_models
        .get(name)
        .map(|compiled| {
            compiled
                .dependencies
                .iter()
                .filter_map(|dep| {
                    compiled_models
                        .get(dep)
                        .map(|dep_compiled| (dep.clone(), compute_checksum(&dep_compiled.sql)))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Find exposures that depend on any of the models being run
/// Returns a list of (exposure_name, exposure_type, depends_on models)
fn find_affected_exposures(
    project: &Project,
    models_to_run: &[String],
) -> Vec<(String, String, Vec<String>)> {
    let model_set: HashSet<&str> = models_to_run.iter().map(|s| s.as_str()).collect();

    project
        .exposures
        .iter()
        .filter(|exposure| {
            exposure
                .depends_on
                .iter()
                .any(|dep| model_set.contains(dep.as_str()))
        })
        .map(|exposure| {
            (
                exposure.name.clone(),
                format!("{}", exposure.exposure_type),
                exposure.depends_on.clone(),
            )
        })
        .collect()
}
