//! Model compilation: loading from cache/manifest, compiling fresh, and DAG resolution.

use anyhow::{Context, Result};
use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::ModelSchema;
use ff_core::selector::Selector;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{GlobalArgs, RunArgs};
use crate::commands::common::{self, parse_hooks_from_config};

/// Compiled model data needed for execution
#[derive(Clone)]
pub(crate) struct CompiledModel {
    pub(crate) sql: String,
    pub(crate) materialization: Materialization,
    pub(crate) schema: Option<String>,
    pub(crate) dependencies: Vec<String>,
    /// Unique key(s) for incremental merge/delete_insert strategies
    pub(crate) unique_key: Option<Vec<String>>,
    /// Incremental strategy (append, merge, delete_insert)
    pub(crate) incremental_strategy: Option<IncrementalStrategy>,
    /// How to handle schema changes for incremental models
    pub(crate) on_schema_change: Option<OnSchemaChange>,
    /// SQL statements to execute before the model runs
    pub(crate) pre_hook: Vec<String>,
    /// SQL statements to execute after the model runs
    pub(crate) post_hook: Vec<String>,
    /// Model schema for contract validation (from .yml file)
    pub(crate) model_schema: Option<ModelSchema>,
    /// Query comment to append when executing SQL
    pub(crate) query_comment: Option<String>,
    /// Whether this model uses Write-Audit-Publish pattern
    pub(crate) wap: bool,
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

/// Load models from cache or compile them fresh
pub(super) fn load_or_compile_models(
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
            let raw_sql = match std::fs::read_to_string(&compiled_path) {
                Ok(sql) => sql,
                Err(_) => {
                    // Fall back to recompiling this model
                    let model = project
                        .get_model(name)
                        .context(format!("Model '{}' not found during recompilation", name))?;
                    let (rendered, _) = jinja
                        .render_with_config(&model.raw_sql)
                        .context(format!("Failed to render template for model '{}'", name))?;
                    rendered
                }
            };

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
                    dependencies: manifest_model
                        .depends_on
                        .iter()
                        .map(|m| m.to_string())
                        .collect(),
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
    let known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();

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
            .map(common::parse_materialization)
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
                    .map(common::parse_incremental_strategy);

                let on_change = config_values
                    .get("on_schema_change")
                    .and_then(|v| v.as_str())
                    .map(common::parse_on_schema_change);

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

/// Load the deferred manifest from --defer path.
///
/// Returns error if manifest doesn't exist or can't be parsed.
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

/// Resolve unselected upstream dependencies from deferred manifest.
///
/// Returns the list of models that should be deferred (not executed).
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
                let dep_str = dep.as_str();
                if !selected_set.contains(dep_str)
                    && !deferred_models.contains(dep_str)
                    && deferred_manifest.get_model(dep_str).is_some()
                {
                    deferred_models.insert(dep_str.to_string());
                    to_check.push(dep_str.to_string());
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
pub(super) fn determine_execution_order(
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
