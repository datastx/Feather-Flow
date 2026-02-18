//! Model compilation: loading from cache/manifest, compiling fresh, and DAG resolution.

use anyhow::{Context, Result};
use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::dag::ModelDag;
use ff_core::model::ModelSchema;
use ff_core::selector::Selector;
use ff_core::Project;
use ff_meta::manifest::Manifest;
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
    /// Whether this is a Python model (executed via `uv run`)
    pub(crate) is_python: bool,
    /// Path to the Python script (only set for Python models)
    pub(crate) script_path: Option<std::path::PathBuf>,
}

/// Compile all models from source templates.
///
/// Previously supported a manifest-based cache path; now always compiles
/// fresh since template rendering is fast and avoids staleness issues.
pub(crate) fn load_or_compile_models(
    project: &Project,
    _args: &RunArgs,
    global: &GlobalArgs,
    comment_ctx: Option<&ff_core::query_comment::QueryCommentContext>,
) -> Result<HashMap<String, CompiledModel>> {
    let all_model_names: Vec<String> = project
        .model_names()
        .into_iter()
        .map(String::from)
        .collect();

    if global.verbose {
        eprintln!("[verbose] Compiling {} models", all_model_names.len());
    }
    compile_all_models(
        project,
        &all_model_names,
        global.target.as_deref(),
        comment_ctx,
    )
}

/// Compile all models fresh
fn compile_all_models(
    project: &Project,
    model_names: &[String],
    target: Option<&str>,
    comment_ctx: Option<&ff_core::query_comment::QueryCommentContext>,
) -> Result<HashMap<String, CompiledModel>> {
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = common::build_jinja_env_with_context(project, target, true);

    let external_tables: HashSet<String> = common::build_external_tables_lookup(project);
    let known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();

    let mut compiled_models = HashMap::with_capacity(model_names.len());

    for name in model_names {
        let model = project
            .get_model(name)
            .with_context(|| format!("Model not found: {}", name))?;

        // Python models skip Jinja/SQL compilation — deps come from YAML
        if model.is_python() {
            let model_deps: Vec<String> = model.depends_on.iter().map(|m| m.to_string()).collect();
            let schema = model
                .config
                .schema
                .clone()
                .or_else(|| project.config.schema.clone());
            let model_schema = model.schema.clone();

            compiled_models.insert(
                name.to_string(),
                CompiledModel {
                    sql: String::new(),
                    materialization: Materialization::Table,
                    schema,
                    dependencies: model_deps,
                    unique_key: None,
                    incremental_strategy: None,
                    on_schema_change: None,
                    pre_hook: Vec::new(),
                    post_hook: Vec::new(),
                    model_schema,
                    query_comment: None,
                    wap: false,
                    is_python: true,
                    script_path: Some(model.path.clone()),
                },
            );
            continue;
        }

        let (rendered, config_values) = jinja
            .render_with_config(&model.raw_sql)
            .with_context(|| format!("Failed to render template for model: {}", name))?;

        let statements = parser
            .parse(&rendered)
            .with_context(|| format!("Failed to parse SQL for model: {}", name))?;

        let deps = extract_dependencies(&statements);
        let (mut model_deps, _, unknown_deps) =
            ff_sql::extractor::categorize_dependencies_with_unknown(
                deps,
                &known_models,
                &external_tables,
            );

        let (func_model_deps, _) = common::resolve_function_dependencies(
            &unknown_deps,
            project,
            &parser,
            &known_models,
            &external_tables,
        );
        model_deps.extend(func_model_deps);
        model_deps.sort();
        model_deps.dedup();

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

        let pre_hook = parse_hooks_from_config(&config_values, "pre_hook");
        let post_hook = parse_hooks_from_config(&config_values, "post_hook");

        let model_schema = model.schema.clone();

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
                is_python: false,
                script_path: None,
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

    Manifest::load(path)
        .with_context(|| format!("Failed to parse deferred manifest at: {}", defer_path))
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

    for model_name in selected_models {
        let Some(compiled) = compiled_models.get(model_name) else {
            continue;
        };
        for dep in &compiled.dependencies {
            if selected_set.contains(dep) {
                continue;
            }
            if deferred_models.contains(dep) {
                continue;
            }
            if deferred_manifest.get_model(dep).is_none() {
                anyhow::bail!(
                    "Model '{}' not found in deferred manifest. It is required by: {}",
                    dep,
                    model_name
                );
            }
            deferred_models.insert(dep.clone());
            if global.verbose {
                eprintln!("[verbose] Deferring {} to production manifest", dep);
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
pub(crate) fn determine_execution_order(
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
    if args.defer.is_some() && args.state.is_none() && args.nodes.is_none() {
        anyhow::bail!(
            "The --defer flag requires either --state or --nodes to specify which models to run"
        );
    }

    let topo_order = dag
        .topological_order()
        .context("Failed to get execution order")?;

    let models_to_run: Vec<String> = if let Some(nodes_str) = &args.nodes {
        // Check if any token is a state: selector
        let mut combined = Vec::new();
        for token in nodes_str.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            let selector = Selector::parse(token).context("Invalid selector")?;
            if selector.requires_state() && reference_manifest.is_none() {
                anyhow::bail!(
                    "state: selector requires --state flag with path to reference manifest"
                );
            }
            let ref_manifest: Option<&dyn ff_core::reference_manifest::ReferenceManifest> =
                reference_manifest.as_ref().map(|m| m as _);
            let matched = selector
                .apply_with_state(&project.models, &dag, ref_manifest)
                .context("Failed to apply selector")?;
            combined.extend(matched);
        }
        // Deduplicate and return in topological order
        let combined_set: std::collections::HashSet<String> = combined.into_iter().collect();
        topo_order
            .iter()
            .filter(|m| combined_set.contains(m.as_str()))
            .cloned()
            .collect()
    } else {
        topo_order.clone()
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
    let deferred_models: HashSet<String> = if let Some(defer_path) = &args.defer {
        let deferred_manifest = load_deferred_manifest(defer_path, global)?;

        // Resolve which models can be deferred
        let deferred = resolve_deferred_dependencies(
            &models_after_exclusion,
            compiled_models,
            &deferred_manifest,
            global,
        )?;

        // Log deferred models summary
        if !deferred.is_empty() {
            println!(
                "Deferring {} model(s) to manifest at: {}",
                deferred.len(),
                defer_path
            );
            for model in &deferred {
                println!("  → {}", model);
            }
            println!();
        }
        deferred
    } else {
        HashSet::new()
    };

    let execution_order: Vec<String> = topo_order
        .into_iter()
        .filter(|m| models_after_exclusion.contains(m) && !deferred_models.contains(m))
        .collect();

    Ok(execution_order)
}
