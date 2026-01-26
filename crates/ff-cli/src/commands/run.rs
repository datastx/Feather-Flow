//! Run command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{GlobalArgs, RunArgs};

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

    let execution_order = determine_execution_order(&compiled_models, args)?;

    if global.verbose {
        eprintln!(
            "[verbose] Running {} models in order: {:?}",
            execution_order.len(),
            execution_order
        );
    }

    println!("Running {} models...\n", execution_order.len());

    create_schemas(&db, &compiled_models, global).await?;

    let (run_results, success_count, failure_count) =
        execute_models(&db, &compiled_models, &execution_order, args).await;

    write_run_results(&project, &run_results, start_time, success_count, failure_count)?;

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
                _ => Materialization::View,
            })
            .unwrap_or(project.config.materialization);

        let schema = config_values
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| project.config.schema.clone());

        compiled_models.insert(
            name.to_string(),
            CompiledModel {
                sql: rendered,
                materialization: mat,
                schema,
                dependencies: model_deps,
            },
        );
    }

    Ok(compiled_models)
}

/// Determine execution order based on DAG and CLI arguments
fn determine_execution_order(
    compiled_models: &HashMap<String, CompiledModel>,
    args: &RunArgs,
) -> Result<Vec<String>> {
    let dependencies: HashMap<String, Vec<String>> = compiled_models
        .iter()
        .map(|(name, model)| (name.clone(), model.dependencies.clone()))
        .collect();

    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;

    let models_to_run: Vec<String> = if let Some(select) = &args.select {
        dag.select(select).context("Invalid selector")?
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

    let execution_order: Vec<String> = dag
        .topological_order()?
        .into_iter()
        .filter(|m| models_to_run.contains(m))
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

/// Execute all models in order
async fn execute_models(
    db: &Arc<dyn Database>,
    compiled_models: &HashMap<String, CompiledModel>,
    execution_order: &[String],
    args: &RunArgs,
) -> (Vec<ModelRunResult>, usize, usize) {
    let mut success_count = 0;
    let mut failure_count = 0;
    let mut run_results: Vec<ModelRunResult> = Vec::new();

    for name in execution_order {
        let compiled = compiled_models.get(name).unwrap();
        let qualified_name = match &compiled.schema {
            Some(s) => format!("{}.{}", s, name),
            None => name.clone(),
        };

        let model_start = Instant::now();

        if args.full_refresh {
            let _ = db.drop_if_exists(&qualified_name).await;
        }

        let result = match compiled.materialization {
            Materialization::View => db.create_view_as(&qualified_name, &compiled.sql, true).await,
            Materialization::Table => {
                db.create_table_as(&qualified_name, &compiled.sql, true).await
            }
        };

        let duration = model_start.elapsed();

        match result {
            Ok(_) => {
                success_count += 1;
                println!(
                    "  ✓ {} ({}) [{}ms]",
                    name,
                    compiled.materialization,
                    duration.as_millis()
                );
                run_results.push(ModelRunResult {
                    model: name.clone(),
                    status: "success".to_string(),
                    materialization: compiled.materialization.to_string(),
                    duration_secs: duration.as_secs_f64(),
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
            }
        }
    }

    (run_results, success_count, failure_count)
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
