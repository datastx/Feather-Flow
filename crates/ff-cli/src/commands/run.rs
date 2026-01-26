//! Run command implementation

use anyhow::{Context, Result};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{GlobalArgs, RunArgs};

/// Execute the run command
pub async fn execute(args: &RunArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Create database connection (use --target override if provided)
    let db_path = global
        .target
        .as_ref()
        .unwrap_or(&project.config.database.path);
    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(db_path).context("Failed to connect to database")?);

    // Create SQL parser and Jinja environment
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = JinjaEnvironment::new(&project.config.vars);

    // Collect known models and external tables
    let external_tables: HashSet<String> = project.config.external_tables.iter().cloned().collect();
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    // Get all model names first
    let all_model_names: Vec<String> = project
        .model_names()
        .into_iter()
        .map(String::from)
        .collect();

    // Compile all models first to get dependencies
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();
    let mut compiled_sql: HashMap<String, String> = HashMap::new();
    let mut materializations: HashMap<String, Materialization> = HashMap::new();
    let mut schemas: HashMap<String, Option<String>> = HashMap::new();

    for name in &all_model_names {
        let model = project
            .get_model(name)
            .context(format!("Model not found: {}", name))?;

        // Render Jinja template
        let (rendered, config_values) = jinja
            .render_with_config(&model.raw_sql)
            .context(format!("Failed to render template for model: {}", name))?;

        // Parse SQL to extract dependencies
        let statements = parser
            .parse(&rendered)
            .context(format!("Failed to parse SQL for model: {}", name))?;

        // Extract and categorize dependencies
        let deps = extract_dependencies(&statements);
        let (model_deps, _) =
            ff_sql::extractor::categorize_dependencies(deps, &known_models, &external_tables);

        // Get materialization
        let mat = config_values
            .get("materialized")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "table" => Materialization::Table,
                _ => Materialization::View,
            })
            .unwrap_or(project.config.materialization);

        // Get schema
        let schema = config_values
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| project.config.schema.clone());

        dependencies.insert(name.to_string(), model_deps);
        compiled_sql.insert(name.to_string(), rendered);
        materializations.insert(name.to_string(), mat);
        schemas.insert(name.to_string(), schema);
    }

    // Build DAG
    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;

    // Get models to run based on args
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

    // Filter to only models in the execution list and get topological order
    let execution_order: Vec<String> = dag
        .topological_order()?
        .into_iter()
        .filter(|m| models_to_run.contains(m))
        .collect();

    if global.verbose {
        eprintln!(
            "[verbose] Running {} models in order: {:?}",
            execution_order.len(),
            execution_order
        );
    }

    println!("Running {} models...\n", execution_order.len());

    // Collect unique schemas that need to be created
    let schemas_to_create: HashSet<String> = schemas.values().filter_map(|s| s.clone()).collect();

    // Create all schemas before running models
    for schema in &schemas_to_create {
        if global.verbose {
            eprintln!("[verbose] Creating schema if not exists: {}", schema);
        }
        db.create_schema_if_not_exists(schema)
            .await
            .context(format!("Failed to create schema: {}", schema))?;
    }

    let mut success_count = 0;
    let mut failure_count = 0;

    for name in &execution_order {
        let model_start = Instant::now();

        // Get the qualified name (with schema if specified)
        let schema = schemas.get(name).and_then(|s| s.as_ref());
        let qualified_name = match schema {
            Some(s) => format!("{}.{}", s, name),
            None => name.clone(),
        };

        let sql = compiled_sql.get(name).unwrap();
        let mat = materializations.get(name).unwrap();

        // Full refresh: drop existing
        if args.full_refresh {
            db.drop_if_exists(&qualified_name)
                .await
                .context(format!("Failed to drop {}", qualified_name))?;
        }

        // Execute based on materialization
        let result = match mat {
            Materialization::View => db.create_view_as(&qualified_name, sql, true).await,
            Materialization::Table => db.create_table_as(&qualified_name, sql, true).await,
        };

        let duration = model_start.elapsed();

        match result {
            Ok(_) => {
                success_count += 1;
                println!("  OK {} ({}) [{:.2}s]", name, mat, duration.as_secs_f64());
            }
            Err(e) => {
                failure_count += 1;
                println!("  FAIL {} - {} [{:.2}s]", name, e, duration.as_secs_f64());
            }
        }
    }

    let total_duration = start_time.elapsed();

    println!();
    println!(
        "Completed: {} succeeded, {} failed in {:.2}s",
        success_count,
        failure_count,
        total_duration.as_secs_f64()
    );

    if failure_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}
