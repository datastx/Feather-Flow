//! Compile command implementation

use anyhow::{Context, Result};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::ModelConfig;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{CompileArgs, GlobalArgs};
use ff_core::source::build_source_lookup;

/// Execute the compile command
pub async fn execute(args: &CompileArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let mut project = Project::load(project_path).context("Failed to load project")?;

    // Merge extra vars if provided
    let mut vars = project.config.vars.clone();
    if let Some(vars_json) = &args.vars {
        let extra_vars: HashMap<String, serde_yaml::Value> =
            serde_json::from_str(vars_json).context("Invalid --vars JSON")?;
        vars.extend(extra_vars);
    }

    // Create SQL parser and Jinja environment with macro support
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&vars, &macro_paths);

    // Filter models if specified
    let model_names = filter_models(&project, &args.models);

    // Collect known models and external tables (including sources)
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    // Add source tables to external tables lookup
    let source_tables = build_source_lookup(&project.sources);
    external_tables.extend(source_tables);
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    println!("Compiling {} models...\n", model_names.len());

    if global.verbose {
        eprintln!("[verbose] Compiling {} models", model_names.len());
    }

    // Output directory
    let output_dir = args
        .output_dir
        .as_ref()
        .map(|p| Path::new(p).to_path_buf())
        .unwrap_or_else(|| project.compiled_dir());

    // Create output directory
    std::fs::create_dir_all(&output_dir).context("Failed to create output directory")?;

    // Store project root for later use (to avoid borrow issues)
    let project_root = project.root.clone();

    // Track dependencies for DAG building
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    // Compile each model
    for name in &model_names {
        let model = project
            .get_model_mut(name)
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
        let (model_deps, ext_deps, unknown_deps) =
            ff_sql::extractor::categorize_dependencies_with_unknown(
                deps,
                &known_models,
                &external_tables,
            );

        // Warn about unknown dependencies
        for unknown in &unknown_deps {
            eprintln!(
                "Warning: Unknown dependency '{}' in model '{}'. Not defined as a model or source.",
                unknown, name
            );
        }

        // Update model with compiled SQL and dependencies
        model.compiled_sql = Some(rendered.clone());
        model.depends_on = model_deps.iter().cloned().collect();
        model.external_deps = ext_deps.iter().cloned().collect();

        // Update model config from captured values
        model.config = ModelConfig {
            materialized: config_values
                .get("materialized")
                .and_then(|v| v.as_str())
                .map(|s| match s {
                    "table" => Materialization::Table,
                    _ => Materialization::View,
                }),
            schema: config_values
                .get("schema")
                .and_then(|v| v.as_str())
                .map(String::from),
            tags: config_values
                .get("tags")
                .and_then(|v| {
                    v.try_iter().ok().map(|iter| {
                        iter.filter_map(|item| item.as_str().map(String::from))
                            .collect()
                    })
                })
                .unwrap_or_default(),
        };

        // Track dependencies for DAG
        dependencies.insert(name.clone(), model_deps);

        // Compute output path preserving directory structure
        // Model path is like: /project/models/staging/stg_orders.sql
        // We want to preserve the relative path from models/ directory
        let output_path = compute_compiled_path(&model.path, &project_root, &output_dir);

        // Create parent directories if needed
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .context(format!("Failed to create directory for model: {}", name))?;
        }

        std::fs::write(&output_path, &rendered)
            .context(format!("Failed to write compiled SQL for model: {}", name))?;

        // Print progress with materialization type
        let mat = model
            .config
            .materialized
            .unwrap_or(project.config.materialization);
        println!("  ✓ {} ({})", name, mat);

        if global.verbose {
            eprintln!("[verbose] Compiled {} -> {}", name, output_path.display());
        }
    }

    // Build and validate DAG
    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;

    // Get execution order
    let execution_order = dag
        .topological_order()
        .context("Circular dependency detected")?;

    if global.verbose {
        eprintln!("[verbose] Execution order: {:?}", execution_order);
    }

    // Build manifest with relative paths
    let mut manifest = Manifest::new(&project.config.name);

    for name in &model_names {
        let model = project.get_model(name).unwrap();
        let compiled_path = compute_compiled_path(&model.path, &project.root, &output_dir);

        manifest.add_model_relative(
            model,
            &compiled_path,
            &project.root,
            project.config.materialization,
            project.config.schema.as_deref(),
        );
    }

    // Add sources to manifest
    for source in &project.sources {
        manifest.add_source(source);
    }

    // Write manifest
    let manifest_path = project.manifest_path();
    manifest
        .save(&manifest_path)
        .context("Failed to write manifest")?;

    println!(
        "\nCompiled {} models to {}",
        model_names.len(),
        output_dir.display()
    );
    println!("Manifest written to {}", manifest_path.display());

    Ok(())
}

/// Filter models based on the --models argument
fn filter_models(project: &Project, models_arg: &Option<String>) -> Vec<String> {
    match models_arg {
        Some(models) => models
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        None => project
            .model_names()
            .into_iter()
            .map(String::from)
            .collect(),
    }
}

/// Compute the output path for a compiled model, preserving directory structure
///
/// Given a model at `/project/models/staging/stg_orders.sql` and output_dir
/// `/project/target/compiled/project/models`, returns
/// `/project/target/compiled/project/models/staging/stg_orders.sql`
fn compute_compiled_path(
    model_path: &Path,
    project_root: &Path,
    output_dir: &Path,
) -> std::path::PathBuf {
    // Try to compute relative path from project root
    if let Ok(relative) = model_path.strip_prefix(project_root) {
        // relative is like "models/staging/stg_orders.sql"
        // We want to strip the first "models/" component since output_dir already includes it
        let components: Vec<_> = relative.components().collect();
        if components.len() > 1 {
            // Skip the first component (e.g., "models") and build the rest
            let subpath: std::path::PathBuf = components[1..].iter().collect();
            return output_dir.join(subpath);
        }
    }

    // Fallback: just use the model name
    let filename = model_path.file_name().unwrap_or_default().to_string_lossy();
    output_dir.join(filename.to_string())
}
