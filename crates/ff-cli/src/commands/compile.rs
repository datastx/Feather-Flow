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

/// Execute the compile command
pub async fn execute(args: &CompileArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let mut project = Project::load(project_path).context("Failed to load project")?;

    let vars = merge_vars(&project.config.vars, &args.vars)?;
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&vars, &macro_paths);

    let model_names = filter_models(&project, &args.models);
    let external_tables = build_external_tables_lookup(&project);
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    if args.parse_only {
        println!("Validating {} models (parse-only)...\n", model_names.len());
    } else {
        println!("Compiling {} models...\n", model_names.len());
    }

    if global.verbose {
        eprintln!("[verbose] Compiling {} models", model_names.len());
    }

    let output_dir = args
        .output_dir
        .as_ref()
        .map(|p| Path::new(p).to_path_buf())
        .unwrap_or_else(|| project.compiled_dir());

    if !args.parse_only {
        std::fs::create_dir_all(&output_dir).context("Failed to create output directory")?;
    }

    let project_root = project.root.clone();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    let default_materialization = project.config.materialization;

    for name in &model_names {
        let model_deps = compile_model(
            &mut project,
            name,
            &jinja,
            &parser,
            &known_models,
            &external_tables,
            &project_root,
            &output_dir,
            global.verbose,
            args.parse_only,
            default_materialization,
        )?;
        dependencies.insert(name.clone(), model_deps);
    }

    // Validate DAG (always done even in parse-only mode)
    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;
    let _ = dag
        .topological_order()
        .context("Circular dependency detected")?;

    if args.parse_only {
        println!(
            "\nValidated {} models successfully (no files written)",
            model_names.len()
        );
    } else {
        // Write manifest only if not in parse-only mode
        write_manifest(&project, &model_names, &output_dir, global.verbose)?;

        println!(
            "\nCompiled {} models to {}",
            model_names.len(),
            output_dir.display()
        );
        println!("Manifest written to {}", project.manifest_path().display());
    }

    Ok(())
}

/// Merge extra vars from --vars argument into project vars
fn merge_vars(
    project_vars: &HashMap<String, serde_yaml::Value>,
    vars_json: &Option<String>,
) -> Result<HashMap<String, serde_yaml::Value>> {
    let mut vars = project_vars.clone();
    if let Some(json) = vars_json {
        let extra_vars: HashMap<String, serde_yaml::Value> =
            serde_json::from_str(json).context("Invalid --vars JSON")?;
        vars.extend(extra_vars);
    }
    Ok(vars)
}

/// Build a lookup set of all external tables including sources
fn build_external_tables_lookup(project: &Project) -> HashSet<String> {
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    let source_tables = build_source_lookup(&project.sources);
    external_tables.extend(source_tables);
    external_tables
}

/// Compile a single model: render template, parse SQL, extract dependencies
#[allow(clippy::too_many_arguments)]
fn compile_model(
    project: &mut Project,
    name: &str,
    jinja: &JinjaEnvironment,
    parser: &SqlParser,
    known_models: &HashSet<String>,
    external_tables: &HashSet<String>,
    project_root: &Path,
    output_dir: &Path,
    verbose: bool,
    parse_only: bool,
    default_materialization: Materialization,
) -> Result<Vec<String>> {
    let model = project
        .get_model_mut(name)
        .context(format!("Model not found: {}", name))?;

    let (rendered, config_values) = jinja
        .render_with_config(&model.raw_sql)
        .context(format!("Failed to render template for model: {}", name))?;

    let statements = parser
        .parse(&rendered)
        .context(format!("Failed to parse SQL for model: {}", name))?;

    let deps = extract_dependencies(&statements);
    let (model_deps, ext_deps, unknown_deps) =
        ff_sql::extractor::categorize_dependencies_with_unknown(
            deps,
            known_models,
            external_tables,
        );

    for unknown in &unknown_deps {
        eprintln!(
            "Warning: Unknown dependency '{}' in model '{}'. Not defined as a model or source.",
            unknown, name
        );
    }

    model.compiled_sql = Some(rendered.clone());
    model.depends_on = model_deps.iter().cloned().collect();
    model.external_deps = ext_deps.iter().cloned().collect();
    model.config = ModelConfig {
        materialized: config_values
            .get("materialized")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "table" => Materialization::Table,
                "incremental" => Materialization::Incremental,
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
        unique_key: config_values
            .get("unique_key")
            .and_then(|v| v.as_str())
            .map(String::from),
        incremental_strategy: config_values
            .get("incremental_strategy")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "merge" => ff_core::config::IncrementalStrategy::Merge,
                "delete_insert" | "delete+insert" => {
                    ff_core::config::IncrementalStrategy::DeleteInsert
                }
                _ => ff_core::config::IncrementalStrategy::Append,
            }),
        on_schema_change: config_values
            .get("on_schema_change")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "fail" => ff_core::config::OnSchemaChange::Fail,
                "append_new_columns" => ff_core::config::OnSchemaChange::AppendNewColumns,
                _ => ff_core::config::OnSchemaChange::Ignore,
            }),
        pre_hook: parse_hooks_from_config(&config_values, "pre_hook"),
        post_hook: parse_hooks_from_config(&config_values, "post_hook"),
    };

    let mat = model.config.materialized.unwrap_or(default_materialization);

    if parse_only {
        println!("  ✓ {} ({}) [validated]", name, mat);
        if verbose {
            eprintln!("[verbose] Validated {} (parse-only mode)", name);
        }
    } else {
        let output_path = compute_compiled_path(&model.path, project_root, output_dir);

        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .context(format!("Failed to create directory for model: {}", name))?;
        }

        std::fs::write(&output_path, &rendered)
            .context(format!("Failed to write compiled SQL for model: {}", name))?;

        println!("  ✓ {} ({})", name, mat);

        if verbose {
            eprintln!("[verbose] Compiled {} -> {}", name, output_path.display());
        }
    }

    Ok(model_deps)
}

/// Write manifest file
fn write_manifest(
    project: &Project,
    model_names: &[String],
    output_dir: &Path,
    verbose: bool,
) -> Result<()> {
    let mut manifest = Manifest::new(&project.config.name);

    for name in model_names {
        let model = project.get_model(name).unwrap();
        let compiled_path = compute_compiled_path(&model.path, &project.root, output_dir);

        manifest.add_model_relative(
            model,
            &compiled_path,
            &project.root,
            project.config.materialization,
            project.config.schema.as_deref(),
        );
    }

    for source in &project.sources {
        manifest.add_source(source);
    }

    let manifest_path = project.manifest_path();
    manifest
        .save(&manifest_path)
        .context("Failed to write manifest")?;

    if verbose {
        eprintln!("[verbose] Manifest written to {}", manifest_path.display());
    }

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
fn compute_compiled_path(
    model_path: &Path,
    project_root: &Path,
    output_dir: &Path,
) -> std::path::PathBuf {
    if let Ok(relative) = model_path.strip_prefix(project_root) {
        let components: Vec<_> = relative.components().collect();
        if components.len() > 1 {
            let subpath: std::path::PathBuf = components[1..].iter().collect();
            return output_dir.join(subpath);
        }
    }

    let filename = model_path.file_name().unwrap_or_default().to_string_lossy();
    output_dir.join(filename.to_string())
}
