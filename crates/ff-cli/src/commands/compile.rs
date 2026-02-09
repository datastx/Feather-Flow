//! Compile command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::ModelConfig;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{
    collect_ephemeral_dependencies, extract_dependencies, inline_ephemeral_ctes, SqlParser,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use crate::cli::{CompileArgs, GlobalArgs, OutputFormat};
use crate::commands::common::{self, RunStatus};

/// Compile result for a single model
#[derive(Debug, Clone, Serialize)]
struct ModelCompileResult {
    model: String,
    status: RunStatus,
    materialization: String,
    output_path: Option<String>,
    dependencies: Vec<String>,
    error: Option<String>,
}

/// Compile results summary for JSON output
#[derive(Debug, Serialize)]
struct CompileResults {
    timestamp: DateTime<Utc>,
    elapsed_secs: f64,
    total_models: usize,
    success_count: usize,
    failure_count: usize,
    manifest_path: Option<String>,
    results: Vec<ModelCompileResult>,
}

use crate::commands::common::{filter_models, parse_hooks_from_config};

/// Intermediate compilation result before ephemeral inlining
struct CompiledModel {
    name: String,
    sql: String,
    materialization: Materialization,
    dependencies: Vec<String>,
    output_path: std::path::PathBuf,
    /// Query comment to append when writing to disk
    query_comment: Option<String>,
}

/// Execute the compile command
pub async fn execute(args: &CompileArgs, global: &GlobalArgs) -> Result<()> {
    use ff_core::config::Config;

    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let mut project = Project::load(project_path).context("Failed to load project")?;
    let json_mode = args.output == OutputFormat::Json;

    // Resolve target from CLI flag or FF_TARGET env var
    let target = Config::resolve_target(global.target.as_deref());

    // Create query comment context if enabled
    let comment_ctx = if project.config.query_comment.enabled {
        Some(ff_core::query_comment::QueryCommentContext::new(
            &project.config.name,
            target.as_deref(),
        ))
    } else {
        None
    };

    // Get vars merged with target overrides, then merge with CLI --vars
    let base_vars = project.config.get_merged_vars(target.as_deref());
    let vars = merge_vars(&base_vars, &args.vars)?;

    if global.verbose {
        if let Some(ref target_name) = target {
            eprintln!("[verbose] Using target '{}' for compilation", target_name);
        }
    }

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&vars, &macro_paths);

    let model_names = filter_models(&project, &args.models);
    let external_tables = common::build_external_tables_lookup(&project);
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    if !json_mode {
        if args.parse_only {
            println!("Validating {} models (parse-only)...\n", model_names.len());
        } else {
            println!("Compiling {} models...\n", model_names.len());
        }
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
    let mut compile_results: Vec<ModelCompileResult> = Vec::new();
    let mut success_count = 0;
    let mut failure_count = 0;

    let default_materialization = project.config.materialization;

    // Phase 1: Compile all models (render Jinja, parse SQL, extract dependencies)
    let mut compiled_models: Vec<CompiledModel> = Vec::new();
    let mut materializations: HashMap<String, Materialization> = HashMap::new();

    for name in &model_names {
        match compile_model_phase1(
            &mut project,
            name,
            &jinja,
            &parser,
            &known_models,
            &external_tables,
            &project_root,
            &output_dir,
            default_materialization,
            comment_ctx.as_ref(),
        ) {
            Ok(compiled) => {
                dependencies.insert(name.clone(), compiled.dependencies.clone());
                materializations.insert(name.clone(), compiled.materialization);
                compiled_models.push(compiled);
            }
            Err(e) => {
                failure_count += 1;
                compile_results.push(ModelCompileResult {
                    model: name.clone(),
                    status: RunStatus::Error,
                    materialization: "unknown".to_string(),
                    output_path: None,
                    dependencies: vec![],
                    error: Some(e.to_string()),
                });
                if !json_mode {
                    println!("  ✗ {} - {}", name, e);
                }
            }
        }
    }

    // Validate DAG (always done even in parse-only mode)
    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;
    let _ = dag
        .topological_order()
        .context("Circular dependency detected")?;

    // Phase 2: Inline ephemeral dependencies and write files
    // Build a map of ephemeral model SQL for inlining
    let ephemeral_sql: HashMap<String, String> = compiled_models
        .iter()
        .filter(|m| m.materialization == Materialization::Ephemeral)
        .map(|m| (m.name.clone(), m.sql.clone()))
        .collect();

    let ephemeral_count = ephemeral_sql.len();
    if global.verbose && ephemeral_count > 0 {
        eprintln!(
            "[verbose] Found {} ephemeral model(s) to inline",
            ephemeral_count
        );
    }

    for compiled in compiled_models {
        // Skip ephemeral models - they don't get written as files
        if compiled.materialization == Materialization::Ephemeral {
            if !json_mode {
                println!("  ✓ {} (ephemeral) [inlined]", compiled.name);
            }
            success_count += 1;
            compile_results.push(ModelCompileResult {
                model: compiled.name,
                status: RunStatus::Success,
                materialization: "ephemeral".to_string(),
                output_path: None, // Ephemeral models don't have output files
                dependencies: compiled.dependencies,
                error: None,
            });
            continue;
        }

        // Inline ephemeral dependencies into this model's SQL
        let final_sql = if !ephemeral_sql.is_empty() {
            // Collect ephemeral dependencies for this model
            let is_ephemeral =
                |name: &str| materializations.get(name) == Some(&Materialization::Ephemeral);
            let get_sql = |name: &str| ephemeral_sql.get(name).cloned();

            let (ephemeral_deps, order) = collect_ephemeral_dependencies(
                &compiled.name,
                &dependencies,
                is_ephemeral,
                get_sql,
            );

            if !ephemeral_deps.is_empty() {
                if global.verbose {
                    eprintln!(
                        "[verbose] Inlining {} ephemeral model(s) into {}",
                        ephemeral_deps.len(),
                        compiled.name
                    );
                }
                inline_ephemeral_ctes(&compiled.sql, &ephemeral_deps, &order)
            } else {
                compiled.sql.clone()
            }
        } else {
            compiled.sql.clone()
        };

        // Write the compiled SQL (with inlined ephemerals)
        if args.parse_only {
            if !json_mode {
                println!(
                    "  ✓ {} ({}) [validated]",
                    compiled.name, compiled.materialization
                );
            }
            if global.verbose {
                eprintln!("[verbose] Validated {} (parse-only mode)", compiled.name);
            }
            success_count += 1;
            compile_results.push(ModelCompileResult {
                model: compiled.name,
                status: RunStatus::Success,
                materialization: compiled.materialization.to_string(),
                output_path: None,
                dependencies: compiled.dependencies,
                error: None,
            });
        } else {
            if let Some(parent) = compiled.output_path.parent() {
                std::fs::create_dir_all(parent).context(format!(
                    "Failed to create directory for model: {}",
                    compiled.name
                ))?;
            }

            // Append query comment to written file, but keep in-memory SQL clean for checksums
            let sql_to_write = match &compiled.query_comment {
                Some(comment) => ff_core::query_comment::append_query_comment(&final_sql, comment),
                None => final_sql.clone(),
            };
            std::fs::write(&compiled.output_path, &sql_to_write).context(format!(
                "Failed to write compiled SQL for model: {}",
                compiled.name
            ))?;

            // Also update the model's compiled_sql with the clean version (no comment)
            if let Some(model) = project.get_model_mut(&compiled.name) {
                model.compiled_sql = Some(final_sql);
            }

            if !json_mode {
                println!("  ✓ {} ({})", compiled.name, compiled.materialization);
            }

            if global.verbose {
                eprintln!(
                    "[verbose] Compiled {} -> {}",
                    compiled.name,
                    compiled.output_path.display()
                );
            }

            success_count += 1;
            compile_results.push(ModelCompileResult {
                model: compiled.name,
                status: RunStatus::Success,
                materialization: compiled.materialization.to_string(),
                output_path: Some(compiled.output_path.display().to_string()),
                dependencies: compiled.dependencies,
                error: None,
            });
        }
    }

    let manifest_path = if !args.parse_only && failure_count == 0 {
        // Write manifest only if not in parse-only mode and no failures
        write_manifest(&project, &model_names, &output_dir, global.verbose)?;
        Some(project.manifest_path().display().to_string())
    } else {
        None
    };

    if json_mode {
        let results = CompileResults {
            timestamp: Utc::now(),
            elapsed_secs: start_time.elapsed().as_secs_f64(),
            total_models: model_names.len(),
            success_count,
            failure_count,
            manifest_path,
            results: compile_results,
        };
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else if args.parse_only {
        println!(
            "\nValidated {} models successfully (no files written)",
            model_names.len()
        );
    } else {
        let non_ephemeral_count = model_names.len() - ephemeral_count;
        if ephemeral_count > 0 {
            println!(
                "\nCompiled {} models ({} ephemeral inlined) to {}",
                non_ephemeral_count,
                ephemeral_count,
                output_dir.display()
            );
        } else {
            println!(
                "\nCompiled {} models to {}",
                model_names.len(),
                output_dir.display()
            );
        }
        if let Some(path) = manifest_path {
            println!("Manifest written to {}", path);
        }
    }

    if failure_count > 0 {
        return Err(crate::commands::common::ExitCode(1).into());
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

/// Compile a single model (phase 1): render template, parse SQL, extract dependencies
/// Does not write files - returns compiled model info for phase 2
#[allow(clippy::too_many_arguments)]
fn compile_model_phase1(
    project: &mut Project,
    name: &str,
    jinja: &JinjaEnvironment,
    parser: &SqlParser,
    known_models: &HashSet<String>,
    external_tables: &HashSet<String>,
    project_root: &Path,
    output_dir: &Path,
    default_materialization: Materialization,
    comment_ctx: Option<&ff_core::query_comment::QueryCommentContext>,
) -> Result<CompiledModel> {
    let model = project
        .get_model_mut(name)
        .context(format!("Model not found: {}", name))?;

    let (rendered, config_values) = jinja
        .render_with_config(&model.raw_sql)
        .context(format!("Failed to render template for model: {}", name))?;

    let statements = parser
        .parse(&rendered)
        .context(format!("Failed to parse SQL for model: {}", name))?;

    // Reject CTEs and derived tables — each transform must be its own model
    ff_sql::validate_no_complex_queries(&statements)
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context(format!("Model '{}' uses forbidden SQL constructs", name))?;

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
    model.external_deps = ext_deps
        .iter()
        .map(|s| ff_core::TableName::new(s.clone()))
        .collect();
    model.config = ModelConfig {
        materialized: config_values
            .get("materialized")
            .and_then(|v| v.as_str())
            .map(common::parse_materialization),
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
            .map(common::parse_incremental_strategy),
        on_schema_change: config_values
            .get("on_schema_change")
            .and_then(|v| v.as_str())
            .map(common::parse_on_schema_change),
        pre_hook: parse_hooks_from_config(&config_values, "pre_hook"),
        post_hook: parse_hooks_from_config(&config_values, "post_hook"),
        wap: config_values.get("wap").map(|v| {
            v.as_str()
                .map(|s| s == "true")
                .unwrap_or_else(|| v.is_true())
        }),
    };

    let mat = model.config.materialized.unwrap_or(default_materialization);
    let output_path = compute_compiled_path(&model.path, project_root, output_dir);

    let query_comment = comment_ctx.map(|ctx| {
        let metadata = ctx.build_metadata(name, &mat.to_string());
        ff_core::query_comment::build_query_comment(&metadata)
    });

    Ok(CompiledModel {
        name: name.to_string(),
        sql: rendered,
        materialization: mat,
        dependencies: model_deps,
        output_path,
        query_comment,
    })
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
        let model = project
            .get_model(name)
            .ok_or_else(|| anyhow::anyhow!("Model '{}' not found in project", name))?;
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
