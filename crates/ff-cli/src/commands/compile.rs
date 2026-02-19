//! Compile command implementation

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
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
use crate::commands::common::{self, load_project, RunStatus};

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

use crate::commands::common::parse_hooks_from_config;

/// Intermediate compilation result before ephemeral inlining
struct CompileOutput {
    name: String,
    sql: String,
    materialization: Materialization,
    dependencies: Vec<String>,
    output_path: std::path::PathBuf,
    /// Query comment to append when writing to disk
    query_comment: Option<String>,
}

/// Context for model compilation (phase 1), grouping shared compilation state.
struct CompileContext<'a> {
    jinja: &'a JinjaEnvironment<'a>,
    parser: &'a SqlParser,
    known_models: &'a HashSet<String>,
    external_tables: &'a HashSet<String>,
    project_root: &'a Path,
    output_dir: &'a Path,
    default_materialization: Materialization,
    comment_ctx: Option<&'a ff_core::query_comment::QueryCommentContext>,
}

impl std::fmt::Debug for CompileContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompileContext")
            .field("project_root", &self.project_root)
            .field("output_dir", &self.output_dir)
            .field("default_materialization", &self.default_materialization)
            .finish_non_exhaustive()
    }
}

/// Execute the compile command
pub(crate) async fn execute(args: &CompileArgs, global: &GlobalArgs) -> Result<()> {
    use ff_core::config::Config;

    let start_time = Instant::now();
    let mut project = load_project(global)?;
    let json_mode = args.output == OutputFormat::Json;

    let target = Config::resolve_target(global.target.as_deref());
    let comment_ctx =
        common::build_query_comment_context(&project.config, global.target.as_deref());

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
    let template_ctx = common::build_template_context(&project, global.target.as_deref(), false);
    let jinja = JinjaEnvironment::with_context(&vars, &macro_paths, &template_ctx);

    // Compile all models first — filtering happens after DAG build
    let all_model_names: Vec<String> = project
        .model_names()
        .into_iter()
        .map(String::from)
        .collect();
    let external_tables = common::build_external_tables_lookup(&project);
    let known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();

    if !json_mode && args.nodes.is_none() {
        if args.parse_only {
            println!(
                "Validating {} models (parse-only)...\n",
                all_model_names.len()
            );
        } else {
            println!("Compiling {} models...\n", all_model_names.len());
        }
    }

    if global.verbose && args.nodes.is_none() {
        eprintln!("[verbose] Compiling {} models", all_model_names.len());
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
    let model_count = all_model_names.len();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::with_capacity(model_count);
    let mut compile_results: Vec<ModelCompileResult> = Vec::with_capacity(model_count);
    let mut success_count = 0;
    let mut failure_count = 0;

    let default_materialization = project.config.materialization;

    let mut compiled_models: Vec<CompileOutput> = Vec::with_capacity(model_count);
    let mut materializations: HashMap<String, Materialization> =
        HashMap::with_capacity(model_count);

    let compile_ctx = CompileContext {
        jinja: &jinja,
        parser: &parser,
        known_models: &known_models,
        external_tables: &external_tables,
        project_root: &project_root,
        output_dir: &output_dir,
        default_materialization,
        comment_ctx: comment_ctx.as_ref(),
    };

    for name in &all_model_names {
        match compile_model_phase1(&mut project, name, &compile_ctx) {
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
                    println!("  \u{2717} {} - {}", name, e);
                }
            }
        }
    }

    // Validate DAG (always done even in parse-only mode)
    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;
    let topo_order = dag
        .topological_order()
        .context("Circular dependency detected")?;

    // Apply selector filtering now that DAG is available
    let model_names: Vec<String> = common::resolve_nodes(&project, &dag, &args.nodes)?;
    let model_names_set: HashSet<String> = model_names.iter().cloned().collect();

    // Filter compiled_models to only those selected
    compiled_models.retain(|m| model_names_set.contains(&m.name));

    if !json_mode && args.nodes.is_some() {
        if args.parse_only {
            println!("Validating {} models (parse-only)...\n", model_names.len());
        } else {
            println!("Compiling {} models...\n", model_names.len());
        }
    }

    if global.verbose && args.nodes.is_some() {
        eprintln!(
            "[verbose] Compiling {} models (of {} total)",
            model_names.len(),
            all_model_names.len()
        );
    }

    // Static analysis phase (DataFusion LogicalPlan)
    if !args.skip_static_analysis {
        let analysis_result = run_static_analysis(
            &project,
            &compiled_models,
            &topo_order,
            &external_tables,
            args,
            global,
            json_mode,
        );
        if let Err(e) = analysis_result {
            if !json_mode {
                eprintln!("Static analysis error: {}", e);
            }
            // Non-fatal: continue with compilation
        }
    }

    let compiled_schemas: HashMap<String, Option<String>> = compiled_models
        .iter()
        .map(|m| {
            let schema = project
                .get_model(&m.name)
                .and_then(|model| model.config.schema.clone())
                .or_else(|| project.config.schema.clone());
            (m.name.clone(), schema)
        })
        .collect();
    let qualification_map = common::build_qualification_map(&project, &compiled_schemas);

    for compiled in &mut compiled_models {
        match ff_sql::qualify_table_references(&compiled.sql, &qualification_map) {
            Ok(qualified) => compiled.sql = qualified,
            Err(e) => {
                eprintln!(
                    "Warning: Failed to qualify table references in '{}': {}",
                    compiled.name, e
                );
            }
        }
    }

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
                println!("  \u{2713} {} (ephemeral) [inlined]", compiled.name);
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
                inline_ephemeral_ctes(&compiled.sql, &ephemeral_deps, &order)?
            } else {
                compiled.sql
            }
        } else {
            compiled.sql
        };

        // Write the compiled SQL (with inlined ephemerals)
        if args.parse_only {
            if !json_mode {
                println!(
                    "  \u{2713} {} ({}) [validated]",
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
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create directory for model: {}", compiled.name)
                })?;
            }

            // Attach query comment to written file, but keep in-memory SQL clean for checksums
            let placement = comment_ctx
                .as_ref()
                .map(|c| c.config.placement)
                .unwrap_or_default();
            let sql_to_write = match &compiled.query_comment {
                Some(comment) => {
                    ff_core::query_comment::attach_query_comment(&final_sql, comment, placement)
                }
                None => final_sql.clone(),
            };
            std::fs::write(&compiled.output_path, &sql_to_write).with_context(|| {
                format!("Failed to write compiled SQL for model: {}", compiled.name)
            })?;

            // Also update the model's compiled_sql with the clean version (no comment)
            if let Some(model) = project.get_model_mut(&compiled.name) {
                model.compiled_sql = Some(final_sql);
            }

            if !json_mode {
                println!(
                    "  \u{2713} {} ({})",
                    compiled.name, compiled.materialization
                );
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

    if !args.parse_only && failure_count == 0 {
        populate_meta_compile(&project, &compile_results, &dependencies, global);
    }

    if json_mode {
        let results = CompileResults {
            timestamp: Utc::now(),
            elapsed_secs: start_time.elapsed().as_secs_f64(),
            total_models: model_names.len(),
            success_count,
            failure_count,
            manifest_path: None,
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

/// Compile a single model (phase 1): render template, parse SQL, extract dependencies.
/// Does not write files - returns compiled model info for phase 2.
fn compile_model_phase1(
    project: &mut Project,
    name: &str,
    ctx: &CompileContext<'_>,
) -> Result<CompileOutput> {
    // Immutable borrow block: render + parse + extract deps + resolve functions
    let (rendered, config_values, model_deps, ext_deps) = {
        let model = project
            .get_model(name)
            .with_context(|| format!("Model not found: {}", name))?;
        let model_ctx = common::build_model_context(model, project);
        let (rendered, config_values) = ctx
            .jinja
            .render_with_config_and_model(&model.raw_sql, Some(&model_ctx))
            .with_context(|| format!("Failed to render template for model: {}", name))?;

        let statements = ctx
            .parser
            .parse(&rendered)
            .with_context(|| format!("Failed to parse SQL for model: {}", name))?;

        // Reject CTEs and derived tables -- each transform must be its own model
        ff_sql::validate_no_complex_queries(&statements)
            .map_err(|e| anyhow::anyhow!("{}", e))
            .with_context(|| format!("Model '{}' uses forbidden SQL constructs", name))?;

        let deps = extract_dependencies(&statements);
        let km: HashSet<&str> = ctx.known_models.iter().map(|s| s.as_str()).collect();
        let (mut model_deps, ext_deps, unknown_deps) =
            ff_sql::extractor::categorize_dependencies_with_unknown(deps, &km, ctx.external_tables);

        // Resolve table function transitive dependencies
        let (func_model_deps, remaining_unknown) = common::resolve_function_dependencies(
            &unknown_deps,
            project,
            ctx.parser,
            &km,
            ctx.external_tables,
        );
        model_deps.extend(func_model_deps);
        model_deps.sort();
        model_deps.dedup();

        for unknown in &remaining_unknown {
            eprintln!(
                "Warning: Unknown dependency '{}' in model '{}'. Not defined as a model or source.",
                unknown, name
            );
        }

        (rendered, config_values, model_deps, ext_deps)
    };

    // Mutable borrow: apply results to model
    let model = project
        .get_model_mut(name)
        .with_context(|| format!("Model not found: {}", name))?;

    model.compiled_sql = Some(rendered.clone());
    model.depends_on = model_deps
        .iter()
        .filter_map(|s| ff_core::ModelName::try_new(s.clone()))
        .collect();
    model.external_deps = ext_deps
        .iter()
        .filter_map(|s| ff_core::TableName::try_new(s.clone()))
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
        meta: config_values
            .get("meta")
            .and_then(|v| {
                v.try_iter().ok().map(|iter| {
                    iter.filter_map(|key| {
                        let key_str = key.as_str()?.to_string();
                        let val = v.get_attr(&key_str).ok()?;
                        Some((key_str, val.to_string()))
                    })
                    .collect()
                })
            })
            .unwrap_or_default(),
    };

    let mat = model
        .config
        .materialized
        .unwrap_or(ctx.default_materialization);
    let output_path = compute_compiled_path(&model.path, ctx.project_root, ctx.output_dir)?;

    let query_comment = ctx.comment_ctx.map(|comment_ctx| {
        let node_path = model
            .path
            .strip_prefix(ctx.project_root)
            .ok()
            .map(|p| p.to_string_lossy().to_string());
        let schema = model.config.schema.as_deref();
        let input = ff_core::query_comment::ModelCommentInput {
            model_name: name,
            materialization: &mat.to_string(),
            node_path: node_path.as_deref(),
            schema,
        };
        comment_ctx.build_comment(&input)
    });

    Ok(CompileOutput {
        name: name.to_string(),
        sql: rendered,
        materialization: mat,
        dependencies: model_deps,
        output_path,
        query_comment,
    })
}

/// Compute the output path for a compiled model, preserving directory structure
fn compute_compiled_path(
    model_path: &Path,
    project_root: &Path,
    output_dir: &Path,
) -> Result<std::path::PathBuf> {
    if let Ok(relative) = model_path.strip_prefix(project_root) {
        let components: Vec<_> = relative.components().collect();
        if components.len() > 1 {
            let subpath: std::path::PathBuf = components[1..].iter().collect();
            return Ok(output_dir.join(subpath));
        }
    }

    let filename = model_path
        .file_name()
        .context("model path must have a filename")?;
    Ok(output_dir.join(filename))
}

/// Populate the meta database with compile-phase data (non-fatal).
fn populate_meta_compile(
    project: &Project,
    results: &[ModelCompileResult],
    dependencies: &HashMap<String, Vec<String>>,
    global: &GlobalArgs,
) {
    let Some(meta_db) = common::open_meta_db(project) else {
        return;
    };
    let node_selector = None;
    let Some((project_id, run_id, model_id_map)) =
        common::populate_meta_phase1(&meta_db, project, "compile", node_selector)
    else {
        return;
    };

    let compile_result = meta_db.transaction(|conn| {
        for result in results {
            if !matches!(result.status, common::RunStatus::Success) {
                continue;
            }
            let Some(&model_id) = model_id_map.get(&result.model) else {
                continue;
            };
            let compiled_sql = project
                .get_model(&result.model)
                .and_then(|m| m.compiled_sql.as_deref())
                .unwrap_or("");
            let compiled_path = result.output_path.as_deref().unwrap_or("");
            let checksum = ff_core::compute_checksum(compiled_sql);

            ff_meta::populate::compilation::update_model_compiled(
                conn,
                model_id,
                compiled_sql,
                compiled_path,
                &checksum,
            )?;

            if let Some(deps) = dependencies.get(&result.model) {
                let dep_ids: Vec<i64> = deps
                    .iter()
                    .filter_map(|d| model_id_map.get(d).copied())
                    .collect();
                ff_meta::populate::compilation::populate_dependencies(conn, model_id, &dep_ids)?;
            }

            let ext_deps: Vec<&str> = project
                .get_model(&result.model)
                .map(|m| m.external_deps.iter().map(|t| t.as_ref()).collect())
                .unwrap_or_default();
            if !ext_deps.is_empty() {
                ff_meta::populate::compilation::populate_external_dependencies(
                    conn, model_id, &ext_deps,
                )?;
            }
        }
        Ok(())
    });

    let status = match compile_result {
        Ok(()) => "success",
        Err(e) => {
            log::warn!("Meta database: compilation population failed: {e}");
            "error"
        }
    };
    common::complete_meta_run(&meta_db, run_id, status);

    if global.verbose {
        eprintln!(
            "[verbose] Meta database populated (project_id={}, run_id={})",
            project_id, run_id
        );
    }
}

/// Run DataFusion-based static analysis on compiled models
fn run_static_analysis(
    project: &Project,
    compiled_models: &[CompileOutput],
    topo_order: &[String],
    external_tables: &HashSet<String>,
    args: &CompileArgs,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<()> {
    if global.verbose {
        eprintln!("[verbose] Running DataFusion static analysis...");
    }

    let sql_sources: HashMap<String, String> = compiled_models
        .iter()
        .map(|m| (m.name.clone(), m.sql.clone()))
        .collect();

    let output = super::common::run_static_analysis_pipeline(
        project,
        &sql_sources,
        topo_order,
        external_tables,
    )?;
    let result = &output.result;

    // Handle --explain flag
    if let Some(ref explain_model) = args.explain {
        if let Some(plan_result) = result.model_plans.get(explain_model) {
            println!("LogicalPlan for '{}':\n", explain_model);
            println!("{}", plan_result.plan.display_indent_schema());
        } else if let Some(err) = result.failures.get(explain_model) {
            eprintln!("Cannot explain '{}': {}", explain_model, err);
        } else {
            eprintln!("Model '{}' not found in compilation results", explain_model);
        }
    }

    let (_, plan_count, failure_count) = common::report_static_analysis_results(
        result,
        &output.overrides,
        |model_name, mismatch, is_error| {
            if !json_mode {
                let label = if is_error { "error" } else { "warn" };
                eprintln!("  [{label}] {model_name}: {mismatch}");
            }
        },
        |model, err| {
            if global.verbose {
                eprintln!("[verbose] Static analysis failed for '{}': {}", model, err);
            }
        },
    );
    if !json_mode && (plan_count > 0 || failure_count > 0) {
        eprintln!(
            "Static analysis: {} models planned, {} failures",
            plan_count, failure_count
        );
    }

    if output.has_errors && global.verbose {
        eprintln!("[verbose] Static analysis found schema errors");
    }

    Ok(())
}
