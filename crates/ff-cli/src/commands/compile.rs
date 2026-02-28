//! Compile command implementation
//!
//! The compile pipeline is organized into 5 discrete stages:
//!
//! 1. **Render** — Render Jinja templates, parse SQL, extract dependencies
//! 2. **DAG Build** — Build dependency graph, topological sort, apply selectors
//! 3. **Analyze** — Static analysis + table name qualification
//! 4. **Resolve** — Ephemeral inlining, final SQL generation, disk writes
//! 5. **Validate** — Post-compile validation checks, meta DB population

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{
    collect_ephemeral_dependencies, extract_dependencies, inline_ephemeral_ctes,
    qualify_statements, SqlParser, Statement,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use crate::cli::{CompileArgs, GlobalArgs, OutputFormat};
use crate::commands::common::{self, load_project, RunStatus};
use crate::commands::validation::{self, ValidationContext};

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

/// Intermediate compilation result before ephemeral inlining
struct CompileOutput {
    name: String,
    sql: String,
    /// Parsed AST statements — kept to avoid re-parsing during qualification.
    statements: Vec<Statement>,
    materialization: Materialization,
    dependencies: Vec<String>,
    output_path: std::path::PathBuf,
    /// Query comment to append when writing to disk
    query_comment: Option<String>,
    /// For incremental models: SQL compiled with `is_exists()=true`.
    /// The primary `sql` field is compiled with `is_exists()=false` (full path).
    incremental_sql: Option<String>,
    /// Parsed AST for the incremental path (for qualification).
    incremental_statements: Option<Vec<Statement>>,
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
    /// Template context for building dual-path Jinja environments
    template_ctx: &'a ff_jinja::TemplateContext,
    /// Merged vars for dual-path rendering
    vars: &'a HashMap<String, serde_yaml::Value>,
    /// Macro paths for dual-path rendering
    macro_paths: &'a [std::path::PathBuf],
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

// ── Stage 1 output ──────────────────────────────────────────────────────

/// Output of Stage 1 (Render): compiled models with extracted dependencies.
struct RenderOutput {
    /// Successfully compiled models
    compiled: Vec<CompileOutput>,
    /// Early failure results (models that failed rendering/parsing)
    failures: Vec<ModelCompileResult>,
    /// Dependencies extracted per model (name → deps list)
    dependencies: HashMap<String, Vec<String>>,
    /// Materialization per model
    materializations: HashMap<String, Materialization>,
}

// ── Stage 2 output ──────────────────────────────────────────────────────

/// Output of Stage 2 (DAG Build): dependency graph and execution order.
struct DagOutput {
    dag: ModelDag,
    topo_order: Vec<String>,
    /// Filtered model names (after selector application)
    selected_models: Vec<String>,
}

// ── Stage 3 output ──────────────────────────────────────────────────────

/// Output of Stage 3 (Analyze): qualified SQL and static analysis.
struct AnalyzeOutput {
    /// Compiled models with table references qualified to 3-part names
    qualified_models: Vec<CompileOutput>,
    /// Qualification map (bare name → database.schema.table)
    qualification_map: HashMap<String, ff_sql::qualify::QualifiedRef>,
    /// Set of ephemeral model names
    ephemeral_models: HashSet<String>,
}

// ── Stage 4 output ──────────────────────────────────────────────────────

/// Output of Stage 4 (Resolve): final SQL with ephemerals inlined, results written.
struct ResolveOutput {
    results: Vec<ModelCompileResult>,
    success_count: usize,
    ephemeral_count: usize,
}

// ── Stage 5 output ──────────────────────────────────────────────────────

/// Output of Stage 5 (Validate): post-compile validation.
struct ValidateOutput {
    failed: bool,
}

/// Execute the compile command.
///
/// Orchestrates the 5-stage pipeline: Render → DAG → Analyze → Resolve → Validate.
pub(crate) async fn execute(args: &CompileArgs, global: &GlobalArgs) -> Result<()> {
    use ff_core::config::Config;

    let start_time = Instant::now();
    let mut project = load_project(global)?;
    let json_mode = args.output == OutputFormat::Json;

    let database = Config::resolve_database(global.database.as_deref());
    let comment_ctx =
        common::build_query_comment_context(&project.config, global.database.as_deref());

    let base_vars = project.config.get_merged_vars(database.as_deref());
    let vars = merge_vars(&base_vars, &args.vars)?;

    if global.verbose {
        if let Some(ref db_name) = database {
            eprintln!(
                "[verbose] Using database connection '{}' for compilation",
                db_name
            );
        }
    }

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let template_ctx = common::build_template_context(&project, global.database.as_deref(), false);
    let jinja = JinjaEnvironment::with_context(&vars, &macro_paths, &template_ctx);

    let all_model_names: Vec<String> = project
        .model_names()
        .into_iter()
        .map(String::from)
        .collect();
    let external_tables = common::build_external_tables_lookup(&project);
    let mut known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();
    for seed in &project.seeds {
        known_models.insert(seed.name.to_string());
    }

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
    let default_materialization = project.config.materialization;

    let compile_ctx = CompileContext {
        jinja: &jinja,
        parser: &parser,
        known_models: &known_models,
        external_tables: &external_tables,
        project_root: &project_root,
        output_dir: &output_dir,
        default_materialization,
        comment_ctx: comment_ctx.as_ref(),
        template_ctx: &template_ctx,
        vars: &vars,
        macro_paths: &macro_paths,
    };

    // ── Stage 1: Render ─────────────────────────────────────────────────
    let render_out = stage_render(&mut project, &all_model_names, &compile_ctx, json_mode);

    // ── Stage 2: DAG Build ──────────────────────────────────────────────
    let dag_out = stage_dag_build(
        &project,
        &render_out.dependencies,
        &args.nodes,
        &all_model_names,
        args,
        global,
        json_mode,
    )?;

    // Filter compiled models to only selected ones
    let selected_set: HashSet<String> = dag_out.selected_models.iter().cloned().collect();
    let compiled_models: Vec<CompileOutput> = render_out
        .compiled
        .into_iter()
        .filter(|m| selected_set.contains(&m.name))
        .collect();

    // ── Stage 3: Analyze ────────────────────────────────────────────────
    let analyze_out = stage_analyze(
        &project,
        compiled_models,
        &dag_out.topo_order,
        &external_tables,
        args,
        global,
        json_mode,
    )?;

    // ── Stage 4: Resolve ────────────────────────────────────────────────
    let resolve_out = stage_resolve(
        analyze_out.qualified_models,
        &render_out.dependencies,
        &render_out.materializations,
        &comment_ctx,
        &mut project,
        &output_dir,
        args,
        global,
        json_mode,
        render_out.failures,
    )?;

    // ── Stage 5: Validate & Persist ─────────────────────────────────────
    let failure_count = resolve_out
        .results
        .iter()
        .filter(|r| matches!(r.status, RunStatus::Error))
        .count();
    let validate_out = stage_validate(
        &project,
        &resolve_out.results,
        &render_out.dependencies,
        &dag_out.dag,
        &dag_out.selected_models,
        &known_models,
        &analyze_out.qualification_map,
        &analyze_out.ephemeral_models,
        failure_count,
        args,
        global,
        json_mode,
    );

    // ── Output ──────────────────────────────────────────────────────────
    if json_mode {
        let results = CompileResults {
            timestamp: Utc::now(),
            elapsed_secs: start_time.elapsed().as_secs_f64(),
            total_models: dag_out.selected_models.len(),
            success_count: resolve_out.success_count,
            failure_count,
            manifest_path: None,
            results: resolve_out.results,
        };
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else if args.parse_only {
        println!(
            "\nValidated {} models successfully (no files written)",
            dag_out.selected_models.len()
        );
    } else {
        let non_ephemeral_count = dag_out.selected_models.len() - resolve_out.ephemeral_count;
        if resolve_out.ephemeral_count > 0 {
            println!(
                "\nCompiled {} models ({} ephemeral inlined) to {}",
                non_ephemeral_count,
                resolve_out.ephemeral_count,
                output_dir.display()
            );
        } else {
            println!(
                "\nCompiled {} models to {}",
                dag_out.selected_models.len(),
                output_dir.display()
            );
        }
    }

    if failure_count > 0 || validate_out.failed {
        return Err(crate::commands::common::ExitCode(1).into());
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// Stage 1: Render
// ═══════════════════════════════════════════════════════════════════════════

/// Stage 1: Render all Jinja templates, parse SQL, and extract dependencies.
fn stage_render(
    project: &mut Project,
    model_names: &[String],
    ctx: &CompileContext<'_>,
    json_mode: bool,
) -> RenderOutput {
    let model_count = model_names.len();
    let mut compiled: Vec<CompileOutput> = Vec::with_capacity(model_count);
    let mut failures: Vec<ModelCompileResult> = Vec::new();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::with_capacity(model_count);
    let mut materializations: HashMap<String, Materialization> =
        HashMap::with_capacity(model_count);

    for name in model_names {
        match compile_model_phase1(project, name, ctx) {
            Ok(output) => {
                dependencies.insert(name.clone(), output.dependencies.clone());
                materializations.insert(name.clone(), output.materialization);
                compiled.push(output);
            }
            Err(e) => {
                failures.push(ModelCompileResult {
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

    RenderOutput {
        compiled,
        failures,
        dependencies,
        materializations,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Stage 2: DAG Build
// ═══════════════════════════════════════════════════════════════════════════

/// Stage 2: Build the dependency DAG, compute topological order, apply selectors.
fn stage_dag_build(
    project: &Project,
    dependencies: &HashMap<String, Vec<String>>,
    nodes_arg: &Option<String>,
    all_model_names: &[String],
    args: &CompileArgs,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<DagOutput> {
    let dag = ModelDag::build(dependencies).context("Failed to build dependency graph")?;
    let topo_order = dag
        .topological_order()
        .context("Circular dependency detected")?;

    let selected_models: Vec<String> = common::resolve_nodes(project, &dag, nodes_arg)?;

    if !json_mode && nodes_arg.is_some() {
        if args.parse_only {
            println!(
                "Validating {} models (parse-only)...\n",
                selected_models.len()
            );
        } else {
            println!("Compiling {} models...\n", selected_models.len());
        }
    }

    if global.verbose && nodes_arg.is_some() {
        eprintln!(
            "[verbose] Compiling {} models (of {} total)",
            selected_models.len(),
            all_model_names.len()
        );
    }

    Ok(DagOutput {
        dag,
        topo_order,
        selected_models,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Stage 3: Analyze
// ═══════════════════════════════════════════════════════════════════════════

/// Stage 3: Run static analysis and qualify table references.
fn stage_analyze(
    project: &Project,
    mut compiled_models: Vec<CompileOutput>,
    topo_order: &[String],
    external_tables: &HashSet<String>,
    args: &CompileArgs,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<AnalyzeOutput> {
    // Static analysis (non-fatal)
    if !args.skip_static_analysis {
        let analysis_result = run_static_analysis(
            project,
            &compiled_models,
            topo_order,
            external_tables,
            args,
            global,
            json_mode,
        );
        if let Err(e) = analysis_result {
            if !json_mode {
                eprintln!("Static analysis error: {}", e);
            }
        }
    }

    // Build qualification map and qualify table references
    let compiled_schemas: HashMap<String, Option<String>> = compiled_models
        .iter()
        .map(|m| {
            let schema = project
                .get_model(&m.name)
                .and_then(|model| model.config.schema.clone())
                .or_else(|| project.config.get_schema(None).map(|s| s.to_string()));
            (m.name.clone(), schema)
        })
        .collect();
    let qualification_map = common::build_qualification_map(project, &compiled_schemas);

    for compiled in &mut compiled_models {
        qualify_statements(&mut compiled.statements, &qualification_map);
        compiled.sql = compiled
            .statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(";\n");

        if let Some(ref mut inc_stmts) = compiled.incremental_statements {
            qualify_statements(inc_stmts, &qualification_map);
            compiled.incremental_sql = Some(
                inc_stmts
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(";\n"),
            );
        }
    }

    let ephemeral_models: HashSet<String> = compiled_models
        .iter()
        .filter(|m| m.materialization == Materialization::Ephemeral)
        .map(|m| m.name.clone())
        .collect();

    Ok(AnalyzeOutput {
        qualified_models: compiled_models,
        qualification_map,
        ephemeral_models,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Stage 4: Resolve
// ═══════════════════════════════════════════════════════════════════════════

/// Stage 4: Inline ephemerals, generate final SQL, write to disk.
#[allow(clippy::too_many_arguments)]
fn stage_resolve(
    compiled_models: Vec<CompileOutput>,
    dependencies: &HashMap<String, Vec<String>>,
    materializations: &HashMap<String, Materialization>,
    comment_ctx: &Option<ff_core::query_comment::QueryCommentContext>,
    project: &mut Project,
    output_dir: &Path,
    args: &CompileArgs,
    global: &GlobalArgs,
    json_mode: bool,
    initial_failures: Vec<ModelCompileResult>,
) -> Result<ResolveOutput> {
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

    let mut results: Vec<ModelCompileResult> = initial_failures;
    let mut success_count = 0;

    for compiled in compiled_models {
        if compiled.materialization == Materialization::Ephemeral {
            if !json_mode {
                println!("  \u{2713} {} (ephemeral) [inlined]", compiled.name);
            }
            success_count += 1;
            results.push(ModelCompileResult {
                model: compiled.name,
                status: RunStatus::Success,
                materialization: "ephemeral".to_string(),
                output_path: None,
                dependencies: compiled.dependencies,
                error: None,
            });
            continue;
        }

        let final_sql = resolve_final_sql(
            &compiled,
            &ephemeral_sql,
            dependencies,
            materializations,
            global,
        )?;

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
            results.push(ModelCompileResult {
                model: compiled.name,
                status: RunStatus::Success,
                materialization: compiled.materialization.to_string(),
                output_path: None,
                dependencies: compiled.dependencies,
                error: None,
            });
        } else {
            write_compiled_output(
                &compiled,
                &final_sql,
                comment_ctx,
                project,
                global,
                json_mode,
            )?;
            success_count += 1;
            results.push(ModelCompileResult {
                model: compiled.name,
                status: RunStatus::Success,
                materialization: compiled.materialization.to_string(),
                output_path: Some(compiled.output_path.display().to_string()),
                dependencies: compiled.dependencies,
                error: None,
            });
        }
    }

    // Compile hooks and tests to target
    if !args.parse_only {
        let hooks_count = compile_hooks_to_target(project, output_dir, global);
        let tests_count = compile_tests_to_target(project, output_dir, global);
        if !json_mode && (hooks_count > 0 || tests_count > 0) {
            println!(
                "  Compiled {} hook(s) and {} test(s) to target",
                hooks_count, tests_count
            );
        }
    }

    Ok(ResolveOutput {
        results,
        success_count,
        ephemeral_count,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Stage 5: Validate & Persist
// ═══════════════════════════════════════════════════════════════════════════

/// Stage 5: Populate meta DB and run post-compile validation checks.
#[allow(clippy::too_many_arguments)]
fn stage_validate(
    project: &Project,
    compile_results: &[ModelCompileResult],
    dependencies: &HashMap<String, Vec<String>>,
    dag: &ModelDag,
    selected_models: &[String],
    known_models: &HashSet<String>,
    qualification_map: &HashMap<String, ff_sql::qualify::QualifiedRef>,
    ephemeral_models: &HashSet<String>,
    failure_count: usize,
    args: &CompileArgs,
    global: &GlobalArgs,
    json_mode: bool,
) -> ValidateOutput {
    // Persist to meta DB
    if !args.parse_only && failure_count == 0 {
        populate_meta_compile(project, compile_results, dependencies, global);
    }

    // Post-compile validation
    let mut validation_failed = false;
    if failure_count == 0 && !json_mode {
        let mut vctx = ValidationContext::new();
        let known_models_ref: HashSet<&str> = known_models.iter().map(|s| s.as_str()).collect();
        let macro_paths = project.config.macro_paths_absolute(&project.root);

        println!();
        validation::validate_duplicates(project, &mut vctx);
        validation::validate_qualified_uniqueness(qualification_map, ephemeral_models, &mut vctx);
        validation::validate_schemas(project, selected_models, &known_models_ref, &mut vctx);
        validation::validate_sources(project);
        validation::validate_macros(&project.config.vars, &macro_paths, &mut vctx);

        if args.governance {
            validation::validate_governance(project, selected_models, &mut vctx);
        }

        validation::validate_documentation(project, selected_models, &mut vctx);
        validation::validate_run_groups(project, dag, &mut vctx);

        // Run SQL rules from meta DB if configured
        if let Some(meta_db) = common::open_meta_db(project) {
            if let Some((_project_id, run_id, _model_id_map)) = common::populate_meta_phase1(
                &meta_db,
                project,
                "compile-validate",
                args.nodes.as_deref(),
            ) {
                validation::validate_rules(project, &meta_db, &mut vctx);
                let status = if vctx.error_count() > 0 {
                    "error"
                } else {
                    "success"
                };
                common::complete_meta_run(&meta_db, run_id, status);
            }
        }

        let error_count = vctx.error_count();
        let warning_count = vctx.warning_count();

        if error_count > 0 || warning_count > 0 {
            println!();
            for issue in &vctx.issues {
                println!("{}", issue);
            }
        }

        if error_count > 0 || (args.strict && warning_count > 0) {
            validation_failed = true;
        }

        if error_count > 0 || warning_count > 0 {
            println!(
                "\nValidation: {} errors, {} warnings",
                error_count, warning_count
            );
        }
    }

    ValidateOutput {
        failed: validation_failed,
    }
}

/// Resolve the final SQL for a non-ephemeral model, inlining any ephemeral dependencies.
fn resolve_final_sql(
    compiled: &CompileOutput,
    ephemeral_sql: &HashMap<String, String>,
    dependencies: &HashMap<String, Vec<String>>,
    materializations: &HashMap<String, Materialization>,
    global: &GlobalArgs,
) -> Result<String> {
    if ephemeral_sql.is_empty() {
        return Ok(compiled.sql.clone());
    }

    let is_ephemeral = |name: &str| materializations.get(name) == Some(&Materialization::Ephemeral);
    let get_sql = |name: &str| ephemeral_sql.get(name).cloned();

    let (ephemeral_deps, order) =
        collect_ephemeral_dependencies(&compiled.name, dependencies, is_ephemeral, get_sql);

    if ephemeral_deps.is_empty() {
        return Ok(compiled.sql.clone());
    }

    if global.verbose {
        eprintln!(
            "[verbose] Inlining {} ephemeral model(s) into {}",
            ephemeral_deps.len(),
            compiled.name
        );
    }
    Ok(inline_ephemeral_ctes(
        &compiled.sql,
        &ephemeral_deps,
        &order,
    )?)
}

/// Write compiled SQL to disk, attaching query comments and updating the in-memory model.
///
/// For incremental models with dual-path compilation, writes:
/// - `<name>.full.sql` — full refresh path (is_exists=false)
/// - `<name>.incremental.sql` — incremental path (is_exists=true)
/// - `<name>.sql` — default path (same as full)
fn write_compiled_output(
    compiled: &CompileOutput,
    final_sql: &str,
    comment_ctx: &Option<ff_core::query_comment::QueryCommentContext>,
    project: &mut Project,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<()> {
    if let Some(parent) = compiled.output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory for model: {}", compiled.name))?;
    }

    let placement = comment_ctx
        .as_ref()
        .map(|c| c.config.placement)
        .unwrap_or_default();
    let sql_to_write = match &compiled.query_comment {
        Some(comment) => {
            ff_core::query_comment::attach_query_comment(final_sql, comment, placement)
        }
        None => final_sql.to_string(),
    };
    std::fs::write(&compiled.output_path, &sql_to_write)
        .with_context(|| format!("Failed to write compiled SQL for model: {}", compiled.name))?;

    // Dual-path output for incremental models
    if let Some(ref inc_sql) = compiled.incremental_sql {
        let inc_to_write = match &compiled.query_comment {
            Some(comment) => {
                ff_core::query_comment::attach_query_comment(inc_sql, comment, placement)
            }
            None => inc_sql.clone(),
        };

        // Write <name>.full.sql
        let full_path = dual_path(&compiled.output_path, "full");
        std::fs::write(&full_path, &sql_to_write).with_context(|| {
            format!("Failed to write full path SQL for model: {}", compiled.name)
        })?;

        // Write <name>.incremental.sql
        let inc_path = dual_path(&compiled.output_path, "incremental");
        std::fs::write(&inc_path, &inc_to_write).with_context(|| {
            format!(
                "Failed to write incremental path SQL for model: {}",
                compiled.name
            )
        })?;

        if global.verbose {
            eprintln!(
                "[verbose] Dual-path compiled {} -> {}, {}",
                compiled.name,
                full_path.display(),
                inc_path.display()
            );
        }
    }

    if let Some(model) = project.get_model_mut(&compiled.name) {
        model.compiled_sql = Some(final_sql.to_string());
    }

    if !json_mode {
        if compiled.incremental_sql.is_some() {
            println!(
                "  \u{2713} {} ({}) [full + incremental]",
                compiled.name, compiled.materialization
            );
        } else {
            println!(
                "  \u{2713} {} ({})",
                compiled.name, compiled.materialization
            );
        }
    }

    if global.verbose {
        eprintln!(
            "[verbose] Compiled {} -> {}",
            compiled.name,
            compiled.output_path.display()
        );
    }

    Ok(())
}

/// Compute a dual-path output filename: `foo.sql` -> `foo.<suffix>.sql`
fn dual_path(base: &Path, suffix: &str) -> std::path::PathBuf {
    let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("model");
    let ext = base.extension().and_then(|e| e.to_str()).unwrap_or("sql");
    let new_name = format!("{}.{}.{}", stem, suffix, ext);
    base.with_file_name(new_name)
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
///
/// For incremental models, performs dual-path compilation:
/// - `sql` / `statements` — full path (is_exists=false)
/// - `incremental_sql` / `incremental_statements` — incremental path (is_exists=true)
fn compile_model_phase1(
    project: &mut Project,
    name: &str,
    ctx: &CompileContext<'_>,
) -> Result<CompileOutput> {
    // Immutable borrow block: render + parse + extract deps + resolve functions
    let (rendered, model_deps, ext_deps, statements, raw_sql) = {
        let model = project
            .get_model(name)
            .with_context(|| format!("Model not found: {}", name))?;
        let model_ctx = common::build_model_context(model, project);
        let rendered = ctx
            .jinja
            .render_with_model(&model.raw_sql, &model_ctx)
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

        // Filter out self-references: a model referencing its own table (e.g.
        // incremental models) should not create a dependency on itself as that
        // would introduce a cycle in the DAG.
        model_deps.retain(|dep| !dep.eq_ignore_ascii_case(name));

        if !remaining_unknown.is_empty() {
            let unknown_list = remaining_unknown.join(", ");
            anyhow::bail!(
                "Unknown dependencies in model '{}': [{}]. \
                 Each table must be defined as a model, seed, source, or function.",
                name,
                unknown_list
            );
        }

        let raw_sql = model.raw_sql.clone();
        (rendered, model_deps, ext_deps, statements, raw_sql)
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
    // Config is already populated from YAML during model loading

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

    // Dual-path compilation for incremental models
    let (final_sql, final_stmts, incremental_sql, incremental_stmts) = if mat
        == Materialization::Incremental
    {
        let model_ref = project
            .get_model(name)
            .with_context(|| format!("Model not found: {}", name))?;
        let model_ctx_dp = common::build_model_context(model_ref, project);

        // Full path: is_exists() = false
        let jinja_full = JinjaEnvironment::with_is_exists(
            ctx.vars,
            ctx.macro_paths,
            Some(ctx.template_ctx),
            false,
        );
        let full_rendered = jinja_full
            .render_with_model(&raw_sql, &model_ctx_dp)
            .with_context(|| format!("Failed to render full path template for model: {}", name))?;
        let full_stmts = ctx
            .parser
            .parse(&full_rendered)
            .with_context(|| format!("Failed to parse full path SQL for model: {}", name))?;

        // Incremental path: is_exists() = true
        let jinja_inc = JinjaEnvironment::with_is_exists(
            ctx.vars,
            ctx.macro_paths,
            Some(ctx.template_ctx),
            true,
        );
        let inc_rendered = jinja_inc
            .render_with_model(&raw_sql, &model_ctx_dp)
            .with_context(|| {
                format!(
                    "Failed to render incremental path template for model: {}",
                    name
                )
            })?;
        let inc_stmts = ctx
            .parser
            .parse(&inc_rendered)
            .with_context(|| format!("Failed to parse incremental path SQL for model: {}", name))?;

        // Update model's compiled_sql to the full path
        if let Some(m) = project.get_model_mut(name) {
            m.compiled_sql = Some(full_rendered.clone());
        }

        (
            full_rendered,
            full_stmts,
            Some(inc_rendered),
            Some(inc_stmts),
        )
    } else {
        (rendered, statements, None, None)
    };

    Ok(CompileOutput {
        name: name.to_string(),
        sql: final_sql,
        statements: final_stmts,
        materialization: mat,
        dependencies: model_deps,
        output_path,
        query_comment,
        incremental_sql,
        incremental_statements: incremental_stmts,
    })
}

// Config is now read exclusively from YAML during model loading.
// The extract_model_config() function has been removed.

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

/// Populate meta database rows for a single successfully compiled model.
fn populate_single_model_compile(
    conn: &ff_meta::DuckDbConnection,
    project: &Project,
    result: &ModelCompileResult,
    model_id: i64,
    dependencies: &HashMap<String, Vec<String>>,
    model_id_map: &HashMap<ff_core::ModelName, i64>,
) -> ff_meta::MetaResult<()> {
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
            .filter_map(|d| model_id_map.get(d.as_str()).copied())
            .collect();
        ff_meta::populate::compilation::populate_dependencies(conn, model_id, &dep_ids)?;
    }

    let ext_deps: Vec<&str> = project
        .get_model(&result.model)
        .map(|m| m.external_deps.iter().map(|t| t.as_ref()).collect())
        .unwrap_or_default();
    if !ext_deps.is_empty() {
        ff_meta::populate::compilation::populate_external_dependencies(conn, model_id, &ext_deps)?;
    }

    Ok(())
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
            let Some(&model_id) = model_id_map.get(result.model.as_str()) else {
                continue;
            };
            populate_single_model_compile(
                conn,
                project,
                result,
                model_id,
                dependencies,
                &model_id_map,
            )?;
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

/// Run DataFusion-based static analysis on compiled models.
///
/// For incremental models with dual-path compilation, static analysis is run on
/// both the full path and incremental path SQL, with diagnostics annotated by path.
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

    // Primary analysis on full path SQL
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
        if let Some(plan_result) = result.model_plans.get(explain_model.as_str()) {
            println!("LogicalPlan for '{}':\n", explain_model);
            println!("{}", plan_result.plan.display_indent_schema());
        } else if let Some(err) = result.failures.get(explain_model.as_str()) {
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
                eprintln!("  [{label}] {model_name} [full]: {mismatch}");
            }
        },
        |model, err| {
            if global.verbose {
                eprintln!(
                    "[verbose] Static analysis failed for '{}' [full]: {}",
                    model, err
                );
            }
        },
    );

    // Run analysis on incremental path SQL (for models that have dual-path output)
    let inc_sources: HashMap<String, String> = compiled_models
        .iter()
        .filter_map(|m| {
            m.incremental_sql
                .as_ref()
                .map(|sql| (m.name.clone(), sql.clone()))
        })
        .collect();

    let (inc_plan_count, inc_failure_count) = if !inc_sources.is_empty() {
        if global.verbose {
            eprintln!(
                "[verbose] Running static analysis on {} incremental path(s)...",
                inc_sources.len()
            );
        }
        // Build SQL sources for incremental analysis: non-incremental models keep
        // their full SQL, incremental models use their incremental SQL.
        let mut inc_all_sources = sql_sources.clone();
        for (name, sql) in &inc_sources {
            inc_all_sources.insert(name.clone(), sql.clone());
        }

        match super::common::run_static_analysis_pipeline(
            project,
            &inc_all_sources,
            topo_order,
            external_tables,
        ) {
            Ok(inc_output) => {
                let (_, pc, fc) = common::report_static_analysis_results(
                    &inc_output.result,
                    &inc_output.overrides,
                    |model_name, mismatch, is_error| {
                        // Only report diagnostics for models that have incremental paths
                        if inc_sources.contains_key(model_name) && !json_mode {
                            let label = if is_error { "error" } else { "warn" };
                            eprintln!("  [{label}] {model_name} [incremental]: {mismatch}");
                        }
                    },
                    |model, err| {
                        if inc_sources.contains_key(model) && global.verbose {
                            eprintln!(
                                "[verbose] Static analysis failed for '{}' [incremental]: {}",
                                model, err
                            );
                        }
                    },
                );
                (pc, fc)
            }
            Err(e) => {
                if global.verbose {
                    eprintln!("[verbose] Incremental path static analysis error: {}", e);
                }
                (0, 0)
            }
        }
    } else {
        (0, 0)
    };

    let total_plans = plan_count + inc_plan_count;
    let total_failures = failure_count + inc_failure_count;
    if !json_mode && (total_plans > 0 || total_failures > 0) {
        eprintln!(
            "Static analysis: {} models planned, {} failures",
            total_plans, total_failures
        );
    }

    if output.has_errors && global.verbose {
        eprintln!("[verbose] Static analysis found schema errors");
    }

    Ok(())
}

/// Compile pre/post hooks to `target/compiled/<model>/hooks/` directory.
///
/// For each model with `pre_hook` or `post_hook` in its config, renders the
/// hook SQL and writes it to the target directory. Multiple hooks are written
/// as `pre_hook_1.sql`, `pre_hook_2.sql`, etc.
///
/// Returns the number of hook files written.
fn compile_hooks_to_target(project: &Project, output_dir: &Path, global: &GlobalArgs) -> usize {
    let mut count = 0;

    for model in project.models.values() {
        let model_name = model.name.as_ref();
        let hooks_dir = output_dir
            .parent()
            .unwrap_or(output_dir)
            .join(model_name)
            .join("hooks");

        let has_hooks = !model.config.pre_hook.is_empty() || !model.config.post_hook.is_empty();
        if !has_hooks {
            continue;
        }

        if let Err(e) = std::fs::create_dir_all(&hooks_dir) {
            eprintln!(
                "[warn] Failed to create hooks directory for {}: {}",
                model_name, e
            );
            continue;
        }

        for (i, hook) in model.config.pre_hook.iter().enumerate() {
            let filename = if model.config.pre_hook.len() == 1 {
                "pre_hook.sql".to_string()
            } else {
                format!("pre_hook_{}.sql", i + 1)
            };
            let path = hooks_dir.join(&filename);
            if let Err(e) = std::fs::write(&path, hook) {
                eprintln!("[warn] Failed to write pre_hook for {}: {}", model_name, e);
            } else {
                count += 1;
                if global.verbose {
                    eprintln!("[verbose] Wrote hook {}", path.display());
                }
            }
        }

        for (i, hook) in model.config.post_hook.iter().enumerate() {
            let filename = if model.config.post_hook.len() == 1 {
                "post_hook.sql".to_string()
            } else {
                format!("post_hook_{}.sql", i + 1)
            };
            let path = hooks_dir.join(&filename);
            if let Err(e) = std::fs::write(&path, hook) {
                eprintln!("[warn] Failed to write post_hook for {}: {}", model_name, e);
            } else {
                count += 1;
                if global.verbose {
                    eprintln!("[verbose] Wrote hook {}", path.display());
                }
            }
        }
    }

    count
}

/// Compile schema tests to `target/compiled/tests/` directory.
///
/// For each model's schema tests (not_null, unique, accepted_values, etc.),
/// generates the test SQL and writes it to
/// `target/compiled/tests/<model>__<test_type>__<column>.sql`.
///
/// Returns the number of test files written.
fn compile_tests_to_target(project: &Project, output_dir: &Path, global: &GlobalArgs) -> usize {
    let tests_dir = output_dir.parent().unwrap_or(output_dir).join("tests");

    let all_tests: Vec<_> = project
        .models
        .values()
        .flat_map(|model| model.get_schema_tests())
        .collect();

    if all_tests.is_empty() {
        return 0;
    }

    if let Err(e) = std::fs::create_dir_all(&tests_dir) {
        eprintln!("[warn] Failed to create tests directory: {}", e);
        return 0;
    }

    let mut count = 0;
    for test in &all_tests {
        let sql = ff_test::generator::generate_test_sql(test);
        let filename = format!("{}__{}__{}.sql", test.model, test.test_type, test.column);
        let path = tests_dir.join(&filename);
        if let Err(e) = std::fs::write(&path, &sql) {
            eprintln!("[warn] Failed to write test SQL for {}: {}", filename, e);
        } else {
            count += 1;
            if global.verbose {
                eprintln!("[verbose] Wrote test {}", path.display());
            }
        }
    }

    count
}
