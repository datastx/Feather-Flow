//! `ff analyze` command — run static analysis passes on SQL models

use anyhow::{Context, Result};
use ff_analysis::{
    apply_severity_overrides, propagate_schemas, AnalysisContext, PlanPassManager, RelSchema,
    SchemaCatalog, Severity, SeverityOverrides,
};
use ff_core::classification::{
    build_classification_lookup, propagate_classifications_topo, ClassificationEdge,
};
use ff_core::dag::ModelDag;
use ff_sql::{extract_column_lineage, extract_dependencies, ExprType, ProjectLineage, SqlParser};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::cli::{AnalyzeArgs, AnalyzeOutput, AnalyzeSeverity, GlobalArgs};
use crate::commands::common::{
    self, build_external_tables_lookup, build_schema_catalog, load_project,
};

/// Execute the analyze command
pub async fn execute(args: &AnalyzeArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = common::build_jinja_env_with_context(&project, global.target.as_deref(), false);

    let known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();

    let external_tables = build_external_tables_lookup(&project);
    let (schema_catalog, yaml_schemas) = build_schema_catalog(&project, &external_tables);

    // Render SQL, extract dependencies and lineage in a single pass
    let mut dep_map: HashMap<String, Vec<String>> = HashMap::with_capacity(project.models.len());
    let mut project_lineage = ProjectLineage::new();
    for (name, model) in &project.models {
        let Ok(rendered) = jinja.render(&model.raw_sql) else {
            dep_map.insert(name.to_string(), vec![]);
            continue;
        };
        let Ok(stmts) = parser.parse(&rendered) else {
            dep_map.insert(name.to_string(), vec![]);
            continue;
        };

        let raw_deps = extract_dependencies(&stmts);
        let model_deps: Vec<String> = raw_deps
            .into_iter()
            .filter(|d| known_models.contains(d.as_str()))
            .collect();
        dep_map.insert(name.to_string(), model_deps);

        if let Some(stmt) = stmts.first() {
            if let Some(lineage) = extract_column_lineage(stmt, name) {
                project_lineage.add_model_lineage(lineage);
            }
        }
    }
    project_lineage.resolve_edges(&known_models);

    let dag = ModelDag::build(&dep_map).context("Failed to build dependency DAG")?;
    let topo_order = dag
        .topological_order()
        .context("Failed to get topological order")?;

    let resolved = super::common::resolve_nodes(&project, &dag, &args.nodes)?;
    let resolved_set: HashSet<String> = resolved.into_iter().collect();

    let order: Vec<String> = topo_order
        .into_iter()
        .filter(|n| resolved_set.contains(n))
        .filter(|n| project.models.contains_key(n.as_str()))
        .collect();

    let sql_sources: HashMap<String, String> = order
        .iter()
        .filter_map(|name| {
            let model = project.models.get(name.as_str())?;
            match jinja.render(&model.raw_sql) {
                Ok(sql) => Some((name.clone(), sql)),
                Err(e) => {
                    if global.verbose {
                        eprintln!("[verbose] Skipping '{}': render error: {}", name, e);
                    }
                    None
                }
            }
        })
        .collect();

    if sql_sources.is_empty() {
        println!("No models to analyze.");
        return Ok(());
    }

    let order: Vec<String> = order
        .into_iter()
        .filter(|n| sql_sources.contains_key(n))
        .collect();

    let severity_overrides =
        SeverityOverrides::from_config(&project.config.analysis.severity_overrides);

    let ctx = AnalysisContext::new(project, dag, yaml_schemas, project_lineage);

    let yaml_string_map: HashMap<String, Arc<RelSchema>> = ctx
        .yaml_schemas()
        .iter()
        .map(|(k, v)| (k.to_string(), Arc::clone(v)))
        .collect();
    let mut plan_catalog: SchemaCatalog = schema_catalog;
    for ext in &external_tables {
        if !plan_catalog.contains_key(ext) {
            plan_catalog.insert(ext.clone(), Arc::new(RelSchema::empty()));
        }
    }

    let (user_fn_stubs, user_table_fn_stubs) =
        ff_analysis::build_user_function_stubs(ctx.project());
    let propagation = propagate_schemas(
        &order,
        &sql_sources,
        &yaml_string_map,
        plan_catalog,
        &user_fn_stubs,
        &user_table_fn_stubs,
    );

    if propagation.model_plans.is_empty() && propagation.failures.is_empty() {
        println!("No models to analyze.");
        return Ok(());
    }

    if global.verbose {
        for (model, err) in &propagation.failures {
            eprintln!("[verbose] Skipping '{}': planning error: {}", model, err);
        }
    }

    let pass_filter: Option<Vec<String>> = args
        .pass
        .as_ref()
        .map(|p| p.split(',').map(|s| s.trim().to_string()).collect());

    let plan_pass_manager = PlanPassManager::with_defaults();
    let diagnostics = plan_pass_manager.run(
        &order,
        &propagation.model_plans,
        &ctx,
        pass_filter.as_deref(),
    );

    // Apply user-configured severity overrides before min-severity filtering
    let diagnostics = apply_severity_overrides(diagnostics, &severity_overrides);

    let min_severity = match args.severity {
        AnalyzeSeverity::Info => Severity::Info,
        AnalyzeSeverity::Warning => Severity::Warning,
        AnalyzeSeverity::Error => Severity::Error,
    };

    let filtered: Vec<_> = diagnostics
        .into_iter()
        .filter(|d| d.severity >= min_severity)
        .collect();

    match args.output {
        AnalyzeOutput::Json => print_json(&filtered)?,
        AnalyzeOutput::Table => print_table(&filtered),
    }

    // Populate meta database (non-fatal)
    if let Some(meta_db) = common::open_meta_db(ctx.project()) {
        if let Some((_project_id, run_id, model_id_map)) =
            common::populate_meta_phase1(&meta_db, ctx.project(), "analyze", args.nodes.as_deref())
        {
            // Persist column lineage edges
            if let Err(e) = meta_db.transaction(|conn| {
                let meta_edges = build_meta_lineage_edges(ctx.lineage(), &model_id_map);
                ff_meta::populate::analysis::populate_column_lineage(conn, &meta_edges)?;

                // Propagate classifications through lineage and persist
                let declared = build_classification_lookup(ctx.project());
                let cls_edges = build_classification_edges(ctx.lineage());
                let effective = propagate_classifications_topo(&order, &cls_edges, &declared);
                let entries = build_effective_entries(&effective, &model_id_map);
                ff_meta::populate::analysis::populate_effective_classifications(conn, &entries)?;
                Ok(())
            }) {
                log::warn!("Meta database: failed to populate lineage/classifications: {e}");
            }

            let status = if filtered.iter().any(|d| d.severity == Severity::Error) {
                "error"
            } else {
                "success"
            };
            common::complete_meta_run(&meta_db, run_id, status);
        }
    }

    let has_errors = filtered.iter().any(|d| d.severity == Severity::Error);
    if has_errors {
        return Err(crate::commands::common::ExitCode(1).into());
    }

    Ok(())
}

/// Print diagnostics as a table
fn print_table(diagnostics: &[ff_analysis::Diagnostic]) {
    if diagnostics.is_empty() {
        println!("No diagnostics found.");
        return;
    }

    let model_w = diagnostics
        .iter()
        .map(|d| d.model.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let sev_w = 8; // "severity" / "warning"
    let code_w = 5;
    let pass_w = diagnostics
        .iter()
        .map(|d| d.pass_name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let msg_w = 80;

    println!(
        "{:<model_w$}  {:<sev_w$}  {:<code_w$}  {:<pass_w$}  MESSAGE",
        "MODEL",
        "SEVERITY",
        "CODE",
        "PASS",
        model_w = model_w,
        sev_w = sev_w,
        code_w = code_w,
        pass_w = pass_w,
    );

    for d in diagnostics {
        let msg = if d.message.len() > msg_w {
            // Find a char-safe boundary to avoid panicking on multi-byte UTF-8
            let truncate_at = d
                .message
                .char_indices()
                .take_while(|&(i, _)| i <= msg_w - 3)
                .last()
                .map(|(i, _)| i)
                .unwrap_or(0);
            format!("{}...", &d.message[..truncate_at])
        } else {
            d.message.clone()
        };
        println!(
            "{:<model_w$}  {:<sev_w$}  {:<code_w$}  {:<pass_w$}  {}",
            d.model,
            d.severity.to_string(),
            d.code,
            d.pass_name,
            msg,
            model_w = model_w,
            sev_w = sev_w,
            code_w = code_w,
            pass_w = pass_w,
        );
    }

    let errors = diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .count();
    let warnings = diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Warning)
        .count();
    let infos = diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Info)
        .count();
    println!(
        "\n{} diagnostics ({} errors, {} warnings, {} info)",
        diagnostics.len(),
        errors,
        warnings,
        infos
    );
}

/// Print diagnostics as JSON
fn print_json(diagnostics: &[ff_analysis::Diagnostic]) -> Result<()> {
    let json = serde_json::to_string_pretty(diagnostics)?;
    println!("{}", json);
    Ok(())
}

/// Convert ff-sql lineage edges to ff-meta lineage edges using model ID lookup.
fn build_meta_lineage_edges(
    lineage: &ProjectLineage,
    model_id_map: &HashMap<String, i64>,
) -> Vec<ff_meta::populate::analysis::LineageEdge> {
    lineage
        .edges
        .iter()
        .filter_map(|edge| {
            let target_model_id = *model_id_map.get(&edge.target_model)?;
            let source_model_id = model_id_map.get(&edge.source_model).copied();
            let lineage_kind = if edge.is_direct && edge.expr_type == ExprType::Column {
                "copy"
            } else {
                "transform"
            };
            Some(ff_meta::populate::analysis::LineageEdge {
                target_model_id,
                target_column: edge.target_column.clone(),
                source_model_id,
                source_table: if source_model_id.is_none() {
                    Some(edge.source_model.clone())
                } else {
                    None
                },
                source_column: edge.source_column.clone(),
                lineage_kind: lineage_kind.to_string(),
                is_direct: edge.is_direct,
            })
        })
        .collect()
}

/// Convert ff-sql lineage edges to classification edges for propagation.
fn build_classification_edges(lineage: &ProjectLineage) -> Vec<ClassificationEdge> {
    lineage
        .edges
        .iter()
        .map(|edge| ClassificationEdge {
            source_model: edge.source_model.clone(),
            source_column: edge.source_column.clone(),
            target_model: edge.target_model.clone(),
            target_column: edge.target_column.clone(),
            is_direct: edge.is_direct,
        })
        .collect()
}

/// Build effective classification entries for meta DB population.
fn build_effective_entries(
    effective: &HashMap<String, HashMap<String, String>>,
    model_id_map: &HashMap<String, i64>,
) -> Vec<ff_meta::populate::analysis::EffectiveClassification> {
    let mut entries = Vec::new();
    for (model, columns) in effective {
        let Some(&model_id) = model_id_map.get(model) else {
            continue;
        };
        for (column, classification) in columns {
            entries.push(ff_meta::populate::analysis::EffectiveClassification {
                model_id,
                column_name: column.clone(),
                effective_classification: classification.clone(),
            });
        }
    }
    entries
}
