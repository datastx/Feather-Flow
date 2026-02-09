//! `ff analyze` command — run static analysis passes on SQL models

use anyhow::{Context, Result};
use ff_analysis::ir::schema::RelSchema;
use ff_analysis::ir::types::{Nullability, SqlType, TypedColumn};
use ff_analysis::{lower_statement, AnalysisContext, PassManager, SchemaCatalog, Severity};
use ff_core::dag::ModelDag;
use ff_core::source::build_source_lookup;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_column_lineage, ProjectLineage, SqlParser};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{AnalyzeArgs, AnalyzeOutput, AnalyzeSeverity, GlobalArgs};

/// Execute the analyze command
pub async fn execute(args: &AnalyzeArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    // Build schema catalog from YAML definitions
    let mut schema_catalog: SchemaCatalog = HashMap::new();
    let mut yaml_schemas: HashMap<String, RelSchema> = HashMap::new();

    for (name, model) in &project.models {
        if let Some(schema) = &model.schema {
            let columns: Vec<TypedColumn> = schema
                .columns
                .iter()
                .map(|col| {
                    let sql_type = col
                        .data_type
                        .as_ref()
                        .map(|dt| ff_analysis::ir::types::parse_sql_type(dt))
                        .unwrap_or_else(|| SqlType::Unknown("no type declared".to_string()));
                    let nullability = if col
                        .constraints
                        .iter()
                        .any(|c| matches!(c, ff_core::ColumnConstraint::NotNull))
                    {
                        Nullability::NotNull
                    } else {
                        Nullability::Unknown
                    };
                    TypedColumn {
                        name: col.name.clone(),
                        source_table: None,
                        sql_type,
                        nullability,
                        provenance: vec![],
                    }
                })
                .collect();
            let rel_schema = RelSchema::new(columns);
            schema_catalog.insert(name.clone(), rel_schema.clone());
            yaml_schemas.insert(name.clone(), rel_schema);
        }
    }

    // Add external tables/sources to catalog
    let source_tables = build_source_lookup(&project.sources);
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    external_tables.extend(source_tables);
    for ext in &external_tables {
        if !schema_catalog.contains_key(ext) {
            schema_catalog.insert(ext.clone(), RelSchema::empty());
        }
    }

    // Build dependency map and DAG
    let dep_map: HashMap<String, Vec<String>> = project
        .models
        .iter()
        .map(|(name, model)| {
            let deps: Vec<String> = model
                .depends_on
                .iter()
                .filter(|d| known_models.contains(*d))
                .cloned()
                .collect();
            (name.clone(), deps)
        })
        .collect();

    let dag = ModelDag::build(&dep_map).context("Failed to build dependency DAG")?;
    let topo_order = dag
        .topological_order()
        .context("Failed to get topological order")?;

    // Determine which models to analyze
    let model_filter: Option<HashSet<String>> = args
        .models
        .as_ref()
        .map(|m| m.split(',').map(|s| s.trim().to_string()).collect());

    // Build lineage
    let mut project_lineage = ProjectLineage::new();
    for (name, model) in &project.models {
        if let Ok(rendered) = jinja.render(&model.raw_sql) {
            if let Ok(stmts) = parser.parse(&rendered) {
                if let Some(stmt) = stmts.first() {
                    if let Some(lineage) = extract_column_lineage(stmt, name) {
                        project_lineage.add_model_lineage(lineage);
                    }
                }
            }
        }
    }
    project_lineage.resolve_edges(&known_models);

    // Lower each model to IR (topological order)
    let mut model_irs: HashMap<String, ff_analysis::RelOp> = HashMap::new();

    for name in &topo_order {
        if let Some(filter) = &model_filter {
            if !filter.contains(name) {
                continue;
            }
        }

        let model = match project.models.get(name) {
            Some(m) => m,
            None => continue,
        };

        let rendered = match jinja.render(&model.raw_sql) {
            Ok(sql) => sql,
            Err(e) => {
                if global.verbose {
                    eprintln!("[verbose] Skipping '{}': render error: {}", name, e);
                }
                continue;
            }
        };

        let stmts = match parser.parse(&rendered) {
            Ok(s) => s,
            Err(e) => {
                if global.verbose {
                    eprintln!("[verbose] Skipping '{}': parse error: {}", name, e);
                }
                continue;
            }
        };

        if let Some(stmt) = stmts.first() {
            match lower_statement(stmt, &schema_catalog) {
                Ok(ir) => {
                    // Add the output schema to catalog for downstream models
                    schema_catalog.insert(name.clone(), ir.schema().clone());
                    model_irs.insert(name.clone(), ir);
                }
                Err(e) => {
                    if global.verbose {
                        eprintln!("[verbose] Skipping '{}': lowering error: {}", name, e);
                    }
                }
            }
        }
    }

    if model_irs.is_empty() {
        println!("No models to analyze.");
        return Ok(());
    }

    // Create analysis context
    let ctx = AnalysisContext::new(project, dag, yaml_schemas, project_lineage);

    // Create pass manager and run passes
    let pass_manager = PassManager::with_defaults();
    let pass_filter: Option<Vec<String>> = args
        .pass
        .as_ref()
        .map(|p| p.split(',').map(|s| s.trim().to_string()).collect());

    let order: Vec<String> = topo_order
        .into_iter()
        .filter(|n| model_irs.contains_key(n))
        .collect();

    let diagnostics = pass_manager.run(&order, &model_irs, &ctx, pass_filter.as_deref());

    // Filter by severity
    let min_severity = match args.severity {
        AnalyzeSeverity::Info => Severity::Info,
        AnalyzeSeverity::Warning => Severity::Warning,
        AnalyzeSeverity::Error => Severity::Error,
    };

    let filtered: Vec<_> = diagnostics
        .into_iter()
        .filter(|d| d.severity >= min_severity)
        .collect();

    // Output
    match args.output {
        AnalyzeOutput::Json => print_json(&filtered)?,
        AnalyzeOutput::Table => print_table(&filtered),
    }

    // Exit code 1 if any Error-severity diagnostics
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

    // Column widths
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
            format!("{}...", &d.message[..msg_w - 3])
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

    // Summary
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
