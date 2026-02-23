//! Lineage command implementation — column-level lineage across models
//!
//! Uses DataFusion LogicalPlan as primary lineage engine, with AST fallback
//! for models that fail DataFusion planning.

use anyhow::{Context, Result};
use ff_core::dag::ModelDag;
use ff_core::SchemaRegistry;
use ff_sql::{
    extract_column_lineage, extract_dependencies, ExprType, LineageEdge, ModelLineage,
    ProjectLineage, SqlParser,
};
use std::collections::{HashMap, HashSet};

use crate::cli::{GlobalArgs, LineageArgs, LineageDirection, LineageOutput};
use crate::commands::common::{self, build_external_tables_lookup, load_project};

/// Execute the lineage command
pub(crate) async fn execute(args: &LineageArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = common::build_jinja_env_with_context(&project, global.target.as_deref(), false);

    let mut known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();
    for seed in &project.seeds {
        known_models.insert(seed.name.as_str());
    }
    for source_file in &project.sources {
        for table in &source_file.tables {
            known_models.insert(&table.name);
        }
    }

    // Phase 1: Render SQL, extract dependencies, build DAG
    let mut dep_map: HashMap<String, Vec<String>> = HashMap::with_capacity(project.models.len());
    let mut rendered_sql: HashMap<String, String> = HashMap::with_capacity(project.models.len());

    for (name, model) in &project.models {
        let rendered = match jinja.render(&model.raw_sql) {
            Ok(sql) => sql,
            Err(e) => {
                if global.verbose {
                    eprintln!(
                        "[verbose] Skipping lineage for '{}': render error: {}",
                        name, e
                    );
                }
                dep_map.insert(name.to_string(), vec![]);
                continue;
            }
        };

        let stmts = match parser.parse(&rendered) {
            Ok(s) => s,
            Err(e) => {
                if global.verbose {
                    eprintln!(
                        "[verbose] Skipping lineage for '{}': parse error: {}",
                        name, e
                    );
                }
                dep_map.insert(name.to_string(), vec![]);
                continue;
            }
        };

        let raw_deps = extract_dependencies(&stmts);
        let model_deps: Vec<String> = raw_deps
            .into_iter()
            .filter(|d| known_models.contains(d.as_str()))
            .collect();
        dep_map.insert(name.to_string(), model_deps);
        rendered_sql.insert(name.to_string(), rendered);
    }

    // Phase 2: Try DataFusion static analysis pipeline for richer lineage
    let external_tables = build_external_tables_lookup(&project);
    let topo_order: Vec<String> = if let Ok(dag) = ModelDag::build(&dep_map) {
        dag.topological_order().unwrap_or_default()
    } else {
        // Fallback to model key order if DAG can't be built
        project.models.keys().map(|k| k.to_string()).collect()
    };

    let sa_output = common::run_static_analysis_pipeline(
        &project,
        &rendered_sql,
        &topo_order,
        &external_tables,
    );

    // Phase 3: Build project lineage — DataFusion primary, AST fallback
    let mut project_lineage = ProjectLineage::new();

    match sa_output {
        Ok(output) => {
            let propagation = &output.result;
            let mut datafusion_models: HashSet<String> = HashSet::new();

            // DataFusion path: convert DataFusion lineage to ff-sql types
            for (model_name, plan_result) in &propagation.model_plans {
                let df_lineage =
                    ff_analysis::extract_plan_column_lineage(model_name.clone(), &plan_result.plan);
                let alias_map = ff_analysis::extract_alias_map(&plan_result.plan);
                let ast_lineage =
                    bridge_datafusion_lineage(&df_lineage, model_name.as_str(), &alias_map);
                project_lineage.add_model_lineage(ast_lineage);
                datafusion_models.insert(model_name.to_string());
            }

            if global.verbose && !propagation.failures.is_empty() {
                for (model, err) in &propagation.failures {
                    eprintln!("[verbose] DataFusion fallback for '{}': {}", model, err);
                }
            }

            // AST fallback for models that failed DataFusion planning
            for name in project.models.keys() {
                if datafusion_models.contains(name.as_str()) {
                    continue;
                }
                if let Some(sql) = rendered_sql.get(name.as_str()) {
                    if let Ok(stmts) = parser.parse(sql) {
                        if let Some(stmt) = stmts.first() {
                            if let Some(lineage) = extract_column_lineage(stmt, name) {
                                project_lineage.add_model_lineage(lineage);
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            // Full AST fallback if the pipeline fails entirely
            if global.verbose {
                eprintln!(
                    "[verbose] Static analysis pipeline failed: {}, using AST fallback",
                    e
                );
            }
            for name in project.models.keys() {
                if let Some(sql) = rendered_sql.get(name.as_str()) {
                    if let Ok(stmts) = parser.parse(sql) {
                        if let Some(stmt) = stmts.first() {
                            if let Some(lineage) = extract_column_lineage(stmt, name) {
                                project_lineage.add_model_lineage(lineage);
                            }
                        }
                    }
                }
            }
        }
    }

    project_lineage.resolve_edges(&known_models);

    let classification_lookup = ff_core::classification::build_classification_lookup(&project);
    project_lineage.propagate_classifications(&classification_lookup);

    // Compute description status from schema registry
    let registry = SchemaRegistry::from_project(&project);
    let desc_lookup = build_description_lookup(&registry, &project);
    project_lineage.compute_description_status(&desc_lookup);

    // Apply classification filter if specified
    if let Some(ref cls) = args.classification {
        project_lineage
            .edges
            .retain(|e| e.classification.as_deref() == Some(cls.as_str()));
    }

    match args.output {
        LineageOutput::Json => print_json(&project_lineage, args)?,
        LineageOutput::Dot => print_dot(&project_lineage, args),
        LineageOutput::Table => print_table(&project_lineage, args),
    }

    Ok(())
}

/// Bridge DataFusion `ModelColumnLineage` to ff-sql `ModelLineage`.
///
/// Converts DataFusion's flat edge list into the per-column structure expected
/// by the ff-sql `ProjectLineage::resolve_edges()` method. Resolves table
/// aliases (e.g. `c` → `stg_customers`) using the LogicalPlan so that
/// `resolve_edges()` can match source tables to known model names.
fn bridge_datafusion_lineage(
    df: &ff_analysis::ModelColumnLineage,
    model_name: &str,
    alias_map: &HashMap<String, String>,
) -> ModelLineage {
    use ff_sql::ColumnRef;

    let mut lineage = ModelLineage {
        model_name: model_name.to_string(),
        columns: Vec::new(),
        inspect_columns: Vec::new(),
        table_aliases: alias_map.clone(), // alias → real table name
        source_tables: HashSet::new(),
    };

    // Helper: resolve a source table name through the alias map
    let resolve_table = |name: &str| -> String {
        alias_map
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    };

    // Group edges by output_column
    let mut column_groups: HashMap<&str, Vec<&ff_analysis::ColumnLineageEdge>> = HashMap::new();
    for edge in &df.edges {
        column_groups
            .entry(&edge.output_column)
            .or_default()
            .push(edge);
    }

    for (output_col, edges) in &column_groups {
        if output_col.is_empty() {
            // Empty output_column = Inspect edges
            for edge in edges {
                if edge.kind == ff_analysis::LineageKind::Inspect {
                    let resolved = if edge.source_table.is_empty() {
                        String::new()
                    } else {
                        resolve_table(&edge.source_table)
                    };
                    let col_ref = if resolved.is_empty() {
                        ColumnRef::simple(&edge.source_column)
                    } else {
                        ColumnRef::qualified(&resolved, &edge.source_column)
                    };
                    lineage.inspect_columns.push(col_ref);
                    if !resolved.is_empty() {
                        lineage.source_tables.insert(resolved);
                    }
                }
            }
            continue;
        }

        let mut col_lineage = ff_sql::ColumnLineage {
            output_column: output_col.to_string(),
            source_columns: HashSet::new(),
            is_direct: false,
            expr_type: ExprType::Unknown,
        };

        let mut all_copy = true;
        for edge in edges {
            let resolved = if edge.source_table.is_empty() {
                String::new()
            } else {
                resolve_table(&edge.source_table)
            };
            let col_ref = if resolved.is_empty() {
                ColumnRef::simple(&edge.source_column)
            } else {
                ColumnRef::qualified(&resolved, &edge.source_column)
            };
            col_lineage.source_columns.insert(col_ref);

            if !resolved.is_empty() {
                lineage.source_tables.insert(resolved);
            }

            match edge.kind {
                ff_analysis::LineageKind::Copy => {}
                ff_analysis::LineageKind::Transform | ff_analysis::LineageKind::Inspect => {
                    all_copy = false;
                }
            }
        }

        if all_copy {
            col_lineage.is_direct = true;
            col_lineage.expr_type = ExprType::Column;
        } else {
            col_lineage.expr_type = ExprType::Expression;
        }

        lineage.columns.push(col_lineage);
    }

    lineage
}

/// Print lineage as JSON
fn print_json(lineage: &ProjectLineage, args: &LineageArgs) -> Result<()> {
    if let (Some(model), Some(column)) = (&args.node, &args.column) {
        // Filter to specific column (recursive traversal)
        let edges: Vec<_> = match args.direction {
            LineageDirection::Upstream => lineage.trace_column_recursive(model, column),
            LineageDirection::Downstream => lineage.column_consumers_recursive(model, column),
            LineageDirection::Both => {
                let mut all = lineage.trace_column_recursive(model, column);
                all.extend(lineage.column_consumers_recursive(model, column));
                all
            }
        };
        println!("{}", serde_json::to_string_pretty(&edges)?);
    } else if let Some(model) = &args.node {
        // Filter to specific model
        let edges: Vec<_> = lineage
            .edges
            .iter()
            .filter(|e| match args.direction {
                LineageDirection::Upstream => e.target_model == *model,
                LineageDirection::Downstream => e.source_model == *model,
                LineageDirection::Both => e.target_model == *model || e.source_model == *model,
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&edges)?);
    } else {
        println!("{}", serde_json::to_string_pretty(lineage)?);
    }
    Ok(())
}

/// Print lineage as DOT graph
fn print_dot(lineage: &ProjectLineage, _args: &LineageArgs) {
    print!("{}", lineage.to_dot());
}

/// Print lineage as a human-readable table
fn print_table(lineage: &ProjectLineage, args: &LineageArgs) {
    if lineage.edges.is_empty() {
        println!("No cross-model column lineage found.");
        return;
    }

    let edges: Vec<&LineageEdge> = if let (Some(model), Some(column)) = (&args.node, &args.column) {
        match args.direction {
            LineageDirection::Upstream => lineage.trace_column_recursive(model, column),
            LineageDirection::Downstream => lineage.column_consumers_recursive(model, column),
            LineageDirection::Both => {
                let mut all = lineage.trace_column_recursive(model, column);
                all.extend(lineage.column_consumers_recursive(model, column));
                all
            }
        }
    } else if let Some(model) = &args.node {
        lineage
            .edges
            .iter()
            .filter(|e| match args.direction {
                LineageDirection::Upstream => e.target_model == *model,
                LineageDirection::Downstream => e.source_model == *model,
                LineageDirection::Both => e.target_model == *model || e.source_model == *model,
            })
            .collect()
    } else {
        lineage.edges.iter().collect()
    };

    if edges.is_empty() {
        println!("No matching lineage edges found.");
        return;
    }

    // Print header
    println!(
        "{:<25} {:<20} {:<25} {:<20} {:<10} {:<12} {:<12} TYPE",
        "SOURCE MODEL",
        "SOURCE COLUMN",
        "TARGET MODEL",
        "TARGET COLUMN",
        "KIND",
        "DESC STATUS",
        "CLASS",
    );
    println!("{}", "-".repeat(145));

    for edge in &edges {
        println!(
            "{:<25} {:<20} {:<25} {:<20} {:<10} {:<12} {:<12} {}",
            edge.source_model,
            edge.source_column,
            edge.target_model,
            edge.target_column,
            edge.kind,
            edge.description_status,
            edge.classification.as_deref().unwrap_or("-"),
            edge.expr_type,
        );
    }

    println!("\n{} lineage edge(s) found.", edges.len());
}

/// Build a lookup of node_name -> { column_name_lowercase -> description }
/// from the schema registry for description status computation.
fn build_description_lookup(
    registry: &SchemaRegistry,
    project: &ff_core::Project,
) -> HashMap<String, HashMap<String, String>> {
    let mut lookup: HashMap<String, HashMap<String, String>> = HashMap::new();

    // Models
    for name in project.models.keys() {
        if let Some(columns) = registry.get_columns(name.as_str()) {
            let mut col_descs = HashMap::new();
            for (col_key, info) in columns {
                if let Some(ref desc) = info.description {
                    col_descs.insert(col_key.clone(), desc.clone());
                }
            }
            if !col_descs.is_empty() {
                lookup.insert(name.to_string(), col_descs);
            }
        }
    }

    // Sources
    for source_file in &project.sources {
        for table in &source_file.tables {
            if let Some(columns) = registry.get_columns(&table.name) {
                let mut col_descs = HashMap::new();
                for (col_key, info) in columns {
                    if let Some(ref desc) = info.description {
                        col_descs.insert(col_key.clone(), desc.clone());
                    }
                }
                if !col_descs.is_empty() {
                    lookup.insert(table.name.clone(), col_descs);
                }
            }
        }
    }

    lookup
}
