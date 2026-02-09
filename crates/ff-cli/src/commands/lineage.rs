//! Lineage command implementation — column-level lineage across models

use anyhow::{Context, Result};
use ff_core::source::build_source_lookup;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_column_lineage, ProjectLineage, SqlParser};
use std::collections::HashSet;
use std::path::Path;

use crate::cli::{GlobalArgs, LineageArgs, LineageDirection, LineageOutput};

/// Execute the lineage command
pub async fn execute(args: &LineageArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    let known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();

    // Build external tables lookup for categorization
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    let source_tables = build_source_lookup(&project.sources);
    external_tables.extend(source_tables);

    let mut project_lineage = ProjectLineage::new();

    // Compile each model and extract lineage
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
                continue;
            }
        };

        if let Some(stmt) = stmts.first() {
            if let Some(lineage) = extract_column_lineage(stmt, name) {
                project_lineage.add_model_lineage(lineage);
            }
        }
    }

    // Resolve cross-model edges
    project_lineage.resolve_edges(&known_models);

    // Propagate classifications from schema definitions
    let classification_lookup = ff_core::classification::build_classification_lookup(&project);
    project_lineage.propagate_classifications(&classification_lookup);

    // Apply classification filter if specified
    if let Some(ref cls) = args.classification {
        project_lineage
            .edges
            .retain(|e| e.classification.as_deref() == Some(cls.as_str()));
    }

    // Filter and output
    match args.output {
        LineageOutput::Json => print_json(&project_lineage, args)?,
        LineageOutput::Dot => print_dot(&project_lineage, args),
        LineageOutput::Table => print_table(&project_lineage, args),
    }

    Ok(())
}

/// Print lineage as JSON
fn print_json(lineage: &ProjectLineage, args: &LineageArgs) -> Result<()> {
    if let (Some(model), Some(column)) = (&args.model, &args.column) {
        // Filter to specific column
        let edges: Vec<_> = match args.direction {
            LineageDirection::Upstream => lineage.trace_column(model, column),
            LineageDirection::Downstream => lineage.column_consumers(model, column),
            LineageDirection::Both => {
                let mut all = lineage.trace_column(model, column);
                all.extend(lineage.column_consumers(model, column));
                all
            }
        };
        println!("{}", serde_json::to_string_pretty(&edges)?);
    } else if let Some(model) = &args.model {
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

    let edges: Vec<_> = if let (Some(model), Some(column)) = (&args.model, &args.column) {
        match args.direction {
            LineageDirection::Upstream => lineage
                .trace_column(model, column)
                .into_iter()
                .cloned()
                .collect(),
            LineageDirection::Downstream => lineage
                .column_consumers(model, column)
                .into_iter()
                .cloned()
                .collect(),
            LineageDirection::Both => {
                let mut all: Vec<_> = lineage
                    .trace_column(model, column)
                    .into_iter()
                    .cloned()
                    .collect();
                all.extend(lineage.column_consumers(model, column).into_iter().cloned());
                all
            }
        }
    } else if let Some(model) = &args.model {
        lineage
            .edges
            .iter()
            .filter(|e| match args.direction {
                LineageDirection::Upstream => e.target_model == *model,
                LineageDirection::Downstream => e.source_model == *model,
                LineageDirection::Both => e.target_model == *model || e.source_model == *model,
            })
            .cloned()
            .collect()
    } else {
        lineage.edges.clone()
    };

    if edges.is_empty() {
        println!("No matching lineage edges found.");
        return;
    }

    // Print header
    println!(
        "{:<25} {:<20} {:<25} {:<20} {:<10} {:<12} TYPE",
        "SOURCE MODEL", "SOURCE COLUMN", "TARGET MODEL", "TARGET COLUMN", "DIRECT?", "CLASS"
    );
    println!("{}", "-".repeat(125));

    for edge in &edges {
        println!(
            "{:<25} {:<20} {:<25} {:<20} {:<10} {:<12} {}",
            edge.source_model,
            edge.source_column,
            edge.target_model,
            edge.target_column,
            if edge.is_direct { "yes" } else { "no" },
            edge.classification.as_deref().unwrap_or("-"),
            edge.expr_type,
        );
    }

    println!("\n{} lineage edge(s) found.", edges.len());
}
