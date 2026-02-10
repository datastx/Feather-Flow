//! Parse command implementation

use anyhow::{Context, Result};
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use std::collections::HashSet;

use crate::cli::{GlobalArgs, ParseArgs, ParseOutput};
use crate::commands::common::{filter_models, load_project};

/// Execute the parse command
pub async fn execute(args: &ParseArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    // Create SQL parser
    let dialect_str = project.config.dialect.to_string();
    let dialect = args.dialect.as_deref().unwrap_or(&dialect_str);
    let parser = SqlParser::from_dialect_name(dialect).context("Invalid SQL dialect")?;

    // Create Jinja environment
    let jinja = JinjaEnvironment::new(&project.config.vars);

    // Filter models if specified
    let model_names = filter_models(&project, &args.models);

    // Collect external tables as a set
    let external_tables: HashSet<String> = project.config.external_tables.iter().cloned().collect();
    let known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();

    if global.verbose {
        eprintln!("[verbose] Parsing {} models", model_names.len());
    }

    // Process each model
    let mut all_deps: Vec<ModelDeps> = Vec::new();

    for name in &model_names {
        let model = project
            .get_model(name)
            .with_context(|| format!("Model not found: {}", name))?;

        // Render Jinja template
        let rendered = jinja
            .render(&model.raw_sql)
            .with_context(|| format!("Failed to render template for model: {}", name))?;

        // Parse SQL
        let statements = parser
            .parse(&rendered)
            .with_context(|| format!("Failed to parse SQL for model: {}", name))?;

        // Extract dependencies
        let deps = extract_dependencies(&statements);

        // Categorize dependencies
        let (model_deps, ext_deps) = ff_sql::extractor::categorize_dependencies(
            deps.clone(),
            &known_models,
            &external_tables,
        );

        all_deps.push(ModelDeps {
            name: name.clone(),
            path: model.path.display().to_string(),
            model_dependencies: model_deps,
            external_dependencies: ext_deps,
            all_tables: deps.into_iter().collect(),
        });
    }

    // Output results based on format
    match args.output {
        ParseOutput::Json => {
            let json =
                serde_json::to_string_pretty(&all_deps).context("Failed to serialize to JSON")?;
            println!("{}", json);
        }
        ParseOutput::Pretty => {
            for model_dep in &all_deps {
                println!("\n{}", "=".repeat(60));
                println!("Model: {}", model_dep.name);
                println!("Path: {}", model_dep.path);
                println!();
                println!("Model Dependencies:");
                if model_dep.model_dependencies.is_empty() {
                    println!("  (none)");
                } else {
                    for dep in &model_dep.model_dependencies {
                        println!("  - {}", dep);
                    }
                }
                println!();
                println!("External Dependencies:");
                if model_dep.external_dependencies.is_empty() {
                    println!("  (none)");
                } else {
                    for dep in &model_dep.external_dependencies {
                        println!("  - {}", dep);
                    }
                }
            }
        }
        ParseOutput::Deps => {
            for model_dep in &all_deps {
                let mut deps: Vec<String> = model_dep.model_dependencies.clone();
                for ext in &model_dep.external_dependencies {
                    deps.push(format!("{} (external)", ext));
                }

                if deps.is_empty() {
                    println!("{}: (no dependencies)", model_dep.name);
                } else {
                    println!("{}: {}", model_dep.name, deps.join(", "));
                }
            }
        }
    }

    Ok(())
}

/// Model dependencies for output
#[derive(Debug, serde::Serialize)]
struct ModelDeps {
    name: String,
    path: String,
    model_dependencies: Vec<String>,
    external_dependencies: Vec<String>,
    all_tables: Vec<String>,
}
