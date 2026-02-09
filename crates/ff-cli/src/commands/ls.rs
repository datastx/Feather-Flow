//! List command implementation

use anyhow::{Context, Result};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_core::exposure::Exposure;
use ff_core::selector::Selector;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::{extract_dependencies, SqlParser};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{GlobalArgs, LsArgs, LsOutput, ResourceType};
use crate::commands::common;

/// Execute the ls command
pub async fn execute(args: &LsArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Create SQL parser and Jinja environment
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = JinjaEnvironment::new(&project.config.vars);

    // Collect known models and external tables
    let external_tables: HashSet<String> = project.config.external_tables.iter().cloned().collect();
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    // Compile all models to get dependencies and config
    let mut model_info: Vec<ModelInfo> = Vec::new();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    for name in project.model_names() {
        let model = project
            .get_model(name)
            .ok_or_else(|| anyhow::anyhow!("Model '{}' not found in project", name))?;

        // Render Jinja template to get config
        let (rendered, config_values) = jinja
            .render_with_config(&model.raw_sql)
            .context(format!("Failed to render template for model: {}", name))?;

        // Parse SQL to extract dependencies
        let statements = parser
            .parse(&rendered)
            .context(format!("Failed to parse SQL for model: {}", name))?;

        // Extract and categorize dependencies
        let deps = extract_dependencies(&statements);
        let (model_deps, ext_deps) =
            ff_sql::extractor::categorize_dependencies(deps, &known_models, &external_tables);

        // Get materialization
        let mat = config_values
            .get("materialized")
            .and_then(|v| v.as_str())
            .map(common::parse_materialization)
            .unwrap_or(project.config.materialization);

        // Get schema
        let schema = config_values
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| project.config.schema.clone());

        dependencies.insert(name.to_string(), model_deps.clone());

        model_info.push(ModelInfo {
            name: name.to_string(),
            resource_type: "model".to_string(),
            path: Some(model.path.display().to_string()),
            materialized: Some(mat),
            schema,
            model_deps,
            external_deps: ext_deps,
        });
    }

    // Add sources to the list
    for source in &project.sources {
        for table in &source.tables {
            let source_name = format!("{}.{}", source.name, table.name);
            model_info.push(ModelInfo {
                name: source_name,
                resource_type: "source".to_string(),
                path: None, // Sources don't have a file path
                materialized: None,
                schema: Some(source.schema.clone()),
                model_deps: Vec::new(),
                external_deps: Vec::new(),
            });
        }
    }

    // Build DAG for selector support
    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;

    // Filter models if selector is provided
    let filtered_names: HashSet<String> = if let Some(selector_str) = &args.select {
        let selector = Selector::parse(selector_str).context("Invalid selector")?;
        selector
            .apply(&project.models, &dag)
            .context("Failed to apply selector")?
            .into_iter()
            .collect()
    } else {
        model_info.iter().map(|m| m.name.clone()).collect()
    };

    // Apply exclusion filter if provided
    let filtered_names: HashSet<String> = if let Some(exclude) = &args.exclude {
        let excluded: HashSet<String> = exclude
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        filtered_names
            .into_iter()
            .filter(|m| !excluded.contains(m))
            .collect()
    } else {
        filtered_names
    };

    // Filter model_info by name
    let filtered_info: Vec<&ModelInfo> = model_info
        .iter()
        .filter(|m| filtered_names.contains(&m.name))
        .collect();

    // Apply resource type filter if provided
    let filtered_info: Vec<&ModelInfo> = if let Some(resource_type) = &args.resource_type {
        let type_str = match resource_type {
            ResourceType::Model => "model",
            ResourceType::Source => "source",
            ResourceType::Seed => "seed",
            ResourceType::Test => "test",
        };
        filtered_info
            .into_iter()
            .filter(|m| m.resource_type == type_str)
            .collect()
    } else {
        filtered_info
    };

    // Apply owner filter if provided
    let filtered_info: Vec<&ModelInfo> = if let Some(owner_filter) = &args.owner {
        let owner_lower = owner_filter.to_lowercase();
        filtered_info
            .into_iter()
            .filter(|m| {
                // Only filter models, not sources
                if m.resource_type != "model" {
                    return true;
                }
                // Get the model to check its owner
                if let Some(model) = project.get_model(&m.name) {
                    if let Some(model_owner) = model.get_owner() {
                        return model_owner.to_lowercase().contains(&owner_lower);
                    }
                }
                false
            })
            .collect()
    } else {
        filtered_info
    };

    // Find affected exposures if requested
    let affected_exposures: Vec<&Exposure> = if args.downstream_exposures {
        // Get the set of model names that are being listed
        let model_names: HashSet<&str> = filtered_info
            .iter()
            .filter(|m| m.resource_type == "model")
            .map(|m| m.name.as_str())
            .collect();

        // Find exposures that depend on any of these models
        find_affected_exposures(&project.exposures, &model_names)
    } else {
        Vec::new()
    };

    // Output based on format
    match args.output {
        LsOutput::Table => print_table(
            &filtered_info,
            &affected_exposures,
            args.downstream_exposures,
        ),
        LsOutput::Json => print_json(
            &filtered_info,
            &affected_exposures,
            args.downstream_exposures,
        )?,
        LsOutput::Tree => print_tree(&filtered_info, &dag)?,
        LsOutput::Path => print_paths(&filtered_info),
    }

    Ok(())
}

/// Find exposures that depend on any of the given models
fn find_affected_exposures<'a>(
    exposures: &'a [Exposure],
    model_names: &HashSet<&str>,
) -> Vec<&'a Exposure> {
    exposures
        .iter()
        .filter(|exposure| {
            exposure
                .depends_on
                .iter()
                .any(|dep| model_names.contains(dep.as_str()))
        })
        .collect()
}

/// Model information for display
#[derive(Debug, serde::Serialize)]
struct ModelInfo {
    name: String,
    #[serde(rename = "type")]
    resource_type: String, // "model" or "source"
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    materialized: Option<Materialization>,
    schema: Option<String>,
    model_deps: Vec<String>,
    external_deps: Vec<String>,
}

/// Print models in table format
fn print_table(models: &[&ModelInfo], exposures: &[&Exposure], show_exposures: bool) {
    // Calculate column widths
    let name_width = models
        .iter()
        .map(|m| m.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let type_width = 7;
    let mat_width = 12;
    let schema_width = models
        .iter()
        .map(|m| m.schema.as_ref().map(|s| s.len()).unwrap_or(1))
        .max()
        .unwrap_or(6)
        .max(6);

    // Print header
    println!(
        "{:<name_width$}  {:<type_width$}  {:<mat_width$}  {:<schema_width$}  DEPENDS_ON",
        "NAME",
        "TYPE",
        "MATERIALIZED",
        "SCHEMA",
        name_width = name_width,
        type_width = type_width,
        mat_width = mat_width,
        schema_width = schema_width
    );

    // Print separator
    println!(
        "{:-<name_width$}  {:-<type_width$}  {:-<mat_width$}  {:-<schema_width$}  {}",
        "",
        "",
        "",
        "",
        "-".repeat(40),
        name_width = name_width,
        type_width = type_width,
        mat_width = mat_width,
        schema_width = schema_width
    );

    // Count models and sources
    let model_count = models.iter().filter(|m| m.resource_type == "model").count();
    let source_count = models
        .iter()
        .filter(|m| m.resource_type == "source")
        .count();

    // Print each model/source
    for model in models {
        let mut deps: Vec<String> = model.model_deps.clone();
        deps.extend(
            model
                .external_deps
                .iter()
                .map(|d| format!("{} (external)", d)),
        );

        let deps_str = if deps.is_empty() {
            "-".to_string()
        } else {
            deps.join(", ")
        };

        let mat_str = model
            .materialized
            .map(|m| m.to_string())
            .unwrap_or_else(|| "-".to_string());

        let schema_str = model.schema.as_deref().unwrap_or("-");

        println!(
            "{:<name_width$}  {:<type_width$}  {:<mat_width$}  {:<schema_width$}  {}",
            model.name,
            model.resource_type,
            mat_str,
            schema_str,
            deps_str,
            name_width = name_width,
            type_width = type_width,
            mat_width = mat_width,
            schema_width = schema_width
        );
    }

    println!();
    if source_count > 0 {
        println!("{} models, {} sources", model_count, source_count);
    } else {
        println!("{} models found", model_count);
    }

    // Print affected exposures section if requested
    if show_exposures && !exposures.is_empty() {
        println!();
        println!("Downstream Exposures ({} affected):", exposures.len());
        println!("{:-<60}", "");

        for exposure in exposures {
            let type_str = format!("{}", exposure.exposure_type);
            let models_affected: Vec<&str> = exposure
                .depends_on
                .iter()
                .filter(|d| models.iter().any(|m| m.name == *d.as_str()))
                .map(|s| s.as_str())
                .collect();

            println!(
                "  {} ({}) - depends on: {}",
                exposure.name,
                type_str,
                models_affected.join(", ")
            );

            if let Some(url) = &exposure.url {
                println!("    URL: {}", url);
            }
        }
    } else if show_exposures && exposures.is_empty() {
        println!();
        println!("No downstream exposures affected.");
    }
}

/// Print models in JSON format
fn print_json(models: &[&ModelInfo], exposures: &[&Exposure], show_exposures: bool) -> Result<()> {
    if show_exposures {
        // Create a combined output with models and affected exposures
        let exposure_info: Vec<ExposureInfo> = exposures
            .iter()
            .map(|e| ExposureInfo {
                name: e.name.clone(),
                exposure_type: format!("{}", e.exposure_type),
                owner: e.owner.name.clone(),
                depends_on: e.depends_on.clone(),
                url: e.url.clone(),
            })
            .collect();

        let output = serde_json::json!({
            "models": models,
            "affected_exposures": exposure_info,
        });
        let json = serde_json::to_string_pretty(&output).context("Failed to serialize to JSON")?;
        println!("{}", json);
    } else {
        let json = serde_json::to_string_pretty(models).context("Failed to serialize to JSON")?;
        println!("{}", json);
    }
    Ok(())
}

/// Exposure info for JSON output
#[derive(Debug, serde::Serialize)]
struct ExposureInfo {
    name: String,
    exposure_type: String,
    owner: String,
    depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

/// Print models in tree format
fn print_tree(models: &[&ModelInfo], _dag: &ModelDag) -> Result<()> {
    // Find root nodes (no dependencies)
    let model_names: HashSet<_> = models.iter().map(|m| &m.name).collect();
    let roots: Vec<_> = models
        .iter()
        .filter(|m| {
            m.model_deps.is_empty() || m.model_deps.iter().all(|d| !model_names.contains(d))
        })
        .collect();

    println!("Dependency Tree:");
    println!();

    for root in roots {
        print_tree_node(root.name.as_str(), models, &model_names, "", true);
    }

    Ok(())
}

/// Recursively print a tree node
fn print_tree_node(
    name: &str,
    models: &[&ModelInfo],
    _all_names: &HashSet<&String>, // reserved for future use (filtering visible nodes)
    prefix: &str,
    is_last: bool,
) {
    let connector = if is_last { "└── " } else { "├── " };
    println!("{}{}{}", prefix, connector, name);

    // Find dependents (models that depend on this one)
    let dependents: Vec<_> = models
        .iter()
        .filter(|m| m.model_deps.contains(&name.to_string()))
        .collect();

    let new_prefix = format!("{}{}   ", prefix, if is_last { " " } else { "│" });

    for (i, dependent) in dependents.iter().enumerate() {
        let is_last_child = i == dependents.len() - 1;
        print_tree_node(
            &dependent.name,
            models,
            _all_names,
            &new_prefix,
            is_last_child,
        );
    }
}

/// Print model file paths only (one per line)
fn print_paths(models: &[&ModelInfo]) {
    for model in models {
        if let Some(path) = &model.path {
            println!("{}", path);
        }
    }
}
