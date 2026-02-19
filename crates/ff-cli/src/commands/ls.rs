//! List command implementation

use anyhow::{Context, Result};
use ff_core::config::Materialization;
use ff_core::dag::ModelDag;
use ff_sql::{extract_dependencies, SqlParser};
use std::collections::{HashMap, HashSet};

use crate::cli::{GlobalArgs, LsArgs, LsOutput, ResourceType};
use crate::commands::common::{self, load_project};

/// Execute the ls command
pub(crate) async fn execute(args: &LsArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = common::build_jinja_env(&project);

    let external_tables = common::build_external_tables_lookup(&project);
    let known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();

    let mut model_info: Vec<ModelInfo> = Vec::new();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    for name in project.model_names() {
        let model = project
            .get_model(name)
            .ok_or_else(|| anyhow::anyhow!("Model '{}' not found in project", name))?;

        let (rendered, config_values) = jinja
            .render_with_config(&model.raw_sql)
            .with_context(|| format!("Failed to render template for model: {}", name))?;

        let statements = parser
            .parse(&rendered)
            .with_context(|| format!("Failed to parse SQL for model: {}", name))?;

        let deps = extract_dependencies(&statements);
        let (model_deps, ext_deps) =
            ff_sql::extractor::categorize_dependencies(deps, &known_models, &external_tables);

        let mat = config_values
            .get("materialized")
            .and_then(|v| v.as_str())
            .map(common::parse_materialization)
            .unwrap_or(project.config.materialization);

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

    for seed in &project.seeds {
        model_info.push(ModelInfo {
            name: seed.name.clone(),
            resource_type: "seed".to_string(),
            path: Some(seed.path.display().to_string()),
            materialized: None,
            schema: seed
                .schema
                .clone()
                .or_else(|| project.config.schema.clone()),
            model_deps: Vec::new(),
            external_deps: Vec::new(),
        });
    }

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

    for func in &project.functions {
        model_info.push(ModelInfo {
            name: func.name.to_string(),
            resource_type: format!("function ({})", func.function_type),
            path: Some(func.sql_path.display().to_string()),
            materialized: None,
            schema: None,
            model_deps: Vec::new(),
            external_deps: Vec::new(),
        });
    }

    let dag = ModelDag::build(&dependencies).context("Failed to build dependency graph")?;

    let filtered_names: HashSet<String> = if args.nodes.is_some() {
        common::resolve_nodes(&project, &dag, &args.nodes)?
            .into_iter()
            .collect()
    } else {
        model_info.iter().map(|m| m.name.clone()).collect()
    };

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

    let filtered_info: Vec<&ModelInfo> = model_info
        .iter()
        .filter(|m| filtered_names.contains(&m.name))
        .collect();

    let filtered_info: Vec<&ModelInfo> = if let Some(resource_type) = &args.resource_type {
        filtered_info
            .into_iter()
            .filter(|m| match resource_type {
                ResourceType::Model => m.resource_type == "model",
                ResourceType::Source => m.resource_type == "source",
                ResourceType::Seed => m.resource_type == "seed",
                ResourceType::Test => m.resource_type == "test",
                ResourceType::Function => m.resource_type.starts_with("function"),
            })
            .collect()
    } else {
        filtered_info
    };

    let filtered_info: Vec<&ModelInfo> = if let Some(owner_filter) = &args.owner {
        let owner_lower = owner_filter.to_lowercase();
        filtered_info
            .into_iter()
            .filter(|m| {
                if m.resource_type != "model" {
                    return true;
                }
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

    match args.output {
        LsOutput::Table => print_table(&filtered_info),
        LsOutput::Json => print_json(&filtered_info)?,
        LsOutput::Tree => print_tree(&filtered_info)?,
        LsOutput::Path => print_paths(&filtered_info),
    }

    Ok(())
}

/// Model information for display
#[derive(Debug, serde::Serialize)]
struct ModelInfo {
    name: String,
    #[serde(rename = "type")]
    resource_type: String, // "model", "source", or "function (scalar|table)"
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    materialized: Option<Materialization>,
    schema: Option<String>,
    model_deps: Vec<String>,
    external_deps: Vec<String>,
}

/// Print models in table format
fn print_table(models: &[&ModelInfo]) {
    let headers = ["NAME", "TYPE", "MATERIALIZED", "SCHEMA", "DEPENDS_ON"];

    let rows: Vec<Vec<String>> = models
        .iter()
        .map(|model| {
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

            let schema_str = model.schema.as_deref().unwrap_or("-").to_string();

            vec![
                model.name.clone(),
                model.resource_type.clone(),
                mat_str,
                schema_str,
                deps_str,
            ]
        })
        .collect();

    common::print_table(&headers, &rows);

    let model_count = models.iter().filter(|m| m.resource_type == "model").count();
    let seed_count = models.iter().filter(|m| m.resource_type == "seed").count();
    let source_count = models
        .iter()
        .filter(|m| m.resource_type == "source")
        .count();
    let function_count = models
        .iter()
        .filter(|m| m.resource_type.starts_with("function"))
        .count();

    println!();
    let mut parts = vec![format!("{} models", model_count)];
    if seed_count > 0 {
        parts.push(format!("{} seeds", seed_count));
    }
    if source_count > 0 {
        parts.push(format!("{} sources", source_count));
    }
    if function_count > 0 {
        parts.push(format!("{} functions", function_count));
    }
    println!("{}", parts.join(", "));
}

/// Print models in JSON format
fn print_json(models: &[&ModelInfo]) -> Result<()> {
    let json = serde_json::to_string_pretty(models).context("Failed to serialize to JSON")?;
    println!("{}", json);
    Ok(())
}

/// Print models in tree format
fn print_tree(models: &[&ModelInfo]) -> Result<()> {
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
        let mut visited = HashSet::new();
        print_tree_node(root.name.as_str(), models, "", true, &mut visited);
    }

    Ok(())
}

/// Recursively print a tree node with cycle detection
fn print_tree_node(
    name: &str,
    models: &[&ModelInfo],
    prefix: &str,
    is_last: bool,
    visited: &mut HashSet<String>,
) {
    let connector = if is_last { "└── " } else { "├── " };

    // Guard against cycles: if we've already visited this node, print it
    // with a marker and stop recursing to avoid infinite loops.
    if !visited.insert(name.to_string()) {
        println!("{}{}{} (cycle)", prefix, connector, name);
        return;
    }

    println!("{}{}{}", prefix, connector, name);

    // Find dependents (models that depend on this one)
    let dependents: Vec<_> = models
        .iter()
        .filter(|m| m.model_deps.iter().any(|d| d == name))
        .collect();

    let new_prefix = format!("{}{}   ", prefix, if is_last { " " } else { "│" });

    for (i, dependent) in dependents.iter().enumerate() {
        let is_last_child = i == dependents.len() - 1;
        print_tree_node(&dependent.name, models, &new_prefix, is_last_child, visited);
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
