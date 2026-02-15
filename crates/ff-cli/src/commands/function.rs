//! Function command implementation — manage user-defined functions (DuckDB macros)

use anyhow::{Context, Result};
use ff_core::function::{FunctionArg, FunctionDef};
use ff_core::Project;
use ff_jinja::JinjaEnvironment;

use crate::cli::{
    FunctionArgs, FunctionCommands, FunctionDeployArgs, FunctionDropArgs, FunctionListArgs,
    FunctionShowArgs, FunctionValidateArgs, GlobalArgs, OutputFormat,
};
use crate::commands::common::{self, create_database_connection, load_project};

/// Filter functions by comma-separated name list, or return all if no filter
fn filter_functions<'a>(filter: &Option<String>, project: &'a Project) -> Vec<&'a FunctionDef> {
    if let Some(names_csv) = filter {
        let names: Vec<&str> = names_csv.split(',').map(|s| s.trim()).collect();
        project
            .functions
            .iter()
            .filter(|f| names.contains(&f.name.as_str()))
            .collect()
    } else {
        project.functions.iter().collect()
    }
}

/// Format function arguments as a human-readable string
fn format_args(args: &[FunctionArg]) -> String {
    args.iter()
        .map(|a| {
            if let Some(d) = &a.default {
                format!("{} {} := {}", a.name, a.data_type, d)
            } else {
                format!("{} {}", a.name, a.data_type)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Execute the function command
pub async fn execute(args: &FunctionArgs, global: &GlobalArgs) -> Result<()> {
    match &args.command {
        FunctionCommands::List(sub) => list(sub, global).await,
        FunctionCommands::Deploy(sub) => deploy(sub, global).await,
        FunctionCommands::Show(sub) => show(sub, global).await,
        FunctionCommands::Validate(sub) => validate(sub, global).await,
        FunctionCommands::Drop(sub) => drop(sub, global).await,
    }
}

/// List all user-defined functions in the project
async fn list(args: &FunctionListArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    if project.functions.is_empty() {
        match args.output {
            OutputFormat::Json => println!("[]"),
            OutputFormat::Text => println!("No functions found."),
        }
        return Ok(());
    }

    match args.output {
        OutputFormat::Json => {
            let items: Vec<serde_json::Value> = project
                .functions
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name.as_str(),
                        "type": format!("{}", f.function_type),
                        "args": f.args.iter().map(|a| {
                            let mut m = serde_json::Map::new();
                            m.insert("name".into(), serde_json::Value::String(a.name.clone()));
                            m.insert("data_type".into(), serde_json::Value::String(a.data_type.clone()));
                            if let Some(d) = &a.default {
                                m.insert("default".into(), serde_json::Value::String(d.clone()));
                            }
                            serde_json::Value::Object(m)
                        }).collect::<Vec<_>>(),
                        "description": f.description.as_deref().unwrap_or(""),
                    })
                })
                .collect();
            let json =
                serde_json::to_string_pretty(&items).context("Failed to serialize functions")?;
            println!("{}", json);
        }
        OutputFormat::Text => {
            let rows: Vec<Vec<String>> = project
                .functions
                .iter()
                .map(|f| {
                    vec![
                        f.name.to_string(),
                        format!("{}", f.function_type),
                        format_args(&f.args),
                        f.description.clone().unwrap_or_default(),
                    ]
                })
                .collect();

            common::print_table(&["NAME", "TYPE", "ARGS", "DESCRIPTION"], &rows);
            println!("\n{} functions found", project.functions.len());
        }
    }

    Ok(())
}

/// Deploy functions to the database as DuckDB macros
async fn deploy(args: &FunctionDeployArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let db = create_database_connection(&project.config, global.target.as_deref())?;
    let jinja = JinjaEnvironment::new(&project.config.vars);

    common::set_project_search_path(&db, &project).await?;

    let functions = filter_functions(&args.functions, &project);

    if functions.is_empty() {
        println!("No functions to deploy.");
        return Ok(());
    }

    let mut success_count = 0u32;
    let mut failure_count = 0u32;

    for func in &functions {
        let rendered_body = jinja
            .render(&func.sql_body)
            .with_context(|| format!("Failed to render SQL for function '{}'", func.name))?;

        let create_sql = func.to_create_sql(&rendered_body);

        match db.deploy_function(&create_sql).await {
            Ok(()) => {
                println!("  Deployed function: {}", func.name);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  Failed to deploy function '{}': {}", func.name, e);
                failure_count += 1;
            }
        }
    }

    println!("\n{success_count} deployed, {failure_count} failed");

    if failure_count > 0 {
        return Err(common::ExitCode(1).into());
    }

    Ok(())
}

/// Show details about a specific function
async fn show(args: &FunctionShowArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let func = project
        .get_function(&args.name)
        .ok_or_else(|| anyhow::anyhow!("Function '{}' not found", args.name))?;

    if args.sql {
        // Show generated CREATE MACRO SQL
        let jinja = JinjaEnvironment::new(&project.config.vars);
        let rendered_body = jinja
            .render(&func.sql_body)
            .with_context(|| format!("Failed to render SQL for function '{}'", func.name))?;
        println!("{}", func.to_create_sql(&rendered_body));
    } else {
        // Show YAML definition details
        println!("Name:        {}", func.name);
        println!("Type:        {}", func.function_type);
        if let Some(desc) = &func.description {
            println!("Description: {}", desc);
        }
        println!("SQL file:    {}", func.sql_path.display());
        println!("YAML file:   {}", func.yaml_path.display());

        if !func.args.is_empty() {
            println!("\nArguments:");
            for arg in &func.args {
                let default_str = arg
                    .default
                    .as_ref()
                    .map(|d| format!(" (default: {})", d))
                    .unwrap_or_default();
                let desc_str = arg
                    .description
                    .as_ref()
                    .map(|d| format!(" -- {}", d))
                    .unwrap_or_default();
                println!(
                    "  {} {}{}{}",
                    arg.name, arg.data_type, default_str, desc_str
                );
            }
        }

        match &func.returns {
            ff_core::function::FunctionReturn::Scalar { data_type } => {
                println!("\nReturns: {}", data_type);
            }
            ff_core::function::FunctionReturn::Table { columns } => {
                println!("\nReturns table:");
                for col in columns {
                    println!("  {} {}", col.name, col.data_type);
                }
            }
        }

        println!("\nSQL body:");
        println!("{}", func.sql_body);
    }

    Ok(())
}

/// Validate all function definitions
async fn validate(_args: &FunctionValidateArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    if project.functions.is_empty() {
        println!("No functions to validate.");
        return Ok(());
    }

    let mut issue_count = 0;

    for func in &project.functions {
        // Check that SQL body is non-empty
        if func.sql_body.trim().is_empty() {
            eprintln!(
                "  [FN001] Function '{}': SQL body is empty ({})",
                func.name,
                func.sql_path.display()
            );
            issue_count += 1;
        }

        // Check arg type strings are parseable
        for arg in &func.args {
            let parsed = ff_analysis::parse_sql_type(&arg.data_type);
            if matches!(parsed, ff_analysis::SqlType::Unknown(_)) {
                eprintln!(
                    "  [FN008] Function '{}': argument '{}' has unknown type '{}'",
                    func.name, arg.name, arg.data_type
                );
                issue_count += 1;
            }
        }

        // Check return type is parseable
        match &func.returns {
            ff_core::function::FunctionReturn::Scalar { data_type } => {
                let parsed = ff_analysis::parse_sql_type(data_type);
                if matches!(parsed, ff_analysis::SqlType::Unknown(_)) {
                    eprintln!(
                        "  [FN008] Function '{}': return type '{}' is unknown",
                        func.name, data_type
                    );
                    issue_count += 1;
                }
            }
            ff_core::function::FunctionReturn::Table { columns } => {
                for col in columns {
                    let parsed = ff_analysis::parse_sql_type(&col.data_type);
                    if matches!(parsed, ff_analysis::SqlType::Unknown(_)) {
                        eprintln!(
                            "  [FN008] Function '{}': return column '{}' has unknown type '{}'",
                            func.name, col.name, col.data_type
                        );
                        issue_count += 1;
                    }
                }
            }
        }
    }

    if issue_count == 0 {
        println!(
            "All {} functions validated successfully.",
            project.functions.len()
        );
    } else {
        eprintln!(
            "\n{} issues found across {} functions.",
            issue_count,
            project.functions.len()
        );
        return Err(common::ExitCode(1).into());
    }

    Ok(())
}

/// Drop deployed functions from the database
async fn drop(args: &FunctionDropArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let db = create_database_connection(&project.config, global.target.as_deref())?;

    let functions = filter_functions(&args.functions, &project);

    if functions.is_empty() {
        println!("No functions to drop.");
        return Ok(());
    }

    let mut success_count = 0u32;
    let mut failure_count = 0u32;

    for func in &functions {
        let drop_sql = func.to_drop_sql();
        match db.drop_function(&drop_sql).await {
            Ok(()) => {
                println!("  Dropped function: {}", func.name);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  Failed to drop function '{}': {}", func.name, e);
                failure_count += 1;
            }
        }
    }

    println!("\n{success_count} dropped, {failure_count} failed");

    if failure_count > 0 {
        return Err(common::ExitCode(1).into());
    }

    Ok(())
}
