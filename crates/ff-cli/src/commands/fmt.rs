//! Format command implementation — format SQL source files with sqlfmt.

use anyhow::Result;
use std::path::PathBuf;

use crate::cli::{FmtArgs, GlobalArgs};
use crate::commands::common::load_project;
use crate::commands::format_helpers;

/// Execute the fmt command.
pub(crate) async fn execute(args: &FmtArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    // Collect all SQL file paths: models + functions
    let mut sql_files: Vec<PathBuf> = Vec::new();

    // Model SQL files
    for model in project.models.values() {
        if model.path.exists() {
            sql_files.push(model.path.clone());
        }
    }

    // Function SQL files
    for func in &project.functions {
        if func.sql_path.exists() {
            sql_files.push(func.sql_path.clone());
        }
    }

    // Filter by node selector if provided
    if let Some(ref nodes_arg) = args.nodes {
        let (_, dag) = crate::commands::common::build_project_dag(&project)?;
        let selected =
            crate::commands::common::resolve_nodes(&project, &dag, &Some(nodes_arg.clone()))?;
        let selected_set: std::collections::HashSet<String> = selected.into_iter().collect();

        sql_files.retain(|path| {
            // Match model SQL files by model name
            for (name, model) in &project.models {
                if model.path == *path && selected_set.contains(name.as_str()) {
                    return true;
                }
            }
            // Match function SQL files by function name
            for func in &project.functions {
                if func.sql_path == *path && selected_set.contains(func.name.as_str()) {
                    return true;
                }
            }
            false
        });
    }

    if sql_files.is_empty() {
        println!("No SQL files to format.");
        return Ok(());
    }

    // Build mode from config + CLI overrides
    let mut format_config = project.config.format.clone();
    if let Some(ll) = args.line_length {
        format_config.line_length = ll;
    }
    if args.no_jinjafmt {
        format_config.no_jinjafmt = true;
    }

    let mut mode = format_helpers::build_sqlfmt_mode(&format_config, project.config.dialect);
    mode.check = args.check;
    mode.diff = args.diff;

    if global.verbose {
        eprintln!(
            "[verbose] Formatting {} SQL files (line_length={}, dialect={}, check={})",
            sql_files.len(),
            mode.line_length,
            mode.dialect_name,
            mode.check,
        );
    }

    let report = format_helpers::format_files(&sql_files, &mode).await;

    report.print_errors();
    println!("{}", report.summary());

    if args.check && report.has_changes() {
        return Err(crate::commands::common::ExitCode(1).into());
    }

    if report.has_errors() {
        return Err(crate::commands::common::ExitCode(1).into());
    }

    Ok(())
}
