//! Format command implementation — format SQL source files with sqlfmt.

use anyhow::{bail, Result};
use std::path::PathBuf;

use crate::cli::{FmtArgs, GlobalArgs};
use crate::commands::common::load_project;
use crate::commands::format_helpers;

/// Collect SQL files from explicit paths (files or directories).
fn collect_from_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for p in paths {
        if !p.exists() {
            bail!("Path does not exist: {}", p.display());
        }
        if p.is_file() {
            files.push(p.clone());
        } else if p.is_dir() {
            collect_sql_in_dir(p, &mut files);
        }
    }
    Ok(files)
}

/// Recursively find .sql files under a directory.
fn collect_sql_in_dir(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_sql_in_dir(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sql") {
            out.push(path);
        }
    }
}

/// Collect SQL files matching a glob pattern.
fn collect_from_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let entries = glob::glob(pattern)
        .map_err(|e| anyhow::anyhow!("Invalid glob pattern '{}': {}", pattern, e))?;
    let mut files = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| anyhow::anyhow!("Glob error: {}", e))?;
        if path.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

/// Execute the fmt command.
pub(crate) async fn execute(args: &FmtArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let mut sql_files: Vec<PathBuf> = if !args.paths.is_empty() {
        collect_from_paths(&args.paths)?
    } else if let Some(ref pattern) = args.glob {
        collect_from_glob(pattern)?
    } else {
        let mut files: Vec<PathBuf> = project
            .models
            .values()
            .filter(|model| model.path.exists())
            .map(|model| model.path.clone())
            .chain(
                project
                    .functions
                    .iter()
                    .filter(|func| func.sql_path.exists())
                    .map(|func| func.sql_path.clone()),
            )
            .collect();

        if let Some(ref nodes_arg) = args.nodes {
            let (_, dag) = crate::commands::common::build_project_dag(&project)?;
            let selected =
                crate::commands::common::resolve_nodes(&project, &dag, &Some(nodes_arg.clone()))?;
            let selected_set: std::collections::HashSet<String> = selected.into_iter().collect();

            files.retain(|path| {
                for (name, model) in &project.models {
                    if model.path == *path && selected_set.contains(name.as_str()) {
                        return true;
                    }
                }
                for func in &project.functions {
                    if func.sql_path == *path && selected_set.contains(func.name.as_str()) {
                        return true;
                    }
                }
                false
            });
        }
        files
    };

    sql_files.sort();
    sql_files.dedup();

    if sql_files.is_empty() {
        println!("No SQL files to format.");
        return Ok(());
    }

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
