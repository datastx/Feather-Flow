//! Clean command implementation

use anyhow::Result;
use std::fs;

use crate::cli::{CleanArgs, GlobalArgs};
use crate::commands::common::load_project;

/// Execute the clean command
pub async fn execute(args: &CleanArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    // Default clean targets if not specified in config
    let clean_targets = if project.config.clean_targets.is_empty() {
        vec!["target".to_string()]
    } else {
        project.config.clean_targets.clone()
    };

    if args.dry_run {
        println!("Dry run - would clean the following directories:");
    } else {
        println!("Cleaning project: {}", project.config.name);
    }

    let mut cleaned_count = 0;
    let mut skipped_count = 0;

    for target in &clean_targets {
        let target_path = project.root.join(target);

        if !target_path.exists() {
            if global.verbose {
                println!("  Skipping (not found): {}", target_path.display());
            }
            skipped_count += 1;
            continue;
        }

        if args.dry_run {
            println!("  Would remove: {}", target_path.display());
            cleaned_count += 1;
            continue;
        }

        match fs::remove_dir_all(&target_path) {
            Ok(_) => {
                println!("  Removed: {}", target_path.display());
                cleaned_count += 1;
            }
            Err(e) => {
                eprintln!("  Failed to remove {}: {}", target_path.display(), e);
            }
        }
    }

    // Clean meta database file
    let meta_path = project.target_dir().join("meta.duckdb");
    if meta_path.exists() {
        if args.dry_run {
            println!("  Would remove: {}", meta_path.display());
        } else {
            match fs::remove_file(&meta_path) {
                Ok(_) => println!("  Removed: {}", meta_path.display()),
                Err(e) => eprintln!("  Failed to remove {}: {}", meta_path.display(), e),
            }
            // Also clean WAL file if it exists
            let wal_path = meta_path.with_extension("duckdb.wal");
            if wal_path.exists() {
                let _ = fs::remove_file(&wal_path);
            }
        }
    }

    println!();
    if args.dry_run {
        println!(
            "Would clean {} director{}, {} not found",
            cleaned_count,
            if cleaned_count == 1 { "y" } else { "ies" },
            skipped_count
        );
    } else {
        println!(
            "Cleaned {} director{}, {} skipped",
            cleaned_count,
            if cleaned_count == 1 { "y" } else { "ies" },
            skipped_count
        );
    }

    Ok(())
}

#[cfg(test)]
#[path = "clean_test.rs"]
mod tests;
