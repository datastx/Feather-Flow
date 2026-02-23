//! Clean command implementation

use anyhow::Result;
use std::fs;

use crate::cli::{CleanArgs, GlobalArgs};
use crate::commands::common::load_project;

/// Execute the clean command
pub(crate) async fn execute(args: &CleanArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    // Default clean targets if not specified in config
    let default_targets = vec!["target".to_string()];
    let clean_targets = if project.config.clean_targets.is_empty() {
        &default_targets
    } else {
        &project.config.clean_targets
    };

    if args.dry_run {
        println!("Dry run - would clean the following directories:");
    } else {
        println!("Cleaning project: {}", project.config.name);
    }

    enum CleanOutcome {
        Cleaned,
        Skipped,
        Failed,
    }

    let mut outcomes: Vec<CleanOutcome> = Vec::with_capacity(clean_targets.len());

    for target in clean_targets {
        let target_path = project.root.join(target);

        if !target_path.exists() {
            if global.verbose {
                println!("  Skipping (not found): {}", target_path.display());
            }
            outcomes.push(CleanOutcome::Skipped);
            continue;
        }

        if args.dry_run {
            println!("  Would remove: {}", target_path.display());
            outcomes.push(CleanOutcome::Cleaned);
            continue;
        }

        match fs::remove_dir_all(&target_path) {
            Ok(_) => {
                println!("  Removed: {}", target_path.display());
                outcomes.push(CleanOutcome::Cleaned);
            }
            Err(e) => {
                eprintln!("  Failed to remove {}: {}", target_path.display(), e);
                outcomes.push(CleanOutcome::Failed);
            }
        }
    }

    let cleaned_count = outcomes
        .iter()
        .filter(|o| matches!(o, CleanOutcome::Cleaned))
        .count();
    let skipped_count = outcomes
        .iter()
        .filter(|o| matches!(o, CleanOutcome::Skipped))
        .count();

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
