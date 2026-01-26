//! Seed command implementation

use anyhow::{Context, Result};
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use std::path::Path;
use std::sync::Arc;

use crate::cli::{GlobalArgs, SeedArgs};

/// Represents a discovered seed file
struct SeedFile {
    /// Name of the seed (filename without .csv extension)
    name: String,
    /// Absolute path to the CSV file
    path: std::path::PathBuf,
}

/// Discover all CSV seed files in seed_paths
fn discover_seeds(project: &Project) -> Vec<SeedFile> {
    let mut seeds = Vec::new();

    for seed_path in project.config.seed_paths_absolute(&project.root) {
        if !seed_path.exists() {
            continue;
        }

        discover_seeds_recursive(&seed_path, &mut seeds);
    }

    // Sort seeds by name for consistent ordering
    seeds.sort_by(|a, b| a.name.cmp(&b.name));
    seeds
}

/// Recursively discover CSV files in a directory
fn discover_seeds_recursive(dir: &Path, seeds: &mut Vec<SeedFile>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            discover_seeds_recursive(&path, seeds);
        } else if path.extension().is_some_and(|e| e == "csv") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                seeds.push(SeedFile {
                    name: stem.to_string(),
                    path,
                });
            }
        }
    }
}

/// Execute the seed command
pub async fn execute(args: &SeedArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Create database connection (use --target override if provided)
    let db_path = global
        .target
        .as_ref()
        .unwrap_or(&project.config.database.path);
    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(db_path).context("Failed to connect to database")?);

    // Discover seeds
    let all_seeds = discover_seeds(&project);

    if all_seeds.is_empty() {
        println!("No seed files found in seed_paths.");
        return Ok(());
    }

    // Filter seeds if --seeds was specified
    let seeds_to_load: Vec<&SeedFile> = if let Some(filter) = &args.seeds {
        let filter_names: std::collections::HashSet<&str> =
            filter.split(',').map(|s| s.trim()).collect();
        all_seeds
            .iter()
            .filter(|s| filter_names.contains(s.name.as_str()))
            .collect()
    } else {
        all_seeds.iter().collect()
    };

    if seeds_to_load.is_empty() {
        println!("No matching seed files found.");
        return Ok(());
    }

    if global.verbose {
        eprintln!(
            "[verbose] Loading {} seeds from paths: {:?}",
            seeds_to_load.len(),
            project.config.seed_paths
        );
    }

    println!("Loading {} seeds...\n", seeds_to_load.len());

    let mut success_count = 0;
    let mut failure_count = 0;
    let mut total_rows: usize = 0;

    for seed in &seeds_to_load {
        // Drop existing table if full_refresh
        if args.full_refresh {
            if global.verbose {
                eprintln!("[verbose] Dropping existing table: {}", seed.name);
            }
            db.drop_if_exists(&seed.name)
                .await
                .context(format!("Failed to drop {}", seed.name))?;
        }

        // Load the CSV file
        let path_str = seed.path.display().to_string();
        let result = db.load_csv(&seed.name, &path_str).await;

        match result {
            Ok(_) => {
                // Get row count
                let row_count = db
                    .query_count(&format!("SELECT * FROM {}", seed.name))
                    .await
                    .unwrap_or(0);

                success_count += 1;
                total_rows += row_count;
                println!("  ✓ {} ({} rows)", seed.name, row_count);
            }
            Err(e) => {
                failure_count += 1;
                println!("  ✗ {} - {}", seed.name, e);
            }
        }
    }

    println!();
    println!("Loaded {} seeds ({} total rows)", success_count, total_rows);

    if failure_count > 0 {
        // Exit code 4 = Database error (per spec - seed loading failures)
        std::process::exit(4);
    }

    Ok(())
}
