//! Seed command implementation

use anyhow::{Context, Result};
use ff_core::seed::{discover_seeds, Seed};
use ff_core::Project;
use ff_db::{CsvLoadOptions, Database, DuckDbBackend};
use std::path::Path;
use std::sync::Arc;

use crate::cli::{GlobalArgs, SeedArgs};

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

    // Discover seeds with their configuration files
    let seed_paths = project.config.seed_paths_absolute(&project.root);
    let all_seeds = discover_seeds(&project.root, &seed_paths);

    if all_seeds.is_empty() {
        println!("No seed files found in seed_paths.");
        return Ok(());
    }

    // Filter seeds if --seeds was specified
    let seeds_to_process: Vec<&Seed> = if let Some(filter) = &args.seeds {
        let filter_names: std::collections::HashSet<&str> =
            filter.split(',').map(|s| s.trim()).collect();
        all_seeds
            .iter()
            .filter(|s| filter_names.contains(s.name.as_str()))
            .collect()
    } else {
        all_seeds.iter().collect()
    };

    // Filter out disabled seeds
    let enabled_seeds: Vec<&Seed> = seeds_to_process
        .into_iter()
        .filter(|s| s.is_enabled())
        .collect();

    if enabled_seeds.is_empty() {
        println!("No matching enabled seed files found.");
        return Ok(());
    }

    // Handle --show-columns mode
    if args.show_columns {
        return show_columns(db.as_ref(), &enabled_seeds, global.verbose).await;
    }

    if global.verbose {
        eprintln!(
            "[verbose] Loading {} seeds from paths: {:?}",
            enabled_seeds.len(),
            project.config.seed_paths
        );
    }

    println!("Loading {} seeds...\n", enabled_seeds.len());

    let mut success_count = 0;
    let mut failure_count = 0;
    let mut total_rows: usize = 0;

    for seed in &enabled_seeds {
        // Drop existing table if full_refresh
        if args.full_refresh {
            let table_name = seed.qualified_name(project.config.schema.as_deref());
            if global.verbose {
                eprintln!("[verbose] Dropping existing table: {}", table_name);
            }
            db.drop_if_exists(&table_name)
                .await
                .context(format!("Failed to drop {}", table_name))?;
        }

        // Load the CSV file with options from configuration
        let path_str = seed.path.display().to_string();
        let options = build_csv_options(seed, project.config.schema.as_deref());
        let table_name = seed.qualified_name(project.config.schema.as_deref());

        if global.verbose {
            eprintln!("[verbose] Loading {} from {}", table_name, path_str);
            if let Some(config) = &seed.config {
                if !config.column_types.is_empty() {
                    eprintln!(
                        "[verbose]   Column type overrides: {:?}",
                        config.column_types
                    );
                }
                if config.delimiter != ',' {
                    eprintln!("[verbose]   Delimiter: {:?}", config.delimiter);
                }
            }
        }

        // Use the base table name for loading (schema is in options)
        let result = db
            .load_csv_with_options(&seed.name, &path_str, options)
            .await;

        match result {
            Ok(_) => {
                // Get row count
                let row_count = db
                    .query_count(&format!("SELECT * FROM {}", table_name))
                    .await
                    .unwrap_or(0);

                success_count += 1;
                total_rows += row_count;

                // Show config info if applicable
                let config_info = if seed.config.is_some() {
                    " (configured)"
                } else {
                    ""
                };
                println!("  ✓ {} ({} rows){}", table_name, row_count, config_info);
            }
            Err(e) => {
                failure_count += 1;
                println!("  ✗ {} - {}", table_name, e);
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

/// Build CSV load options from seed configuration
fn build_csv_options(seed: &Seed, default_schema: Option<&str>) -> CsvLoadOptions {
    CsvLoadOptions {
        delimiter: Some(seed.delimiter()),
        column_types: seed.column_types(),
        quote_columns: seed.quote_columns(),
        schema: seed
            .target_schema()
            .map(String::from)
            .or_else(|| default_schema.map(String::from)),
    }
}

/// Show inferred columns for seeds without loading
async fn show_columns(db: &dyn Database, seeds: &[&Seed], verbose: bool) -> Result<()> {
    println!("Inferring schema for {} seeds...\n", seeds.len());

    for seed in seeds {
        let path_str = seed.path.display().to_string();

        if verbose {
            eprintln!("[verbose] Inferring schema for: {}", path_str);
        }

        match db.infer_csv_schema(&path_str).await {
            Ok(schema) => {
                println!("{}:", seed.name);

                // Check for column type overrides
                let type_overrides = seed.column_types();

                for (col_name, inferred_type) in &schema {
                    if let Some(override_type) = type_overrides.get(col_name) {
                        println!(
                            "  {} {} (overridden from {})",
                            col_name, override_type, inferred_type
                        );
                    } else {
                        println!("  {} {}", col_name, inferred_type);
                    }
                }

                // Show config info if present
                if let Some(config) = &seed.config {
                    let mut config_notes = Vec::new();
                    if let Some(schema) = &config.schema {
                        config_notes.push(format!("schema: {}", schema));
                    }
                    if config.delimiter != ',' {
                        config_notes.push(format!("delimiter: {:?}", config.delimiter));
                    }
                    if !config_notes.is_empty() {
                        println!("  [{}]", config_notes.join(", "));
                    }
                }

                println!();
            }
            Err(e) => {
                println!("{}: Error - {}\n", seed.name, e);
            }
        }
    }

    Ok(())
}
