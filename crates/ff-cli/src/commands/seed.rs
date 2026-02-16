//! Seed command implementation

use anyhow::{Context, Result};
use ff_core::seed::{discover_seeds, Seed};
use ff_core::sql_utils::quote_qualified;
use ff_db::{CsvLoadOptions, Database};

use crate::cli::{GlobalArgs, SeedArgs};
use crate::commands::common::{self, load_project};

/// Execute the seed command
pub async fn execute(args: &SeedArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    let seed_paths = project.config.seed_paths_absolute(&project.root);
    let all_seeds = discover_seeds(&seed_paths)?;

    if all_seeds.is_empty() {
        println!("No seed files found in seed_paths.");
        return Ok(());
    }

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

    let enabled_seeds: Vec<&Seed> = seeds_to_process
        .into_iter()
        .filter(|s| s.is_enabled())
        .collect();

    if enabled_seeds.is_empty() {
        println!("No matching enabled seed files found.");
        return Ok(());
    }

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
        if args.full_refresh {
            let table_name = seed.qualified_name(project.config.schema.as_deref());
            if global.verbose {
                eprintln!("[verbose] Dropping existing table: {}", table_name);
            }
            db.drop_if_exists(&table_name)
                .await
                .with_context(|| format!("Failed to drop {}", table_name))?;
        }

        let path_str = seed.path.display().to_string();
        let options = build_csv_options(seed, project.config.schema.as_deref());
        let table_name = seed.qualified_name(project.config.schema.as_deref());

        if global.verbose {
            eprintln!("[verbose] Loading {} from {}", table_name, path_str);
            log_seed_config(seed);
        }

        let result = db
            .load_csv_with_options(&seed.name, &path_str, options)
            .await;

        match result {
            Ok(_) => {
                let row_count = match db
                    .query_count(&format!("SELECT * FROM {}", quote_qualified(&table_name)))
                    .await
                {
                    Ok(count) => count,
                    Err(e) => {
                        eprintln!("[warn] Failed to count rows for {}: {}", table_name, e);
                        0
                    }
                };

                success_count += 1;
                total_rows += row_count;

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
        return Err(crate::commands::common::ExitCode(4).into());
    }

    Ok(())
}

fn log_seed_config(seed: &Seed) {
    let Some(config) = &seed.config else {
        return;
    };
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

fn build_csv_options(seed: &Seed, default_schema: Option<&str>) -> CsvLoadOptions {
    let mut opts = CsvLoadOptions::new()
        .with_delimiter(seed.delimiter())
        .with_column_types(seed.column_types().clone())
        .with_quote_columns(seed.quote_columns());

    if let Some(schema) = seed.target_schema().or(default_schema) {
        opts = opts.with_schema(schema);
    }

    opts
}

async fn show_columns(db: &dyn Database, seeds: &[&Seed], verbose: bool) -> Result<()> {
    println!("Inferring schema for {} seeds...\n", seeds.len());

    for seed in seeds {
        let path_str = seed.path.display().to_string();

        if verbose {
            eprintln!("[verbose] Inferring schema for: {}", path_str);
        }

        let schema = match db.infer_csv_schema(&path_str).await {
            Ok(s) => s,
            Err(e) => {
                println!("{}: Error - {}\n", seed.name, e);
                continue;
            }
        };

        display_seed_schema(&seed.name, &schema, seed);
    }

    Ok(())
}

/// Display inferred schema for a single seed, noting any type overrides.
fn display_seed_schema(name: &str, schema: &[(String, String)], seed: &Seed) {
    println!("{}:", name);

    let type_overrides = seed.column_types();
    for (col_name, inferred_type) in schema {
        if let Some(override_type) = type_overrides.get(col_name) {
            println!(
                "  {} {} (overridden from {})",
                col_name, override_type, inferred_type
            );
        } else {
            println!("  {} {}", col_name, inferred_type);
        }
    }

    if let Some(config) = &seed.config {
        let mut config_notes = Vec::new();
        if let Some(s) = &config.schema {
            config_notes.push(format!("schema: {}", s));
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
