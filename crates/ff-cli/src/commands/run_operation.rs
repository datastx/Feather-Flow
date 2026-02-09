//! Run-operation command implementation - execute standalone SQL operations

use anyhow::{Context, Result};
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use ff_jinja::JinjaEnvironment;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{GlobalArgs, RunOperationArgs};

/// Execute the run-operation command
pub async fn execute(args: &RunOperationArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Create Jinja environment with macros
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    // Parse args JSON if provided
    let macro_args: HashMap<String, serde_json::Value> = if let Some(args_json) = &args.args {
        serde_json::from_str(args_json).context("Invalid JSON in --args")?
    } else {
        HashMap::new()
    };

    // Build the macro call template
    let template = if macro_args.is_empty() {
        format!("{{{{ {}() }}}}", args.macro_name)
    } else {
        // Build keyword arguments
        let kwargs: Vec<String> = macro_args
            .iter()
            .map(|(k, v)| {
                let value_str = match v {
                    serde_json::Value::String(s) => format!("'{}'", s),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => v.to_string(),
                };
                format!("{}={}", k, value_str)
            })
            .collect();
        format!("{{{{ {}({}) }}}}", args.macro_name, kwargs.join(", "))
    };

    if global.verbose {
        eprintln!("[verbose] Rendering macro template: {}", template);
    }

    // Render the macro
    let (sql, _) = jinja
        .render_with_config(&template)
        .context(format!("Failed to render macro: {}", args.macro_name))?;

    let sql = sql.trim();
    if sql.is_empty() {
        return Err(anyhow::anyhow!(
            "Macro '{}' returned empty SQL",
            args.macro_name
        ));
    }

    if global.verbose {
        eprintln!("[verbose] Generated SQL:\n{}", sql);
    }

    // Create database connection using target resolution (matches test/seed/freshness commands)
    use ff_core::config::Config;
    let target = Config::resolve_target(global.target.as_deref());
    let db_config = project
        .config
        .get_database_config(target.as_deref())
        .context("Failed to get database configuration")?;

    if global.verbose {
        if let Some(ref target_name) = target {
            eprintln!(
                "[verbose] Using target '{}' with database: {}",
                target_name, db_config.path
            );
        }
    }

    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(&db_config.path).context("Failed to connect to database")?);

    println!("Running operation: {}\n", args.macro_name);

    // Execute the SQL
    let result = db.execute(sql).await;

    let duration = start_time.elapsed();

    match result {
        Ok(rows_affected) => {
            println!("  ✓ {} completed", args.macro_name);
            if rows_affected > 0 {
                println!("    {} rows affected", rows_affected);
            }
            println!("\nTotal time: {}ms", duration.as_millis());
            Ok(())
        }
        Err(e) => {
            println!("  ✗ {} failed: {}", args.macro_name, e);
            println!("\nTotal time: {}ms", duration.as_millis());
            Err(crate::commands::common::ExitCode(4).into())
        }
    }
}
