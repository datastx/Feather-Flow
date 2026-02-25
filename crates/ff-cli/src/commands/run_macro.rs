//! Run-macro command implementation - execute standalone SQL macros

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::time::Instant;

use crate::cli::{GlobalArgs, RunMacroArgs};
use crate::commands::common::{self, load_project};

/// Execute the run-macro command
pub(crate) async fn execute(args: &RunMacroArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project = load_project(global)?;

    let jinja = common::build_jinja_env(&project);

    let macro_args: HashMap<String, serde_json::Value> = if let Some(args_json) = &args.args {
        serde_json::from_str(args_json).context("Invalid JSON in --args")?
    } else {
        HashMap::new()
    };

    let template = if macro_args.is_empty() {
        format!("{{{{ {}() }}}}", args.macro_name)
    } else {
        let kwargs: Vec<String> = macro_args
            .iter()
            .map(|(k, v)| {
                let value_str = match v {
                    serde_json::Value::String(s) => {
                        // Escape backslashes and single quotes to prevent Jinja injection
                        let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
                        format!("'{}'", escaped)
                    }
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

    let (sql, _) = jinja
        .render_with_config(&template)
        .with_context(|| format!("Failed to render macro: {}", args.macro_name))?;

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

    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    println!("Running operation: {}\n", args.macro_name);

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
