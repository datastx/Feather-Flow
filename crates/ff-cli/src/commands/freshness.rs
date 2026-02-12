//! Freshness command implementation
//!
//! Unified freshness checking for both models and sources.

use anyhow::{Context, Result};
use ff_core::model::{FreshnessPeriod, FreshnessThreshold};
use ff_core::source::SourceFile;
use ff_core::sql_utils::{quote_ident, quote_qualified};
use ff_core::Project;
use ff_db::Database;
use serde::Serialize;
use std::sync::Arc;

use crate::cli::{FreshnessArgs, FreshnessOutput as CliOutput, GlobalArgs};
use crate::commands::common::{self, load_project, FreshnessStatus};

/// Execute the freshness command
pub async fn execute(args: &FreshnessArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    // Determine what to check: both by default, or whichever flag is set
    let check_models = !args.sources || args.models;
    let check_sources = !args.models || args.sources;

    // Parse select filter
    let select_filter: Option<std::collections::HashSet<String>> = args
        .select
        .as_ref()
        .map(|s| s.split(',').map(|name| name.trim().to_string()).collect());

    let mut results: Vec<FreshnessResult> = Vec::new();

    // Check model freshness
    if check_models {
        let model_results =
            check_models_freshness(&db, &project, select_filter.as_ref(), global.verbose).await;
        results.extend(model_results);
    }

    // Check source freshness
    if check_sources {
        let source_results =
            check_sources_freshness(&db, &project, select_filter.as_ref(), global.verbose).await;
        results.extend(source_results);
    }

    if results.is_empty() {
        println!("No resources with freshness configuration found.");
        return Ok(());
    }

    // Output results
    match args.output {
        CliOutput::Table => print_table(&results),
        CliOutput::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&results)
                    .context("Failed to serialize freshness results")?
            );
        }
    }

    // Write JSON file if requested
    if args.write_json {
        let output_path = project.root.join("target/freshness.json");
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .context("Failed to create target directory for freshness output")?;
        }
        let json = serde_json::to_string_pretty(&FreshnessJsonOutput {
            results: results.clone(),
            checked_at: current_timestamp(),
        })?;
        std::fs::write(&output_path, &json).with_context(|| {
            format!(
                "Failed to write freshness results to '{}'",
                output_path.display()
            )
        })?;
        println!("\nResults written to: {}", output_path.display());
    }

    // Summary
    let pass_count = results
        .iter()
        .filter(|r| r.status == FreshnessStatus::Pass)
        .count();
    let warn_count = results
        .iter()
        .filter(|r| r.status == FreshnessStatus::Warn)
        .count();
    let error_count = results
        .iter()
        .filter(|r| {
            matches!(
                r.status,
                FreshnessStatus::Error | FreshnessStatus::RuntimeError
            )
        })
        .count();

    println!();
    if error_count > 0 {
        println!(
            "Freshness check: {} passed, {} warnings, {} errors",
            pass_count, warn_count, error_count
        );
        return Err(crate::commands::common::ExitCode(1).into());
    } else if warn_count > 0 {
        println!(
            "Freshness check: {} passed, {} warnings",
            pass_count, warn_count
        );
    } else {
        println!("Freshness check: {} passed", pass_count);
    }

    Ok(())
}

/// Unified freshness check result
#[derive(Debug, Clone, Serialize)]
struct FreshnessResult {
    /// Resource type: "model" or "source"
    resource_type: String,
    /// Resource name (model name or source name)
    name: String,
    /// For sources: the specific table being checked
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<String>,
    status: FreshnessStatus,
    loaded_at: Option<String>,
    age_seconds: Option<u64>,
    age_human: Option<String>,
    warn_threshold_seconds: Option<u64>,
    error_threshold_seconds: Option<u64>,
    error: Option<String>,
}

/// Output structure for JSON file
#[derive(Debug, Clone, Serialize)]
struct FreshnessJsonOutput {
    results: Vec<FreshnessResult>,
    checked_at: String,
}

// ---------------------------------------------------------------------------
// Model freshness
// ---------------------------------------------------------------------------

/// Check freshness for models with freshness config
async fn check_models_freshness(
    db: &Arc<dyn Database>,
    project: &Project,
    select: Option<&std::collections::HashSet<String>>,
    verbose: bool,
) -> Vec<FreshnessResult> {
    let models_to_check: Vec<&str> = project
        .models
        .values()
        .filter(|m| {
            m.schema
                .as_ref()
                .map(|s| s.has_freshness())
                .unwrap_or(false)
        })
        .filter(|m| select.map(|f| f.contains(m.name.as_str())).unwrap_or(true))
        .map(|m| m.name.as_str())
        .collect();

    if models_to_check.is_empty() {
        return Vec::new();
    }

    if verbose {
        eprintln!(
            "[verbose] Checking freshness for {} model(s)",
            models_to_check.len()
        );
    }

    let default_schema = project.config.schema.as_deref();
    let mut results = Vec::new();

    for model_name in &models_to_check {
        let result = check_single_model(db, project, model_name, default_schema).await;
        results.push(result);
    }

    results
}

/// Check freshness for a single model
async fn check_single_model(
    db: &Arc<dyn Database>,
    project: &Project,
    model_name: &str,
    default_schema: Option<&str>,
) -> FreshnessResult {
    let model = match project.get_model(model_name) {
        Some(m) => m,
        None => {
            return FreshnessResult {
                resource_type: "model".to_string(),
                name: model_name.to_string(),
                table_name: None,
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_seconds: None,
                age_human: None,
                warn_threshold_seconds: None,
                error_threshold_seconds: None,
                error: Some(format!("Model '{}' not found", model_name)),
            };
        }
    };

    let freshness_config = match model
        .schema
        .as_ref()
        .and_then(|s: &ff_core::model::ModelSchema| s.get_freshness())
    {
        Some(f) => f,
        None => {
            return FreshnessResult {
                resource_type: "model".to_string(),
                name: model_name.to_string(),
                table_name: None,
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_seconds: None,
                age_human: None,
                warn_threshold_seconds: None,
                error_threshold_seconds: None,
                error: Some("No freshness configuration".to_string()),
            };
        }
    };

    // Get qualified table name
    let qualified_name = match model.target_schema(default_schema) {
        Some(schema) => format!("{}.{}", schema, model_name),
        None => model_name.to_string(),
    };

    // Query max timestamp
    let query = format!(
        "SELECT CAST(MAX({}) AS VARCHAR) as max_ts FROM {}",
        quote_ident(&freshness_config.loaded_at_field),
        quote_qualified(&qualified_name)
    );

    let max_timestamp = match db.query_one(&query).await {
        Ok(ts) => ts,
        Err(e) => {
            return FreshnessResult {
                resource_type: "model".to_string(),
                name: model_name.to_string(),
                table_name: None,
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_seconds: None,
                age_human: None,
                warn_threshold_seconds: freshness_config
                    .warn_after
                    .as_ref()
                    .map(|t: &FreshnessThreshold| t.to_seconds()),
                error_threshold_seconds: freshness_config
                    .error_after
                    .as_ref()
                    .map(|t: &FreshnessThreshold| t.to_seconds()),
                error: Some(format!("Query failed: {}", e)),
            };
        }
    };

    // Parse timestamp and calculate age
    let (loaded_at, age_seconds) = match &max_timestamp {
        Some(ts) => {
            let age = parse_timestamp_age(ts);
            (Some(ts.clone()), age)
        }
        None => (None, None),
    };

    // Determine status
    let warn_threshold = freshness_config
        .warn_after
        .as_ref()
        .map(|t: &FreshnessThreshold| t.to_seconds());
    let error_threshold = freshness_config
        .error_after
        .as_ref()
        .map(|t: &FreshnessThreshold| t.to_seconds());

    let status = determine_status_seconds(age_seconds, warn_threshold, error_threshold);
    let has_data = loaded_at.is_some();

    FreshnessResult {
        resource_type: "model".to_string(),
        name: model_name.to_string(),
        table_name: None,
        status,
        loaded_at,
        age_seconds,
        age_human: age_seconds.map(format_duration),
        warn_threshold_seconds: warn_threshold,
        error_threshold_seconds: error_threshold,
        error: if age_seconds.is_none() && !has_data {
            Some("No data or null timestamps".to_string())
        } else {
            None
        },
    }
}

// ---------------------------------------------------------------------------
// Source freshness
// ---------------------------------------------------------------------------

/// Check freshness for sources with freshness config
async fn check_sources_freshness(
    db: &Arc<dyn Database>,
    project: &Project,
    select: Option<&std::collections::HashSet<String>>,
    verbose: bool,
) -> Vec<FreshnessResult> {
    // Collect source tables with freshness config
    let tables_with_freshness: Vec<(&SourceFile, _)> = project
        .sources
        .iter()
        .filter(|s| select.map(|f| f.contains(&s.name)).unwrap_or(true))
        .flat_map(|source| {
            source
                .tables
                .iter()
                .filter(|t| t.freshness.is_some())
                .map(move |table| (source, table))
        })
        .collect();

    if tables_with_freshness.is_empty() {
        return Vec::new();
    }

    if verbose {
        eprintln!(
            "[verbose] Checking freshness for {} source table(s)",
            tables_with_freshness.len()
        );
    }

    let now = chrono::Utc::now();
    let mut results = Vec::new();

    for (source, table) in &tables_with_freshness {
        let Some(freshness_config) = table.freshness.as_ref() else {
            continue;
        };
        let qualified_name = source.get_qualified_name(table);

        if verbose {
            eprintln!(
                "[verbose] Checking freshness for {}.{} using column {}",
                source.name, table.name, freshness_config.loaded_at_field
            );
        }

        // Query the max value of the loaded_at field
        let query = format!(
            "SELECT MAX({}) as max_loaded_at FROM {}",
            quote_ident(&freshness_config.loaded_at_field),
            quote_qualified(&qualified_name)
        );

        let result =
            build_source_freshness_result(db, source, table, freshness_config, now, &query).await;

        results.push(result);
    }

    results
}

async fn build_source_freshness_result(
    db: &Arc<dyn Database>,
    source: &SourceFile,
    table: &ff_core::source::SourceTable,
    freshness_config: &ff_core::model::FreshnessConfig,
    now: chrono::DateTime<chrono::Utc>,
    query: &str,
) -> FreshnessResult {
    let query_result = db.query_one(query).await;

    let loaded_at_str = match query_result {
        Ok(Some(s)) => s,
        Ok(None) => {
            return FreshnessResult {
                resource_type: "source".to_string(),
                name: source.name.clone(),
                table_name: Some(table.name.clone()),
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_seconds: None,
                age_human: None,
                warn_threshold_seconds: None,
                error_threshold_seconds: None,
                error: Some("No data in table or NULL loaded_at".to_string()),
            };
        }
        Err(e) => {
            return FreshnessResult {
                resource_type: "source".to_string(),
                name: source.name.clone(),
                table_name: Some(table.name.clone()),
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_seconds: None,
                age_human: None,
                warn_threshold_seconds: None,
                error_threshold_seconds: None,
                error: Some(e.to_string()),
            };
        }
    };

    let Some(loaded_at_ts) = common::parse_timestamp(&loaded_at_str) else {
        return FreshnessResult {
            resource_type: "source".to_string(),
            name: source.name.clone(),
            table_name: Some(table.name.clone()),
            status: FreshnessStatus::RuntimeError,
            loaded_at: Some(loaded_at_str.clone()),
            age_seconds: None,
            age_human: None,
            warn_threshold_seconds: None,
            error_threshold_seconds: None,
            error: Some(format!("Could not parse timestamp: {}", loaded_at_str)),
        };
    };

    let age = now.signed_duration_since(loaded_at_ts);
    let age_secs = age.num_seconds().max(0) as u64;

    let warn_secs = freshness_config
        .warn_after
        .as_ref()
        .map(|p| period_to_seconds(p.count, &p.period));
    let error_secs = freshness_config
        .error_after
        .as_ref()
        .map(|p| period_to_seconds(p.count, &p.period));

    let status = determine_status_seconds(Some(age_secs), warn_secs, error_secs);

    FreshnessResult {
        resource_type: "source".to_string(),
        name: source.name.clone(),
        table_name: Some(table.name.clone()),
        status,
        loaded_at: Some(loaded_at_str),
        age_seconds: Some(age_secs),
        age_human: Some(format_duration(age_secs)),
        warn_threshold_seconds: warn_secs,
        error_threshold_seconds: error_secs,
        error: None,
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Parse timestamp string and calculate age in seconds from now.
fn parse_timestamp_age(ts: &str) -> Option<u64> {
    let parsed = common::parse_timestamp(ts)?;
    let now = chrono::Utc::now();
    let age = now.signed_duration_since(parsed);
    let secs = age.num_seconds();
    if secs >= 0 {
        Some(secs as u64)
    } else {
        None
    }
}

/// Convert a freshness period to seconds
fn period_to_seconds(count: u32, period: &FreshnessPeriod) -> u64 {
    match period {
        FreshnessPeriod::Minute => count as u64 * 60,
        FreshnessPeriod::Hour => count as u64 * 3600,
        FreshnessPeriod::Day => count as u64 * 86400,
    }
}

/// Determine freshness status from age and threshold (all in seconds)
fn determine_status_seconds(
    age_seconds: Option<u64>,
    warn_seconds: Option<u64>,
    error_seconds: Option<u64>,
) -> FreshnessStatus {
    match age_seconds {
        Some(age) => {
            if let Some(error_th) = error_seconds {
                if age > error_th {
                    return FreshnessStatus::Error;
                }
            }
            if let Some(warn_th) = warn_seconds {
                if age > warn_th {
                    return FreshnessStatus::Warn;
                }
            }
            FreshnessStatus::Pass
        }
        None => FreshnessStatus::RuntimeError,
    }
}

/// Format duration in human-readable form
fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86400 {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        format!("{}h {}m", hours, minutes)
    } else {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        format!("{}d {}h", days, hours)
    }
}

/// Get current timestamp in ISO format
fn current_timestamp() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Return ANSI-colored status text for terminal display.
fn colored_status(status: &FreshnessStatus) -> &'static str {
    match status {
        FreshnessStatus::Pass => "\x1b[32mpass\x1b[0m",
        FreshnessStatus::Warn => "\x1b[33mwarn\x1b[0m",
        FreshnessStatus::Error => "\x1b[31merror\x1b[0m",
        FreshnessStatus::RuntimeError => "\x1b[31mruntime_error\x1b[0m",
    }
}

/// Return plain-text status string (used for column-width calculation).
fn plain_status(status: &FreshnessStatus) -> &'static str {
    match status {
        FreshnessStatus::Pass => "pass",
        FreshnessStatus::Warn => "warn",
        FreshnessStatus::Error => "error",
        FreshnessStatus::RuntimeError => "runtime_error",
    }
}

/// Print results as a table with a TYPE column
fn print_table(results: &[FreshnessResult]) {
    let headers = ["TYPE", "NAME", "TABLE", "STATUS", "AGE", "LOADED_AT"];

    // Build plain-text rows for width calculation
    let rows: Vec<Vec<String>> = results
        .iter()
        .map(|r| {
            vec![
                r.resource_type.clone(),
                r.name.clone(),
                r.table_name.as_deref().unwrap_or("-").to_string(),
                plain_status(&r.status).to_string(),
                r.age_human.as_deref().unwrap_or("-").to_string(),
                r.loaded_at.as_deref().unwrap_or("-").to_string(),
            ]
        })
        .collect();

    let widths = common::calculate_column_widths(&headers, &rows);
    common::print_table_header(&headers, &widths);

    // Print rows with ANSI-colored status
    for result in results {
        let table_display = result.table_name.as_deref().unwrap_or("-");
        let age_display = result.age_human.as_deref().unwrap_or("-");
        let loaded_at_display = result.loaded_at.as_deref().unwrap_or("-");

        let status_colored = colored_status(&result.status);
        let status_plain_len = plain_status(&result.status).len();
        let status_padding = widths[3].saturating_sub(status_plain_len);

        println!(
            "{:<w0$}  {:<w1$}  {:<w2$}  {}{}  {:<w4$}  {}",
            result.resource_type,
            result.name,
            table_display,
            status_colored,
            " ".repeat(status_padding),
            age_display,
            loaded_at_display,
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w4 = widths[4],
        );

        if let Some(error) = &result.error {
            println!("  \x1b[90m{}\x1b[0m", error);
        }
    }
}

#[cfg(test)]
#[path = "freshness_test.rs"]
mod tests;
