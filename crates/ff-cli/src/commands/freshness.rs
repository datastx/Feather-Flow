//! Freshness command implementation
//!
//! Check model freshness based on SLA configurations defined in schema files.

use anyhow::{Context, Result};
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{FreshnessArgs, FreshnessOutput as CliOutput, GlobalArgs};

/// Execute the freshness command
pub async fn execute(args: &FreshnessArgs, global: &GlobalArgs) -> Result<()> {
    use ff_core::config::Config;

    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Resolve target from CLI flag or FF_TARGET env var
    let target = Config::resolve_target(global.target.as_deref());

    // Get database config, applying target overrides if specified
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

    let db: Arc<dyn Database> = Arc::new(DuckDbBackend::new(&db_config.path)?);

    // Collect models with freshness config
    let models_to_check: Vec<&str> = if let Some(filter) = &args.models {
        filter.split(',').map(|s| s.trim()).collect()
    } else {
        // All models with freshness config
        project
            .models
            .values()
            .filter(|m| {
                m.schema
                    .as_ref()
                    .map(|s| s.has_freshness())
                    .unwrap_or(false)
            })
            .map(|m| m.name.as_str())
            .collect()
    };

    if models_to_check.is_empty() {
        println!("No models with freshness configuration found.");
        return Ok(());
    }

    let default_schema = project.config.schema.as_deref();

    // Check freshness for each model
    let mut results: Vec<FreshnessResult> = Vec::new();

    for model_name in &models_to_check {
        let result = check_model_freshness(&db, &project, model_name, default_schema).await;
        results.push(result);
    }

    // Output results
    match args.output {
        CliOutput::Table => print_table(&results),
        CliOutput::Json => {
            println!("{}", serde_json::to_string_pretty(&results)?);
        }
    }

    // Write JSON file if requested
    if args.write_json {
        let output_path = project_path.join("target/freshness.json");
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(&FreshnessJsonOutput {
            results: results.clone(),
            checked_at: current_timestamp(),
        })?;
        std::fs::write(&output_path, json)?;
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
        std::process::exit(1);
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

/// Freshness check result
#[derive(Debug, Clone, Serialize)]
struct FreshnessResult {
    model: String,
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

/// Freshness status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum FreshnessStatus {
    Pass,
    Warn,
    Error,
    RuntimeError,
}

impl std::fmt::Display for FreshnessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FreshnessStatus::Pass => write!(f, "pass"),
            FreshnessStatus::Warn => write!(f, "warn"),
            FreshnessStatus::Error => write!(f, "error"),
            FreshnessStatus::RuntimeError => write!(f, "runtime_error"),
        }
    }
}

/// Check freshness for a single model
async fn check_model_freshness(
    db: &Arc<dyn Database>,
    project: &Project,
    model_name: &str,
    default_schema: Option<&str>,
) -> FreshnessResult {
    let model = match project.get_model(model_name) {
        Some(m) => m,
        None => {
            return FreshnessResult {
                model: model_name.to_string(),
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

    let freshness_config = match model.schema.as_ref().and_then(|s| s.get_freshness()) {
        Some(f) => f,
        None => {
            return FreshnessResult {
                model: model_name.to_string(),
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
        freshness_config.loaded_at_field, qualified_name
    );

    let max_timestamp = match db.query_one(&query).await {
        Ok(ts) => ts,
        Err(e) => {
            return FreshnessResult {
                model: model_name.to_string(),
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_seconds: None,
                age_human: None,
                warn_threshold_seconds: freshness_config
                    .warn_after
                    .as_ref()
                    .map(|t| t.to_seconds()),
                error_threshold_seconds: freshness_config
                    .error_after
                    .as_ref()
                    .map(|t| t.to_seconds()),
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
    let warn_threshold = freshness_config.warn_after.as_ref().map(|t| t.to_seconds());
    let error_threshold = freshness_config
        .error_after
        .as_ref()
        .map(|t| t.to_seconds());

    let status = match age_seconds {
        Some(age) => {
            if let Some(error_th) = error_threshold {
                if age > error_th {
                    FreshnessStatus::Error
                } else if let Some(warn_th) = warn_threshold {
                    if age > warn_th {
                        FreshnessStatus::Warn
                    } else {
                        FreshnessStatus::Pass
                    }
                } else {
                    FreshnessStatus::Pass
                }
            } else if let Some(warn_th) = warn_threshold {
                if age > warn_th {
                    FreshnessStatus::Warn
                } else {
                    FreshnessStatus::Pass
                }
            } else {
                FreshnessStatus::Pass
            }
        }
        None => FreshnessStatus::RuntimeError,
    };

    let has_data = loaded_at.is_some();

    FreshnessResult {
        model: model_name.to_string(),
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

/// Parse timestamp string and calculate age in seconds from now
fn parse_timestamp_age(ts: &str) -> Option<u64> {
    // Try various timestamp formats
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.fZ",
    ];

    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();

    for fmt in &formats {
        if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(ts, fmt) {
            let timestamp_secs = parsed.and_utc().timestamp() as u64;
            if timestamp_secs <= now {
                return Some(now - timestamp_secs);
            }
        }
    }

    // Try parsing as date only
    if let Ok(date) = chrono::NaiveDate::parse_from_str(ts, "%Y-%m-%d") {
        let datetime = date.and_hms_opt(0, 0, 0)?;
        let timestamp_secs = datetime.and_utc().timestamp() as u64;
        if timestamp_secs <= now {
            return Some(now - timestamp_secs);
        }
    }

    None
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

/// Print results as a table
fn print_table(results: &[FreshnessResult]) {
    // Calculate column widths
    let model_width = results
        .iter()
        .map(|r| r.model.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let status_width = 12;
    let age_width = 12;

    // Header
    println!(
        "{:<model_width$}  {:<status_width$}  {:<age_width$}  LOADED_AT",
        "MODEL",
        "STATUS",
        "AGE",
        model_width = model_width,
        status_width = status_width,
        age_width = age_width,
    );
    println!(
        "{:-<model_width$}  {:-<status_width$}  {:-<age_width$}  {:-<20}",
        "",
        "",
        "",
        "",
        model_width = model_width,
        status_width = status_width,
        age_width = age_width,
    );

    // Rows
    for result in results {
        let status_display = match result.status {
            FreshnessStatus::Pass => "\x1b[32mpass\x1b[0m",
            FreshnessStatus::Warn => "\x1b[33mwarn\x1b[0m",
            FreshnessStatus::Error => "\x1b[31merror\x1b[0m",
            FreshnessStatus::RuntimeError => "\x1b[31mruntime_error\x1b[0m",
        };

        let age_display = result.age_human.as_deref().unwrap_or("-");
        let loaded_at_display = result.loaded_at.as_deref().unwrap_or("-");

        println!(
            "{:<model_width$}  {:<status_width$}  {:<age_width$}  {}",
            result.model,
            status_display,
            age_display,
            loaded_at_display,
            model_width = model_width,
            status_width = status_width,
            age_width = age_width,
        );

        if let Some(error) = &result.error {
            println!("  \x1b[90m{}\x1b[0m", error);
        }
    }
}
