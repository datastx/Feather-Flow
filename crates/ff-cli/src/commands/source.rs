//! Source command implementation

use anyhow::{Context, Result};
use chrono::Utc;
use ff_core::model::FreshnessPeriod;
use ff_core::source::SourceFile;
use ff_core::sql_utils::{quote_ident, quote_qualified};
use ff_core::Project;
use serde::Serialize;

use crate::cli::{FreshnessOutput, GlobalArgs, SourceArgs, SourceCommands, SourceFreshnessArgs};
use crate::commands::common::{self, load_project, FreshnessStatus};

/// Execute the source command
pub async fn execute(args: &SourceArgs, global: &GlobalArgs) -> Result<()> {
    match &args.command {
        SourceCommands::Freshness(freshness_args) => {
            execute_freshness(freshness_args, global).await
        }
    }
}

/// Freshness check result
#[derive(Debug, Clone, Serialize)]
struct FreshnessResult {
    source: String,
    table: String,
    status: FreshnessStatus,
    loaded_at: Option<String>,
    age_hours: Option<f64>,
    warn_threshold_hours: Option<f64>,
    error_threshold_hours: Option<f64>,
    error: Option<String>,
}

/// Execute freshness check
async fn execute_freshness(args: &SourceFreshnessArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    // Filter sources if specified
    let sources_to_check: Vec<&SourceFile> = if let Some(source_filter) = &args.sources {
        let filter_set: std::collections::HashSet<_> = source_filter
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        project
            .sources
            .iter()
            .filter(|s| filter_set.contains(&s.name))
            .collect()
    } else {
        project.sources.iter().collect()
    };

    // Collect tables with freshness config
    let tables_with_freshness: Vec<_> = sources_to_check
        .iter()
        .flat_map(|source| {
            source
                .tables
                .iter()
                .filter(|t| t.freshness.is_some())
                .map(move |table| (source, table))
        })
        .collect();

    if tables_with_freshness.is_empty() {
        println!("No sources with freshness configuration found.");
        return Ok(());
    }

    println!(
        "Checking freshness for {} source table(s)...\n",
        tables_with_freshness.len()
    );

    let mut results: Vec<FreshnessResult> = Vec::new();
    let now = Utc::now();

    for (source, table) in &tables_with_freshness {
        let Some(freshness_config) = table.freshness.as_ref() else {
            continue;
        };
        let qualified_name = source.get_qualified_name(table);

        if global.verbose {
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

        let result = match db.query_one(&query).await {
            Ok(Some(loaded_at_str)) => {
                // Parse the timestamp
                let loaded_at = common::parse_timestamp(&loaded_at_str);

                match loaded_at {
                    Some(loaded_at_ts) => {
                        let age = now.signed_duration_since(loaded_at_ts);
                        let age_hours = age.num_seconds() as f64 / 3600.0;

                        // Calculate thresholds
                        let warn_hours = freshness_config
                            .warn_after
                            .as_ref()
                            .map(|p| period_to_hours(p.count, &p.period));
                        let error_hours = freshness_config
                            .error_after
                            .as_ref()
                            .map(|p| period_to_hours(p.count, &p.period));

                        // Determine status
                        let status = determine_status(age_hours, warn_hours, error_hours);

                        FreshnessResult {
                            source: source.name.clone(),
                            table: table.name.clone(),
                            status,
                            loaded_at: Some(loaded_at_str),
                            age_hours: Some(age_hours),
                            warn_threshold_hours: warn_hours,
                            error_threshold_hours: error_hours,
                            error: None,
                        }
                    }
                    None => FreshnessResult {
                        source: source.name.clone(),
                        table: table.name.clone(),
                        status: FreshnessStatus::RuntimeError,
                        loaded_at: Some(loaded_at_str.clone()),
                        age_hours: None,
                        warn_threshold_hours: None,
                        error_threshold_hours: None,
                        error: Some(format!("Could not parse timestamp: {}", loaded_at_str)),
                    },
                }
            }
            Ok(None) => FreshnessResult {
                source: source.name.clone(),
                table: table.name.clone(),
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_hours: None,
                warn_threshold_hours: None,
                error_threshold_hours: None,
                error: Some("No data in table or NULL loaded_at".to_string()),
            },
            Err(e) => FreshnessResult {
                source: source.name.clone(),
                table: table.name.clone(),
                status: FreshnessStatus::RuntimeError,
                loaded_at: None,
                age_hours: None,
                warn_threshold_hours: None,
                error_threshold_hours: None,
                error: Some(e.to_string()),
            },
        };

        results.push(result);
    }

    // Output results
    match args.output {
        FreshnessOutput::Table => print_table(&results),
        FreshnessOutput::Json => print_json(&results)?,
    }

    // Write results to target/sources.json
    write_results_to_file(&project, &results)?;

    // Exit with error code if any errors or runtime errors
    let has_errors = results
        .iter()
        .any(|r| r.status == FreshnessStatus::Error || r.status == FreshnessStatus::RuntimeError);
    if has_errors {
        return Err(crate::commands::common::ExitCode(1).into());
    }

    Ok(())
}

/// Write freshness results to target/sources.json
fn write_results_to_file(project: &Project, results: &[FreshnessResult]) -> Result<()> {
    let sources_path = project.target_dir().join("sources.json");
    common::write_json_results(&sources_path, results)
}

/// Convert period to hours
fn period_to_hours(count: u32, period: &FreshnessPeriod) -> f64 {
    match period {
        FreshnessPeriod::Minute => count as f64 / 60.0,
        FreshnessPeriod::Hour => count as f64,
        FreshnessPeriod::Day => count as f64 * 24.0,
    }
}

/// Determine freshness status based on age and thresholds
fn determine_status(
    age_hours: f64,
    warn_hours: Option<f64>,
    error_hours: Option<f64>,
) -> FreshnessStatus {
    if let Some(error_threshold) = error_hours {
        if age_hours > error_threshold {
            return FreshnessStatus::Error;
        }
    }

    if let Some(warn_threshold) = warn_hours {
        if age_hours > warn_threshold {
            return FreshnessStatus::Warn;
        }
    }

    FreshnessStatus::Pass
}

/// Format duration in human-readable form
fn format_duration(hours: f64) -> String {
    if hours < 1.0 {
        format!("{:.0}m", hours * 60.0)
    } else if hours < 24.0 {
        format!("{:.1}h", hours)
    } else {
        format!("{:.1}d", hours / 24.0)
    }
}

/// Print results in table format
fn print_table(results: &[FreshnessResult]) {
    let headers = ["SOURCE", "TABLE", "STATUS", "AGE", "MAX_LOADED"];

    // Pre-build rows for width calculation
    let rows: Vec<Vec<String>> = results
        .iter()
        .map(|result| {
            let status_symbol = match result.status {
                FreshnessStatus::Pass => "PASS",
                FreshnessStatus::Warn => "WARN",
                FreshnessStatus::Error => "ERROR",
                FreshnessStatus::RuntimeError => "ERR",
            };

            let age_str = result
                .age_hours
                .map(format_duration)
                .unwrap_or_else(|| "-".to_string());

            let loaded_str = result
                .loaded_at
                .as_ref()
                .map(|s| {
                    // Truncate to first 19 chars (YYYY-MM-DD HH:MM:SS)
                    if s.len() > 19 {
                        s[..19].to_string()
                    } else {
                        s.clone()
                    }
                })
                .unwrap_or_else(|| "-".to_string());

            vec![
                result.source.clone(),
                result.table.clone(),
                status_symbol.to_string(),
                age_str,
                loaded_str,
            ]
        })
        .collect();

    // Calculate widths and print header + separator
    let widths = common::calculate_column_widths(&headers, &rows);
    common::print_table_header(&headers, &widths);

    // Count results by status
    let mut pass_count = 0;
    let mut warn_count = 0;
    let mut error_count = 0;
    let mut runtime_error_count = 0;

    // Print each result row, with interleaved error messages
    for (result, row) in results.iter().zip(&rows) {
        match result.status {
            FreshnessStatus::Pass => pass_count += 1,
            FreshnessStatus::Warn => warn_count += 1,
            FreshnessStatus::Error => error_count += 1,
            FreshnessStatus::RuntimeError => runtime_error_count += 1,
        }

        println!("{}", common::format_table_row(row, &widths));

        if let Some(err) = &result.error {
            println!("    Error: {}", err);
        }
    }

    println!();
    println!(
        "Total: {} pass, {} warn, {} error, {} runtime_error",
        pass_count, warn_count, error_count, runtime_error_count
    );
}

/// Print results in JSON format
fn print_json(results: &[FreshnessResult]) -> Result<()> {
    let json = serde_json::to_string_pretty(results).context("Failed to serialize to JSON")?;
    println!("{}", json);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp_various_formats() {
        assert!(common::parse_timestamp("2024-01-15 10:30:00").is_some());
        assert!(common::parse_timestamp("2024-01-15 10:30:00.123").is_some());
        assert!(common::parse_timestamp("2024-01-15T10:30:00Z").is_some());
        assert!(common::parse_timestamp("2024-01-15T10:30:00.123Z").is_some());
    }

    #[test]
    fn test_period_to_hours() {
        assert_eq!(period_to_hours(30, &FreshnessPeriod::Minute), 0.5);
        assert_eq!(period_to_hours(2, &FreshnessPeriod::Hour), 2.0);
        assert_eq!(period_to_hours(1, &FreshnessPeriod::Day), 24.0);
    }

    #[test]
    fn test_determine_status() {
        // No thresholds - always pass
        assert_eq!(determine_status(100.0, None, None), FreshnessStatus::Pass);

        // Under both thresholds
        assert_eq!(
            determine_status(1.0, Some(2.0), Some(4.0)),
            FreshnessStatus::Pass
        );

        // Over warn but under error
        assert_eq!(
            determine_status(3.0, Some(2.0), Some(4.0)),
            FreshnessStatus::Warn
        );

        // Over error
        assert_eq!(
            determine_status(5.0, Some(2.0), Some(4.0)),
            FreshnessStatus::Error
        );
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0.5), "30m");
        assert_eq!(format_duration(2.5), "2.5h");
        assert_eq!(format_duration(48.0), "2.0d");
    }
}
