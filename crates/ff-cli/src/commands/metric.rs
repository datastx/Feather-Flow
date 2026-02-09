//! Metric command implementation
//!
//! Commands for working with semantic layer metrics.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::metric::Metric;
use ff_core::Project;
use serde::Serialize;
use std::time::Instant;

use crate::cli::{GlobalArgs, MetricArgs, OutputFormat};
use crate::commands::common::{self, load_project};

/// Metric information for JSON output
#[derive(Debug, Serialize)]
struct MetricInfo {
    name: String,
    label: Option<String>,
    description: Option<String>,
    model: String,
    calculation: String,
    expression: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    dimensions: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    filters: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
}

impl From<&Metric> for MetricInfo {
    fn from(m: &Metric) -> Self {
        MetricInfo {
            name: m.name.clone(),
            label: m.label.clone(),
            description: m.description.clone(),
            model: m.model.clone(),
            calculation: format!("{}", m.calculation),
            expression: m.expression.clone(),
            timestamp: m.timestamp.clone(),
            dimensions: m.dimensions.clone(),
            filters: m.filters.clone(),
            tags: m.tags.clone(),
            owner: m.owner.clone(),
        }
    }
}

/// Metric query result
#[derive(Debug, Serialize)]
struct MetricResult {
    name: String,
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    executed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_secs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// List results for JSON output
#[derive(Debug, Serialize)]
struct MetricListResults {
    timestamp: DateTime<Utc>,
    metric_count: usize,
    metrics: Vec<MetricInfo>,
}

/// Execute the metric command
pub async fn execute(args: &MetricArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let json_mode = args.output == OutputFormat::Json;

    if args.list {
        list_metrics(&project, json_mode, global.verbose)?;
    } else if let Some(ref metric_name) = args.name {
        show_metric(&project, metric_name, args, global, json_mode).await?;
    } else {
        list_metrics(&project, json_mode, global.verbose)?;
    }

    Ok(())
}

/// List all metrics in the project
fn list_metrics(project: &Project, json_mode: bool, verbose: bool) -> Result<()> {
    if project.metrics.is_empty() {
        if json_mode {
            let results = MetricListResults {
                timestamp: Utc::now(),
                metric_count: 0,
                metrics: vec![],
            };
            println!("{}", serde_json::to_string_pretty(&results)?);
        } else {
            println!("No metrics found in project.");
        }
        return Ok(());
    }

    if verbose {
        eprintln!("[verbose] Found {} metrics", project.metrics.len());
    }

    if json_mode {
        let metrics: Vec<MetricInfo> = project.metrics.iter().map(MetricInfo::from).collect();
        let results = MetricListResults {
            timestamp: Utc::now(),
            metric_count: metrics.len(),
            metrics,
        };
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        let headers = ["NAME", "MODEL", "CALCULATION", "EXPRESSION"];

        let rows: Vec<Vec<String>> = project
            .metrics
            .iter()
            .map(|metric| {
                vec![
                    metric.name.clone(),
                    metric.model.clone(),
                    format!("{}", metric.calculation),
                    metric.expression.clone(),
                ]
            })
            .collect();

        common::print_table(&headers, &rows);

        println!();
        println!("{} metrics found", project.metrics.len());
    }

    Ok(())
}

/// Show SQL for a specific metric
async fn show_metric(
    project: &Project,
    metric_name: &str,
    args: &MetricArgs,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<()> {
    let metric = project
        .get_metric(metric_name)
        .context(format!("Metric not found: {}", metric_name))?;

    let sql = metric.generate_sql();

    if args.execute {
        execute_metric(project, metric, &sql, global, json_mode).await
    } else {
        if json_mode {
            let result = MetricResult {
                name: metric.name.clone(),
                sql: sql.clone(),
                executed: Some(false),
                row_count: None,
                duration_secs: None,
                error: None,
            };
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Metric: {}", metric.name);
            if let Some(ref label) = metric.label {
                println!("Label: {}", label);
            }
            if let Some(ref desc) = metric.description {
                println!("Description: {}", desc);
            }
            println!("Model: {}", metric.model);
            println!("Calculation: {}", metric.calculation);
            println!();
            println!("Generated SQL:");
            println!("---");
            println!("{}", sql);
            println!("---");

            if !metric.dimensions.is_empty() {
                println!("\nDimensions: {}", metric.dimensions.join(", "));
            }
            if !metric.filters.is_empty() {
                println!("Filters: {}", metric.filters.join(", "));
            }
        }
        Ok(())
    }
}

/// Execute a metric query against the database
async fn execute_metric(
    project: &Project,
    metric: &Metric,
    sql: &str,
    global: &GlobalArgs,
    json_mode: bool,
) -> Result<()> {
    let start_time = Instant::now();

    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    if global.verbose {
        eprintln!("[verbose] Executing metric SQL");
        eprintln!("[verbose] SQL:\n{}", sql);
    }

    // Execute the query
    let result = db.execute(sql).await;

    let elapsed = start_time.elapsed();

    match result {
        Ok(_) => {
            // Try to get row count
            let count_sql = format!(
                "SELECT COUNT(*) FROM ({}) AS _metric_query",
                sql.trim_end_matches(';')
            );
            let row_count = db.query_count(&count_sql).await.ok();

            if json_mode {
                let result = MetricResult {
                    name: metric.name.clone(),
                    sql: sql.to_string(),
                    executed: Some(true),
                    row_count,
                    duration_secs: Some(elapsed.as_secs_f64()),
                    error: None,
                };
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("Metric: {}", metric.name);
                println!("Status: ✓ executed successfully");
                if let Some(count) = row_count {
                    println!("Rows: {}", count);
                }
                println!("Duration: {}ms", elapsed.as_millis());
                println!();
                println!("SQL:");
                println!("---");
                println!("{}", sql);
                println!("---");
            }
        }
        Err(e) => {
            if json_mode {
                let result = MetricResult {
                    name: metric.name.clone(),
                    sql: sql.to_string(),
                    executed: Some(true),
                    row_count: None,
                    duration_secs: Some(elapsed.as_secs_f64()),
                    error: Some(e.to_string()),
                };
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("Metric: {}", metric.name);
                println!("Status: ✗ execution failed");
                println!("Error: {}", e);
                println!();
                println!("SQL:");
                println!("---");
                println!("{}", sql);
                println!("---");
            }
            return Err(crate::commands::common::ExitCode(1).into());
        }
    }

    Ok(())
}
