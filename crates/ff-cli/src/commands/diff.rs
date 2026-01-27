//! Diff command implementation
//!
//! Compare model output between two databases.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::Config;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::cli::{DiffArgs, GlobalArgs, OutputFormat};

/// Difference summary for JSON output
#[derive(Debug, Serialize)]
struct DiffSummary {
    timestamp: DateTime<Utc>,
    model: String,
    current_db: String,
    compare_db: String,
    current_row_count: usize,
    compare_row_count: usize,
    row_count_diff: isize,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    removed_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    changed_rows: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    sample_differences: Vec<RowDifference>,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// A single row difference
#[derive(Debug, Serialize)]
struct RowDifference {
    key: String,
    diff_type: String, // "new", "removed", or "changed"
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    current_values: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    compare_values: HashMap<String, String>,
}

/// Execute the diff command
pub async fn execute(args: &DiffArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;
    let json_mode = args.output == OutputFormat::Json;

    // Verify the model exists
    let model = project
        .get_model(&args.model)
        .context(format!("Model not found: {}", args.model))?;

    if global.verbose {
        eprintln!("[verbose] Comparing model: {}", args.model);
        eprintln!("[verbose] Compare database: {}", args.compare_to);
    }

    // Resolve target from CLI flag or FF_TARGET env var
    let target = Config::resolve_target(global.target.as_deref());

    // Get current database config
    let db_config = project
        .config
        .get_database_config(target.as_deref())
        .context("Failed to get database configuration")?;

    // Connect to current database
    let current_db: Arc<dyn Database> = Arc::new(
        DuckDbBackend::new(&db_config.path).context("Failed to connect to current database")?,
    );

    // Connect to comparison database
    let compare_db: Arc<dyn Database> = Arc::new(
        DuckDbBackend::new(&args.compare_to).context("Failed to connect to comparison database")?,
    );

    // Get table name (model name or schema-qualified name)
    let table_name = model
        .config
        .schema
        .as_ref()
        .map(|s| format!("{}.{}", s, args.model))
        .unwrap_or_else(|| args.model.clone());

    // Perform the comparison
    let result = compare_tables(
        &table_name,
        current_db.as_ref(),
        compare_db.as_ref(),
        args,
        global,
    )
    .await;

    match result {
        Ok(summary) => {
            if json_mode {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                print_text_summary(&summary);
            }

            if summary.row_count_diff != 0 || summary.changed_rows.unwrap_or(0) > 0 {
                // Exit with non-zero code if there are differences
                std::process::exit(1);
            }
        }
        Err(e) => {
            if json_mode {
                let summary = DiffSummary {
                    timestamp: Utc::now(),
                    model: args.model.clone(),
                    current_db: db_config.path.clone(),
                    compare_db: args.compare_to.clone(),
                    current_row_count: 0,
                    compare_row_count: 0,
                    row_count_diff: 0,
                    new_rows: None,
                    removed_rows: None,
                    changed_rows: None,
                    sample_differences: vec![],
                    success: false,
                    error: Some(e.to_string()),
                };
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                eprintln!("Error comparing model: {}", e);
            }
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Compare two tables and return a summary
async fn compare_tables(
    table_name: &str,
    current_db: &dyn Database,
    compare_db: &dyn Database,
    args: &DiffArgs,
    global: &GlobalArgs,
) -> Result<DiffSummary> {
    // Get row counts
    let current_count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
    let compare_count_sql = current_count_sql.clone();

    let current_count = current_db
        .query_count(&current_count_sql)
        .await
        .context("Failed to count rows in current database")?;

    let compare_count = compare_db
        .query_count(&compare_count_sql)
        .await
        .context("Failed to count rows in comparison database")?;

    if global.verbose {
        eprintln!("[verbose] Current row count: {}", current_count);
        eprintln!("[verbose] Compare row count: {}", compare_count);
    }

    let row_count_diff = current_count as isize - compare_count as isize;

    // Get column names from current table
    let columns = get_table_columns(current_db, table_name).await?;

    // Filter columns if specified
    let columns_to_compare: Vec<String> = if let Some(ref cols) = args.columns {
        let specified: HashSet<String> = cols.split(',').map(|s| s.trim().to_string()).collect();
        columns
            .iter()
            .filter(|c| specified.contains(*c))
            .cloned()
            .collect()
    } else {
        columns.clone()
    };

    // Determine key column
    let key_columns: Vec<String> = if let Some(ref key) = args.key {
        key.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        // Try to find a primary key or use first column
        vec![columns
            .first()
            .cloned()
            .unwrap_or_else(|| "rowid".to_string())]
    };

    if global.verbose {
        eprintln!("[verbose] Key columns: {:?}", key_columns);
        eprintln!("[verbose] Columns to compare: {:?}", columns_to_compare);
    }

    // Get sample differences
    let (new_rows, removed_rows, changed_rows, sample_diffs) = find_differences(
        table_name,
        current_db,
        compare_db,
        &key_columns,
        &columns_to_compare,
        args.sample_size,
        global,
    )
    .await?;

    Ok(DiffSummary {
        timestamp: Utc::now(),
        model: args.model.clone(),
        current_db: "current".to_string(),
        compare_db: args.compare_to.clone(),
        current_row_count: current_count,
        compare_row_count: compare_count,
        row_count_diff,
        new_rows: Some(new_rows),
        removed_rows: Some(removed_rows),
        changed_rows: Some(changed_rows),
        sample_differences: sample_diffs,
        success: true,
        error: None,
    })
}

/// Get column names from a table
async fn get_table_columns(db: &dyn Database, table_name: &str) -> Result<Vec<String>> {
    let schema = db
        .get_table_schema(table_name)
        .await
        .context(format!("Failed to get schema for table: {}", table_name))?;

    Ok(schema.into_iter().map(|(name, _)| name).collect())
}

/// Find actual row differences between tables
async fn find_differences(
    table_name: &str,
    current_db: &dyn Database,
    compare_db: &dyn Database,
    key_columns: &[String],
    columns: &[String],
    sample_size: usize,
    global: &GlobalArgs,
) -> Result<(usize, usize, usize, Vec<RowDifference>)> {
    let key_expr = key_columns.join(", ");
    let col_list = columns.join(", ");

    // Sample rows from current
    let sample_current_sql = format!(
        "SELECT {} FROM {} ORDER BY {} LIMIT {}",
        col_list,
        table_name,
        key_expr,
        sample_size * 2 // Get more rows for better comparison
    );

    let sample_compare_sql = sample_current_sql.clone();

    let current_sample = current_db
        .query_sample_rows(&sample_current_sql, sample_size * 2)
        .await
        .unwrap_or_default();

    let compare_sample = compare_db
        .query_sample_rows(&sample_compare_sql, sample_size * 2)
        .await
        .unwrap_or_default();

    if global.verbose {
        eprintln!(
            "[verbose] Sampled {} rows from current, {} from compare",
            current_sample.len(),
            compare_sample.len()
        );
    }

    // Convert to sets for comparison (using the full row string)
    let current_set: HashSet<&String> = current_sample.iter().collect();
    let compare_set: HashSet<&String> = compare_sample.iter().collect();

    // Find differences in sample
    let mut sample_diffs = Vec::new();
    let mut new_count = 0;
    let mut removed_count = 0;
    let changed_count = 0;

    // Rows in current but not in compare (new or changed)
    for row_str in &current_sample {
        if !compare_set.contains(row_str) {
            if sample_diffs.len() < sample_size {
                // Parse the row string to extract values
                let values: Vec<&str> = row_str.split(", ").collect();

                let key_value = key_columns
                    .iter()
                    .enumerate()
                    .filter_map(|(j, k)| values.get(j).map(|v| format!("{}={}", k, v)))
                    .collect::<Vec<_>>()
                    .join(", ");

                let current_values: HashMap<String, String> = columns
                    .iter()
                    .enumerate()
                    .filter_map(|(j, c)| values.get(j).map(|v| (c.clone(), v.to_string())))
                    .collect();

                sample_diffs.push(RowDifference {
                    key: key_value,
                    diff_type: "new_or_changed".to_string(),
                    current_values,
                    compare_values: HashMap::new(),
                });
            }
            new_count += 1;
        }
    }

    // Rows in compare but not in current (removed)
    for row_str in &compare_sample {
        if !current_set.contains(row_str) {
            if sample_diffs.len() < sample_size {
                let values: Vec<&str> = row_str.split(", ").collect();

                let key_value = key_columns
                    .iter()
                    .enumerate()
                    .filter_map(|(j, k)| values.get(j).map(|v| format!("{}={}", k, v)))
                    .collect::<Vec<_>>()
                    .join(", ");

                let compare_values: HashMap<String, String> = columns
                    .iter()
                    .enumerate()
                    .filter_map(|(j, c)| values.get(j).map(|v| (c.clone(), v.to_string())))
                    .collect();

                sample_diffs.push(RowDifference {
                    key: key_value,
                    diff_type: "removed".to_string(),
                    current_values: HashMap::new(),
                    compare_values,
                });
            }
            removed_count += 1;
        }
    }

    Ok((new_count, removed_count, changed_count, sample_diffs))
}

/// Print a human-readable summary
fn print_text_summary(summary: &DiffSummary) {
    println!("Model: {}", summary.model);
    println!(
        "Row count: {} (current) vs {} (compare) [{:+} rows]",
        summary.current_row_count, summary.compare_row_count, summary.row_count_diff
    );
    println!();

    if let Some(new) = summary.new_rows {
        println!("New/changed rows (in sample): {}", new);
    }
    if let Some(removed) = summary.removed_rows {
        println!("Removed rows (in sample): {}", removed);
    }
    if let Some(changed) = summary.changed_rows {
        if changed > 0 {
            println!("Changed rows: {}", changed);
        }
    }

    if !summary.sample_differences.is_empty() {
        println!();
        println!("Sample differences:");
        println!("{}", "-".repeat(60));

        for diff in &summary.sample_differences {
            println!("  Key: {}", diff.key);
            println!("  Type: {}", diff.diff_type);

            if !diff.current_values.is_empty() {
                println!("  Current: {:?}", diff.current_values);
            }
            if !diff.compare_values.is_empty() {
                println!("  Compare: {:?}", diff.compare_values);
            }
            println!();
        }
    }

    if summary.row_count_diff == 0
        && summary.new_rows.unwrap_or(0) == 0
        && summary.removed_rows.unwrap_or(0) == 0
    {
        println!();
        println!("✓ No differences found in sample");
    }
}
