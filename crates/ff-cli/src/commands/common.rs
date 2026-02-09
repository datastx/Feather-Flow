//! Shared utilities for CLI commands

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::config::{Config, IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::source::build_source_lookup;
use ff_core::Project;
use ff_db::{Database, DuckDbBackend};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use crate::cli::GlobalArgs;

/// Error type representing a non-zero process exit code.
///
/// Use `return Err(ExitCode(N).into())` instead of `std::process::exit(N)`
/// so that RAII destructors run and cleanup happens properly.
#[derive(Debug)]
pub struct ExitCode(pub i32);

impl fmt::Display for ExitCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Empty display — main.rs handles the exit code without printing
        write!(f, "")
    }
}

impl std::error::Error for ExitCode {}

/// Status for model run / compile / snapshot operations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Success,
    Error,
    Skipped,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunStatus::Success => write!(f, "success"),
            RunStatus::Error => write!(f, "error"),
            RunStatus::Skipped => write!(f, "skipped"),
        }
    }
}

/// Status for schema test results.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TestStatus {
    Pass,
    Fail,
    Error,
}

impl fmt::Display for TestStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestStatus::Pass => write!(f, "pass"),
            TestStatus::Fail => write!(f, "fail"),
            TestStatus::Error => write!(f, "error"),
        }
    }
}

/// Parse hook SQL strings from captured Jinja config values.
///
/// Handles both single-string and array-of-strings representations.
pub fn parse_hooks_from_config(
    config_values: &HashMap<String, minijinja::Value>,
    key: &str,
) -> Vec<String> {
    config_values
        .get(key)
        .map(|v| {
            if let Some(s) = v.as_str() {
                // Single string hook
                vec![s.to_string()]
            } else if v.kind() == minijinja::value::ValueKind::Seq {
                // Array of hooks
                v.try_iter()
                    .map(|iter| {
                        iter.filter_map(|item| item.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        })
        .unwrap_or_default()
}

/// Filter models from the project based on an optional comma-separated list.
///
/// If `models_arg` is `None`, returns all model names.
pub fn filter_models(project: &Project, models_arg: &Option<String>) -> Vec<String> {
    match models_arg {
        Some(models) => models
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        None => project
            .model_names()
            .into_iter()
            .map(String::from)
            .collect(),
    }
}

/// Build a lookup set of all external tables including sources.
pub fn build_external_tables_lookup(project: &Project) -> HashSet<String> {
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    let source_tables = build_source_lookup(&project.sources);
    external_tables.extend(source_tables);
    external_tables
}

/// Parse a materialization string from Jinja config values.
pub fn parse_materialization(s: &str) -> Materialization {
    match s {
        "table" => Materialization::Table,
        "incremental" => Materialization::Incremental,
        "ephemeral" => Materialization::Ephemeral,
        _ => Materialization::View,
    }
}

/// Parse an incremental strategy string from Jinja config values.
pub fn parse_incremental_strategy(s: &str) -> IncrementalStrategy {
    match s {
        "merge" => IncrementalStrategy::Merge,
        "delete+insert" | "delete_insert" => IncrementalStrategy::DeleteInsert,
        _ => IncrementalStrategy::Append,
    }
}

/// Parse an on_schema_change string from Jinja config values.
pub fn parse_on_schema_change(s: &str) -> OnSchemaChange {
    match s {
        "fail" => OnSchemaChange::Fail,
        "append_new_columns" => OnSchemaChange::AppendNewColumns,
        _ => OnSchemaChange::Ignore,
    }
}

/// Parse various timestamp formats into a UTC DateTime.
pub fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];

    for fmt in &formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
    }

    // Try parsing as RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try date-only format
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(datetime) = date.and_hms_opt(0, 0, 0) {
            return Some(DateTime::from_naive_utc_and_offset(datetime, Utc));
        }
    }

    None
}

/// Freshness status shared between source and model freshness commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessStatus {
    Pass,
    Warn,
    Error,
    RuntimeError,
}

impl fmt::Display for FreshnessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FreshnessStatus::Pass => write!(f, "pass"),
            FreshnessStatus::Warn => write!(f, "warn"),
            FreshnessStatus::Error => write!(f, "error"),
            FreshnessStatus::RuntimeError => write!(f, "runtime_error"),
        }
    }
}

/// Load a project from the directory specified in global CLI arguments.
///
/// Converts the `project_dir` string to a `Path` and delegates to
/// `Project::load`, adding context on failure.
pub fn load_project(global: &GlobalArgs) -> Result<Project> {
    let project_path = Path::new(&global.project_dir);
    Project::load(project_path).context("Failed to load project")
}

/// Generic wrapper for command results written to JSON.
///
/// Many commands (run, snapshot, etc.) produce a JSON file with the same
/// envelope: a timestamp, elapsed seconds, success/failure counts, and a
/// vec of per-item results.  `CommandResults<T>` captures that pattern so
/// each command only needs to define its per-item result type.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CommandResults<T: Serialize> {
    pub timestamp: DateTime<Utc>,
    pub elapsed_secs: f64,
    pub success_count: usize,
    pub failure_count: usize,
    pub results: Vec<T>,
}

/// Serialize `data` as pretty-printed JSON and write it to `path`.
///
/// Creates any missing parent directories before writing.  Returns an
/// `anyhow::Result` with context describing which step failed.
pub(crate) fn write_json_results<T: Serialize + ?Sized>(path: &Path, data: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("Failed to create target directory")?;
    }
    let json = serde_json::to_string_pretty(data).context("Failed to serialize results")?;
    std::fs::write(path, json).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Table-printing utilities
// ---------------------------------------------------------------------------

/// Calculate column widths for a table given headers and row data.
///
/// For each column, returns the maximum width across the header and all
/// row values so that data aligns when printed with left-padding.
pub fn calculate_column_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    widths
}

/// Print a formatted table to stdout.
///
/// Calculates column widths from `headers` and `rows`, then prints
/// a left-aligned header row, a separator line of dashes, and each
/// data row.  Columns are separated by two spaces.
///
/// # Examples
///
/// ```ignore
/// print_table(
///     &["NAME", "TYPE"],
///     &[vec!["orders".into(), "model".into()]],
/// );
/// // NAME    TYPE
/// // ------  -----
/// // orders  model
/// ```
pub fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let widths = calculate_column_widths(headers, rows);

    // Print header
    let header_parts: Vec<String> = headers
        .iter()
        .zip(&widths)
        .map(|(h, &w)| format!("{:<width$}", h, width = w))
        .collect();
    println!("{}", header_parts.join("  "));

    // Print separator
    let sep_parts: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep_parts.join("  "));

    // Print rows
    for row in rows {
        let row_parts: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, &w)| format!("{:<width$}", cell, width = w))
            .collect();
        println!("{}", row_parts.join("  "));
    }
}

/// Print just the header and separator lines for a table.
///
/// This is useful for commands that need to print rows individually
/// (e.g. to interleave extra output like error messages between rows).
/// Use [`calculate_column_widths`] to obtain the `widths` parameter.
pub fn print_table_header(headers: &[&str], widths: &[usize]) {
    let header_parts: Vec<String> = headers
        .iter()
        .zip(widths)
        .map(|(h, &w)| format!("{:<width$}", h, width = w))
        .collect();
    println!("{}", header_parts.join("  "));

    let sep_parts: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep_parts.join("  "));
}

/// Format a single table row as a string using pre-computed column widths.
///
/// Each cell is left-aligned and padded to the corresponding width.
/// Columns are separated by two spaces.
pub fn format_table_row(row: &[String], widths: &[usize]) -> String {
    let parts: Vec<String> = row
        .iter()
        .zip(widths)
        .map(|(cell, &w)| format!("{:<width$}", cell, width = w))
        .collect();
    parts.join("  ")
}

/// Create a database connection from a config and optional target override.
///
/// Resolves the target via `Config::resolve_target`, gets the database
/// configuration with `Config::get_database_config`, and creates a
/// `DuckDbBackend` wrapped in an `Arc<dyn Database>`.
pub fn create_database_connection(
    config: &Config,
    target: Option<&str>,
) -> Result<Arc<dyn Database>> {
    let resolved_target = Config::resolve_target(target);
    let db_config = config
        .get_database_config(resolved_target.as_deref())
        .context("Failed to get database configuration")?;
    let db: Arc<dyn Database> =
        Arc::new(DuckDbBackend::new(&db_config.path).context("Failed to connect to database")?);
    Ok(db)
}
