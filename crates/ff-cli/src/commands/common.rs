//! Shared utilities for CLI commands

use chrono::{DateTime, Utc};
use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::source::build_source_lookup;
use ff_core::Project;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;

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
