//! SQL rules engine: execute rule queries against the meta database.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;
use ff_core::rules::{RuleFile, RuleSeverity};

/// Result of executing a single rule.
#[derive(Debug)]
pub struct RuleResult {
    /// Rule name.
    pub name: String,
    /// Rule file path.
    pub path: String,
    /// Rule severity.
    pub severity: RuleSeverity,
    /// Rule description (from header).
    pub description: Option<String>,
    /// Whether the rule passed (zero violations).
    pub passed: bool,
    /// Number of violations found.
    pub violation_count: usize,
    /// Error message if the rule SQL failed to execute.
    pub error: Option<String>,
}

/// A single rule violation extracted from a query result row.
#[derive(Debug)]
pub struct RuleViolation {
    /// Rule name.
    pub rule_name: String,
    /// Rule file path.
    pub rule_path: String,
    /// Severity.
    pub severity: RuleSeverity,
    /// Entity name (model, source, etc.) if available.
    pub entity_name: Option<String>,
    /// Violation message.
    pub message: String,
    /// Additional context as JSON.
    pub context_json: Option<String>,
}

/// Execute a single rule against the meta database.
///
/// Each row returned by the rule's SQL query is a violation. The column
/// contract extracts `message`, `entity_name`, and additional context
/// from the result columns.
pub fn execute_rule(
    conn: &Connection,
    rule: &RuleFile,
) -> MetaResult<(RuleResult, Vec<RuleViolation>)> {
    let mut stmt = match conn.prepare(&rule.sql) {
        Ok(s) => s,
        Err(e) => {
            return Ok((make_error_result(rule, format!("SQL error: {e}")), vec![]));
        }
    };

    let (raw_column_names, raw_rows) = match crate::row_helpers::execute_and_collect(&mut stmt) {
        Ok(result) => result,
        Err(e) => {
            return Ok((make_error_result(rule, format!("Query error: {e}")), vec![]));
        }
    };

    let column_names: Vec<String> = raw_column_names
        .into_iter()
        .map(|n| n.to_lowercase())
        .collect();

    let message_idx = find_column_index(&column_names, &["violation", "message"]);
    let entity_idx = find_column_index(&column_names, &["model_name", "entity_name"]);

    let violations: Vec<RuleViolation> = raw_rows
        .into_iter()
        .map(|values| {
            let pairs: Vec<(&str, String)> = column_names
                .iter()
                .zip(values)
                .map(|(n, v)| (n.as_str(), v))
                .collect();

            let message = extract_message(&pairs, message_idx);
            let entity_name = entity_idx.map(|i| pairs[i].1.clone());
            let context_json = build_context_json(&pairs, &column_names, message_idx, entity_idx);

            RuleViolation {
                rule_name: rule.name.clone(),
                rule_path: rule.path.display().to_string(),
                severity: rule.severity,
                entity_name,
                message,
                context_json,
            }
        })
        .collect();

    let passed = violations.is_empty();
    Ok((
        RuleResult {
            name: rule.name.clone(),
            path: rule.path.display().to_string(),
            severity: rule.severity,
            description: rule.description.clone(),
            passed,
            violation_count: violations.len(),
            error: None,
        },
        violations,
    ))
}

/// Execute all rules and return results.
pub fn execute_all_rules(
    conn: &Connection,
    rules: &[RuleFile],
) -> MetaResult<(Vec<RuleResult>, Vec<RuleViolation>)> {
    let mut all_results = Vec::with_capacity(rules.len());
    let mut all_violations = Vec::new();

    for rule in rules {
        let (result, violations) = execute_rule(conn, rule)?;
        all_results.push(result);
        all_violations.extend(violations);
    }

    Ok((all_results, all_violations))
}

/// Record rule violations in the meta database.
pub fn populate_rule_violations(
    conn: &Connection,
    run_id: i64,
    violations: &[RuleViolation],
) -> MetaResult<()> {
    for v in violations {
        let severity_str = v.severity.to_string();
        conn.execute(
            "INSERT INTO ff_meta.rule_violations (run_id, rule_name, rule_path, severity, entity_name, message, context_json)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                run_id,
                v.rule_name,
                v.rule_path,
                severity_str,
                v.entity_name,
                v.message,
                v.context_json,
            ],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert rule_violations: {e}")))?;
    }
    Ok(())
}

fn make_error_result(rule: &RuleFile, error: String) -> RuleResult {
    RuleResult {
        name: rule.name.clone(),
        path: rule.path.display().to_string(),
        severity: rule.severity,
        description: rule.description.clone(),
        passed: false,
        violation_count: 0,
        error: Some(error),
    }
}

fn find_column_index(column_names: &[String], candidates: &[&str]) -> Option<usize> {
    candidates
        .iter()
        .find_map(|candidate| column_names.iter().position(|c| c == candidate))
}

fn extract_message(values: &[(&str, String)], message_idx: Option<usize>) -> String {
    if let Some(idx) = message_idx {
        return values[idx].1.clone();
    }
    for (_, val) in values {
        if val != "null" {
            return val.clone();
        }
    }
    "Rule violation (no message column)".to_string()
}

fn build_context_json(
    values: &[(&str, String)],
    column_names: &[String],
    message_idx: Option<usize>,
    entity_idx: Option<usize>,
) -> Option<String> {
    let skip_indices: std::collections::HashSet<usize> = [message_idx, entity_idx]
        .iter()
        .filter_map(|&idx| idx)
        .collect();

    let context: Vec<(&str, &str)> = column_names
        .iter()
        .enumerate()
        .filter(|(i, _)| !skip_indices.contains(i))
        .map(|(i, name)| (name.as_str(), values[i].1.as_str()))
        .collect();

    if context.is_empty() {
        return None;
    }

    let map: serde_json::Map<String, serde_json::Value> = context
        .into_iter()
        .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
        .collect();

    Some(serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default())
}

#[cfg(test)]
#[path = "rules_test.rs"]
mod tests;
