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

    // Collect all rows as raw string values.
    // We use query_map which executes the statement and yields rows.
    let raw_rows: Vec<Vec<String>> = match stmt.query_map([], |row| {
        let col_count = row.as_ref().column_count();
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val = get_column_as_string(row, i);
            vals.push(val);
        }
        Ok(vals)
    }) {
        Ok(mapped) => {
            let mut collected = Vec::new();
            for row_result in mapped {
                let row = row_result
                    .map_err(|e| MetaError::QueryError(format!("rule row error: {e}")))?;
                collected.push(row);
            }
            collected
        }
        Err(e) => {
            return Ok((make_error_result(rule, format!("Query error: {e}")), vec![]));
        }
    };

    // After query_map, the mutable borrow is released and we can read metadata.
    let column_count = stmt.column_count();
    let column_names: Vec<String> = (0..column_count)
        .map(|i| stmt.column_name(i).map_or("?", |v| v).to_lowercase())
        .collect();

    let message_idx = find_column_index(&column_names, &["violation", "message"]);
    let entity_idx = find_column_index(&column_names, &["model_name", "entity_name"]);

    let violations: Vec<RuleViolation> = raw_rows
        .into_iter()
        .map(|values| {
            let pairs: Vec<(String, String)> = column_names
                .iter()
                .zip(values)
                .map(|(n, v)| (n.clone(), v))
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

/// Extract a column value as a string, handling multiple DuckDB types.
fn get_column_as_string(row: &duckdb::Row<'_>, idx: usize) -> String {
    if let Ok(Some(s)) = row.get::<_, Option<String>>(idx) {
        return s;
    }
    if let Ok(Some(n)) = row.get::<_, Option<i64>>(idx) {
        return n.to_string();
    }
    if let Ok(Some(f)) = row.get::<_, Option<f64>>(idx) {
        return f.to_string();
    }
    if let Ok(Some(b)) = row.get::<_, Option<bool>>(idx) {
        return b.to_string();
    }
    "null".to_string()
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
    for candidate in candidates {
        if let Some(idx) = column_names.iter().position(|c| c == candidate) {
            return Some(idx);
        }
    }
    None
}

fn extract_message(values: &[(String, String)], message_idx: Option<usize>) -> String {
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
    values: &[(String, String)],
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
