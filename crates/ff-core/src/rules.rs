//! Rules engine configuration and discovery.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Configuration for the SQL rules engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulesConfig {
    /// Directories containing rule SQL files.
    #[serde(default)]
    pub paths: Vec<String>,

    /// Default severity for rules that don't specify one in their header.
    #[serde(default = "default_severity")]
    pub severity: RuleSeverity,

    /// Behavior when a rule fails: `fail` exits non-zero, `warn` continues.
    #[serde(default = "default_on_failure")]
    pub on_failure: OnRuleFailure,
}

impl Default for RulesConfig {
    fn default() -> Self {
        Self {
            paths: Vec::new(),
            severity: default_severity(),
            on_failure: default_on_failure(),
        }
    }
}

/// Severity of a rule violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleSeverity {
    Error,
    Warn,
}

impl std::fmt::Display for RuleSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleSeverity::Error => write!(f, "error"),
            RuleSeverity::Warn => write!(f, "warn"),
        }
    }
}

/// Behavior when a rule fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnRuleFailure {
    Fail,
    Warn,
}

fn default_severity() -> RuleSeverity {
    RuleSeverity::Error
}

fn default_on_failure() -> OnRuleFailure {
    OnRuleFailure::Fail
}

/// A parsed rule file with its metadata and SQL body.
#[derive(Debug, Clone)]
pub struct RuleFile {
    /// Rule name (from header or filename).
    pub name: String,
    /// Severity (from header or config default).
    pub severity: RuleSeverity,
    /// Human-readable description (from header).
    pub description: Option<String>,
    /// The SQL query to execute.
    pub sql: String,
    /// Path to the rule file.
    pub path: PathBuf,
}

/// Discover and parse rule files from the given directories.
///
/// Each `.sql` file in the rule directories is parsed for header comments
/// that specify the rule name, severity, and description. Files without
/// a `-- rule:` header use the filename (without extension) as the name.
pub fn discover_rules(
    rule_dirs: &[PathBuf],
    default_severity: RuleSeverity,
) -> Result<Vec<RuleFile>, crate::error::CoreError> {
    let mut rules = Vec::new();

    for dir in rule_dirs {
        if !dir.exists() || !dir.is_dir() {
            continue;
        }
        let entries =
            std::fs::read_dir(dir).map_err(|e| crate::error::CoreError::ConfigInvalid {
                message: format!("Failed to read rules directory '{}': {e}", dir.display()),
            })?;

        for entry in entries {
            let entry = entry.map_err(|e| crate::error::CoreError::ConfigInvalid {
                message: format!("Failed reading rules dir entry: {e}"),
            })?;
            let path: PathBuf = entry.path();
            if path.extension().is_none_or(|e| e != "sql") {
                continue;
            }
            let content = std::fs::read_to_string(&path).map_err(|e| {
                crate::error::CoreError::ConfigInvalid {
                    message: format!("Failed to read rule file '{}': {e}", path.display()),
                }
            })?;
            let rule = parse_rule_file(&path, &content, default_severity);
            rules.push(rule);
        }
    }

    rules.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rules)
}

/// Resolve rule paths relative to a project root.
pub fn resolve_rule_paths(paths: &[String], root: &Path) -> Vec<PathBuf> {
    paths.iter().map(|p| root.join(p)).collect()
}

/// Parse a rule file, extracting header comments for metadata.
fn parse_rule_file(path: &Path, content: &str, default_severity: RuleSeverity) -> RuleFile {
    let mut name = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let mut severity = default_severity;
    let mut description = None;
    let mut sql_start = 0;

    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("--") {
            break;
        }
        let comment = trimmed.trim_start_matches("--").trim();
        if let Some(value) = comment.strip_prefix("rule:") {
            name = value.trim().to_string();
        } else if let Some(value) = comment.strip_prefix("severity:") {
            severity = match value.trim().to_lowercase().as_str() {
                "warn" | "warning" => RuleSeverity::Warn,
                _ => RuleSeverity::Error,
            };
        } else if let Some(value) = comment.strip_prefix("description:") {
            description = Some(value.trim().to_string());
        }
        sql_start += line.len() + 1; // +1 for newline
    }

    let sql = content[sql_start.min(content.len())..].trim().to_string();

    RuleFile {
        name,
        severity,
        description,
        sql,
        path: path.to_path_buf(),
    }
}

#[cfg(test)]
#[path = "rules_test.rs"]
mod tests;
