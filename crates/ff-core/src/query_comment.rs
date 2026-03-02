//! Query comment generation for SQL observability.
//!
//! Generates `ff_metadata` JSON block comments that are appended or prepended
//! to compiled SQL. These comments help trace queries in database logs back to
//! their originating model, project, and invocation.
//!
//! The comment format, placement, and included fields are all configurable via
//! `query_comment` in `featherflow.yml`.

use crate::config::{CommentInclude, CommentPlacement, CommentStyle, QueryCommentConfig};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

/// Context shared across all models in a single invocation.
#[derive(Debug, Clone)]
pub struct QueryCommentContext {
    /// Project name from featherflow.yml
    pub project_name: String,
    /// Target name (e.g., "dev", "prod") if specified
    pub target: Option<String>,
    /// Unique identifier for this invocation
    pub invocation_id: String,
    /// Timestamp when compilation started
    pub compiled_at: DateTime<Utc>,
    /// OS user who invoked the command
    pub user: String,
    /// Featherflow version
    pub featherflow_version: String,
    /// Comment configuration (controls format, placement, and included fields)
    pub config: QueryCommentConfig,
    /// Execution ID for this run (set during `ff run`, None during compile-only)
    pub execution_id: Option<String>,
    /// Run mode (set during `ff run`)
    pub run_mode: Option<String>,
    /// Whether --full-refresh is active (set during `ff run`)
    pub is_full_refresh: Option<bool>,
}

/// Per-model fields passed when building a query comment.
#[derive(Debug, Clone)]
pub struct ModelCommentInput<'a> {
    /// Model name
    pub model_name: &'a str,
    /// Materialization type (view, table, incremental)
    pub materialization: &'a str,
    /// Relative path of the model SQL file inside the project
    pub node_path: Option<&'a str>,
    /// Target schema the model writes to
    pub schema: Option<&'a str>,
}

/// Full metadata for a single model's query comment.
///
/// Only serialized into JSON indirectly via `build_metadata_value` so that
/// the `include` filter is respected. This struct is kept for programmatic
/// access when needed.
#[derive(Debug, Clone, Serialize)]
pub struct QueryCommentMetadata {
    /// Model name
    pub model: String,
    /// Project name
    pub project: String,
    /// Materialization type (view, table, incremental)
    pub materialization: String,
    /// RFC 3339 timestamp of compilation
    pub compiled_at: String,
    /// Target name if specified
    pub target: Option<String>,
    /// Unique invocation identifier
    pub invocation_id: String,
    /// OS user who invoked the command
    pub user: String,
    /// Featherflow version
    pub featherflow_version: String,
    /// Relative path to the model source file
    pub node_path: Option<String>,
    /// Target schema
    pub schema: Option<String>,
    /// Custom key-value pairs from config (legacy)
    #[serde(flatten)]
    pub custom_vars: HashMap<String, String>,
    /// Custom key-value pairs with env var interpolation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<HashMap<String, String>>,
    /// Unique ID for this run invocation (populated during `ff run`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// Run mode: "models", "test", "build" (populated during `ff run`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_mode: Option<String>,
    /// Whether this run uses --full-refresh (populated during `ff run`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_full_refresh: Option<bool>,
}

impl QueryCommentContext {
    /// Create a new context for this invocation.
    pub fn new(project_name: &str, target: Option<&str>, config: QueryCommentConfig) -> Self {
        Self {
            project_name: project_name.to_string(),
            target: target.map(String::from),
            invocation_id: Uuid::new_v4().to_string(),
            compiled_at: Utc::now(),
            user: whoami(),
            featherflow_version: env!("CARGO_PKG_VERSION").to_string(),
            config,
            execution_id: None,
            run_mode: None,
            is_full_refresh: None,
        }
    }

    /// Set runtime fields for this context (used during `ff run`).
    pub fn with_runtime_fields(mut self, run_mode: &str, is_full_refresh: bool) -> Self {
        self.execution_id = Some(Uuid::new_v4().to_string());
        self.run_mode = Some(run_mode.to_string());
        self.is_full_refresh = Some(is_full_refresh);
        self
    }

    /// Build full metadata for a specific model.
    pub fn build_metadata(&self, input: &ModelCommentInput<'_>) -> QueryCommentMetadata {
        let custom_fields = self
            .config
            .custom_fields
            .as_ref()
            .map(resolve_custom_fields);

        QueryCommentMetadata {
            model: input.model_name.to_string(),
            project: self.project_name.clone(),
            materialization: input.materialization.to_string(),
            compiled_at: self.compiled_at.to_rfc3339(),
            target: self.target.clone(),
            invocation_id: self.invocation_id.clone(),
            user: self.user.clone(),
            featherflow_version: self.featherflow_version.clone(),
            node_path: input.node_path.map(String::from),
            schema: input.schema.map(String::from),
            custom_vars: self.config.custom_vars.clone(),
            custom_fields,
            execution_id: self.execution_id.clone(),
            run_mode: self.run_mode.clone(),
            is_full_refresh: self.is_full_refresh,
        }
    }

    /// Build the query comment string for a model, respecting all config options.
    pub fn build_comment(&self, input: &ModelCommentInput<'_>) -> String {
        let metadata = self.build_metadata(input);
        let value = filter_metadata(&metadata, &self.config.include);
        format_comment(&value, self.config.style)
    }
}

/// Filter metadata fields based on the `include` configuration.
fn filter_metadata(metadata: &QueryCommentMetadata, include: &CommentInclude) -> Value {
    let mut map = Map::new();

    if include.model {
        map.insert("model".into(), Value::String(metadata.model.clone()));
    }
    if include.project {
        map.insert("project".into(), Value::String(metadata.project.clone()));
    }
    if include.materialization {
        map.insert(
            "materialization".into(),
            Value::String(metadata.materialization.clone()),
        );
    }
    if include.compiled_at {
        map.insert(
            "compiled_at".into(),
            Value::String(metadata.compiled_at.clone()),
        );
    }
    if include.target {
        match &metadata.target {
            Some(t) => map.insert("target".into(), Value::String(t.clone())),
            None => map.insert("target".into(), Value::Null),
        };
    }
    if include.invocation_id {
        map.insert(
            "invocation_id".into(),
            Value::String(metadata.invocation_id.clone()),
        );
    }
    if include.user {
        map.insert("user".into(), Value::String(metadata.user.clone()));
    }
    if include.featherflow_version {
        map.insert(
            "featherflow_version".into(),
            Value::String(metadata.featherflow_version.clone()),
        );
    }
    if include.node_path {
        if let Some(ref p) = metadata.node_path {
            map.insert("node_path".into(), Value::String(p.clone()));
        }
    }
    if include.schema {
        if let Some(ref s) = metadata.schema {
            map.insert("schema".into(), Value::String(s.clone()));
        }
    }

    for (k, v) in &metadata.custom_vars {
        map.insert(k.clone(), Value::String(v.clone()));
    }

    if let Some(ref fields) = metadata.custom_fields {
        for (k, v) in fields {
            map.insert(k.clone(), Value::String(v.clone()));
        }
    }

    if let Some(ref eid) = metadata.execution_id {
        map.insert("execution_id".into(), Value::String(eid.clone()));
    }
    if let Some(ref mode) = metadata.run_mode {
        map.insert("run_mode".into(), Value::String(mode.clone()));
    }
    if let Some(full_refresh) = metadata.is_full_refresh {
        map.insert("is_full_refresh".into(), Value::Bool(full_refresh));
    }

    Value::Object(map)
}

/// Format a metadata JSON value into a SQL block comment.
fn format_comment(value: &Value, style: CommentStyle) -> String {
    let json = match style {
        CommentStyle::Compact => {
            serde_json::to_string(value).expect("serde_json::to_string cannot fail on Value")
        }
        CommentStyle::Pretty => {
            serde_json::to_string_pretty(value).expect("serde_json::to_string cannot fail on Value")
        }
    };
    match style {
        CommentStyle::Compact => format!("/* ff_metadata: {} */", json),
        CommentStyle::Pretty => format!("/*\nff_metadata:\n{}\n*/", json),
    }
}

/// Attach a query comment to SQL respecting the configured placement.
pub fn attach_query_comment(sql: &str, comment: &str, placement: CommentPlacement) -> String {
    match placement {
        CommentPlacement::Append => format!("{}\n{}", sql, comment),
        CommentPlacement::Prepend => format!("{}\n{}", comment, sql),
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;

    pub(crate) fn build_query_comment(metadata: &QueryCommentMetadata) -> String {
        let include = CommentInclude::default();
        let value = filter_metadata(metadata, &include);
        format_comment(&value, CommentStyle::Compact)
    }

    pub(crate) fn append_query_comment(sql: &str, comment: &str) -> String {
        attach_query_comment(sql, comment, CommentPlacement::Append)
    }
}

/// Strip a query comment from SQL (for reading cached compiled files).
///
/// Handles both append (trailing) and prepend (leading) comments.
pub fn strip_query_comment(sql: &str) -> &str {
    if let Some(idx) = sql.rfind("\n/* ff_metadata:") {
        return &sql[..idx];
    }
    if let Some(idx) = sql.rfind("\n/*\nff_metadata:") {
        return &sql[..idx];
    }
    if sql.starts_with("/* ff_metadata:") {
        if let Some(end) = sql.find("*/") {
            let rest = &sql[end + 2..];
            return rest.strip_prefix('\n').unwrap_or(rest);
        }
    }
    if sql.starts_with("/*\nff_metadata:") {
        if let Some(end) = sql.find("*/") {
            let rest = &sql[end + 2..];
            return rest.strip_prefix('\n').unwrap_or(rest);
        }
    }
    sql
}

/// Interpolate environment variable references in a string value.
///
/// Replaces `$VAR_NAME` patterns with the corresponding environment variable
/// value. Unknown env vars resolve to an empty string. Only bare `$IDENT`
/// patterns are supported (not `${IDENT}` or nested expansions).
pub fn interpolate_env_vars(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' {
            let mut var_name = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_ascii_alphanumeric() || c == '_' {
                    var_name.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            if var_name.is_empty() {
                result.push('$');
            } else {
                result.push_str(&std::env::var(&var_name).unwrap_or_default());
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// Resolve custom fields by interpolating env vars in their values.
fn resolve_custom_fields(fields: &HashMap<String, String>) -> HashMap<String, String> {
    fields
        .iter()
        .map(|(k, v)| (k.clone(), interpolate_env_vars(v)))
        .collect()
}

/// Get the current OS user.
fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
#[path = "query_comment_test.rs"]
mod tests;
