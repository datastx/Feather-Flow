//! Query comment generation for SQL observability.
//!
//! Generates `ff_metadata` JSON block comments that are appended to compiled SQL
//! and executed SQL. These comments help trace queries in database logs back to
//! their originating model, project, and invocation.

use chrono::{DateTime, Utc};
use serde::Serialize;
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
}

/// Metadata for a single model's query comment.
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
}

impl QueryCommentContext {
    /// Create a new context for this invocation.
    pub fn new(project_name: &str, target: Option<&str>) -> Self {
        Self {
            project_name: project_name.to_string(),
            target: target.map(String::from),
            invocation_id: Uuid::new_v4().to_string(),
            compiled_at: Utc::now(),
            user: whoami(),
            featherflow_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Build metadata for a specific model.
    pub fn build_metadata(&self, model_name: &str, materialization: &str) -> QueryCommentMetadata {
        QueryCommentMetadata {
            model: model_name.to_string(),
            project: self.project_name.clone(),
            materialization: materialization.to_string(),
            compiled_at: self.compiled_at.to_rfc3339(),
            target: self.target.clone(),
            invocation_id: self.invocation_id.clone(),
            user: self.user.clone(),
            featherflow_version: self.featherflow_version.clone(),
        }
    }
}

/// Build the query comment string for a model.
pub fn build_query_comment(metadata: &QueryCommentMetadata) -> String {
    let json = match serde_json::to_string(metadata) {
        Ok(j) => j,
        Err(e) => {
            log::warn!("Failed to serialize query comment metadata: {}", e);
            return String::new();
        }
    };
    format!("\n/* ff_metadata: {} */", json)
}

/// Append a query comment to SQL.
pub fn append_query_comment(sql: &str, comment: &str) -> String {
    format!("{}{}", sql, comment)
}

/// Strip a query comment from SQL (for reading cached compiled files).
pub fn strip_query_comment(sql: &str) -> &str {
    if let Some(idx) = sql.rfind("\n/* ff_metadata:") {
        &sql[..idx]
    } else {
        sql
    }
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
