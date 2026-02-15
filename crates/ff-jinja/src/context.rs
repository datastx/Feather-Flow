//! Template context types for Jinja rendering.
//!
//! Provides static (per-invocation) and per-model context variables
//! that are injected into Jinja templates as globals or render context.

use serde::Serialize;

/// Static context for a CLI invocation, set once on the environment.
///
/// Contains project-level and invocation-level variables like
/// `project_name`, `target`, `run_id`, `ff_version`, and `executing`.
#[derive(Debug, Clone)]
pub struct TemplateContext {
    /// Project name from `featherflow.yml`
    pub project_name: String,
    /// Current target configuration
    pub target: TargetContext,
    /// Whether this is an execution run (`true`) or compile/validate (`false`)
    pub executing: bool,
    /// Unique identifier for this CLI invocation (UUID v4)
    pub run_id: String,
    /// ISO 8601 timestamp when the run started
    pub run_started_at: String,
    /// Feather-Flow version from Cargo.toml
    pub ff_version: String,
}

impl TemplateContext {
    /// Create a new template context, generating `run_id` and `run_started_at`.
    pub fn new(project_name: String, target: TargetContext, executing: bool) -> Self {
        Self {
            project_name,
            target,
            executing,
            run_id: uuid::Uuid::new_v4().to_string(),
            run_started_at: chrono::Utc::now().to_rfc3339(),
            ff_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl Default for TemplateContext {
    fn default() -> Self {
        Self::new(String::new(), TargetContext::default(), false)
    }
}

/// Target configuration exposed to templates as `{{ target.name }}`, etc.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TargetContext {
    /// Target name (e.g. "dev", "prod")
    pub name: String,
    /// Default schema for the target
    pub schema: Option<String>,
    /// Database type (e.g. "duckdb")
    pub database_type: String,
}

/// Per-model context injected when rendering each model template.
///
/// Accessible as `{{ model.name }}`, `{{ model.materialized }}`, etc.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ModelContext {
    /// Model name
    pub name: String,
    /// Schema override for this model
    pub schema: Option<String>,
    /// Materialization type (e.g. "view", "table", "incremental")
    pub materialized: String,
    /// Tags applied to this model
    pub tags: Vec<String>,
    /// Relative path to the model SQL file
    pub path: String,
}
