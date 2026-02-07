//! Pass infrastructure — composable analysis passes over the IR

pub mod join_keys;
pub mod nullability;
pub mod type_inference;
pub mod unused_columns;

use crate::context::AnalysisContext;
use crate::ir::relop::RelOp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Diagnostic severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational — no action required
    Info,
    /// Warning — potential issue worth reviewing
    Warning,
    /// Error — likely bug or incorrect behavior
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "info"),
            Severity::Warning => write!(f, "warning"),
            Severity::Error => write!(f, "error"),
        }
    }
}

/// A diagnostic message produced by an analysis pass
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    /// Diagnostic code (e.g. "A001")
    pub code: String,
    /// Severity level
    pub severity: Severity,
    /// Human-readable message
    pub message: String,
    /// Model that produced this diagnostic
    pub model: String,
    /// Optional column reference
    pub column: Option<String>,
    /// Optional hint for how to fix
    pub hint: Option<String>,
    /// Name of the pass that produced this diagnostic
    pub pass_name: String,
}

/// Per-model analysis pass trait
pub trait AnalysisPass: Send + Sync {
    /// Pass name (used for filtering and display)
    fn name(&self) -> &'static str;
    /// Human-readable description
    fn description(&self) -> &'static str;
    /// Run the pass on a single model's IR
    fn run_model(&self, model_name: &str, ir: &RelOp, ctx: &AnalysisContext) -> Vec<Diagnostic>;
}

/// Cross-model (DAG-level) analysis pass trait
pub trait DagPass: Send + Sync {
    /// Pass name
    fn name(&self) -> &'static str;
    /// Human-readable description
    fn description(&self) -> &'static str;
    /// Run the pass across all models
    fn run_project(
        &self,
        models: &HashMap<String, RelOp>,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic>;
}

/// Manages and runs analysis passes
pub struct PassManager {
    model_passes: Vec<Box<dyn AnalysisPass>>,
    dag_passes: Vec<Box<dyn DagPass>>,
}

impl PassManager {
    /// Create a PassManager with all built-in passes registered
    pub fn with_defaults() -> Self {
        Self {
            model_passes: vec![
                Box::new(type_inference::TypeInference),
                Box::new(nullability::NullabilityPropagation),
                Box::new(join_keys::JoinKeyAnalysis),
            ],
            dag_passes: vec![Box::new(unused_columns::UnusedColumnDetection)],
        }
    }

    /// Run all passes on the given models, returning collected diagnostics
    ///
    /// Models are processed in the order provided (should be topological).
    /// Model passes run first on each model, then DAG passes run across all models.
    pub fn run(
        &self,
        model_order: &[String],
        models: &HashMap<String, RelOp>,
        ctx: &AnalysisContext,
        pass_filter: Option<&[String]>,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Run model-level passes
        for name in model_order {
            if let Some(ir) = models.get(name) {
                for pass in &self.model_passes {
                    if let Some(filter) = pass_filter {
                        if !filter.iter().any(|f| f == pass.name()) {
                            continue;
                        }
                    }
                    diagnostics.extend(pass.run_model(name, ir, ctx));
                }
            }
        }

        // Run DAG-level passes
        for pass in &self.dag_passes {
            if let Some(filter) = pass_filter {
                if !filter.iter().any(|f| f == pass.name()) {
                    continue;
                }
            }
            diagnostics.extend(pass.run_project(models, ctx));
        }

        diagnostics
    }

    /// List all available pass names
    pub fn pass_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.model_passes.iter().map(|p| p.name()).collect();
        names.extend(self.dag_passes.iter().map(|p| p.name()));
        names
    }
}
