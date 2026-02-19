//! DataFusion LogicalPlan-based pass infrastructure
//!
//! All analysis passes operate on DataFusion LogicalPlans, providing richer
//! type information and leveraging DataFusion's planner infrastructure.

use std::collections::HashMap;

use datafusion_expr::LogicalPlan;
use ff_core::ModelName;

use crate::context::AnalysisContext;
use crate::datafusion_bridge::propagation::ModelPlanResult;

use super::Diagnostic;

/// Per-model analysis pass that operates on DataFusion LogicalPlans.
///
/// The `ctx` parameter provides project-wide metadata (YAML schemas, DAG
/// structure, lineage). Passes that don't need project context may ignore it.
pub trait PlanPass: Send + Sync {
    /// Pass name (used for filtering and display)
    fn name(&self) -> &'static str;
    /// Human-readable description
    fn description(&self) -> &'static str;
    /// Run the pass on a single model's LogicalPlan
    fn run_model(
        &self,
        model_name: &str,
        plan: &LogicalPlan,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic>;
}

/// Cross-model (DAG-level) analysis pass that operates on DataFusion LogicalPlans
pub trait DagPlanPass: Send + Sync {
    /// Pass name
    fn name(&self) -> &'static str;
    /// Human-readable description
    fn description(&self) -> &'static str;
    /// Run the pass across all models
    fn run_project(
        &self,
        models: &HashMap<ModelName, ModelPlanResult>,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic>;
}

/// Manages and runs LogicalPlan-based analysis passes.
pub struct PlanPassManager {
    model_passes: Vec<Box<dyn PlanPass>>,
    dag_passes: Vec<Box<dyn DagPlanPass>>,
}

impl PlanPassManager {
    /// Create a PlanPassManager with all built-in LogicalPlan passes
    pub fn with_defaults() -> Self {
        Self {
            model_passes: vec![
                Box::new(super::plan_type_inference::PlanTypeInference),
                Box::new(super::plan_nullability::PlanNullability),
                Box::new(super::plan_join_keys::PlanJoinKeys),
            ],
            dag_passes: vec![
                Box::new(super::plan_unused_columns::PlanUnusedColumns),
                Box::new(super::plan_cross_model::CrossModelConsistency),
            ],
        }
    }

    /// Run all passes, returning collected diagnostics
    ///
    /// Models are processed in the order provided (should be topological).
    pub fn run(
        &self,
        model_order: &[ModelName],
        models: &HashMap<ModelName, ModelPlanResult>,
        ctx: &AnalysisContext,
        pass_filter: Option<&[String]>,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        for name in model_order {
            if let Some(result) = models.get(name) {
                for pass in &self.model_passes {
                    if !super::should_run_pass(pass.name(), pass_filter) {
                        continue;
                    }
                    diagnostics.extend(pass.run_model(name, &result.plan, ctx));
                }
            }
        }

        for pass in &self.dag_passes {
            if !super::should_run_pass(pass.name(), pass_filter) {
                continue;
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
