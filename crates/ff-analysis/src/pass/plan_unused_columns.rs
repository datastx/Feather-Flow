//! UnusedColumnDetection DagPlanPass — finds columns produced but never consumed downstream (A020)
//!
//! Operates on DataFusion LogicalPlans. DataFusion expands `SELECT *` into
//! explicit column references during planning, so wildcards are handled
//! transparently without any special-case logic.

use std::collections::{HashMap, HashSet};

use datafusion_expr::{Expr, LogicalPlan};
use ff_core::ModelName;

use crate::context::AnalysisContext;
use crate::datafusion_bridge::propagation::ModelPlanResult;

use super::expr_utils::collect_column_refs;
use super::plan_pass::DagPlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// Unused column detection pass (DAG-level, DataFusion LogicalPlan)
pub(crate) struct PlanUnusedColumns;

impl DagPlanPass for PlanUnusedColumns {
    fn name(&self) -> &'static str {
        "plan_unused_columns"
    }

    fn description(&self) -> &'static str {
        "Detects columns produced by a model but never used by any downstream model"
    }

    fn run_project(
        &self,
        models: &HashMap<ModelName, ModelPlanResult>,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        let mut sorted_names: Vec<&ModelName> = models.keys().collect();
        sorted_names.sort();

        for model_name in sorted_names {
            check_model_unused_columns(model_name, models, ctx, &mut diagnostics);
        }

        diagnostics
    }
}

/// Check a single model for columns produced but never consumed downstream (A020)
fn check_model_unused_columns(
    model_name: &ModelName,
    models: &HashMap<ModelName, ModelPlanResult>,
    ctx: &AnalysisContext,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let Some(result) = models.get(model_name) else {
        return;
    };
    let output_columns: Vec<String> = result
        .plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let dependents = ctx.dag().dependents(model_name);
    if dependents.is_empty() {
        return;
    }

    let consumed = collect_consumed_columns(model_name, &dependents, models, ctx);
    diagnostics.extend(
        output_columns
            .iter()
            .filter(|col_name| !consumed.contains(&col_name.to_lowercase()))
            .map(|col_name| Diagnostic {
                code: DiagnosticCode::A020,
                severity: Severity::Info,
                message: format!(
                    "Column '{}' produced but never used by any downstream model",
                    col_name
                ),
                model: model_name.clone(),
                column: Some(col_name.clone()),
                hint: Some("Consider removing this column to simplify the model".to_string()),
                pass_name: "plan_unused_columns".into(),
            }),
    );
}

/// Collect all column names from `source_model` that are referenced by downstream models
fn collect_consumed_columns(
    source_model: &str,
    dependents: &[String],
    models: &HashMap<ModelName, ModelPlanResult>,
    ctx: &AnalysisContext,
) -> HashSet<String> {
    let mut consumed = HashSet::new();

    // Use lineage edges to find which columns are consumed
    for edge in &ctx.lineage().edges {
        if edge.source_model == source_model {
            consumed.insert(edge.source_column.to_lowercase());
        }
    }

    for dep_name in dependents {
        if let Some(dep_result) = models.get(dep_name.as_str()) {
            collect_column_refs_from_plan(&dep_result.plan, &mut consumed);
        }
    }

    consumed
}

/// Walk a LogicalPlan tree and collect referenced column names (lowercased)
fn collect_column_refs_from_plan(plan: &LogicalPlan, consumed: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Projection(proj) => {
            for expr in &proj.expr {
                collect_column_refs_lowercase(expr, consumed);
            }
            collect_column_refs_from_plan(&proj.input, consumed);
        }
        LogicalPlan::Filter(filter) => {
            collect_column_refs_lowercase(&filter.predicate, consumed);
            collect_column_refs_from_plan(&filter.input, consumed);
        }
        LogicalPlan::Join(join) => {
            collect_columns_from_join(join, consumed);
            collect_column_refs_from_plan(&join.left, consumed);
            collect_column_refs_from_plan(&join.right, consumed);
        }
        LogicalPlan::Aggregate(agg) => {
            for expr in &agg.group_expr {
                collect_column_refs_lowercase(expr, consumed);
            }
            for expr in &agg.aggr_expr {
                collect_column_refs_lowercase(expr, consumed);
            }
            collect_column_refs_from_plan(&agg.input, consumed);
        }
        LogicalPlan::Sort(sort) => {
            for sort_expr in &sort.expr {
                collect_column_refs_lowercase(&sort_expr.expr, consumed);
            }
            collect_column_refs_from_plan(&sort.input, consumed);
        }
        _ => {
            for input in plan.inputs() {
                collect_column_refs_from_plan(input, consumed);
            }
        }
    }
}

/// Collect column refs from JOIN keys and optional filter expression
fn collect_columns_from_join(
    join: &datafusion_expr::logical_plan::Join,
    consumed: &mut HashSet<String>,
) {
    for (left_key, right_key) in &join.on {
        collect_column_refs_lowercase(left_key, consumed);
        collect_column_refs_lowercase(right_key, consumed);
    }
    if let Some(ref filter) = join.filter {
        collect_column_refs_lowercase(filter, consumed);
    }
}

/// Thin wrapper: collect column refs via shared helper, then lowercase for case-insensitive matching
fn collect_column_refs_lowercase(expr: &Expr, consumed: &mut HashSet<String>) {
    let mut raw = HashSet::new();
    collect_column_refs(expr, &mut raw);
    consumed.extend(raw.into_iter().map(|s| s.to_lowercase()));
}
