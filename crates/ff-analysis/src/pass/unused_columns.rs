//! UnusedColumnDetection DagPass — finds columns produced but never consumed downstream (A020-A029)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::RelOp;
use crate::pass::{DagPass, Diagnostic, DiagnosticCode, Severity};
use std::collections::{HashMap, HashSet};

/// Unused column detection pass (DAG-level)
pub(crate) struct UnusedColumnDetection;

impl DagPass for UnusedColumnDetection {
    fn name(&self) -> &'static str {
        "unused_columns"
    }

    fn description(&self) -> &'static str {
        "Detects columns produced by a model but never used by any downstream model"
    }

    fn run_project(
        &self,
        models: &HashMap<String, RelOp>,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Sort model names for deterministic diagnostic ordering
        let mut sorted_names: Vec<&String> = models.keys().collect();
        sorted_names.sort();

        // For each model, determine which of its output columns are consumed downstream
        for model_name in sorted_names {
            let ir = &models[model_name];
            let output_columns = get_output_columns(ir);

            // Check if this model has any downstream dependents
            let dependents = ctx.dag.dependents(model_name);
            if dependents.is_empty() {
                // Terminal model — skip, as it's a final output
                continue;
            }

            // A021: Check for SELECT * in downstream models (can't detect unused)
            let has_wildcard = has_select_star(ir);
            if has_wildcard {
                diagnostics.push(Diagnostic {
                    code: DiagnosticCode::A021,
                    severity: Severity::Info,
                    message: format!(
                        "Model '{}' uses SELECT * — cannot detect unused columns",
                        model_name
                    ),
                    model: model_name.clone(),
                    column: None,
                    hint: Some(
                        "Enumerate columns explicitly to enable unused column detection"
                            .to_string(),
                    ),
                    pass_name: "unused_columns".into(),
                });
                continue;
            }

            // Collect all columns consumed by downstream models from this model
            let consumed = collect_consumed_columns(model_name, &dependents, models, ctx);

            // A020: Column produced but never consumed
            for col_name in &output_columns {
                if !consumed.contains(&col_name.to_lowercase()) {
                    diagnostics.push(Diagnostic {
                        code: DiagnosticCode::A020,
                        severity: Severity::Info,
                        message: format!(
                            "Column '{}' produced but never used by any downstream model",
                            col_name
                        ),
                        model: model_name.clone(),
                        column: Some(col_name.clone()),
                        hint: Some(
                            "Consider removing this column to simplify the model".to_string(),
                        ),
                        pass_name: "unused_columns".into(),
                    });
                }
            }
        }

        diagnostics
    }
}

/// Get the list of output column names from a model's IR
fn get_output_columns(ir: &RelOp) -> Vec<String> {
    ir.schema().columns.iter().map(|c| c.name.clone()).collect()
}

/// Check if the IR contains a SELECT * at the top level
fn has_select_star(ir: &RelOp) -> bool {
    match ir {
        RelOp::Project { columns, .. } => columns
            .iter()
            .any(|(_, expr)| matches!(expr, TypedExpr::Wildcard { .. })),
        _ => false,
    }
}

/// Collect all column names from `source_model` that are referenced by downstream models
fn collect_consumed_columns(
    source_model: &str,
    dependents: &[String],
    models: &HashMap<String, RelOp>,
    ctx: &AnalysisContext,
) -> HashSet<String> {
    let mut consumed = HashSet::new();

    // Use lineage edges to find which columns are consumed
    for edge in &ctx.lineage.edges {
        if edge.source_model == source_model {
            consumed.insert(edge.source_column.to_lowercase());
        }
    }

    // Also walk downstream IR to find column references
    for dep_name in dependents {
        if let Some(dep_ir) = models.get(dep_name) {
            collect_column_refs_from_ir(dep_ir, &mut consumed);
        }
    }

    consumed
}

/// Walk an IR tree and collect column references
fn collect_column_refs_from_ir(ir: &RelOp, consumed: &mut HashSet<String>) {
    match ir {
        RelOp::Scan { .. } => {
            // Scans don't directly consume columns — the projection above tells us which are used
        }
        RelOp::Project { input, columns, .. } => {
            collect_column_refs_from_ir(input, consumed);
            for (_, expr) in columns {
                collect_column_refs_from_expr(expr, consumed);
            }
        }
        RelOp::Filter {
            input, predicate, ..
        } => {
            collect_column_refs_from_ir(input, consumed);
            collect_column_refs_from_expr(predicate, consumed);
        }
        RelOp::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_column_refs_from_ir(left, consumed);
            collect_column_refs_from_ir(right, consumed);
            if let Some(cond) = condition {
                collect_column_refs_from_expr(cond, consumed);
            }
        }
        RelOp::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            collect_column_refs_from_ir(input, consumed);
            for g in group_by {
                collect_column_refs_from_expr(g, consumed);
            }
            for (_, agg) in aggregates {
                collect_column_refs_from_expr(agg, consumed);
            }
        }
        RelOp::Sort {
            input, order_by, ..
        } => {
            collect_column_refs_from_ir(input, consumed);
            for sk in order_by {
                collect_column_refs_from_expr(&sk.expr, consumed);
            }
        }
        RelOp::Limit { input, .. } => {
            collect_column_refs_from_ir(input, consumed);
        }
        RelOp::SetOp { left, right, .. } => {
            collect_column_refs_from_ir(left, consumed);
            collect_column_refs_from_ir(right, consumed);
        }
    }
}

/// Collect column names referenced in an expression
fn collect_column_refs_from_expr(expr: &TypedExpr, consumed: &mut HashSet<String>) {
    match expr {
        TypedExpr::ColumnRef { column, .. } => {
            consumed.insert(column.to_lowercase());
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            collect_column_refs_from_expr(left, consumed);
            collect_column_refs_from_expr(right, consumed);
        }
        TypedExpr::UnaryOp { expr: inner, .. } => {
            collect_column_refs_from_expr(inner, consumed);
        }
        TypedExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_refs_from_expr(arg, consumed);
            }
        }
        TypedExpr::Cast { expr: inner, .. } => {
            collect_column_refs_from_expr(inner, consumed);
        }
        TypedExpr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_refs_from_expr(op, consumed);
            }
            for c in conditions {
                collect_column_refs_from_expr(c, consumed);
            }
            for r in results {
                collect_column_refs_from_expr(r, consumed);
            }
            if let Some(e) = else_result {
                collect_column_refs_from_expr(e, consumed);
            }
        }
        TypedExpr::IsNull { expr: inner, .. } => {
            collect_column_refs_from_expr(inner, consumed);
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "unused_columns_test.rs"]
mod tests;
