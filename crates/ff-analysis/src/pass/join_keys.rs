//! JoinKeyAnalysis pass — inspects join conditions for potential issues (A030-A039)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::{JoinType, RelOp};
use crate::pass::{AnalysisPass, Diagnostic, DiagnosticCode, Severity};

/// Join key analysis pass
pub(crate) struct JoinKeyAnalysis;

impl AnalysisPass for JoinKeyAnalysis {
    fn name(&self) -> &'static str {
        "join_keys"
    }

    fn description(&self) -> &'static str {
        "Checks join conditions for type mismatches and Cartesian products"
    }

    fn run_model(&self, model_name: &str, ir: &RelOp, _ctx: &AnalysisContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        walk_for_joins(model_name, ir, &mut diagnostics);
        diagnostics
    }
}

/// Walk the RelOp tree looking for Join nodes
fn walk_for_joins(model: &str, op: &RelOp, diags: &mut Vec<Diagnostic>) {
    match op {
        RelOp::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            walk_for_joins(model, left, diags);
            walk_for_joins(model, right, diags);

            // A032: Cross join detected
            if *join_type == JoinType::Cross {
                diags.push(Diagnostic {
                    code: DiagnosticCode::A032,
                    severity: Severity::Info,
                    message: "Cross join (Cartesian product) detected".to_string(),
                    model: model.to_string(),
                    column: None,
                    hint: Some(
                        "Ensure this is intentional; cross joins can produce very large result sets"
                            .to_string(),
                    ),
                    pass_name: "join_keys".into(),
                });
                return;
            }

            match condition {
                Some(cond) => analyze_join_condition(model, cond, diags),
                None => {
                    // No condition on a non-cross join is unusual
                    if *join_type != JoinType::Cross {
                        diags.push(Diagnostic {
                            code: DiagnosticCode::A032,
                            severity: Severity::Info,
                            message: format!("{} JOIN without ON condition", join_type),
                            model: model.to_string(),
                            column: None,
                            hint: Some("This may produce a Cartesian product".to_string()),
                            pass_name: "join_keys".into(),
                        });
                    }
                }
            }
        }

        RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Aggregate { input, .. }
        | RelOp::Sort { input, .. }
        | RelOp::Limit { input, .. } => {
            walk_for_joins(model, input, diags);
        }

        RelOp::SetOp { left, right, .. } => {
            walk_for_joins(model, left, diags);
            walk_for_joins(model, right, diags);
        }

        RelOp::Scan { .. } => {}
    }
}

/// Analyze a join condition expression for type mismatches and non-equi conditions
fn analyze_join_condition(model: &str, expr: &TypedExpr, diags: &mut Vec<Diagnostic>) {
    if let TypedExpr::BinaryOp {
        left, op, right, ..
    } = expr
    {
        if op.is_eq() {
            // A030: Join key type mismatch
            check_join_key_types(model, left, right, diags);
        } else if op.is_logical() {
            // Recurse into AND/OR conditions
            analyze_join_condition(model, left, diags);
            analyze_join_condition(model, right, diags);
        } else {
            // A033: Non-equi join detected
            diags.push(Diagnostic {
                code: DiagnosticCode::A033,
                severity: Severity::Info,
                message: format!("Non-equi join condition detected (operator: {})", op),
                model: model.to_string(),
                column: None,
                hint: Some("Non-equi joins may have performance implications".to_string()),
                pass_name: "join_keys".into(),
            });
        }
    }
}

/// Check if join key columns have compatible types
fn check_join_key_types(
    model: &str,
    left: &TypedExpr,
    right: &TypedExpr,
    diags: &mut Vec<Diagnostic>,
) {
    let left_type = left.resolved_type();
    let right_type = right.resolved_type();

    // Skip if either is unknown
    if left_type.is_unknown() || right_type.is_unknown() {
        return;
    }

    if !left_type.is_compatible_with(right_type) {
        let left_desc = describe_join_key(left);
        let right_desc = describe_join_key(right);

        diags.push(Diagnostic {
            code: DiagnosticCode::A030,
            severity: Severity::Warning,
            message: format!(
                "Join key type mismatch: '{}' ({}) = '{}' ({})",
                left_desc,
                left_type.display_name(),
                right_desc,
                right_type.display_name()
            ),
            model: model.to_string(),
            column: None,
            hint: Some("Add explicit CASTs to ensure matching types".to_string()),
            pass_name: "join_keys".into(),
        });
    }
}

/// Get a human-readable description of a join key expression
fn describe_join_key(expr: &TypedExpr) -> String {
    match expr {
        TypedExpr::ColumnRef {
            table: Some(t),
            column,
            ..
        } => format!("{}.{}", t, column),
        TypedExpr::ColumnRef { column, .. } => column.clone(),
        _ => "expr".to_string(),
    }
}

#[cfg(test)]
#[path = "join_keys_test.rs"]
mod tests;
