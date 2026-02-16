//! JoinKeyAnalysis pass on DataFusion LogicalPlans (A030-A033)
//!
//! Inspects join conditions for type mismatches, cross joins, and
//! non-equi join conditions.

use datafusion_expr::{Expr, ExprSchemable, LogicalPlan};

use crate::context::AnalysisContext;
use crate::datafusion_bridge::types::arrow_to_sql_type;

use super::expr_utils::expr_display_name;
use super::plan_pass::PlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// Join key analysis pass (DataFusion LogicalPlan)
pub(crate) struct PlanJoinKeys;

impl PlanPass for PlanJoinKeys {
    fn name(&self) -> &'static str {
        "plan_join_keys"
    }

    fn description(&self) -> &'static str {
        "Checks join conditions for type mismatches and Cartesian products"
    }

    fn run_model(
        &self,
        model_name: &str,
        plan: &LogicalPlan,
        _ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        walk_for_joins(model_name, plan, &mut diagnostics);
        diagnostics
    }
}

/// Walk the LogicalPlan tree looking for Join nodes
fn walk_for_joins(model: &str, plan: &LogicalPlan, diags: &mut Vec<Diagnostic>) {
    match plan {
        LogicalPlan::Join(join) => {
            walk_for_joins(model, &join.left, diags);
            walk_for_joins(model, &join.right, diags);

            if join.on.is_empty() && join.filter.is_none() {
                diags.push(Diagnostic {
                    code: DiagnosticCode::A032,
                    severity: Severity::Info,
                    message: format!("{:?} JOIN without any condition", join.join_type),
                    model: model.to_string(),
                    column: None,
                    hint: Some("This may produce a Cartesian product".to_string()),
                    pass_name: "plan_join_keys".into(),
                });
                return;
            }

            check_equi_join_types(model, join, diags);

            if let Some(ref filter_expr) = join.filter {
                check_non_equi_condition(model, filter_expr, diags);
            }
        }

        _ => {
            for input in plan.inputs() {
                walk_for_joins(model, input, diags);
            }
        }
    }
}

/// Check equi-join key pairs for type mismatches (A030)
fn check_equi_join_types(
    model: &str,
    join: &datafusion_expr::logical_plan::Join,
    diags: &mut Vec<Diagnostic>,
) {
    let left_schema = join.left.schema();
    let right_schema = join.right.schema();

    for (left_key, right_key) in &join.on {
        let left_type = left_key.get_type(left_schema.as_ref()).ok();
        let right_type = right_key.get_type(right_schema.as_ref()).ok();
        let (Some(lt), Some(rt)) = (left_type, right_type) else {
            continue;
        };
        let l_sql = arrow_to_sql_type(&lt);
        let r_sql = arrow_to_sql_type(&rt);
        if l_sql.is_compatible_with(&r_sql) {
            continue;
        }
        diags.push(Diagnostic {
            code: DiagnosticCode::A030,
            severity: Severity::Warning,
            message: format!(
                "Join key type mismatch: '{}' ({}) = '{}' ({})",
                expr_display_name(left_key),
                l_sql.display_name(),
                expr_display_name(right_key),
                r_sql.display_name()
            ),
            model: model.to_string(),
            column: None,
            hint: Some("Add explicit CASTs to ensure matching types".to_string()),
            pass_name: "plan_join_keys".into(),
        });
    }
}

/// Check if a join filter contains non-equi conditions (A033)
fn check_non_equi_condition(model: &str, expr: &Expr, diags: &mut Vec<Diagnostic>) {
    match expr {
        Expr::BinaryExpr(bin) => {
            use datafusion_expr::Operator;
            match bin.op {
                Operator::And => {
                    // Recurse into AND branches
                    check_non_equi_condition(model, &bin.left, diags);
                    check_non_equi_condition(model, &bin.right, diags);
                }
                Operator::Eq => {
                    // Equi condition — fine, no diagnostic
                }
                other => {
                    diags.push(Diagnostic {
                        code: DiagnosticCode::A033,
                        severity: Severity::Info,
                        message: format!("Non-equi join condition detected (operator: {})", other),
                        model: model.to_string(),
                        column: None,
                        hint: Some("Non-equi joins may have performance implications".to_string()),
                        pass_name: "plan_join_keys".into(),
                    });
                }
            }
        }
        _ => {
            // Non-binary expression in join filter — unusual
        }
    }
}
