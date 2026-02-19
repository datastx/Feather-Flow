//! TypeInference pass on DataFusion LogicalPlans (A002-A005)
//!
//! A001 (Unknown type) is no longer emitted — DataFusion's planner always
//! resolves types to concrete Arrow DataTypes, eliminating false UNKNOWN
//! diagnostics that the custom IR produced.

use arrow::datatypes::DataType as ArrowDataType;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan};
use ff_core::ModelName;

use crate::context::AnalysisContext;
use crate::datafusion_bridge::types::arrow_to_sql_type;
use crate::types::SqlType;

use super::expr_utils::expr_display_name;
use super::plan_pass::PlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// Type inference analysis pass (DataFusion LogicalPlan)
pub(crate) struct PlanTypeInference;

impl PlanPass for PlanTypeInference {
    fn name(&self) -> &'static str {
        "plan_type_inference"
    }

    fn description(&self) -> &'static str {
        "Checks for type mismatches in UNIONs, lossy casts, and aggregate type issues"
    }

    fn run_model(
        &self,
        model_name: &str,
        plan: &LogicalPlan,
        _ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        walk_plan(model_name, plan, &mut diagnostics);
        diagnostics
    }
}

/// Recursively walk the LogicalPlan tree looking for type issues
fn walk_plan(model: &str, plan: &LogicalPlan, diags: &mut Vec<Diagnostic>) {
    match plan {
        LogicalPlan::Union(union) => {
            if union.inputs.len() >= 2 {
                let first_schema = union.inputs[0].schema();

                for (i, input) in union.inputs.iter().enumerate().skip(1) {
                    check_union_column_types(model, first_schema, input.schema(), i + 1, diags);
                }
            }

            for input in &union.inputs {
                walk_plan(model, input, diags);
            }
        }

        LogicalPlan::Aggregate(agg) => {
            // A004: SUM/AVG on string column
            for expr in &agg.aggr_expr {
                check_aggregate_type(model, expr, &agg.input, diags);
            }
            walk_plan(model, &agg.input, diags);
        }

        LogicalPlan::Projection(proj) => {
            for expr in &proj.expr {
                check_cast_expr(model, expr, &proj.input, diags);
            }
            walk_plan(model, &proj.input, diags);
        }

        LogicalPlan::Filter(filter) => {
            check_cast_expr(model, &filter.predicate, &filter.input, diags);
            walk_plan(model, &filter.input, diags);
        }

        _ => {
            for input in plan.inputs() {
                walk_plan(model, input, diags);
            }
        }
    }
}

/// Check a single UNION branch against the first branch for column count (A003) and type (A002) mismatches.
fn check_union_column_types(
    model: &str,
    first_schema: &DFSchemaRef,
    input_schema: &DFSchemaRef,
    branch_idx: usize,
    diags: &mut Vec<Diagnostic>,
) {
    let first_len = first_schema.fields().len();
    let input_len = input_schema.fields().len();

    if input_len != first_len {
        diags.push(Diagnostic {
            code: DiagnosticCode::A003,
            severity: Severity::Error,
            message: format!(
                "UNION branch {} has {} columns, but first branch has {}",
                branch_idx, input_len, first_len
            ),
            model: ModelName::new(model),
            column: None,
            hint: None,
            pass_name: "plan_type_inference".into(),
        });
        return;
    }

    for (l, r) in first_schema
        .fields()
        .iter()
        .zip(input_schema.fields().iter())
    {
        let l_type = arrow_to_sql_type(l.data_type());
        let r_type = arrow_to_sql_type(r.data_type());
        if !l_type.is_compatible_with(&r_type) {
            diags.push(Diagnostic {
                code: DiagnosticCode::A002,
                severity: Severity::Warning,
                message: format!(
                    "Type mismatch in UNION column '{}': {} vs {}",
                    l.name(),
                    l_type.display_name(),
                    r_type.display_name()
                ),
                model: ModelName::new(model),
                column: Some(l.name().clone()),
                hint: Some("Add explicit CASTs to ensure matching types".to_string()),
                pass_name: "plan_type_inference".into(),
            });
        }
    }
}

/// Check aggregate function argument types (A004)
fn check_aggregate_type(
    model: &str,
    expr: &Expr,
    input_plan: &LogicalPlan,
    diags: &mut Vec<Diagnostic>,
) {
    let agg_func = match expr {
        Expr::AggregateFunction(f) => f,
        Expr::Alias(alias) => {
            check_aggregate_type(model, &alias.expr, input_plan, diags);
            return;
        }
        _ => return,
    };

    let func_name = agg_func.func.name().to_uppercase();
    if !matches!(func_name.as_str(), "SUM" | "AVG") {
        return;
    }

    for arg in &agg_func.params.args {
        let Ok(dt) = arg.get_type(input_plan.schema().as_ref()) else {
            continue;
        };
        if !is_string_type(&dt) {
            continue;
        }
        let col_name = expr_display_name(expr);
        diags.push(Diagnostic {
            code: DiagnosticCode::A004,
            severity: Severity::Warning,
            message: format!("{}() applied to string column '{}'", func_name, col_name),
            model: ModelName::new(model),
            column: Some(col_name),
            hint: Some("Ensure the column is numeric, or add a CAST".to_string()),
            pass_name: "plan_type_inference".into(),
        });
    }
}

/// Check expressions for lossy casts (A005)
fn check_cast_expr(
    model: &str,
    expr: &Expr,
    input_plan: &LogicalPlan,
    diags: &mut Vec<Diagnostic>,
) {
    match expr {
        Expr::Cast(cast) => {
            let Ok(source_arrow) = cast.expr.get_type(input_plan.schema().as_ref()) else {
                check_cast_expr(model, &cast.expr, input_plan, diags);
                return;
            };
            let source = arrow_to_sql_type(&source_arrow);
            let target = arrow_to_sql_type(&cast.data_type);
            if is_lossy_cast(&source, &target) {
                let context = expr_display_name(expr);
                diags.push(Diagnostic {
                    code: DiagnosticCode::A005,
                    severity: Severity::Info,
                    message: format!(
                        "Potentially lossy cast from {} to {} in '{}'",
                        source.display_name(),
                        target.display_name(),
                        context
                    ),
                    model: ModelName::new(model),
                    column: Some(context),
                    hint: Some("Consider using TRY_CAST for safer conversion".to_string()),
                    pass_name: "plan_type_inference".into(),
                });
            }
            check_cast_expr(model, &cast.expr, input_plan, diags);
        }
        Expr::BinaryExpr(bin) => {
            check_cast_expr(model, &bin.left, input_plan, diags);
            check_cast_expr(model, &bin.right, input_plan, diags);
        }
        Expr::Alias(alias) => {
            check_cast_expr(model, &alias.expr, input_plan, diags);
        }
        Expr::ScalarFunction(func) => {
            for arg in &func.args {
                check_cast_expr(model, arg, input_plan, diags);
            }
        }
        Expr::Case(case) => {
            if let Some(ref operand) = case.expr {
                check_cast_expr(model, operand, input_plan, diags);
            }
            for (when, then) in &case.when_then_expr {
                check_cast_expr(model, when, input_plan, diags);
                check_cast_expr(model, then, input_plan, diags);
            }
            if let Some(ref else_expr) = case.else_expr {
                check_cast_expr(model, else_expr, input_plan, diags);
            }
        }
        _ => {}
    }
}

fn is_string_type(dt: &ArrowDataType) -> bool {
    matches!(
        dt,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View
    )
}

/// Check if a cast is potentially lossy
fn is_lossy_cast(source: &SqlType, target: &SqlType) -> bool {
    matches!(
        (source, target),
        (SqlType::Float { .. }, SqlType::Integer { .. })
            | (SqlType::Decimal { .. }, SqlType::Integer { .. })
            | (SqlType::String { .. }, SqlType::Integer { .. })
            | (SqlType::String { .. }, SqlType::Float { .. })
            | (SqlType::String { .. }, SqlType::Decimal { .. })
            | (SqlType::String { .. }, SqlType::Boolean)
            | (SqlType::String { .. }, SqlType::Date)
            | (SqlType::String { .. }, SqlType::Timestamp)
            | (SqlType::Timestamp, SqlType::Date)
    )
}
