//! TypeInference pass — checks type correctness across the IR (A001-A009)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::{RelOp, SetOpKind};
use crate::ir::types::SqlType;
use crate::pass::{AnalysisPass, Diagnostic, DiagnosticCode, Severity};

/// Type inference analysis pass
pub(crate) struct TypeInference;

impl AnalysisPass for TypeInference {
    fn name(&self) -> &'static str {
        "type_inference"
    }

    fn description(&self) -> &'static str {
        "Checks for type mismatches, unknown types, and lossy casts"
    }

    fn run_model(&self, model_name: &str, ir: &RelOp, _ctx: &AnalysisContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        walk_relop(model_name, ir, &mut diagnostics);
        diagnostics
    }
}

/// Recursively walk the RelOp tree looking for type issues
fn walk_relop(model: &str, op: &RelOp, diags: &mut Vec<Diagnostic>) {
    match op {
        RelOp::Scan { schema, .. } => {
            // A001: Unknown type for output column
            for col in &schema.columns {
                if col.sql_type.is_unknown() {
                    diags.push(Diagnostic {
                        code: DiagnosticCode::A001,
                        severity: Severity::Info,
                        message: format!(
                            "Unknown type for column '{}': {}",
                            col.name,
                            col.sql_type.display_name()
                        ),
                        model: model.to_string(),
                        column: Some(col.name.clone()),
                        hint: Some("Add a 'data_type' annotation in the YAML schema".to_string()),
                        pass_name: "type_inference".into(),
                    });
                }
            }
        }

        RelOp::Project { input, columns, .. } => {
            walk_relop(model, input, diags);
            // Check projected expressions for type issues
            for (name, expr) in columns {
                check_expr_types(model, name, expr, diags);
            }
        }

        RelOp::Filter {
            input, predicate, ..
        } => {
            walk_relop(model, input, diags);
            check_expr_types(model, "<filter>", predicate, diags);
        }

        RelOp::Join {
            left,
            right,
            condition,
            ..
        } => {
            walk_relop(model, left, diags);
            walk_relop(model, right, diags);
            if let Some(cond) = condition {
                check_expr_types(model, "<join>", cond, diags);
            }
        }

        RelOp::Aggregate {
            input, aggregates, ..
        } => {
            walk_relop(model, input, diags);
            for (name, expr) in aggregates {
                check_aggregate_type(model, name, expr, diags);
            }
        }

        RelOp::Sort { input, .. } => {
            walk_relop(model, input, diags);
        }

        RelOp::Limit { input, .. } => {
            walk_relop(model, input, diags);
        }

        RelOp::SetOp {
            left, right, op, ..
        } => {
            walk_relop(model, left, diags);
            walk_relop(model, right, diags);

            let left_schema = left.schema();
            let right_schema = right.schema();

            // A003: UNION operands have different column counts
            if left_schema.len() != right_schema.len() {
                diags.push(Diagnostic {
                    code: DiagnosticCode::A003,
                    severity: Severity::Error,
                    message: format!(
                        "{} operands have different column counts: left={}, right={}",
                        set_op_name(op),
                        left_schema.len(),
                        right_schema.len()
                    ),
                    model: model.to_string(),
                    column: None,
                    hint: None,
                    pass_name: "type_inference".into(),
                });
            } else {
                // A002: Type mismatch in UNION columns
                for (l, r) in left_schema.columns.iter().zip(right_schema.columns.iter()) {
                    if !l.sql_type.is_compatible_with(&r.sql_type) {
                        diags.push(Diagnostic {
                            code: DiagnosticCode::A002,
                            severity: Severity::Warning,
                            message: format!(
                                "Type mismatch in {} column '{}': left is {}, right is {}",
                                set_op_name(op),
                                l.name,
                                l.sql_type.display_name(),
                                r.sql_type.display_name()
                            ),
                            model: model.to_string(),
                            column: Some(l.name.clone()),
                            hint: Some("Add explicit CASTs to ensure matching types".to_string()),
                            pass_name: "type_inference".into(),
                        });
                    }
                }
            }
        }
    }
}

/// Check a typed expression for type issues
fn check_expr_types(model: &str, context: &str, expr: &TypedExpr, diags: &mut Vec<Diagnostic>) {
    match expr {
        TypedExpr::Cast {
            expr: inner,
            target_type,
            ..
        } => {
            // A005: Lossy cast (float→int, string→int, etc.)
            let source_type = inner.resolved_type();
            if is_lossy_cast(source_type, target_type) {
                diags.push(Diagnostic {
                    code: DiagnosticCode::A005,
                    severity: Severity::Info,
                    message: format!(
                        "Potentially lossy cast from {} to {} in '{}'",
                        source_type.display_name(),
                        target_type.display_name(),
                        context
                    ),
                    model: model.to_string(),
                    column: Some(context.to_string()),
                    hint: Some("Consider using TRY_CAST for safer conversion".to_string()),
                    pass_name: "type_inference".into(),
                });
            }
            // Recurse into the inner expression
            check_expr_types(model, context, inner, diags);
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            check_expr_types(model, context, left, diags);
            check_expr_types(model, context, right, diags);
        }
        TypedExpr::UnaryOp { expr: inner, .. } => {
            check_expr_types(model, context, inner, diags);
        }
        TypedExpr::FunctionCall { args, .. } => {
            for arg in args {
                check_expr_types(model, context, arg, diags);
            }
        }
        TypedExpr::Case {
            conditions,
            results,
            else_result,
            ..
        } => {
            for c in conditions {
                check_expr_types(model, context, c, diags);
            }
            for r in results {
                check_expr_types(model, context, r, diags);
            }
            if let Some(e) = else_result {
                check_expr_types(model, context, e, diags);
            }
        }
        _ => {}
    }
}

/// Check aggregate function argument types
fn check_aggregate_type(
    model: &str,
    col_name: &str,
    expr: &TypedExpr,
    diags: &mut Vec<Diagnostic>,
) {
    if let TypedExpr::FunctionCall { name, args, .. } = expr {
        // A004: SUM/AVG on string type
        if matches!(name.as_str(), "SUM" | "AVG") {
            if let Some(arg) = args.first() {
                if arg.resolved_type().is_string() {
                    diags.push(Diagnostic {
                        code: DiagnosticCode::A004,
                        severity: Severity::Warning,
                        message: format!("{}() applied to string column '{}'", name, col_name),
                        model: model.to_string(),
                        column: Some(col_name.to_string()),
                        hint: Some("Ensure the column is numeric, or add a CAST".to_string()),
                        pass_name: "type_inference".into(),
                    });
                }
            }
        }
    }
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

fn set_op_name(op: &SetOpKind) -> &'static str {
    match op {
        SetOpKind::Union | SetOpKind::UnionAll => "UNION",
        SetOpKind::Intersect => "INTERSECT",
        SetOpKind::Except => "EXCEPT",
    }
}

#[cfg(test)]
#[path = "type_inference_test.rs"]
mod tests;
