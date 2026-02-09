//! JoinKeyAnalysis pass — inspects join conditions for potential issues (A030-A039)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::{JoinType, RelOp};
use crate::ir::types::SqlType;
use crate::pass::{AnalysisPass, Diagnostic, DiagnosticCode, Severity};

/// Join key analysis pass
pub struct JoinKeyAnalysis;

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
                    pass_name: "join_keys".to_string(),
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
                            pass_name: "join_keys".to_string(),
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
                pass_name: "join_keys".to_string(),
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

    if !types_join_compatible(left_type, right_type) {
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
            pass_name: "join_keys".to_string(),
        });
    }
}

/// Check if two types are compatible for join keys
fn types_join_compatible(a: &SqlType, b: &SqlType) -> bool {
    match (a, b) {
        (SqlType::Integer { .. }, SqlType::Integer { .. }) => true,
        (SqlType::Float { .. }, SqlType::Float { .. }) => true,
        (SqlType::Integer { .. }, SqlType::Float { .. })
        | (SqlType::Float { .. }, SqlType::Integer { .. }) => true,
        (SqlType::Decimal { .. }, SqlType::Integer { .. })
        | (SqlType::Integer { .. }, SqlType::Decimal { .. }) => true,
        (SqlType::Decimal { .. }, SqlType::Float { .. })
        | (SqlType::Float { .. }, SqlType::Decimal { .. }) => true,
        (SqlType::Decimal { .. }, SqlType::Decimal { .. }) => true,
        (SqlType::String { .. }, SqlType::String { .. }) => true,
        (SqlType::Date, SqlType::Date) => true,
        (SqlType::Timestamp, SqlType::Timestamp) => true,
        (SqlType::Date, SqlType::Timestamp) | (SqlType::Timestamp, SqlType::Date) => true,
        (SqlType::Boolean, SqlType::Boolean) => true,
        // Incompatible: string vs numeric, etc.
        _ => false,
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
mod tests {
    use super::*;
    use crate::ir::expr::{BinOp, TypedExpr};
    use crate::ir::relop::{JoinType, RelOp};
    use crate::ir::schema::RelSchema;
    use crate::ir::types::{Nullability, SqlType, TypedColumn};
    use ff_core::dag::ModelDag;
    use ff_core::Project;
    use ff_sql::ProjectLineage;
    use std::collections::HashMap;
    use std::path::Path;

    fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            source_table: None,
            sql_type: ty,
            nullability: null,
            provenance: vec![],
        }
    }

    fn make_ctx() -> AnalysisContext {
        let project = Project::load(Path::new("../../tests/fixtures/sample_project")).unwrap();
        let dag = ModelDag::build(&HashMap::new()).unwrap();
        AnalysisContext::new(project, dag, HashMap::new(), ProjectLineage::new())
    }

    #[test]
    fn test_a030_join_key_type_mismatch() {
        let left = RelOp::Scan {
            table_name: "orders".to_string(),
            alias: Some("o".to_string()),
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "customers".to_string(),
            alias: Some("c".to_string()),
            schema: RelSchema::new(vec![make_col(
                "order_id",
                SqlType::String { max_length: None },
                Nullability::NotNull,
            )]),
        };
        let merged = RelSchema::merge(left.schema(), right.schema());
        let ir = RelOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(TypedExpr::BinaryOp {
                left: Box::new(TypedExpr::ColumnRef {
                    table: Some("o".to_string()),
                    column: "id".to_string(),
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr::ColumnRef {
                    table: Some("c".to_string()),
                    column: "order_id".to_string(),
                    resolved_type: SqlType::String { max_length: None },
                    nullability: Nullability::NotNull,
                }),
                resolved_type: SqlType::Boolean,
                nullability: Nullability::NotNull,
            }),
            schema: merged,
        };

        let ctx = make_ctx();
        let diags = JoinKeyAnalysis.run_model("test_model", &ir, &ctx);

        assert!(
            diags.iter().any(|d| d.code == DiagnosticCode::A030),
            "Expected A030 for INTEGER = VARCHAR join key type mismatch"
        );
        let a030 = diags
            .iter()
            .find(|d| d.code == DiagnosticCode::A030)
            .unwrap();
        assert_eq!(a030.severity, Severity::Warning);
    }

    #[test]
    fn test_a032_cross_join() {
        let left = RelOp::Scan {
            table_name: "a".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "x",
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "y",
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let merged = RelSchema::merge(left.schema(), right.schema());
        let ir = RelOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Cross,
            condition: None,
            schema: merged,
        };

        let ctx = make_ctx();
        let diags = JoinKeyAnalysis.run_model("test_model", &ir, &ctx);

        assert!(
            diags.iter().any(|d| d.code == DiagnosticCode::A032),
            "Expected A032 for cross join"
        );
        let a032 = diags
            .iter()
            .find(|d| d.code == DiagnosticCode::A032)
            .unwrap();
        assert!(a032.message.contains("Cross join"));
    }

    #[test]
    fn test_a033_non_equi_join() {
        let left = RelOp::Scan {
            table_name: "a".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let merged = RelSchema::merge(left.schema(), right.schema());
        let ir = RelOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(TypedExpr::BinaryOp {
                left: Box::new(TypedExpr::ColumnRef {
                    table: Some("a".to_string()),
                    column: "val".to_string(),
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                }),
                op: BinOp::Gt,
                right: Box::new(TypedExpr::ColumnRef {
                    table: Some("b".to_string()),
                    column: "val".to_string(),
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                }),
                resolved_type: SqlType::Boolean,
                nullability: Nullability::NotNull,
            }),
            schema: merged,
        };

        let ctx = make_ctx();
        let diags = JoinKeyAnalysis.run_model("test_model", &ir, &ctx);

        assert!(
            diags.iter().any(|d| d.code == DiagnosticCode::A033),
            "Expected A033 for non-equi join (>)"
        );
    }

    #[test]
    fn test_compatible_join_keys_no_diagnostic() {
        let left = RelOp::Scan {
            table_name: "a".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer { bits: 64 },
                Nullability::NotNull,
            )]),
        };
        let merged = RelSchema::merge(left.schema(), right.schema());
        let ir = RelOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(TypedExpr::BinaryOp {
                left: Box::new(TypedExpr::ColumnRef {
                    table: Some("a".to_string()),
                    column: "id".to_string(),
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr::ColumnRef {
                    table: Some("b".to_string()),
                    column: "id".to_string(),
                    resolved_type: SqlType::Integer { bits: 64 },
                    nullability: Nullability::NotNull,
                }),
                resolved_type: SqlType::Boolean,
                nullability: Nullability::NotNull,
            }),
            schema: merged,
        };

        let ctx = make_ctx();
        let diags = JoinKeyAnalysis.run_model("test_model", &ir, &ctx);

        // INT32 = INT64 are compatible — no A030
        assert!(
            !diags.iter().any(|d| d.code == DiagnosticCode::A030),
            "Compatible join key types (INT32 = INT64) should not produce A030"
        );
    }
}
