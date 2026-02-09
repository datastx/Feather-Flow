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
                        pass_name: "type_inference".to_string(),
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
                    pass_name: "type_inference".to_string(),
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
                            pass_name: "type_inference".to_string(),
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
                    pass_name: "type_inference".to_string(),
                });
            }
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
                        pass_name: "type_inference".to_string(),
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
mod tests {
    use super::*;
    use crate::ir::expr::TypedExpr;
    use crate::ir::relop::{RelOp, SetOpKind};
    use crate::ir::schema::RelSchema;
    use crate::ir::types::{FloatBitWidth, IntBitWidth, Nullability, SqlType, TypedColumn};
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
    fn test_a001_unknown_type_for_column() {
        let ir = RelOp::Scan {
            table_name: "test_table".to_string(),
            alias: None,
            schema: RelSchema::new(vec![
                make_col(
                    "id",
                    SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    Nullability::NotNull,
                ),
                make_col(
                    "data",
                    SqlType::Unknown("no type declared".to_string()),
                    Nullability::Unknown,
                ),
            ]),
        };
        let ctx = make_ctx();
        let pass = TypeInference;
        let diags = pass.run_model("test_model", &ir, &ctx);

        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, DiagnosticCode::A001);
        assert_eq!(diags[0].severity, Severity::Info);
        assert!(diags[0].message.contains("data"));
    }

    #[test]
    fn test_a002_union_type_mismatch() {
        let left = RelOp::Scan {
            table_name: "a".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::String { max_length: None },
                Nullability::NotNull,
            )]),
        };
        let ir = RelOp::SetOp {
            left: Box::new(left),
            right: Box::new(right),
            op: SetOpKind::Union,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            )]),
        };
        let ctx = make_ctx();
        let diags = TypeInference.run_model("test_model", &ir, &ctx);

        assert!(diags.iter().any(|d| d.code == DiagnosticCode::A002));
        let a002 = diags
            .iter()
            .find(|d| d.code == DiagnosticCode::A002)
            .unwrap();
        assert_eq!(a002.severity, Severity::Warning);
        assert!(a002.message.contains("val"));
    }

    #[test]
    fn test_a003_union_column_count_mismatch() {
        let left = RelOp::Scan {
            table_name: "a".to_string(),
            alias: None,
            schema: RelSchema::new(vec![
                make_col(
                    "x",
                    SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    Nullability::NotNull,
                ),
                make_col(
                    "y",
                    SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    Nullability::NotNull,
                ),
            ]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "x",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            )]),
        };
        let ir = RelOp::SetOp {
            left: Box::new(left),
            right: Box::new(right),
            op: SetOpKind::UnionAll,
            schema: RelSchema::empty(),
        };
        let ctx = make_ctx();
        let diags = TypeInference.run_model("test_model", &ir, &ctx);

        assert!(diags.iter().any(|d| d.code == DiagnosticCode::A003));
        let a003 = diags
            .iter()
            .find(|d| d.code == DiagnosticCode::A003)
            .unwrap();
        assert_eq!(a003.severity, Severity::Error);
        assert!(a003.message.contains("left=2"));
        assert!(a003.message.contains("right=1"));
    }

    #[test]
    fn test_a004_sum_on_string_column() {
        let input = RelOp::Scan {
            table_name: "t".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "name",
                SqlType::String { max_length: None },
                Nullability::Nullable,
            )]),
        };
        let ir = RelOp::Aggregate {
            input: Box::new(input),
            group_by: vec![],
            aggregates: vec![(
                "total".to_string(),
                TypedExpr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![TypedExpr::ColumnRef {
                        table: None,
                        column: "name".to_string(),
                        resolved_type: SqlType::String { max_length: None },
                        nullability: Nullability::Nullable,
                    }],
                    resolved_type: SqlType::String { max_length: None },
                    nullability: Nullability::Nullable,
                },
            )],
            schema: RelSchema::new(vec![make_col(
                "total",
                SqlType::String { max_length: None },
                Nullability::Nullable,
            )]),
        };
        let ctx = make_ctx();
        let diags = TypeInference.run_model("test_model", &ir, &ctx);

        assert!(diags.iter().any(|d| d.code == DiagnosticCode::A004));
        let a004 = diags
            .iter()
            .find(|d| d.code == DiagnosticCode::A004)
            .unwrap();
        assert_eq!(a004.severity, Severity::Warning);
        assert!(a004.message.contains("SUM"));
    }

    #[test]
    fn test_a005_lossy_cast() {
        let input = RelOp::Scan {
            table_name: "t".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        };
        let ir = RelOp::Project {
            input: Box::new(input),
            columns: vec![(
                "truncated".to_string(),
                TypedExpr::Cast {
                    expr: Box::new(TypedExpr::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                        resolved_type: SqlType::Float {
                            bits: FloatBitWidth::F64,
                        },
                        nullability: Nullability::Nullable,
                    }),
                    target_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    nullability: Nullability::Nullable,
                },
            )],
            schema: RelSchema::new(vec![make_col(
                "truncated",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::Nullable,
            )]),
        };
        let ctx = make_ctx();
        let diags = TypeInference.run_model("test_model", &ir, &ctx);

        assert!(diags.iter().any(|d| d.code == DiagnosticCode::A005));
        let a005 = diags
            .iter()
            .find(|d| d.code == DiagnosticCode::A005)
            .unwrap();
        assert_eq!(a005.severity, Severity::Info);
        assert!(a005.message.contains("lossy"));
    }

    #[test]
    fn test_no_diagnostics_for_clean_scan() {
        let ir = RelOp::Scan {
            table_name: "clean_table".to_string(),
            alias: None,
            schema: RelSchema::new(vec![
                make_col(
                    "id",
                    SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    Nullability::NotNull,
                ),
                make_col(
                    "name",
                    SqlType::String { max_length: None },
                    Nullability::Nullable,
                ),
            ]),
        };
        let ctx = make_ctx();
        let diags = TypeInference.run_model("test_model", &ir, &ctx);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_compatible_union_no_diagnostics() {
        let left = RelOp::Scan {
            table_name: "a".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer {
                    bits: IntBitWidth::I64,
                },
                Nullability::NotNull,
            )]),
        };
        let ir = RelOp::SetOp {
            left: Box::new(left),
            right: Box::new(right),
            op: SetOpKind::Union,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer {
                    bits: IntBitWidth::I64,
                },
                Nullability::NotNull,
            )]),
        };
        let ctx = make_ctx();
        let diags = TypeInference.run_model("test_model", &ir, &ctx);
        // INTEGER and BIGINT are compatible — no A002
        assert!(diags.iter().all(|d| d.code != DiagnosticCode::A002));
    }
}
