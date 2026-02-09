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
            pass_name: "join_keys".to_string(),
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
mod tests {
    use super::*;
    use crate::ir::expr::{BinOp, TypedExpr};
    use crate::ir::relop::{JoinType, RelOp};
    use crate::ir::schema::RelSchema;
    use crate::ir::types::{IntBitWidth, Nullability, SqlType};
    use crate::test_utils::*;

    #[test]
    fn test_a030_join_key_type_mismatch() {
        let left = RelOp::Scan {
            table_name: "orders".to_string(),
            alias: Some("o".to_string()),
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
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
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
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
                "y",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
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
                    bits: IntBitWidth::I32,
                },
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
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    nullability: Nullability::NotNull,
                }),
                op: BinOp::Gt,
                right: Box::new(TypedExpr::ColumnRef {
                    table: Some("b".to_string()),
                    column: "val".to_string(),
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
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
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I64,
                },
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
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    nullability: Nullability::NotNull,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr::ColumnRef {
                    table: Some("b".to_string()),
                    column: "id".to_string(),
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I64,
                    },
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

    // ── A030: Additional join key type mismatch tests ────────────────────

    #[test]
    fn test_a030_boolean_vs_integer() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("flag", boolean(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("count", int32(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "flag", boolean(), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "count", int32(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A030);
    }

    #[test]
    fn test_a030_date_vs_varchar() {
        let left = make_scan_alias("a", "a", vec![make_col("dt", date(), Nullability::NotNull)]);
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("dt_str", varchar(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "dt", date(), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "dt_str", varchar(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A030);
    }

    #[test]
    fn test_a030_uuid_vs_integer() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("uuid_id", uuid(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("int_id", int32(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "uuid_id", uuid(), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "int_id", int32(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A030);
    }

    #[test]
    fn test_a030_compound_join_one_mismatch() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::NotNull),
            ],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("count", int32(), Nullability::NotNull),
            ],
        );
        // a.id = b.id AND a.name = b.count (name is VARCHAR, count is INT → mismatch)
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                bin_op(
                    col_ref(Some("a"), "id", int32(), Nullability::NotNull),
                    BinOp::Eq,
                    col_ref(Some("b"), "id", int32(), Nullability::NotNull),
                ),
                BinOp::And,
                bin_op(
                    col_ref(Some("a"), "name", varchar(), Nullability::NotNull),
                    BinOp::Eq,
                    col_ref(Some("b"), "count", int32(), Nullability::NotNull),
                ),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A030);
        // Should only be 1 mismatch (the name=count pair)
        assert_eq!(count_diagnostics(&diags, DiagnosticCode::A030), 1);
    }

    #[test]
    fn test_a030_float_vs_decimal_compatible() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("val", float64(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("val", decimal(10, 2), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "val", float64(), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "val", decimal(10, 2), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_no_diagnostic(&diags, DiagnosticCode::A030);
    }

    #[test]
    fn test_a030_varchar_vs_varchar_compatible() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("code", varchar(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("code", varchar(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "code", varchar(), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "code", varchar(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_no_diagnostic(&diags, DiagnosticCode::A030);
    }

    #[test]
    fn test_a030_unknown_type_compatible() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("val", unknown("no type"), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("val", int32(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "val", unknown("no type"), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "val", int32(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_no_diagnostic(&diags, DiagnosticCode::A030);
    }

    // ── A032: Additional cross join tests ────────────────────────────────

    #[test]
    fn test_a032_inner_join_without_on() {
        let left = make_scan("a", vec![make_col("x", int32(), Nullability::NotNull)]);
        let right = make_scan("b", vec![make_col("y", int32(), Nullability::NotNull)]);
        let ir = make_join(left, right, JoinType::Inner, None);
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A032);
    }

    #[test]
    fn test_a032_left_join_without_on() {
        let left = make_scan("a", vec![make_col("x", int32(), Nullability::NotNull)]);
        let right = make_scan("b", vec![make_col("y", int32(), Nullability::NotNull)]);
        let ir = make_join(left, right, JoinType::LeftOuter, None);
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A032);
    }

    #[test]
    fn test_a032_inner_join_with_on_no_diagnostic() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("id", int32(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("id", int32(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "id", int32(), Nullability::NotNull),
                BinOp::Eq,
                col_ref(Some("b"), "id", int32(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_no_diagnostic(&diags, DiagnosticCode::A032);
    }

    // ── A033: Additional non-equi join tests ─────────────────────────────

    #[test]
    fn test_a033_less_than_join() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("start_date", date(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("end_date", date(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "start_date", date(), Nullability::NotNull),
                BinOp::Lt,
                col_ref(Some("b"), "end_date", date(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A033);
    }

    #[test]
    fn test_a033_not_equals_join() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("id", int32(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("id", int32(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "id", int32(), Nullability::NotNull),
                BinOp::NotEq,
                col_ref(Some("b"), "id", int32(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A033);
    }

    #[test]
    fn test_a033_gte_join() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("rank", int32(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![make_col("min_rank", int32(), Nullability::NotNull)],
        );
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                col_ref(Some("a"), "rank", int32(), Nullability::NotNull),
                BinOp::GtEq,
                col_ref(Some("b"), "min_rank", int32(), Nullability::NotNull),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_has_diagnostic(&diags, DiagnosticCode::A033);
    }

    #[test]
    fn test_a033_range_join_two_inequalities() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![make_col("val", int32(), Nullability::NotNull)],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![
                make_col("low", int32(), Nullability::NotNull),
                make_col("high", int32(), Nullability::NotNull),
            ],
        );
        // a.val >= b.low AND a.val <= b.high
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                bin_op(
                    col_ref(Some("a"), "val", int32(), Nullability::NotNull),
                    BinOp::GtEq,
                    col_ref(Some("b"), "low", int32(), Nullability::NotNull),
                ),
                BinOp::And,
                bin_op(
                    col_ref(Some("a"), "val", int32(), Nullability::NotNull),
                    BinOp::LtEq,
                    col_ref(Some("b"), "high", int32(), Nullability::NotNull),
                ),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_eq!(
            count_diagnostics(&diags, DiagnosticCode::A033),
            2,
            "Range join should produce 2 non-equi diagnostics"
        );
    }

    #[test]
    fn test_a033_mixed_equi_and_non_equi() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("rank", int32(), Nullability::NotNull),
            ],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("rank", int32(), Nullability::NotNull),
            ],
        );
        // a.id = b.id AND a.rank > b.rank
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                bin_op(
                    col_ref(Some("a"), "id", int32(), Nullability::NotNull),
                    BinOp::Eq,
                    col_ref(Some("b"), "id", int32(), Nullability::NotNull),
                ),
                BinOp::And,
                bin_op(
                    col_ref(Some("a"), "rank", int32(), Nullability::NotNull),
                    BinOp::Gt,
                    col_ref(Some("b"), "rank", int32(), Nullability::NotNull),
                ),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        // Only the > condition should fire A033
        assert_eq!(count_diagnostics(&diags, DiagnosticCode::A033), 1);
        // The = condition should not fire A030 (same types)
        assert_no_diagnostic(&diags, DiagnosticCode::A030);
    }

    #[test]
    fn test_a033_compound_equi_join_no_diagnostic() {
        let left = make_scan_alias(
            "a",
            "a",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("code", varchar(), Nullability::NotNull),
            ],
        );
        let right = make_scan_alias(
            "b",
            "b",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("code", varchar(), Nullability::NotNull),
            ],
        );
        // a.id = b.id AND a.code = b.code
        let ir = make_join(
            left,
            right,
            JoinType::Inner,
            Some(bin_op(
                bin_op(
                    col_ref(Some("a"), "id", int32(), Nullability::NotNull),
                    BinOp::Eq,
                    col_ref(Some("b"), "id", int32(), Nullability::NotNull),
                ),
                BinOp::And,
                bin_op(
                    col_ref(Some("a"), "code", varchar(), Nullability::NotNull),
                    BinOp::Eq,
                    col_ref(Some("b"), "code", varchar(), Nullability::NotNull),
                ),
            )),
        );
        let diags = JoinKeyAnalysis.run_model("m", &ir, &make_ctx());
        assert_no_diagnostic(&diags, DiagnosticCode::A033);
    }
}
