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
mod tests {
    use super::*;
    use crate::ir::expr::TypedExpr;
    use crate::ir::relop::RelOp;
    use crate::ir::schema::RelSchema;
    use crate::ir::types::{IntBitWidth, Nullability, SqlType};
    use crate::test_utils::*;

    #[test]
    fn test_a021_select_star_warning() {
        // Model that uses SELECT *
        let ir = RelOp::Project {
            input: Box::new(RelOp::Scan {
                table_name: "source".to_string(),
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
            }),
            columns: vec![("*".to_string(), TypedExpr::Wildcard { table: None })],
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

        // stg has a dependent so it's not terminal
        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), ir);
        // fct model just reads from stg
        models.insert(
            "fct".to_string(),
            RelOp::Scan {
                table_name: "stg".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            },
        );

        let ctx = make_ctx_with_dag(&dep_map);
        let pass = UnusedColumnDetection;
        let diags = pass.run_project(&models, &ctx);

        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagnosticCode::A021 && d.model == "stg"),
            "Expected A021 for model using SELECT *"
        );
    }

    #[test]
    fn test_terminal_model_skipped() {
        // Terminal model (no dependents) should not produce diagnostics
        let ir = RelOp::Project {
            input: Box::new(RelOp::Scan {
                table_name: "source".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            }),
            columns: vec![(
                "id".to_string(),
                TypedExpr::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    nullability: Nullability::NotNull,
                },
            )],
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            )]),
        };

        let mut dep_map = HashMap::new();
        dep_map.insert("terminal".to_string(), vec![]);

        let mut models = HashMap::new();
        models.insert("terminal".to_string(), ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        assert!(
            diags.is_empty(),
            "Terminal model should not produce unused column diagnostics"
        );
    }

    #[test]
    fn test_a020_unused_column_detected() {
        // stg produces id, name, internal_code
        // fct only references id and name
        let stg_ir = RelOp::Project {
            input: Box::new(RelOp::Scan {
                table_name: "raw".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            }),
            columns: vec![
                (
                    "id".to_string(),
                    TypedExpr::ColumnRef {
                        table: None,
                        column: "id".to_string(),
                        resolved_type: SqlType::Integer {
                            bits: IntBitWidth::I32,
                        },
                        nullability: Nullability::NotNull,
                    },
                ),
                (
                    "name".to_string(),
                    TypedExpr::ColumnRef {
                        table: None,
                        column: "name".to_string(),
                        resolved_type: SqlType::String { max_length: None },
                        nullability: Nullability::Nullable,
                    },
                ),
                (
                    "internal_code".to_string(),
                    TypedExpr::ColumnRef {
                        table: None,
                        column: "internal_code".to_string(),
                        resolved_type: SqlType::String { max_length: None },
                        nullability: Nullability::Nullable,
                    },
                ),
            ],
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
                make_col(
                    "internal_code",
                    SqlType::String { max_length: None },
                    Nullability::Nullable,
                ),
            ]),
        };

        let fct_ir = RelOp::Project {
            input: Box::new(RelOp::Scan {
                table_name: "stg".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            }),
            columns: vec![
                (
                    "id".to_string(),
                    TypedExpr::ColumnRef {
                        table: None,
                        column: "id".to_string(),
                        resolved_type: SqlType::Integer {
                            bits: IntBitWidth::I32,
                        },
                        nullability: Nullability::NotNull,
                    },
                ),
                (
                    "name".to_string(),
                    TypedExpr::ColumnRef {
                        table: None,
                        column: "name".to_string(),
                        resolved_type: SqlType::String { max_length: None },
                        nullability: Nullability::Nullable,
                    },
                ),
            ],
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

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), stg_ir);
        models.insert("fct".to_string(), fct_ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        let a020s: Vec<_> = diags
            .iter()
            .filter(|d| d.code == DiagnosticCode::A020 && d.model == "stg")
            .collect();
        assert!(
            a020s
                .iter()
                .any(|d| d.column.as_deref() == Some("internal_code")),
            "Expected A020 for 'internal_code' which is produced by stg but not consumed by fct"
        );
    }

    // ── A020: Additional unused column tests ────────────────────────────

    #[test]
    fn test_a020_multiple_unused_columns() {
        // stg produces id, name, code, internal
        // fct only references id
        let stg_ir = make_project(
            make_scan("raw", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "name".to_string(),
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                ),
                (
                    "code".to_string(),
                    col_ref(None, "code", varchar(), Nullability::Nullable),
                ),
                (
                    "internal".to_string(),
                    col_ref(None, "internal", varchar(), Nullability::Nullable),
                ),
            ],
        );

        let fct_ir = make_project(
            make_scan("stg", vec![]),
            vec![(
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            )],
        );

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), stg_ir);
        models.insert("fct".to_string(), fct_ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        let a020s: Vec<_> = diags
            .iter()
            .filter(|d| d.code == DiagnosticCode::A020 && d.model == "stg")
            .collect();
        assert!(
            a020s.len() >= 3,
            "Expected at least 3 unused columns, got {}",
            a020s.len()
        );
    }

    #[test]
    fn test_a020_all_columns_consumed_no_diagnostic() {
        // stg produces id, name
        // fct references both id and name
        let stg_ir = make_project(
            make_scan("raw", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "name".to_string(),
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                ),
            ],
        );

        let fct_ir = make_project(
            make_scan("stg", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "name".to_string(),
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                ),
            ],
        );

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), stg_ir);
        models.insert("fct".to_string(), fct_ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        assert_no_diagnostic(&diags, DiagnosticCode::A020);
    }

    #[test]
    fn test_a020_column_used_in_where_not_unused() {
        // stg produces id, status
        // fct references id in SELECT and status in WHERE
        let stg_ir = make_project(
            make_scan("raw", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "status".to_string(),
                    col_ref(None, "status", varchar(), Nullability::Nullable),
                ),
            ],
        );

        let fct_ir = make_filter(
            make_project(
                make_scan("stg", vec![]),
                vec![(
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                )],
            ),
            col_ref(None, "status", varchar(), Nullability::Nullable),
        );

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), stg_ir);
        models.insert("fct".to_string(), fct_ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        // status is consumed in the WHERE clause, so it should not be flagged
        let status_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.code == DiagnosticCode::A020 && d.column.as_deref() == Some("status"))
            .collect();
        assert!(
            status_diags.is_empty(),
            "Column 'status' used in WHERE should not be flagged as unused"
        );
    }

    #[test]
    fn test_a020_diamond_dag_all_consumed() {
        // stg produces id, name, amount
        // dim references id, name
        // fct references id, amount
        // All columns are consumed across the two downstreams
        let stg_ir = make_project(
            make_scan("raw", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "name".to_string(),
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                ),
                (
                    "amount".to_string(),
                    col_ref(None, "amount", float64(), Nullability::Nullable),
                ),
            ],
        );

        let dim_ir = make_project(
            make_scan("stg", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "name".to_string(),
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                ),
            ],
        );

        let fct_ir = make_project(
            make_scan("stg", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "amount".to_string(),
                    col_ref(None, "amount", float64(), Nullability::Nullable),
                ),
            ],
        );

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("dim".to_string(), vec!["stg".to_string()]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), stg_ir);
        models.insert("dim".to_string(), dim_ir);
        models.insert("fct".to_string(), fct_ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        // All stg columns are consumed by at least one downstream
        let stg_a020s: Vec<_> = diags
            .iter()
            .filter(|d| d.code == DiagnosticCode::A020 && d.model == "stg")
            .collect();
        assert!(
            stg_a020s.is_empty(),
            "All stg columns are consumed in diamond DAG, got: {:?}",
            stg_a020s.iter().map(|d| &d.column).collect::<Vec<_>>()
        );
    }

    // ── A021: Additional SELECT * tests ─────────────────────────────────

    #[test]
    fn test_a021_select_t_star_in_non_terminal() {
        // Model uses SELECT t.* (qualified wildcard) — should also trigger A021
        let ir = RelOp::Project {
            input: Box::new(make_scan(
                "source",
                vec![
                    make_col("id", int32(), Nullability::NotNull),
                    make_col("name", varchar(), Nullability::Nullable),
                ],
            )),
            columns: vec![(
                "t.*".to_string(),
                TypedExpr::Wildcard {
                    table: Some("t".to_string()),
                },
            )],
            schema: RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ]),
        };

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), ir);
        models.insert("fct".to_string(), make_scan("stg", vec![]));

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        assert_has_diagnostic(&diags, DiagnosticCode::A021);
    }

    #[test]
    fn test_a021_explicit_columns_no_diagnostic() {
        // Model uses explicit column list — A021 should NOT fire
        let ir = make_project(
            make_scan("source", vec![]),
            vec![
                (
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                ),
                (
                    "name".to_string(),
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                ),
            ],
        );

        let mut dep_map = HashMap::new();
        dep_map.insert("stg".to_string(), vec![]);
        dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

        let mut models = HashMap::new();
        models.insert("stg".to_string(), ir);
        models.insert(
            "fct".to_string(),
            make_project(
                make_scan("stg", vec![]),
                vec![(
                    "id".to_string(),
                    col_ref(None, "id", int32(), Nullability::NotNull),
                )],
            ),
        );

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        assert_no_diagnostic(&diags, DiagnosticCode::A021);
    }

    #[test]
    fn test_a021_select_star_in_terminal_no_diagnostic() {
        // Terminal model (no dependents) with SELECT * — should NOT trigger A021
        let ir = RelOp::Project {
            input: Box::new(make_scan(
                "source",
                vec![make_col("id", int32(), Nullability::NotNull)],
            )),
            columns: vec![("*".to_string(), TypedExpr::Wildcard { table: None })],
            schema: RelSchema::new(vec![make_col("id", int32(), Nullability::NotNull)]),
        };

        let mut dep_map = HashMap::new();
        dep_map.insert("terminal".to_string(), vec![]);

        let mut models = HashMap::new();
        models.insert("terminal".to_string(), ir);

        let ctx = make_ctx_with_dag(&dep_map);
        let diags = UnusedColumnDetection.run_project(&models, &ctx);

        assert!(
            diags.is_empty(),
            "Terminal model with SELECT * should produce no diagnostics"
        );
    }
}
