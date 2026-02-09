//! UnusedColumnDetection DagPass — finds columns produced but never consumed downstream (A020-A029)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::RelOp;
use crate::pass::{DagPass, Diagnostic, DiagnosticCode, Severity};
use std::collections::{HashMap, HashSet};

/// Unused column detection pass (DAG-level)
pub struct UnusedColumnDetection;

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

        // For each model, determine which of its output columns are consumed downstream
        for (model_name, ir) in models {
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
                    pass_name: "unused_columns".to_string(),
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
                        pass_name: "unused_columns".to_string(),
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
    use crate::ir::types::{Nullability, SqlType, TypedColumn};
    use ff_core::dag::ModelDag;
    use ff_core::Project;
    use ff_sql::ProjectLineage;
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

    fn make_ctx_with_dag(dep_map: &HashMap<String, Vec<String>>) -> AnalysisContext {
        let project = Project::load(Path::new("../../tests/fixtures/sample_project")).unwrap();
        let dag = ModelDag::build(dep_map).unwrap();
        AnalysisContext::new(project, dag, HashMap::new(), ProjectLineage::new())
    }

    #[test]
    fn test_a021_select_star_warning() {
        // Model that uses SELECT *
        let ir = RelOp::Project {
            input: Box::new(RelOp::Scan {
                table_name: "source".to_string(),
                alias: None,
                schema: RelSchema::new(vec![
                    make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                    make_col(
                        "name",
                        SqlType::String { max_length: None },
                        Nullability::Nullable,
                    ),
                ]),
            }),
            columns: vec![("*".to_string(), TypedExpr::Wildcard { table: None })],
            schema: RelSchema::new(vec![
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
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
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                },
            )],
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer { bits: 32 },
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
                        resolved_type: SqlType::Integer { bits: 32 },
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
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
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
                        resolved_type: SqlType::Integer { bits: 32 },
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
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
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
}
