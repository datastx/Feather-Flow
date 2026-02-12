//! NullabilityPropagation pass — tracks how joins affect nullability (A010-A019)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::{JoinType, RelOp};
use crate::ir::types::Nullability;
use crate::pass::{AnalysisPass, Diagnostic, DiagnosticCode, Severity};
use std::collections::HashSet;

/// Nullability propagation analysis pass
pub(crate) struct NullabilityPropagation;

impl AnalysisPass for NullabilityPropagation {
    fn name(&self) -> &'static str {
        "nullability"
    }

    fn description(&self) -> &'static str {
        "Detects nullable columns from JOINs used without null guards"
    }

    fn run_model(&self, model_name: &str, ir: &RelOp, ctx: &AnalysisContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Collect columns that become nullable due to JOINs
        let mut nullable_from_join: HashSet<String> = HashSet::new();
        collect_join_nullable_columns(ir, &mut nullable_from_join);

        // Collect columns that have null guards (COALESCE, IS NULL checks)
        let mut guarded_columns: HashSet<String> = HashSet::new();
        collect_null_guarded_columns(ir, &mut guarded_columns);

        // A010: Nullable column from JOIN used without null guard
        for qualified_name in &nullable_from_join {
            // Extract the bare column name for display and guard lookup
            let col_name = qualified_name
                .rsplit_once('.')
                .map(|(_, col)| col)
                .unwrap_or(qualified_name);
            // Check both qualified and unqualified guard sets
            if !guarded_columns.contains(qualified_name) && !guarded_columns.contains(col_name) {
                diagnostics.push(Diagnostic {
                    code: DiagnosticCode::A010,
                    severity: Severity::Warning,
                    message: format!(
                        "Column '{}' is nullable after JOIN but used without a null guard (e.g., COALESCE)",
                        col_name
                    ),
                    model: model_name.to_string(),
                    column: Some(col_name.to_string()),
                    hint: Some("Wrap with COALESCE() or add an IS NOT NULL filter".to_string()),
                    pass_name: "nullability".into(),
                });
            }
        }

        // A011: Column declared NOT NULL in YAML but nullable after JOIN
        if let Some(yaml_schema) = ctx.model_schema(model_name) {
            for col in &yaml_schema.columns {
                // Check if this column name appears in the nullable set
                // (either as a bare name or as a suffix of a qualified key)
                let is_nullable = nullable_from_join.contains(&col.name)
                    || nullable_from_join
                        .iter()
                        .any(|k| k.rsplit_once('.').map(|(_, c)| c) == Some(&col.name));
                if col.nullability == Nullability::NotNull && is_nullable {
                    diagnostics.push(Diagnostic {
                        code: DiagnosticCode::A011,
                        severity: Severity::Warning,
                        message: format!(
                            "Column '{}' is declared NOT NULL in YAML but becomes nullable after JOIN",
                            col.name
                        ),
                        model: model_name.to_string(),
                        column: Some(col.name.clone()),
                        hint: Some("Add a COALESCE or filter to ensure NOT NULL".to_string()),
                        pass_name: "nullability".into(),
                    });
                }
            }
        }

        // A012: IS NULL check on always-NotNull column
        check_redundant_null_checks(model_name, ir, &mut diagnostics);

        diagnostics
    }
}

/// Build a qualified key for a column: "table.column" if source_table is set, else just "column"
fn qualified_key(col: &crate::ir::types::TypedColumn) -> String {
    match &col.source_table {
        Some(table) => format!("{}.{}", table, col.name),
        None => col.name.clone(),
    }
}

/// Collect column keys that become nullable due to outer joins.
///
/// Keys are qualified as "table.column" when `source_table` is available,
/// preventing collisions when two tables share a column name.
fn collect_join_nullable_columns(op: &RelOp, nullable: &mut HashSet<String>) {
    match op {
        RelOp::Join {
            left,
            right,
            join_type,
            ..
        } => {
            collect_join_nullable_columns(left, nullable);
            collect_join_nullable_columns(right, nullable);

            match join_type {
                JoinType::LeftOuter => {
                    // Right side becomes nullable
                    for col in &right.schema().columns {
                        nullable.insert(qualified_key(col));
                    }
                }
                JoinType::RightOuter => {
                    // Left side becomes nullable
                    for col in &left.schema().columns {
                        nullable.insert(qualified_key(col));
                    }
                }
                JoinType::FullOuter => {
                    // Both sides become nullable
                    for col in &left.schema().columns {
                        nullable.insert(qualified_key(col));
                    }
                    for col in &right.schema().columns {
                        nullable.insert(qualified_key(col));
                    }
                }
                _ => {}
            }
        }
        RelOp::Project { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Filter { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Aggregate { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Sort { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Limit { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::SetOp { left, right, .. } => {
            collect_join_nullable_columns(left, nullable);
            collect_join_nullable_columns(right, nullable);
        }
        RelOp::Scan { .. } => {}
    }
}

/// Collect columns that have null guards applied (COALESCE, IS NOT NULL filter)
fn collect_null_guarded_columns(op: &RelOp, guarded: &mut HashSet<String>) {
    match op {
        RelOp::Project { input, columns, .. } => {
            collect_null_guarded_columns(input, guarded);
            for (_name, expr) in columns {
                collect_coalesce_columns(expr, guarded);
            }
        }
        RelOp::Filter {
            input, predicate, ..
        } => {
            collect_null_guarded_columns(input, guarded);
            // IS NOT NULL in a WHERE clause guards the column
            collect_is_not_null_columns(predicate, guarded);
        }
        RelOp::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_null_guarded_columns(left, guarded);
            collect_null_guarded_columns(right, guarded);
            if let Some(cond) = condition {
                collect_is_not_null_columns(cond, guarded);
            }
        }
        RelOp::Aggregate { input, .. } => collect_null_guarded_columns(input, guarded),
        RelOp::Sort { input, .. } => collect_null_guarded_columns(input, guarded),
        RelOp::Limit { input, .. } => collect_null_guarded_columns(input, guarded),
        RelOp::SetOp { left, right, .. } => {
            collect_null_guarded_columns(left, guarded);
            collect_null_guarded_columns(right, guarded);
        }
        RelOp::Scan { .. } => {}
    }
}

/// Collect column names wrapped in COALESCE
fn collect_coalesce_columns(expr: &TypedExpr, guarded: &mut HashSet<String>) {
    match expr {
        TypedExpr::FunctionCall { name, args, .. } if name == "COALESCE" => {
            for arg in args {
                if let TypedExpr::ColumnRef { table, column, .. } = arg {
                    guarded.insert(column.clone());
                    if let Some(t) = table {
                        guarded.insert(format!("{}.{}", t, column));
                    }
                }
            }
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            collect_coalesce_columns(left, guarded);
            collect_coalesce_columns(right, guarded);
        }
        TypedExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_coalesce_columns(arg, guarded);
            }
        }
        TypedExpr::Case {
            results,
            else_result,
            ..
        } => {
            for r in results {
                collect_coalesce_columns(r, guarded);
            }
            if let Some(e) = else_result {
                collect_coalesce_columns(e, guarded);
            }
        }
        _ => {}
    }
}

/// Collect columns from IS NOT NULL expressions
fn collect_is_not_null_columns(expr: &TypedExpr, guarded: &mut HashSet<String>) {
    match expr {
        TypedExpr::IsNull {
            expr: inner,
            negated: true,
        } => {
            if let TypedExpr::ColumnRef { column, .. } = inner.as_ref() {
                guarded.insert(column.clone());
            }
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            collect_is_not_null_columns(left, guarded);
            collect_is_not_null_columns(right, guarded);
        }
        _ => {}
    }
}

/// Check for redundant IS NULL/IS NOT NULL on always-NotNull columns
fn check_redundant_null_checks(model: &str, op: &RelOp, diags: &mut Vec<Diagnostic>) {
    match op {
        RelOp::Filter {
            input, predicate, ..
        } => {
            check_redundant_null_checks(model, input, diags);
            check_is_null_on_not_null(model, predicate, input.schema(), diags);
        }
        RelOp::Project { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::Join { left, right, .. } => {
            check_redundant_null_checks(model, left, diags);
            check_redundant_null_checks(model, right, diags);
        }
        RelOp::Aggregate { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::Sort { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::Limit { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::SetOp { left, right, .. } => {
            check_redundant_null_checks(model, left, diags);
            check_redundant_null_checks(model, right, diags);
        }
        RelOp::Scan { .. } => {}
    }
}

/// Check if IS NULL is used on an always-NotNull column
fn check_is_null_on_not_null(
    model: &str,
    expr: &TypedExpr,
    schema: &crate::ir::schema::RelSchema,
    diags: &mut Vec<Diagnostic>,
) {
    match expr {
        TypedExpr::IsNull {
            expr: inner,
            negated,
        } => {
            let TypedExpr::ColumnRef { column, .. } = inner.as_ref() else {
                return;
            };
            let Some(col) = schema.find_column(column) else {
                return;
            };
            if col.nullability != Nullability::NotNull {
                return;
            }
            let check_type = if *negated { "IS NOT NULL" } else { "IS NULL" };
            diags.push(Diagnostic {
                code: DiagnosticCode::A012,
                severity: Severity::Info,
                message: format!(
                    "{} check on column '{}' which is always NOT NULL",
                    check_type, column
                ),
                model: model.to_string(),
                column: Some(column.clone()),
                hint: Some("This check is redundant and can be removed".to_string()),
                pass_name: "nullability".into(),
            });
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            check_is_null_on_not_null(model, left, schema, diags);
            check_is_null_on_not_null(model, right, schema, diags);
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "nullability_test.rs"]
mod tests;
