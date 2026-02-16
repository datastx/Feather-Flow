//! Shared expression-tree utilities for analysis passes.

use std::collections::HashSet;

use datafusion_expr::Expr;

/// Recursively extract all column name references from a DataFusion expression tree.
///
/// Walks through aliases, binary expressions, scalar/aggregate functions,
/// casts, CASE expressions, IS NULL/NOT, BETWEEN, LIKE, and IN lists.
pub(crate) fn collect_column_refs(expr: &Expr, cols: &mut HashSet<String>) {
    match expr {
        Expr::Column(col) => {
            cols.insert(col.name.clone());
        }
        Expr::Alias(alias) => collect_column_refs(&alias.expr, cols),
        Expr::BinaryExpr(bin) => {
            collect_column_refs(&bin.left, cols);
            collect_column_refs(&bin.right, cols);
        }
        Expr::ScalarFunction(func) => {
            for arg in &func.args {
                collect_column_refs(arg, cols);
            }
        }
        Expr::AggregateFunction(func) => {
            for arg in &func.params.args {
                collect_column_refs(arg, cols);
            }
        }
        Expr::Cast(cast) => collect_column_refs(&cast.expr, cols),
        Expr::TryCast(try_cast) => collect_column_refs(&try_cast.expr, cols),
        Expr::Case(case) => {
            if let Some(ref operand) = case.expr {
                collect_column_refs(operand, cols);
            }
            for (when, then) in &case.when_then_expr {
                collect_column_refs(when, cols);
                collect_column_refs(then, cols);
            }
            if let Some(ref else_expr) = case.else_expr {
                collect_column_refs(else_expr, cols);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Not(inner) | Expr::Negative(inner) => {
            collect_column_refs(inner, cols);
        }
        Expr::Between(between) => {
            collect_column_refs(&between.expr, cols);
            collect_column_refs(&between.low, cols);
            collect_column_refs(&between.high, cols);
        }
        Expr::Like(like) => {
            collect_column_refs(&like.expr, cols);
            collect_column_refs(&like.pattern, cols);
        }
        Expr::InList(in_list) => {
            collect_column_refs(&in_list.expr, cols);
            for item in &in_list.list {
                collect_column_refs(item, cols);
            }
        }
        _ => {}
    }
}

/// Get a human-readable display name for a DataFusion expression.
///
/// For columns, includes the optional table qualifier (`table.column`).
/// For aliases, returns the alias name. Falls back to `schema_name()`.
pub(crate) fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => {
            if let Some(ref relation) = col.relation {
                format!("{}.{}", relation, col.name)
            } else {
                col.name.clone()
            }
        }
        Expr::Alias(alias) => alias.name.clone(),
        _ => expr.schema_name().to_string(),
    }
}
