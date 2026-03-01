//! Shared expression-tree utilities for analysis passes.

use std::collections::HashSet;

use datafusion_expr::Expr;

/// Walk a DataFusion expression tree, invoking `collector` for every
/// [`datafusion_common::Column`] encountered.
///
/// Recurses through aliases, binary expressions, scalar/aggregate functions,
/// casts, CASE expressions, IS NULL/NOT, BETWEEN, LIKE, and IN lists.
pub(crate) fn walk_expr_columns<F>(expr: &Expr, collector: &mut F)
where
    F: FnMut(&datafusion_common::Column),
{
    match expr {
        Expr::Column(col) => {
            collector(col);
        }
        Expr::Alias(alias) => walk_expr_columns(&alias.expr, collector),
        Expr::BinaryExpr(bin) => {
            walk_expr_columns(&bin.left, collector);
            walk_expr_columns(&bin.right, collector);
        }
        Expr::ScalarFunction(func) => {
            for arg in &func.args {
                walk_expr_columns(arg, collector);
            }
        }
        Expr::AggregateFunction(func) => {
            for arg in &func.params.args {
                walk_expr_columns(arg, collector);
            }
        }
        Expr::Cast(cast) => walk_expr_columns(&cast.expr, collector),
        Expr::TryCast(try_cast) => walk_expr_columns(&try_cast.expr, collector),
        Expr::Case(case) => {
            for_each_case_subexpr(case, |e| walk_expr_columns(e, collector));
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Not(inner) | Expr::Negative(inner) => {
            walk_expr_columns(inner, collector);
        }
        Expr::Between(between) => {
            walk_expr_columns(&between.expr, collector);
            walk_expr_columns(&between.low, collector);
            walk_expr_columns(&between.high, collector);
        }
        Expr::Like(like) => {
            walk_expr_columns(&like.expr, collector);
            walk_expr_columns(&like.pattern, collector);
        }
        Expr::InList(in_list) => {
            walk_expr_columns(&in_list.expr, collector);
            for item in &in_list.list {
                walk_expr_columns(item, collector);
            }
        }
        Expr::WindowFunction(wf) => {
            for arg in &wf.params.args {
                walk_expr_columns(arg, collector);
            }
            for expr in &wf.params.partition_by {
                walk_expr_columns(expr, collector);
            }
            for sort in &wf.params.order_by {
                walk_expr_columns(&sort.expr, collector);
            }
        }
        Expr::InSubquery(in_sub) => {
            walk_expr_columns(&in_sub.expr, collector);
            // Don't cross into the subquery plan boundary
        }
        Expr::Exists(_) => {}
        Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => {
            walk_expr_columns(inner, collector);
        }
        _ => {}
    }
}

/// Recursively extract all column name references from a DataFusion expression tree.
pub(crate) fn collect_column_refs(expr: &Expr, cols: &mut HashSet<String>) {
    walk_expr_columns(expr, &mut |col| {
        cols.insert(col.name.clone());
    });
}

/// Iterate over every sub-expression inside a [`datafusion_expr::expr::Case`].
///
/// Calls `f` on the optional operand, every WHEN and THEN expression, and
/// the optional ELSE expression — in that order.
pub(crate) fn for_each_case_subexpr(case: &datafusion_expr::expr::Case, mut f: impl FnMut(&Expr)) {
    if let Some(ref operand) = case.expr {
        f(operand);
    }
    for (when, then) in &case.when_then_expr {
        f(when);
        f(then);
    }
    if let Some(ref else_expr) = case.else_expr {
        f(else_expr);
    }
}

/// Iterate over the *output-producing* sub-expressions of a CASE: each THEN
/// branch and the optional ELSE. Skips the operand and WHEN conditions, which
/// don't contribute to the output value.
pub(crate) fn for_each_case_output_expr(
    case: &datafusion_expr::expr::Case,
    mut f: impl FnMut(&Expr),
) {
    for (_when, then) in &case.when_then_expr {
        f(then);
    }
    if let Some(ref else_expr) = case.else_expr {
        f(else_expr);
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
