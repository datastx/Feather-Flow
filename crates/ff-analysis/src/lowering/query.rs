//! Query-level lowering: Query → Sort → Limit → body

use crate::error::AnalysisResult;
use crate::ir::relop::{RelOp, SetOpKind, SortKey};
use crate::ir::schema::RelSchema;
use crate::lowering::expr::lower_expr;
use crate::lowering::select::lower_select;
use crate::lowering::SchemaCatalog;
use sqlparser::ast::{Expr, OrderByExpr, Query, SetExpr, SetOperator, SetQuantifier, Value};

/// Lower a Query AST node into a RelOp
pub(crate) fn lower_query(query: &Query, catalog: &SchemaCatalog) -> AnalysisResult<RelOp> {
    // Lower the body (SELECT, UNION, etc.)
    let mut plan = lower_set_expr(&query.body, catalog)?;

    // Wrap with Sort if ORDER BY is present
    if let Some(ref order_by) = query.order_by {
        if !order_by.exprs.is_empty() {
            let schema = plan.schema().clone();
            let sort_keys = lower_order_by(&order_by.exprs, &schema);
            plan = RelOp::Sort {
                schema: schema.clone(),
                input: Box::new(plan),
                order_by: sort_keys,
            };
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present
    let has_limit = query.limit.is_some();
    let has_offset = query.offset.is_some();
    if has_limit || has_offset {
        let schema = plan.schema().clone();
        let limit_val = query.limit.as_ref().and_then(expr_to_u64);
        let offset_val = query.offset.as_ref().and_then(|o| expr_to_u64(&o.value));
        plan = RelOp::Limit {
            schema,
            input: Box::new(plan),
            limit: limit_val,
            offset: offset_val,
        };
    }

    Ok(plan)
}

/// Lower a SetExpr (SELECT, UNION, INTERSECT, EXCEPT)
fn lower_set_expr(set_expr: &SetExpr, catalog: &SchemaCatalog) -> AnalysisResult<RelOp> {
    match set_expr {
        SetExpr::Select(select) => lower_select(select, catalog),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left_plan = lower_set_expr(left, catalog)?;
            let right_plan = lower_set_expr(right, catalog)?;
            let schema = left_plan.schema().clone();
            let op_kind = match (op, set_quantifier) {
                (SetOperator::Union, SetQuantifier::All) => SetOpKind::UnionAll,
                (SetOperator::Union, _) => SetOpKind::Union,
                (SetOperator::Intersect, _) => SetOpKind::Intersect,
                (SetOperator::Except, _) => SetOpKind::Except,
            };
            Ok(RelOp::SetOp {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                op: op_kind,
                schema,
            })
        }
        SetExpr::Query(q) => lower_query(q, catalog),
        _ => {
            // VALUES, TABLE, etc.
            Ok(RelOp::Scan {
                table_name: "<unsupported>".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            })
        }
    }
}

/// Lower ORDER BY expressions into SortKeys
fn lower_order_by(exprs: &[OrderByExpr], schema: &RelSchema) -> Vec<SortKey> {
    exprs
        .iter()
        .map(|obe| {
            let expr = lower_expr(&obe.expr, schema);
            let ascending = obe.asc.unwrap_or(true);
            let nulls_first = obe.nulls_first.unwrap_or(!ascending);
            SortKey {
                expr,
                ascending,
                nulls_first,
            }
        })
        .collect()
}

/// Try to extract a u64 from a literal expression (for LIMIT/OFFSET)
fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    }
}
