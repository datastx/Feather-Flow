//! Query-level lowering: Query → Sort → Limit → body

use crate::error::{AnalysisError, AnalysisResult};
use crate::ir::relop::{RelOp, SetOpKind, SortKey};
use crate::ir::schema::RelSchema;
use crate::lowering::expr::lower_expr;
use crate::lowering::select::lower_select;
use crate::lowering::SchemaCatalog;
use sqlparser::ast::{
    Expr, LimitClause, OrderByExpr, OrderByKind, Query, SetExpr, SetOperator, SetQuantifier, Value,
};

/// Lower a Query AST node into a RelOp
pub(crate) fn lower_query(query: &Query, catalog: &SchemaCatalog) -> AnalysisResult<RelOp> {
    // Lower the body (SELECT, UNION, etc.)
    let mut plan = lower_set_expr(&query.body, catalog)?;

    // Wrap with Sort if ORDER BY is present
    if let Some(ref order_by) = query.order_by {
        if let OrderByKind::Expressions(ref exprs) = order_by.kind {
            if !exprs.is_empty() {
                let schema = plan.schema().clone();
                let sort_keys = lower_order_by(exprs, &schema);
                plan = RelOp::Sort {
                    schema: schema.clone(),
                    input: Box::new(plan),
                    order_by: sort_keys,
                };
            }
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present
    if let Some(ref limit_clause) = query.limit_clause {
        let (limit_expr, offset_expr) = match limit_clause {
            LimitClause::LimitOffset { limit, offset, .. } => {
                (limit.as_ref(), offset.as_ref().map(|o| &o.value))
            }
            LimitClause::OffsetCommaLimit { offset, limit } => (Some(limit), Some(offset)),
        };

        let limit_val = limit_expr.and_then(expr_to_u64);
        let offset_val = offset_expr.and_then(expr_to_u64);

        if limit_val.is_some() || offset_val.is_some() {
            let schema = plan.schema().clone();
            plan = RelOp::Limit {
                schema,
                input: Box::new(plan),
                limit: limit_val,
                offset: offset_val,
            };
        }
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
                (op, _) => {
                    return Err(AnalysisError::UnsupportedConstruct {
                        model: String::new(),
                        construct: format!("unsupported set operator: {:?}", op),
                    })
                }
            };
            Ok(RelOp::SetOp {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                op: op_kind,
                schema,
            })
        }
        SetExpr::Query(q) => lower_query(q, catalog),
        other => {
            // VALUES, TABLE, etc.
            Err(AnalysisError::UnsupportedConstruct {
                model: String::new(),
                construct: format!("unsupported SetExpr: {:?}", std::mem::discriminant(other)),
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
            let ascending = obe.options.asc.unwrap_or(true);
            let nulls_first = obe.options.nulls_first.unwrap_or(!ascending);
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
        Expr::Value(val_with_span) => match &val_with_span.value {
            Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}
