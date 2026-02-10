//! Join lowering with nullability semantics

use crate::error::AnalysisResult;
use crate::ir::relop::{JoinType, RelOp};
use crate::ir::schema::RelSchema;
use crate::ir::types::Nullability;
use crate::lowering::expr::lower_expr;
use crate::lowering::SchemaCatalog;
use sqlparser::ast::{Join, JoinConstraint, JoinOperator};

/// Lower a JOIN clause, updating nullability based on join type
pub(crate) fn lower_join(
    left: RelOp,
    join: &Join,
    catalog: &SchemaCatalog,
) -> AnalysisResult<RelOp> {
    let right = crate::lowering::select::lower_table_factor(&join.relation, catalog)?;

    let left_label = left.table_label().map(str::to_owned);
    let right_label = right.table_label().map(str::to_owned);

    let (join_type, condition) = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => (
            JoinType::Inner,
            extract_join_condition(constraint, left_label.as_deref(), right_label.as_deref()),
        ),
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => (
            JoinType::LeftOuter,
            extract_join_condition(constraint, left_label.as_deref(), right_label.as_deref()),
        ),
        JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => (
            JoinType::RightOuter,
            extract_join_condition(constraint, left_label.as_deref(), right_label.as_deref()),
        ),
        JoinOperator::FullOuter(constraint) => (
            JoinType::FullOuter,
            extract_join_condition(constraint, left_label.as_deref(), right_label.as_deref()),
        ),
        JoinOperator::CrossJoin(_) => (JoinType::Cross, None),
        other => {
            log::warn!(
                "Unrecognized join operator {:?}, treating as INNER JOIN",
                other
            );
            (JoinType::Inner, None)
        }
    };

    // Compute output schema with nullability adjustments
    let left_schema = match join_type {
        JoinType::RightOuter | JoinType::FullOuter => {
            left.schema().with_nullability(Nullability::Nullable)
        }
        _ => left.schema().clone(),
    };

    let right_schema = match join_type {
        JoinType::LeftOuter | JoinType::FullOuter => {
            right.schema().with_nullability(Nullability::Nullable)
        }
        _ => right.schema().clone(),
    };

    let output_schema = RelSchema::merge(&left_schema, &right_schema);

    // Lower the join condition expression
    let condition_expr = condition.map(|expr| lower_expr(&expr, &output_schema));

    Ok(RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition: condition_expr,
        schema: output_schema,
    })
}

/// Extract the join condition expression from a JoinConstraint.
///
/// For `USING(col1, col2)`, synthesises the equivalent
/// `left_table.col1 = right_table.col1 AND left_table.col2 = right_table.col2`
/// so downstream passes see properly qualified condition expressions.
/// If table labels are unavailable (e.g. compound sub-plans), falls back
/// to unqualified column references.
fn extract_join_condition(
    constraint: &JoinConstraint,
    left_table: Option<&str>,
    right_table: Option<&str>,
) -> Option<sqlparser::ast::Expr> {
    match constraint {
        JoinConstraint::On(expr) => Some(expr.clone()),
        JoinConstraint::Using(columns) => {
            if columns.is_empty() {
                return None;
            }
            let mut iter = columns.iter();
            let first = iter.next().unwrap();
            let mut condition = build_eq_for_object_name(first, left_table, right_table);
            for col in iter {
                condition = sqlparser::ast::Expr::BinaryOp {
                    left: Box::new(condition),
                    op: sqlparser::ast::BinaryOperator::And,
                    right: Box::new(build_eq_for_object_name(col, left_table, right_table)),
                };
            }
            Some(condition)
        }
        JoinConstraint::Natural => None,
        JoinConstraint::None => None,
    }
}

/// Build a qualified `left_table.col = right_table.col` equality expression
/// from an ObjectName (for USING expansion).
///
/// When a table qualifier is `None` (compound sub-plans with no single source),
/// the column reference is left unqualified on that side.
fn build_eq_for_object_name(
    name: &sqlparser::ast::ObjectName,
    left_table: Option<&str>,
    right_table: Option<&str>,
) -> sqlparser::ast::Expr {
    let idents: Vec<sqlparser::ast::Ident> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident().cloned())
        .collect();

    let left_expr = qualify_column(&idents, left_table);
    let right_expr = qualify_column(&idents, right_table);

    sqlparser::ast::Expr::BinaryOp {
        left: Box::new(left_expr),
        op: sqlparser::ast::BinaryOperator::Eq,
        right: Box::new(right_expr),
    }
}

/// Build a column expression, optionally qualified with a table name.
fn qualify_column(idents: &[sqlparser::ast::Ident], table: Option<&str>) -> sqlparser::ast::Expr {
    match table {
        Some(tbl) => {
            let mut qualified = vec![sqlparser::ast::Ident::new(tbl)];
            qualified.extend(idents.iter().cloned());
            sqlparser::ast::Expr::CompoundIdentifier(qualified)
        }
        None => {
            if idents.len() == 1 {
                sqlparser::ast::Expr::Identifier(idents[0].clone())
            } else {
                sqlparser::ast::Expr::CompoundIdentifier(idents.to_vec())
            }
        }
    }
}
