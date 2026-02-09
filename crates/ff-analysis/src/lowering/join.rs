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

    let (join_type, condition) = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            (JoinType::Inner, extract_join_condition(constraint))
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (JoinType::LeftOuter, extract_join_condition(constraint))
        }
        JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
            (JoinType::RightOuter, extract_join_condition(constraint))
        }
        JoinOperator::FullOuter(constraint) => {
            (JoinType::FullOuter, extract_join_condition(constraint))
        }
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
    let condition_expr = condition.map(|expr| lower_expr(expr, &output_schema));

    Ok(RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition: condition_expr,
        schema: output_schema,
    })
}

/// Extract the join condition expression from a JoinConstraint
fn extract_join_condition(constraint: &JoinConstraint) -> Option<&sqlparser::ast::Expr> {
    match constraint {
        JoinConstraint::On(expr) => Some(expr),
        JoinConstraint::Using(_) => None,
        JoinConstraint::Natural => None,
        JoinConstraint::None => None,
    }
}
