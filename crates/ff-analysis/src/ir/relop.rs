//! Relational operators — the nodes of the IR plan tree

use super::expr::TypedExpr;
use super::schema::RelSchema;
use serde::{Deserialize, Serialize};

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join
    Inner,
    /// Left outer join
    LeftOuter,
    /// Right outer join
    RightOuter,
    /// Full outer join
    FullOuter,
    /// Cross join (cartesian product)
    Cross,
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::LeftOuter => write!(f, "LEFT"),
            JoinType::RightOuter => write!(f, "RIGHT"),
            JoinType::FullOuter => write!(f, "FULL OUTER"),
            JoinType::Cross => write!(f, "CROSS"),
        }
    }
}

/// Set operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOpKind {
    /// UNION (deduplicated)
    Union,
    /// UNION ALL
    UnionAll,
    /// INTERSECT
    Intersect,
    /// EXCEPT
    Except,
}

/// Sort key for ORDER BY
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    /// Expression to sort by
    pub expr: TypedExpr,
    /// Ascending (true) or descending (false)
    pub ascending: bool,
    /// NULLS FIRST (true) or NULLS LAST (false)
    pub nulls_first: bool,
}

/// Relational operator — a node in the query plan tree.
///
/// Each variant carries a `schema: RelSchema` that describes the output columns
/// at that point in the plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelOp {
    /// Table/model scan
    Scan {
        /// Table or model name
        table_name: String,
        /// Optional alias
        alias: Option<String>,
        /// Output schema
        schema: RelSchema,
    },

    /// Column projection (SELECT clause)
    Project {
        /// Input relation
        input: Box<RelOp>,
        /// Projected columns: (output_name, expression)
        columns: Vec<(String, TypedExpr)>,
        /// Output schema
        schema: RelSchema,
    },

    /// Row filter (WHERE / HAVING clause)
    Filter {
        /// Input relation
        input: Box<RelOp>,
        /// Filter predicate
        predicate: TypedExpr,
        /// Output schema (same as input)
        schema: RelSchema,
    },

    /// Join of two relations
    Join {
        /// Left input
        left: Box<RelOp>,
        /// Right input
        right: Box<RelOp>,
        /// Join type
        join_type: JoinType,
        /// Join condition (None for CROSS JOIN)
        condition: Option<TypedExpr>,
        /// Output schema
        schema: RelSchema,
    },

    /// Aggregation (GROUP BY)
    Aggregate {
        /// Input relation
        input: Box<RelOp>,
        /// GROUP BY expressions
        group_by: Vec<TypedExpr>,
        /// Aggregate expressions: (output_name, aggregate_expr)
        aggregates: Vec<(String, TypedExpr)>,
        /// Output schema
        schema: RelSchema,
    },

    /// Ordering (ORDER BY)
    Sort {
        /// Input relation
        input: Box<RelOp>,
        /// Sort keys
        order_by: Vec<SortKey>,
        /// Output schema (same as input)
        schema: RelSchema,
    },

    /// Row limiting (LIMIT / OFFSET)
    Limit {
        /// Input relation
        input: Box<RelOp>,
        /// Maximum rows to return
        limit: Option<u64>,
        /// Rows to skip
        offset: Option<u64>,
        /// Output schema (same as input)
        schema: RelSchema,
    },

    /// Set operation (UNION, INTERSECT, EXCEPT)
    SetOp {
        /// Left operand
        left: Box<RelOp>,
        /// Right operand
        right: Box<RelOp>,
        /// Set operation type
        op: SetOpKind,
        /// Output schema (from left side)
        schema: RelSchema,
    },
}

impl RelOp {
    /// Get the output schema of this operator
    pub fn schema(&self) -> &RelSchema {
        match self {
            RelOp::Scan { schema, .. } => schema,
            RelOp::Project { schema, .. } => schema,
            RelOp::Filter { schema, .. } => schema,
            RelOp::Join { schema, .. } => schema,
            RelOp::Aggregate { schema, .. } => schema,
            RelOp::Sort { schema, .. } => schema,
            RelOp::Limit { schema, .. } => schema,
            RelOp::SetOp { schema, .. } => schema,
        }
    }

    /// Get the effective table label for this operator, if it has a single source.
    ///
    /// For `Scan` nodes this is the alias (if present) or the table name.
    /// For compound operators (Join, SetOp, etc.) there is no single source,
    /// so `None` is returned.
    pub fn table_label(&self) -> Option<&str> {
        match self {
            RelOp::Scan {
                table_name, alias, ..
            } => Some(alias.as_deref().unwrap_or(table_name)),
            _ => None,
        }
    }
}
