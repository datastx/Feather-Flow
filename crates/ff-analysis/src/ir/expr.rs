//! Typed expression tree for the SQL IR

use super::types::{Nullability, SqlType};
use serde::{Deserialize, Serialize};

/// A literal value in the IR
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiteralValue {
    /// Null literal
    Null,
    /// Boolean literal
    Boolean(bool),
    /// Integer literal
    Integer(i64),
    /// Float literal
    Float(f64),
    /// String literal
    String(String),
}

/// Binary operator (stored as string to avoid serde dependency on sqlparser)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinOp(pub String);

impl BinOp {
    pub fn from_sqlparser(op: &sqlparser::ast::BinaryOperator) -> Self {
        Self(format!("{op}"))
    }

    /// Check if this is an equality operator
    pub fn is_eq(&self) -> bool {
        self.0 == "="
    }

    /// Check if this is a comparison operator
    pub fn is_comparison(&self) -> bool {
        matches!(self.0.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=")
    }

    /// Check if this is a logical operator
    pub fn is_logical(&self) -> bool {
        matches!(self.0.as_str(), "AND" | "OR" | "XOR")
    }
}

/// Unary operator (stored as string to avoid serde dependency on sqlparser)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnOp(pub String);

impl UnOp {
    pub fn from_sqlparser(op: &sqlparser::ast::UnaryOperator) -> Self {
        Self(format!("{op}"))
    }
}

/// Typed expression tree — every node carries resolved type and nullability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypedExpr {
    /// Reference to a column in the current scope
    ColumnRef {
        /// Optional table qualifier
        table: Option<String>,
        /// Column name
        column: String,
        /// Resolved type of this column
        resolved_type: SqlType,
        /// Nullability of this column reference
        nullability: Nullability,
    },

    /// Literal value
    Literal {
        /// The literal value
        value: LiteralValue,
        /// Type of the literal
        resolved_type: SqlType,
    },

    /// Binary operation (a + b, a = b, a AND b, etc.)
    BinaryOp {
        /// Left operand
        left: Box<TypedExpr>,
        /// Operator
        op: BinOp,
        /// Right operand
        right: Box<TypedExpr>,
        /// Resolved type of the expression
        resolved_type: SqlType,
        /// Nullability: nullable if either operand is nullable
        nullability: Nullability,
    },

    /// Unary operation (NOT, -, etc.)
    UnaryOp {
        /// Operator
        op: UnOp,
        /// Operand
        expr: Box<TypedExpr>,
        /// Resolved type
        resolved_type: SqlType,
        /// Nullability
        nullability: Nullability,
    },

    /// Function call (COUNT, SUM, COALESCE, etc.)
    FunctionCall {
        /// Function name (uppercased)
        name: String,
        /// Function arguments
        args: Vec<TypedExpr>,
        /// Resolved return type
        resolved_type: SqlType,
        /// Nullability of the result
        nullability: Nullability,
    },

    /// Explicit CAST expression
    Cast {
        /// Expression being cast
        expr: Box<TypedExpr>,
        /// Target type
        target_type: SqlType,
        /// Nullability (inherited from source expression)
        nullability: Nullability,
    },

    /// CASE WHEN ... THEN ... ELSE ... END
    Case {
        /// Optional operand (CASE x WHEN ...)
        operand: Option<Box<TypedExpr>>,
        /// WHEN conditions
        conditions: Vec<TypedExpr>,
        /// THEN results
        results: Vec<TypedExpr>,
        /// ELSE result
        else_result: Option<Box<TypedExpr>>,
        /// Resolved type (unified across branches)
        resolved_type: SqlType,
        /// Nullable if any branch is nullable or ELSE is missing
        nullability: Nullability,
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        /// Expression being tested
        expr: Box<TypedExpr>,
        /// True for IS NOT NULL
        negated: bool,
    },

    /// Scalar subquery (opaque — type noted but interior not analyzed)
    Subquery {
        /// Type of the scalar result
        resolved_type: SqlType,
        /// Nullability
        nullability: Nullability,
    },

    /// SELECT * or table.* — expanded during lowering when possible
    Wildcard {
        /// Optional table qualifier
        table: Option<String>,
    },

    /// Catch-all for unsupported SQL expressions (graceful degradation)
    Unsupported {
        /// Description of what we couldn't lower
        description: String,
        /// Best-effort type (usually Unknown)
        resolved_type: SqlType,
        /// Best-effort nullability (usually Unknown)
        nullability: Nullability,
    },
}

impl TypedExpr {
    /// Get the resolved type of this expression
    pub fn resolved_type(&self) -> &SqlType {
        static BOOLEAN: SqlType = SqlType::Boolean;
        match self {
            TypedExpr::ColumnRef { resolved_type, .. } => resolved_type,
            TypedExpr::Literal { resolved_type, .. } => resolved_type,
            TypedExpr::BinaryOp { resolved_type, .. } => resolved_type,
            TypedExpr::UnaryOp { resolved_type, .. } => resolved_type,
            TypedExpr::FunctionCall { resolved_type, .. } => resolved_type,
            TypedExpr::Cast { target_type, .. } => target_type,
            TypedExpr::Case { resolved_type, .. } => resolved_type,
            TypedExpr::IsNull { .. } => &BOOLEAN,
            TypedExpr::Subquery { resolved_type, .. } => resolved_type,
            TypedExpr::Wildcard { .. } => resolved_type_unknown(),
            TypedExpr::Unsupported { resolved_type, .. } => resolved_type,
        }
    }

    /// Get the nullability of this expression
    pub fn nullability(&self) -> Nullability {
        match self {
            TypedExpr::ColumnRef { nullability, .. } => *nullability,
            TypedExpr::Literal { value, .. } => match value {
                LiteralValue::Null => Nullability::Nullable,
                _ => Nullability::NotNull,
            },
            TypedExpr::BinaryOp { nullability, .. } => *nullability,
            TypedExpr::UnaryOp { nullability, .. } => *nullability,
            TypedExpr::FunctionCall { nullability, .. } => *nullability,
            TypedExpr::Cast { nullability, .. } => *nullability,
            TypedExpr::Case { nullability, .. } => *nullability,
            TypedExpr::IsNull { .. } => Nullability::NotNull,
            TypedExpr::Subquery { nullability, .. } => *nullability,
            TypedExpr::Wildcard { .. } => Nullability::Unknown,
            TypedExpr::Unsupported { nullability, .. } => *nullability,
        }
    }
}

/// Static unknown type for wildcard (avoids returning reference to temporary)
fn resolved_type_unknown() -> &'static SqlType {
    // Using a leaked Box to create a static reference — only called for Wildcard
    use std::sync::OnceLock;
    static UNKNOWN: OnceLock<SqlType> = OnceLock::new();
    UNKNOWN.get_or_init(|| SqlType::Unknown("wildcard".to_string()))
}
