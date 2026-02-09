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

/// Binary operator enum with variants for common SQL operators
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinOp {
    /// Equality (=)
    Eq,
    /// Inequality (<> or !=)
    NotEq,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    LtEq,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    GtEq,
    /// Logical AND
    And,
    /// Logical OR
    Or,
    /// Addition (+)
    Plus,
    /// Subtraction (-)
    Minus,
    /// Multiplication (*)
    Multiply,
    /// Division (/)
    Divide,
    /// Modulo (%)
    Modulo,
    /// String concatenation (||)
    StringConcat,
    /// Fallback for operators not explicitly listed
    Other(String),
}

impl BinOp {
    /// Convert from a sqlparser BinaryOperator
    pub fn from_sqlparser(op: &sqlparser::ast::BinaryOperator) -> Self {
        use sqlparser::ast::BinaryOperator as BO;
        match op {
            BO::Eq => BinOp::Eq,
            BO::NotEq => BinOp::NotEq,
            BO::Lt => BinOp::Lt,
            BO::LtEq => BinOp::LtEq,
            BO::Gt => BinOp::Gt,
            BO::GtEq => BinOp::GtEq,
            BO::And => BinOp::And,
            BO::Or => BinOp::Or,
            BO::Plus => BinOp::Plus,
            BO::Minus => BinOp::Minus,
            BO::Multiply => BinOp::Multiply,
            BO::Divide => BinOp::Divide,
            BO::Modulo => BinOp::Modulo,
            BO::StringConcat => BinOp::StringConcat,
            other => BinOp::Other(format!("{other}")),
        }
    }

    /// Check if this is an equality operator
    pub fn is_eq(&self) -> bool {
        matches!(self, BinOp::Eq)
    }

    /// Check if this is a comparison operator
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
        )
    }

    /// Check if this is a logical operator
    pub fn is_logical(&self) -> bool {
        matches!(self, BinOp::And | BinOp::Or)
    }
}

impl std::fmt::Display for BinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinOp::Eq => write!(f, "="),
            BinOp::NotEq => write!(f, "<>"),
            BinOp::Lt => write!(f, "<"),
            BinOp::LtEq => write!(f, "<="),
            BinOp::Gt => write!(f, ">"),
            BinOp::GtEq => write!(f, ">="),
            BinOp::And => write!(f, "AND"),
            BinOp::Or => write!(f, "OR"),
            BinOp::Plus => write!(f, "+"),
            BinOp::Minus => write!(f, "-"),
            BinOp::Multiply => write!(f, "*"),
            BinOp::Divide => write!(f, "/"),
            BinOp::Modulo => write!(f, "%"),
            BinOp::StringConcat => write!(f, "||"),
            BinOp::Other(s) => write!(f, "{s}"),
        }
    }
}

/// Unary operator enum with variants for common SQL unary operators
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnOp {
    /// Logical NOT
    Not,
    /// Unary minus (negation)
    Minus,
    /// Unary plus
    Plus,
    /// Fallback for operators not explicitly listed
    Other(String),
}

impl UnOp {
    /// Convert from a sqlparser UnaryOperator
    pub fn from_sqlparser(op: &sqlparser::ast::UnaryOperator) -> Self {
        use sqlparser::ast::UnaryOperator as UO;
        match op {
            UO::Not => UnOp::Not,
            UO::Minus => UnOp::Minus,
            UO::Plus => UnOp::Plus,
            other => UnOp::Other(format!("{other}")),
        }
    }
}

impl std::fmt::Display for UnOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnOp::Not => write!(f, "NOT"),
            UnOp::Minus => write!(f, "-"),
            UnOp::Plus => write!(f, "+"),
            UnOp::Other(s) => write!(f, "{s}"),
        }
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
