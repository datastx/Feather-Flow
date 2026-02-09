//! Expression lowering: sqlparser Expr → TypedExpr

use crate::ir::expr::{BinOp, LiteralValue, TypedExpr, UnOp};
use crate::ir::schema::RelSchema;
use crate::ir::types::{Nullability, SqlType};
use sqlparser::ast::{
    self, BinaryOperator, CastKind, CharacterLength, DataType, Expr, Function, FunctionArg,
    FunctionArgExpr, UnaryOperator, Value,
};

/// Lower a sqlparser Expr into a TypedExpr, resolving column refs against the given schema
pub fn lower_expr(expr: &Expr, schema: &RelSchema) -> TypedExpr {
    match expr {
        Expr::Identifier(ident) => lower_column_ref(None, &ident.value, schema),

        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                lower_column_ref(Some(&idents[0].value), &idents[1].value, schema)
            } else if idents.len() == 1 {
                lower_column_ref(None, &idents[0].value, schema)
            } else {
                // schema.table.column — use last two parts
                let table = &idents[idents.len() - 2].value;
                let column = &idents[idents.len() - 1].value;
                lower_column_ref(Some(table), column, schema)
            }
        }

        Expr::Value(val) => lower_value(val),

        Expr::BinaryOp { left, op, right } => {
            let left_expr = lower_expr(left, schema);
            let right_expr = lower_expr(right, schema);
            let resolved_type = infer_binary_type(&left_expr, op, &right_expr);
            let nullability = left_expr.nullability().combine(right_expr.nullability());
            TypedExpr::BinaryOp {
                left: Box::new(left_expr),
                op: BinOp::from_sqlparser(op),
                right: Box::new(right_expr),
                resolved_type,
                nullability,
            }
        }

        Expr::UnaryOp { op, expr: inner } => {
            let inner_expr = lower_expr(inner, schema);
            let resolved_type = infer_unary_type(op, &inner_expr);
            let nullability = inner_expr.nullability();
            TypedExpr::UnaryOp {
                op: UnOp::from_sqlparser(op),
                expr: Box::new(inner_expr),
                resolved_type,
                nullability,
            }
        }

        Expr::Function(func) => lower_function(func, schema),

        Expr::Cast {
            expr: inner,
            data_type,
            kind,
            ..
        } => {
            let inner_expr = lower_expr(inner, schema);
            let target_type = lower_data_type(data_type);
            let nullability = match kind {
                CastKind::TryCast => Nullability::Nullable,
                _ => inner_expr.nullability(),
            };
            TypedExpr::Cast {
                expr: Box::new(inner_expr),
                target_type,
                nullability,
            }
        }

        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => lower_case(operand, conditions, results, else_result, schema),

        Expr::IsNull(inner) => TypedExpr::IsNull {
            expr: Box::new(lower_expr(inner, schema)),
            negated: false,
        },

        Expr::IsNotNull(inner) => TypedExpr::IsNull {
            expr: Box::new(lower_expr(inner, schema)),
            negated: true,
        },

        Expr::Nested(inner) => lower_expr(inner, schema),

        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let inner_expr = lower_expr(inner, schema);
            let low_expr = lower_expr(low, schema);
            let high_expr = lower_expr(high, schema);
            let combined_nullability = inner_expr
                .nullability()
                .combine(low_expr.nullability().combine(high_expr.nullability()));
            // BETWEEN: expr >= low AND expr <= high
            // NOT BETWEEN: expr < low OR expr > high
            let (low_op, high_op, combine_op) = if *negated {
                (
                    &BinaryOperator::Lt,
                    &BinaryOperator::Gt,
                    &BinaryOperator::Or,
                )
            } else {
                (
                    &BinaryOperator::GtEq,
                    &BinaryOperator::LtEq,
                    &BinaryOperator::And,
                )
            };
            let left_cmp = TypedExpr::BinaryOp {
                left: Box::new(inner_expr.clone()),
                op: BinOp::from_sqlparser(low_op),
                right: Box::new(low_expr),
                resolved_type: SqlType::Boolean,
                nullability: combined_nullability,
            };
            let right_cmp = TypedExpr::BinaryOp {
                left: Box::new(inner_expr),
                op: BinOp::from_sqlparser(high_op),
                right: Box::new(high_expr),
                resolved_type: SqlType::Boolean,
                nullability: combined_nullability,
            };
            TypedExpr::BinaryOp {
                left: Box::new(left_cmp),
                op: BinOp::from_sqlparser(combine_op),
                right: Box::new(right_cmp),
                resolved_type: SqlType::Boolean,
                nullability: combined_nullability,
            }
        }

        Expr::InList {
            expr: lhs,
            list,
            negated,
        } => {
            if list.is_empty() {
                // Empty IN list: always false (or true for NOT IN)
                return TypedExpr::Literal {
                    value: LiteralValue::Boolean(!negated),
                    resolved_type: SqlType::Boolean,
                };
            }

            let lhs_expr = lower_expr(lhs, schema);
            let (cmp_op, combine_op) = if *negated {
                (BinOp::NotEq, BinOp::And)
            } else {
                (BinOp::Eq, BinOp::Or)
            };

            // Build a chain: lhs = list[0] OR lhs = list[1] OR ...
            let mut result = {
                let rhs = lower_expr(&list[0], schema);
                let nullability = lhs_expr.nullability().combine(rhs.nullability());
                TypedExpr::BinaryOp {
                    left: Box::new(lhs_expr.clone()),
                    op: cmp_op.clone(),
                    right: Box::new(rhs),
                    resolved_type: SqlType::Boolean,
                    nullability,
                }
            };

            for item in &list[1..] {
                let rhs = lower_expr(item, schema);
                let cmp_nullability = lhs_expr.nullability().combine(rhs.nullability());
                let cmp = TypedExpr::BinaryOp {
                    left: Box::new(lhs_expr.clone()),
                    op: cmp_op.clone(),
                    right: Box::new(rhs),
                    resolved_type: SqlType::Boolean,
                    nullability: cmp_nullability,
                };
                let combined_nullability = result.nullability().combine(cmp.nullability());
                result = TypedExpr::BinaryOp {
                    left: Box::new(result),
                    op: combine_op.clone(),
                    right: Box::new(cmp),
                    resolved_type: SqlType::Boolean,
                    nullability: combined_nullability,
                };
            }

            result
        }

        Expr::Subquery(_) => TypedExpr::Subquery {
            resolved_type: SqlType::Unknown("subquery".to_string()),
            nullability: Nullability::Nullable,
        },

        Expr::Exists { .. } => TypedExpr::Unsupported {
            description: "EXISTS".to_string(),
            resolved_type: SqlType::Boolean,
            nullability: Nullability::NotNull,
        },

        Expr::Like { .. } | Expr::ILike { .. } => TypedExpr::Unsupported {
            description: "LIKE".to_string(),
            resolved_type: SqlType::Boolean,
            nullability: Nullability::Unknown,
        },

        Expr::Wildcard => TypedExpr::Wildcard { table: None },

        Expr::QualifiedWildcard(name) => TypedExpr::Wildcard {
            table: Some(
                name.0
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
            ),
        },

        // Catch-all for anything we don't explicitly handle
        other => TypedExpr::Unsupported {
            description: format!("{other:?}").chars().take(100).collect(),
            resolved_type: SqlType::Unknown("unsupported expr".to_string()),
            nullability: Nullability::Unknown,
        },
    }
}

/// Lower a column reference, resolving against the current schema
fn lower_column_ref(table: Option<&str>, column: &str, schema: &RelSchema) -> TypedExpr {
    let resolved = if let Some(t) = table {
        schema.find_qualified(t, column)
    } else {
        schema.find_column(column)
    };

    match resolved {
        Some(col) => TypedExpr::ColumnRef {
            table: table.map(|s| s.to_string()),
            column: column.to_string(),
            resolved_type: col.sql_type.clone(),
            nullability: col.nullability,
        },
        None => TypedExpr::ColumnRef {
            table: table.map(|s| s.to_string()),
            column: column.to_string(),
            resolved_type: SqlType::Unknown("unresolved".to_string()),
            nullability: Nullability::Unknown,
        },
    }
}

/// Lower a literal Value to TypedExpr
fn lower_value(val: &Value) -> TypedExpr {
    match val {
        Value::Number(n, _long) => {
            if let Ok(i) = n.parse::<i64>() {
                TypedExpr::Literal {
                    value: LiteralValue::Integer(i),
                    resolved_type: SqlType::Integer { bits: 64 },
                }
            } else if let Ok(f) = n.parse::<f64>() {
                TypedExpr::Literal {
                    value: LiteralValue::Float(f),
                    resolved_type: SqlType::Float { bits: 64 },
                }
            } else {
                TypedExpr::Literal {
                    value: LiteralValue::String(n.clone()),
                    resolved_type: SqlType::Unknown("unparseable number".to_string()),
                }
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => TypedExpr::Literal {
            value: LiteralValue::String(s.clone()),
            resolved_type: SqlType::String { max_length: None },
        },
        Value::Boolean(b) => TypedExpr::Literal {
            value: LiteralValue::Boolean(*b),
            resolved_type: SqlType::Boolean,
        },
        Value::Null => TypedExpr::Literal {
            value: LiteralValue::Null,
            resolved_type: SqlType::Unknown("null".to_string()),
        },
        _ => TypedExpr::Unsupported {
            description: "literal".to_string(),
            resolved_type: SqlType::Unknown("unsupported literal".to_string()),
            nullability: Nullability::Unknown,
        },
    }
}

/// Lower a Function call to TypedExpr
fn lower_function(func: &Function, schema: &RelSchema) -> TypedExpr {
    let name = func.name.to_string().to_uppercase();
    let args = extract_function_args(&func.args, schema);

    let (resolved_type, nullability) = infer_function_type(&name, &args);

    TypedExpr::FunctionCall {
        name,
        args,
        resolved_type,
        nullability,
    }
}

/// Extract typed args from a function's argument list
fn extract_function_args(args: &ast::FunctionArguments, schema: &RelSchema) -> Vec<TypedExpr> {
    match args {
        ast::FunctionArguments::None => vec![],
        ast::FunctionArguments::Subquery(_) => vec![TypedExpr::Subquery {
            resolved_type: SqlType::Unknown("subquery arg".to_string()),
            nullability: Nullability::Nullable,
        }],
        ast::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(arg_expr) | FunctionArg::Named { arg: arg_expr, .. } => {
                    match arg_expr {
                        FunctionArgExpr::Expr(e) => lower_expr(e, schema),
                        FunctionArgExpr::Wildcard => TypedExpr::Wildcard { table: None },
                        FunctionArgExpr::QualifiedWildcard(name) => TypedExpr::Wildcard {
                            table: Some(
                                name.0
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect::<Vec<_>>()
                                    .join("."),
                            ),
                        },
                    }
                }
            })
            .collect(),
    }
}

/// Infer the return type of a function from its name and arguments
fn infer_function_type(name: &str, args: &[TypedExpr]) -> (SqlType, Nullability) {
    match name {
        "COUNT" | "COUNT_STAR" => (SqlType::Integer { bits: 64 }, Nullability::NotNull),

        "SUM" => {
            let arg_type = args
                .first()
                .map(|a| a.resolved_type().clone())
                .unwrap_or(SqlType::Unknown("no arg".to_string()));
            (arg_type, Nullability::Nullable)
        }
        "AVG" => (SqlType::Float { bits: 64 }, Nullability::Nullable),
        "MIN" | "MAX" => {
            let arg_type = args
                .first()
                .map(|a| a.resolved_type().clone())
                .unwrap_or(SqlType::Unknown("no arg".to_string()));
            let arg_null = args
                .first()
                .map(|a| a.nullability())
                .unwrap_or(Nullability::Unknown);
            (arg_type, arg_null)
        }

        "COALESCE" => {
            let arg_type = args
                .first()
                .map(|a| a.resolved_type().clone())
                .unwrap_or(SqlType::Unknown("no arg".to_string()));
            let has_non_null_fallback =
                args.iter().any(|a| a.nullability() == Nullability::NotNull);
            let nullability = if has_non_null_fallback {
                Nullability::NotNull
            } else {
                Nullability::Nullable
            };
            (arg_type, nullability)
        }

        "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "SUBSTRING" | "CONCAT" => {
            let nullability = args
                .first()
                .map(|a| a.nullability())
                .unwrap_or(Nullability::Unknown);
            (SqlType::String { max_length: None }, nullability)
        }

        "LENGTH" | "CHAR_LENGTH" | "LEN" => {
            let nullability = args
                .first()
                .map(|a| a.nullability())
                .unwrap_or(Nullability::Unknown);
            (SqlType::Integer { bits: 32 }, nullability)
        }

        "NOW" | "CURRENT_TIMESTAMP" => (SqlType::Timestamp, Nullability::NotNull),
        "CURRENT_DATE" => (SqlType::Date, Nullability::NotNull),
        "DATE_TRUNC" | "DATE_PART" | "EXTRACT" => {
            (SqlType::Integer { bits: 64 }, Nullability::Nullable)
        }

        "IF" | "IIF" | "IFNULL" | "NULLIF" => {
            let arg_type = args
                .get(1)
                .map(|a| a.resolved_type().clone())
                .unwrap_or(SqlType::Unknown("no arg".to_string()));
            (arg_type, Nullability::Nullable)
        }

        "BOOL_AND" | "BOOL_OR" | "EVERY" => (SqlType::Boolean, Nullability::Nullable),

        _ => (
            SqlType::Unknown(format!("function {name}")),
            Nullability::Unknown,
        ),
    }
}

/// Infer the result type of a binary operation
fn infer_binary_type(left: &TypedExpr, op: &BinaryOperator, right: &TypedExpr) -> SqlType {
    match op {
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => SqlType::Boolean,

        BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => SqlType::Boolean,

        BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply => {
            promote_numeric(left.resolved_type(), right.resolved_type())
        }

        BinaryOperator::Divide => match (left.resolved_type(), right.resolved_type()) {
            (SqlType::Decimal { .. }, _) | (_, SqlType::Decimal { .. }) => SqlType::Decimal {
                precision: None,
                scale: None,
            },
            _ => SqlType::Float { bits: 64 },
        },

        BinaryOperator::Modulo => left.resolved_type().clone(),

        BinaryOperator::StringConcat => SqlType::String { max_length: None },

        _ => SqlType::Unknown("binary op".to_string()),
    }
}

/// Infer the result type of a unary operation
fn infer_unary_type(op: &UnaryOperator, expr: &TypedExpr) -> SqlType {
    match op {
        UnaryOperator::Not => SqlType::Boolean,
        UnaryOperator::Minus | UnaryOperator::Plus => expr.resolved_type().clone(),
        _ => SqlType::Unknown("unary op".to_string()),
    }
}

/// Promote two numeric types to the wider one
fn promote_numeric(left: &SqlType, right: &SqlType) -> SqlType {
    match (left, right) {
        (SqlType::Decimal { .. }, _) | (_, SqlType::Decimal { .. }) => SqlType::Decimal {
            precision: None,
            scale: None,
        },
        (SqlType::Float { bits: a }, SqlType::Float { bits: b }) => {
            SqlType::Float { bits: (*a).max(*b) }
        }
        (SqlType::Float { bits }, _) | (_, SqlType::Float { bits }) => {
            SqlType::Float { bits: *bits }
        }
        (SqlType::Integer { bits: a }, SqlType::Integer { bits: b }) => {
            SqlType::Integer { bits: (*a).max(*b) }
        }
        _ => left.clone(),
    }
}

/// Lower a CASE expression
fn lower_case(
    operand: &Option<Box<Expr>>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: &Option<Box<Expr>>,
    schema: &RelSchema,
) -> TypedExpr {
    let operand_expr = operand.as_ref().map(|e| Box::new(lower_expr(e, schema)));
    let condition_exprs: Vec<_> = conditions.iter().map(|e| lower_expr(e, schema)).collect();
    let result_exprs: Vec<_> = results.iter().map(|e| lower_expr(e, schema)).collect();
    let else_expr = else_result
        .as_ref()
        .map(|e| Box::new(lower_expr(e, schema)));

    let resolved_type = result_exprs
        .first()
        .map(|e| e.resolved_type().clone())
        .unwrap_or(SqlType::Unknown("empty case".to_string()));

    let has_else = else_expr.is_some();
    let any_nullable = result_exprs
        .iter()
        .any(|e| e.nullability() == Nullability::Nullable);
    let nullability = if !has_else || any_nullable {
        Nullability::Nullable
    } else {
        Nullability::NotNull
    };

    TypedExpr::Case {
        operand: operand_expr,
        conditions: condition_exprs,
        results: result_exprs,
        else_result: else_expr,
        resolved_type,
        nullability,
    }
}

/// Convert a sqlparser DataType to our SqlType
pub fn lower_data_type(dt: &DataType) -> SqlType {
    match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::TinyInt(_) => SqlType::Integer { bits: 8 },
        DataType::SmallInt(_) => SqlType::Integer { bits: 16 },
        DataType::Int(_) | DataType::Integer(_) => SqlType::Integer { bits: 32 },
        DataType::BigInt(_) => SqlType::Integer { bits: 64 },
        DataType::Float(_) | DataType::Real => SqlType::Float { bits: 32 },
        DataType::Double | DataType::DoublePrecision => SqlType::Float { bits: 64 },
        DataType::Decimal(info) | DataType::Numeric(info) => {
            let (precision, scale) = match info {
                ast::ExactNumberInfo::PrecisionAndScale(p, s) => (Some(*p as u16), Some(*s as u16)),
                ast::ExactNumberInfo::Precision(p) => (Some(*p as u16), None),
                ast::ExactNumberInfo::None => (None, None),
            };
            SqlType::Decimal { precision, scale }
        }
        DataType::Varchar(len) | DataType::CharVarying(len) => {
            let max_length = extract_char_length(len);
            SqlType::String { max_length }
        }
        DataType::Char(len) | DataType::Character(len) => {
            let max_length = extract_char_length(len);
            SqlType::String { max_length }
        }
        DataType::Text => SqlType::String { max_length: None },
        DataType::Date => SqlType::Date,
        DataType::Time(..) => SqlType::Time,
        DataType::Timestamp(..) | DataType::Datetime(..) => SqlType::Timestamp,
        DataType::Interval => SqlType::Interval,
        DataType::Blob(_) | DataType::Binary(_) | DataType::Varbinary(_) | DataType::Bytea => {
            SqlType::Binary
        }
        _ => SqlType::Unknown(format!("{dt}")),
    }
}

/// Extract length from CharacterLength enum
fn extract_char_length(len: &Option<CharacterLength>) -> Option<u32> {
    match len {
        Some(CharacterLength::IntegerLength { length, .. }) => Some(*length as u32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lower_literal_integer() {
        let expr = lower_value(&Value::Number("42".to_string(), false));
        assert!(matches!(
            expr,
            TypedExpr::Literal {
                value: LiteralValue::Integer(42),
                ..
            }
        ));
    }

    #[test]
    fn test_lower_literal_string() {
        let expr = lower_value(&Value::SingleQuotedString("hello".to_string()));
        assert!(
            matches!(expr, TypedExpr::Literal { value: LiteralValue::String(ref s), .. } if s == "hello")
        );
    }

    #[test]
    fn test_promote_numeric() {
        assert_eq!(
            promote_numeric(
                &SqlType::Integer { bits: 32 },
                &SqlType::Integer { bits: 64 }
            ),
            SqlType::Integer { bits: 64 }
        );
        assert_eq!(
            promote_numeric(&SqlType::Integer { bits: 32 }, &SqlType::Float { bits: 64 }),
            SqlType::Float { bits: 64 }
        );
    }
}
