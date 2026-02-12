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
            &SqlType::Integer {
                bits: IntBitWidth::I32
            },
            &SqlType::Integer {
                bits: IntBitWidth::I64
            }
        ),
        SqlType::Integer {
            bits: IntBitWidth::I64
        }
    );
    assert_eq!(
        promote_numeric(
            &SqlType::Integer {
                bits: IntBitWidth::I32
            },
            &SqlType::Float {
                bits: FloatBitWidth::F64
            }
        ),
        SqlType::Float {
            bits: FloatBitWidth::F64
        }
    );
}
