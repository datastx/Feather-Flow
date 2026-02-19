use super::*;

#[test]
fn test_parse_basic_types() {
    assert_eq!(parse_sql_type("boolean"), SqlType::Boolean);
    assert_eq!(
        parse_sql_type("INT"),
        SqlType::Integer {
            bits: IntBitWidth::I32
        }
    );
    assert_eq!(
        parse_sql_type("bigint"),
        SqlType::Integer {
            bits: IntBitWidth::I64
        }
    );
    assert_eq!(
        parse_sql_type("FLOAT"),
        SqlType::Float {
            bits: FloatBitWidth::F32
        }
    );
    assert_eq!(
        parse_sql_type("DOUBLE"),
        SqlType::Float {
            bits: FloatBitWidth::F64
        }
    );
    assert_eq!(
        parse_sql_type("VARCHAR"),
        SqlType::String { max_length: None }
    );
    assert_eq!(parse_sql_type("DATE"), SqlType::Date);
    assert_eq!(parse_sql_type("TIMESTAMP"), SqlType::Timestamp);
}

#[test]
fn test_parse_parameterized_types() {
    assert_eq!(
        parse_sql_type("VARCHAR(255)"),
        SqlType::String {
            max_length: Some(255)
        }
    );
    assert_eq!(
        parse_sql_type("DECIMAL(10,2)"),
        SqlType::Decimal {
            precision: Some(10),
            scale: Some(2)
        }
    );
}

#[test]
fn test_parse_unknown_type() {
    assert!(matches!(
        parse_sql_type("SOMECUSTOMTYPE"),
        SqlType::Unknown(_)
    ));
}

#[test]
fn test_type_compatibility() {
    assert!(SqlType::Integer {
        bits: IntBitWidth::I32
    }
    .is_compatible_with(&SqlType::Integer {
        bits: IntBitWidth::I64
    }));
    assert!(SqlType::Integer {
        bits: IntBitWidth::I32
    }
    .is_compatible_with(&SqlType::Float {
        bits: FloatBitWidth::F64
    }));
    assert!(!SqlType::Integer {
        bits: IntBitWidth::I32
    }
    .is_compatible_with(&SqlType::String { max_length: None }));
}

#[test]
fn test_nullability_combine() {
    assert_eq!(
        Nullability::NotNull.combine(Nullability::NotNull),
        Nullability::NotNull
    );
    assert_eq!(
        Nullability::NotNull.combine(Nullability::Nullable),
        Nullability::Nullable
    );
    assert_eq!(
        Nullability::Unknown.combine(Nullability::NotNull),
        Nullability::Unknown
    );
}
