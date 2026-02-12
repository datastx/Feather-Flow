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
    assert_eq!(
        parse_sql_type("DECIMAL(18)"),
        SqlType::Decimal {
            precision: Some(18),
            scale: None
        }
    );
}

#[test]
fn test_parse_unknown_type() {
    let result = parse_sql_type("SOMECUSTOMTYPE");
    assert!(matches!(result, SqlType::Unknown(_)));
}

#[test]
fn test_parse_hugeint() {
    assert_eq!(parse_sql_type("HUGEINT"), SqlType::HugeInt);
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
        Nullability::Nullable.combine(Nullability::NotNull),
        Nullability::Nullable
    );
    assert_eq!(
        Nullability::Unknown.combine(Nullability::NotNull),
        Nullability::Unknown
    );
}

#[test]
fn test_sql_type_display() {
    assert_eq!(
        SqlType::Integer {
            bits: IntBitWidth::I32
        }
        .display_name(),
        "INTEGER"
    );
    assert_eq!(
        SqlType::Float {
            bits: FloatBitWidth::F64
        }
        .display_name(),
        "DOUBLE"
    );
    assert_eq!(
        SqlType::String {
            max_length: Some(100)
        }
        .display_name(),
        "VARCHAR(100)"
    );
}

#[test]
fn test_int_bit_width_display() {
    assert_eq!(IntBitWidth::I8.to_string(), "8");
    assert_eq!(IntBitWidth::I16.to_string(), "16");
    assert_eq!(IntBitWidth::I32.to_string(), "32");
    assert_eq!(IntBitWidth::I64.to_string(), "64");
}

#[test]
fn test_float_bit_width_display() {
    assert_eq!(FloatBitWidth::F32.to_string(), "32");
    assert_eq!(FloatBitWidth::F64.to_string(), "64");
}

#[test]
fn test_int_bit_width_ordering() {
    assert!(IntBitWidth::I8 < IntBitWidth::I16);
    assert!(IntBitWidth::I16 < IntBitWidth::I32);
    assert!(IntBitWidth::I32 < IntBitWidth::I64);
}

#[test]
fn test_float_bit_width_ordering() {
    assert!(FloatBitWidth::F32 < FloatBitWidth::F64);
}

#[test]
fn test_parse_json_uuid() {
    assert_eq!(parse_sql_type("JSON"), SqlType::Json);
    assert_eq!(parse_sql_type("JSONB"), SqlType::Json);
    assert_eq!(parse_sql_type("UUID"), SqlType::Uuid);
}

#[test]
fn test_parse_array_type() {
    assert_eq!(
        parse_sql_type("INTEGER[]"),
        SqlType::Array(Box::new(SqlType::Integer {
            bits: IntBitWidth::I32
        }))
    );
    assert_eq!(
        parse_sql_type("VARCHAR[]"),
        SqlType::Array(Box::new(SqlType::String { max_length: None }))
    );
}

#[test]
fn test_parse_struct_type() {
    let result = parse_sql_type("STRUCT(name VARCHAR, age INT)");
    match result {
        SqlType::Struct(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "name");
            assert_eq!(fields[0].1, SqlType::String { max_length: None });
            assert_eq!(fields[1].0, "age");
            assert_eq!(
                fields[1].1,
                SqlType::Integer {
                    bits: IntBitWidth::I32
                }
            );
        }
        other => panic!("Expected Struct, got {:?}", other),
    }
}

#[test]
fn test_parse_map_type() {
    let result = parse_sql_type("MAP(VARCHAR, INTEGER)");
    match result {
        SqlType::Map { key, value } => {
            assert_eq!(*key, SqlType::String { max_length: None });
            assert_eq!(
                *value,
                SqlType::Integer {
                    bits: IntBitWidth::I32
                }
            );
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}

#[test]
fn test_new_type_display() {
    assert_eq!(SqlType::HugeInt.display_name(), "HUGEINT");
    assert_eq!(SqlType::Json.display_name(), "JSON");
    assert_eq!(SqlType::Uuid.display_name(), "UUID");
    assert_eq!(
        SqlType::Array(Box::new(SqlType::Integer {
            bits: IntBitWidth::I32
        }))
        .display_name(),
        "INTEGER[]"
    );
    assert_eq!(
        SqlType::Map {
            key: Box::new(SqlType::String { max_length: None }),
            value: Box::new(SqlType::Integer {
                bits: IntBitWidth::I32
            }),
        }
        .display_name(),
        "MAP(VARCHAR, INTEGER)"
    );
}

#[test]
fn test_hugeint_is_numeric() {
    assert!(SqlType::HugeInt.is_numeric());
    assert!(SqlType::HugeInt.is_compatible_with(&SqlType::Integer {
        bits: IntBitWidth::I64
    }));
}
