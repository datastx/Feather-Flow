use super::*;

#[test]
fn test_roundtrip_basic_types() {
    let types = vec![
        SqlType::Boolean,
        SqlType::Integer {
            bits: IntBitWidth::I32,
        },
        SqlType::Integer {
            bits: IntBitWidth::I64,
        },
        SqlType::Float {
            bits: FloatBitWidth::F32,
        },
        SqlType::Float {
            bits: FloatBitWidth::F64,
        },
        SqlType::String { max_length: None },
        SqlType::Date,
        SqlType::Timestamp,
        SqlType::Binary,
    ];

    for ty in types {
        let arrow = sql_type_to_arrow(&ty);
        let roundtripped = arrow_to_sql_type(&arrow);
        assert_eq!(
            ty, roundtripped,
            "Roundtrip failed for {:?} -> {:?} -> {:?}",
            ty, arrow, roundtripped
        );
    }
}

#[test]
fn test_roundtrip_decimal() {
    let ty = SqlType::Decimal {
        precision: Some(10),
        scale: Some(2),
    };
    let arrow = sql_type_to_arrow(&ty);
    assert_eq!(arrow, ArrowDataType::Decimal128(10, 2));
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(back, ty);
}

#[test]
fn test_roundtrip_array() {
    let ty = SqlType::Array(Box::new(SqlType::Integer {
        bits: IntBitWidth::I32,
    }));
    let arrow = sql_type_to_arrow(&ty);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_roundtrip_struct() {
    let ty = SqlType::Struct(vec![
        ("name".to_string(), SqlType::String { max_length: None }),
        (
            "age".to_string(),
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
        ),
    ]);
    let arrow = sql_type_to_arrow(&ty);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_roundtrip_map() {
    let ty = SqlType::Map {
        key: Box::new(SqlType::String { max_length: None }),
        value: Box::new(SqlType::Integer {
            bits: IntBitWidth::I32,
        }),
    };
    let arrow = sql_type_to_arrow(&ty);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_hugeint_roundtrip() {
    let arrow = sql_type_to_arrow(&SqlType::HugeInt);
    assert_eq!(arrow, ArrowDataType::Decimal128(38, 0));
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(
        back,
        SqlType::HugeInt,
        "HugeInt should roundtrip through Decimal128(38,0)"
    );
}

#[test]
fn test_json_uuid_to_arrow() {
    // JSON and UUID map to Utf8, so roundtrip produces String
    assert_eq!(sql_type_to_arrow(&SqlType::Json), ArrowDataType::Utf8);
    assert_eq!(sql_type_to_arrow(&SqlType::Uuid), ArrowDataType::Utf8);
}

// ── Unsigned integer widening tests ─────────────────────────────────

#[test]
fn test_uint8_widens_to_i16() {
    let result = arrow_to_sql_type(&ArrowDataType::UInt8);
    assert_eq!(
        result,
        SqlType::Integer {
            bits: IntBitWidth::I16
        }
    );
}

#[test]
fn test_uint16_widens_to_i32() {
    let result = arrow_to_sql_type(&ArrowDataType::UInt16);
    assert_eq!(
        result,
        SqlType::Integer {
            bits: IntBitWidth::I32
        }
    );
}

#[test]
fn test_uint32_widens_to_i64() {
    let result = arrow_to_sql_type(&ArrowDataType::UInt32);
    assert_eq!(
        result,
        SqlType::Integer {
            bits: IntBitWidth::I64
        }
    );
}

#[test]
fn test_uint64_widens_to_hugeint() {
    let result = arrow_to_sql_type(&ArrowDataType::UInt64);
    assert_eq!(result, SqlType::HugeInt);
}

// ── Additional type coverage ────────────────────────────────────────

#[test]
fn test_roundtrip_time() {
    let ty = SqlType::Time;
    let arrow = sql_type_to_arrow(&ty);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_roundtrip_interval() {
    let ty = SqlType::Interval;
    let arrow = sql_type_to_arrow(&ty);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_roundtrip_i8() {
    let ty = SqlType::Integer {
        bits: IntBitWidth::I8,
    };
    let arrow = sql_type_to_arrow(&ty);
    assert_eq!(arrow, ArrowDataType::Int8);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_roundtrip_i16() {
    let ty = SqlType::Integer {
        bits: IntBitWidth::I16,
    };
    let arrow = sql_type_to_arrow(&ty);
    assert_eq!(arrow, ArrowDataType::Int16);
    let back = arrow_to_sql_type(&arrow);
    assert_eq!(ty, back);
}

#[test]
fn test_unknown_type_to_arrow() {
    let ty = SqlType::Unknown("anything".to_string());
    let arrow = sql_type_to_arrow(&ty);
    assert_eq!(arrow, ArrowDataType::Utf8, "Unknown types default to Utf8");
}

#[test]
fn test_null_arrow_to_unknown() {
    let result = arrow_to_sql_type(&ArrowDataType::Null);
    assert!(matches!(result, SqlType::Unknown(_)));
}

#[test]
fn test_float16_maps_to_f32() {
    let result = arrow_to_sql_type(&ArrowDataType::Float16);
    assert_eq!(
        result,
        SqlType::Float {
            bits: FloatBitWidth::F32
        }
    );
}

#[test]
fn test_large_utf8_maps_to_string() {
    let result = arrow_to_sql_type(&ArrowDataType::LargeUtf8);
    assert_eq!(result, SqlType::String { max_length: None });
}

#[test]
fn test_date64_maps_to_date() {
    let result = arrow_to_sql_type(&ArrowDataType::Date64);
    assert_eq!(result, SqlType::Date);
}

#[test]
fn test_large_binary_maps_to_binary() {
    let result = arrow_to_sql_type(&ArrowDataType::LargeBinary);
    assert_eq!(result, SqlType::Binary);
}

#[test]
fn test_decimal_default_precision_scale() {
    // No precision/scale → uses defaults 38, 0
    let ty = SqlType::Decimal {
        precision: None,
        scale: None,
    };
    let arrow = sql_type_to_arrow(&ty);
    assert_eq!(arrow, ArrowDataType::Decimal128(38, 0));
}
