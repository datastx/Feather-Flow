//! Conversion between Feather-Flow SqlType and Arrow DataType

use crate::ir::types::{FloatBitWidth, IntBitWidth, SqlType};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields};

/// Convert a Feather-Flow SqlType to an Arrow DataType
pub fn sql_type_to_arrow(sql_type: &SqlType) -> ArrowDataType {
    match sql_type {
        SqlType::Boolean => ArrowDataType::Boolean,
        SqlType::Integer {
            bits: IntBitWidth::I8,
        } => ArrowDataType::Int8,
        SqlType::Integer {
            bits: IntBitWidth::I16,
        } => ArrowDataType::Int16,
        SqlType::Integer {
            bits: IntBitWidth::I32,
        } => ArrowDataType::Int32,
        SqlType::Integer {
            bits: IntBitWidth::I64,
        } => ArrowDataType::Int64,
        SqlType::HugeInt => ArrowDataType::Decimal128(38, 0),
        SqlType::Float {
            bits: FloatBitWidth::F32,
        } => ArrowDataType::Float32,
        SqlType::Float {
            bits: FloatBitWidth::F64,
        } => ArrowDataType::Float64,
        SqlType::Decimal {
            precision, scale, ..
        } => {
            let p = precision.unwrap_or(38).min(38) as u8;
            let s = scale.unwrap_or(0).min(127) as i8;
            ArrowDataType::Decimal128(p, s)
        }
        SqlType::String { .. } => ArrowDataType::Utf8,
        SqlType::Date => ArrowDataType::Date32,
        SqlType::Time => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        SqlType::Timestamp => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        SqlType::Interval => ArrowDataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
        SqlType::Binary => ArrowDataType::Binary,
        SqlType::Json => ArrowDataType::Utf8, // JSON stored as string in Arrow
        SqlType::Uuid => ArrowDataType::Utf8, // UUID stored as string in Arrow
        SqlType::Array(inner) => {
            let arrow_inner = sql_type_to_arrow(inner);
            ArrowDataType::List(Field::new("item", arrow_inner, true).into())
        }
        SqlType::Struct(fields) => {
            let arrow_fields: Fields = fields
                .iter()
                .map(|(name, ty)| Field::new(name, sql_type_to_arrow(ty), true))
                .collect();
            ArrowDataType::Struct(arrow_fields)
        }
        SqlType::Map { key, value } => {
            let key_field = Field::new("key", sql_type_to_arrow(key), false);
            let value_field = Field::new("value", sql_type_to_arrow(value), true);
            let entries = Field::new(
                "entries",
                ArrowDataType::Struct(Fields::from(vec![key_field, value_field])),
                false,
            );
            ArrowDataType::Map(entries.into(), false)
        }
        SqlType::Unknown(desc) => {
            log::debug!("Mapping Unknown type '{}' to Utf8", desc);
            ArrowDataType::Utf8
        }
    }
}

/// Convert an Arrow DataType back to a Feather-Flow SqlType
///
/// Unsigned integer types are widened to the next larger signed type to
/// guarantee lossless representation (e.g. UInt8 max 255 fits in Int16 but
/// not Int8). The mapping is:
///
/// | Arrow unsigned | SqlType signed |
/// |----------------|----------------|
/// | UInt8 (0..255) | Integer I16    |
/// | UInt16 (0..65535) | Integer I32 |
/// | UInt32 (0..4B)   | Integer I64  |
/// | UInt64 (0..18E18) | HugeInt    |
pub fn arrow_to_sql_type(arrow_type: &ArrowDataType) -> SqlType {
    match arrow_type {
        ArrowDataType::Boolean => SqlType::Boolean,
        ArrowDataType::Int8 => SqlType::Integer {
            bits: IntBitWidth::I8,
        },
        ArrowDataType::Int16 => SqlType::Integer {
            bits: IntBitWidth::I16,
        },
        ArrowDataType::Int32 => SqlType::Integer {
            bits: IntBitWidth::I32,
        },
        ArrowDataType::Int64 => SqlType::Integer {
            bits: IntBitWidth::I64,
        },
        // Unsigned → next-wider signed: UInt8 max 255 doesn't fit in I8 (max 127)
        ArrowDataType::UInt8 => SqlType::Integer {
            bits: IntBitWidth::I16,
        },
        ArrowDataType::UInt16 => SqlType::Integer {
            bits: IntBitWidth::I32,
        },
        ArrowDataType::UInt32 => SqlType::Integer {
            bits: IntBitWidth::I64,
        },
        ArrowDataType::UInt64 => SqlType::HugeInt,
        ArrowDataType::Float16 | ArrowDataType::Float32 => SqlType::Float {
            bits: FloatBitWidth::F32,
        },
        ArrowDataType::Float64 => SqlType::Float {
            bits: FloatBitWidth::F64,
        },
        ArrowDataType::Decimal128(38, 0) => SqlType::HugeInt,
        ArrowDataType::Decimal128(p, s) | ArrowDataType::Decimal256(p, s) => SqlType::Decimal {
            precision: Some(*p as u16),
            scale: Some((*s).max(0) as u16),
        },
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            SqlType::String { max_length: None }
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => SqlType::Date,
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => SqlType::Time,
        ArrowDataType::Timestamp(_, _) => SqlType::Timestamp,
        ArrowDataType::Interval(_) => SqlType::Interval,
        ArrowDataType::Duration(_) => SqlType::Interval,
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
            SqlType::Binary
        }
        ArrowDataType::FixedSizeBinary(_) => SqlType::Binary,
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            let inner = arrow_to_sql_type(field.data_type());
            SqlType::Array(Box::new(inner))
        }
        ArrowDataType::Struct(fields) => {
            let sql_fields: Vec<(String, SqlType)> = fields
                .iter()
                .map(|f| (f.name().clone(), arrow_to_sql_type(f.data_type())))
                .collect();
            SqlType::Struct(sql_fields)
        }
        ArrowDataType::Map(field, _) => {
            if let ArrowDataType::Struct(entries) = field.data_type() {
                if entries.len() == 2 {
                    let key = arrow_to_sql_type(entries[0].data_type());
                    let value = arrow_to_sql_type(entries[1].data_type());
                    return SqlType::Map {
                        key: Box::new(key),
                        value: Box::new(value),
                    };
                }
            }
            SqlType::Unknown("MAP".to_string())
        }
        ArrowDataType::Null => SqlType::Unknown("NULL".to_string()),
        _ => SqlType::Unknown(format!("{arrow_type:?}")),
    }
}

#[cfg(test)]
mod tests {
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
}
