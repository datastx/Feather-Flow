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
            let p = precision.unwrap_or(38) as u8;
            let s = scale.unwrap_or(0) as i8;
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
        SqlType::Unknown(_) => ArrowDataType::Utf8, // Default fallback
    }
}

/// Convert an Arrow DataType back to a Feather-Flow SqlType
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
}
