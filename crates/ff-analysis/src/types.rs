//! Core type system for SQL analysis
//!
//! Moved from `ir::types` — these types are shared between the DataFusion
//! bridge and analysis passes, and no longer belong inside the deleted IR layer.

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Valid bit widths for integer types
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IntBitWidth {
    /// 8-bit (TINYINT)
    I8,
    /// 16-bit (SMALLINT)
    I16,
    /// 32-bit (INTEGER)
    I32,
    /// 64-bit (BIGINT)
    I64,
}

impl std::fmt::Display for IntBitWidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntBitWidth::I8 => write!(f, "8"),
            IntBitWidth::I16 => write!(f, "16"),
            IntBitWidth::I32 => write!(f, "32"),
            IntBitWidth::I64 => write!(f, "64"),
        }
    }
}

/// Valid bit widths for floating-point types
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FloatBitWidth {
    /// 32-bit (FLOAT / REAL)
    F32,
    /// 64-bit (DOUBLE)
    F64,
}

impl std::fmt::Display for FloatBitWidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatBitWidth::F32 => write!(f, "32"),
            FloatBitWidth::F64 => write!(f, "64"),
        }
    }
}

/// SQL data types normalized from sqlparser's DataType
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlType {
    /// Boolean (BOOL, BOOLEAN)
    Boolean,
    /// Integer types: TINYINT(8), SMALLINT(16), INT(32), BIGINT(64)
    Integer { bits: IntBitWidth },
    /// 128-bit integer (DuckDB HUGEINT)
    HugeInt,
    /// Floating-point: FLOAT(32), DOUBLE(64)
    Float { bits: FloatBitWidth },
    /// Exact numeric with optional precision and scale
    Decimal {
        precision: Option<u16>,
        scale: Option<u16>,
    },
    /// Character/string types with optional max length
    String { max_length: Option<u32> },
    /// DATE
    Date,
    /// TIME
    Time,
    /// TIMESTAMP / DATETIME
    Timestamp,
    /// INTERVAL
    Interval,
    /// BINARY / BLOB
    Binary,
    /// JSON (DuckDB native JSON type)
    Json,
    /// UUID
    Uuid,
    /// Array/List type (DuckDB INTEGER[], VARCHAR[], etc.)
    Array(Box<SqlType>),
    /// Struct type (DuckDB STRUCT(name VARCHAR, age INT))
    Struct(Vec<(String, SqlType)>),
    /// Map type (DuckDB MAP(key_type, value_type))
    Map {
        key: Box<SqlType>,
        value: Box<SqlType>,
    },
    /// Type could not be determined; carries a reason
    Unknown(String),
}

impl SqlType {
    /// Returns true if this is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            SqlType::Integer { .. }
                | SqlType::HugeInt
                | SqlType::Float { .. }
                | SqlType::Decimal { .. }
        )
    }

    /// Returns true if this is a string type
    pub fn is_string(&self) -> bool {
        matches!(self, SqlType::String { .. })
    }

    /// Returns true if this type is unknown
    pub fn is_unknown(&self) -> bool {
        matches!(self, SqlType::Unknown(_))
    }

    /// Check if two types are compatible (e.g. for UNION columns or join keys).
    ///
    /// Numeric types (Integer, Float, Decimal) are mutually compatible,
    /// String types are compatible with each other, Date/Timestamp are
    /// compatible, and Unknown is compatible with anything.
    pub fn is_compatible_with(&self, other: &SqlType) -> bool {
        if self.is_unknown() || other.is_unknown() {
            return true;
        }
        // Numeric family: all numeric types are mutually compatible
        if self.is_numeric() && other.is_numeric() {
            return true;
        }
        matches!(
            (self, other),
            (SqlType::Boolean, SqlType::Boolean)
                | (SqlType::String { .. }, SqlType::String { .. })
                | (SqlType::Date, SqlType::Date)
                | (SqlType::Time, SqlType::Time)
                | (SqlType::Timestamp, SqlType::Timestamp)
                | (SqlType::Date, SqlType::Timestamp)
                | (SqlType::Timestamp, SqlType::Date)
                | (SqlType::Binary, SqlType::Binary)
                | (SqlType::Json, SqlType::Json)
                | (SqlType::Json, SqlType::String { .. })
                | (SqlType::String { .. }, SqlType::Json)
                | (SqlType::Uuid, SqlType::Uuid)
                | (SqlType::Uuid, SqlType::String { .. })
                | (SqlType::String { .. }, SqlType::Uuid)
                | (SqlType::Interval, SqlType::Interval)
        ) || matches!((self, other),
            (SqlType::Array(a), SqlType::Array(b)) if a.is_compatible_with(b)
        ) || matches!((self, other),
            (SqlType::Struct(a), SqlType::Struct(b))
                if a.len() == b.len() && a.iter().zip(b.iter()).all(|((_, ta), (_, tb))| ta.is_compatible_with(tb))
        ) || matches!((self, other),
            (SqlType::Map { key: k1, value: v1 }, SqlType::Map { key: k2, value: v2 })
                if k1.is_compatible_with(k2) && v1.is_compatible_with(v2)
        )
    }

    /// Human-readable display name
    pub fn display_name(&self) -> Cow<'static, str> {
        match self {
            SqlType::Boolean => Cow::Borrowed("BOOLEAN"),
            SqlType::Integer {
                bits: IntBitWidth::I8,
            } => Cow::Borrowed("TINYINT"),
            SqlType::Integer {
                bits: IntBitWidth::I16,
            } => Cow::Borrowed("SMALLINT"),
            SqlType::Integer {
                bits: IntBitWidth::I32,
            } => Cow::Borrowed("INTEGER"),
            SqlType::Integer {
                bits: IntBitWidth::I64,
            } => Cow::Borrowed("BIGINT"),
            SqlType::HugeInt => Cow::Borrowed("HUGEINT"),
            SqlType::Float {
                bits: FloatBitWidth::F32,
            } => Cow::Borrowed("FLOAT"),
            SqlType::Float {
                bits: FloatBitWidth::F64,
            } => Cow::Borrowed("DOUBLE"),
            SqlType::Decimal {
                precision: Some(p),
                scale: Some(s),
            } => Cow::Owned(format!("DECIMAL({p},{s})")),
            SqlType::Decimal {
                precision: Some(p), ..
            } => Cow::Owned(format!("DECIMAL({p})")),
            SqlType::Decimal { .. } => Cow::Borrowed("DECIMAL"),
            SqlType::String {
                max_length: Some(n),
            } => Cow::Owned(format!("VARCHAR({n})")),
            SqlType::String { .. } => Cow::Borrowed("VARCHAR"),
            SqlType::Date => Cow::Borrowed("DATE"),
            SqlType::Time => Cow::Borrowed("TIME"),
            SqlType::Timestamp => Cow::Borrowed("TIMESTAMP"),
            SqlType::Interval => Cow::Borrowed("INTERVAL"),
            SqlType::Binary => Cow::Borrowed("BINARY"),
            SqlType::Json => Cow::Borrowed("JSON"),
            SqlType::Uuid => Cow::Borrowed("UUID"),
            SqlType::Array(inner) => Cow::Owned(format!("{}[]", inner.display_name())),
            SqlType::Struct(fields) => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|(name, ty)| format!("{} {}", name, ty.display_name()))
                    .collect();
                Cow::Owned(format!("STRUCT({})", field_strs.join(", ")))
            }
            SqlType::Map { key, value } => Cow::Owned(format!(
                "MAP({}, {})",
                key.display_name(),
                value.display_name()
            )),
            SqlType::Unknown(reason) => Cow::Owned(format!("UNKNOWN({reason})")),
        }
    }
}

impl std::fmt::Display for SqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// Nullability state of a column or expression
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Nullability {
    /// Guaranteed not null
    NotNull,
    /// May contain nulls
    Nullable,
    /// Nullability could not be determined
    Unknown,
}

impl Nullability {
    /// Combine two nullability states: if either is nullable, result is nullable
    pub fn combine(self, other: Nullability) -> Nullability {
        match (self, other) {
            (Nullability::Nullable, _) | (_, Nullability::Nullable) => Nullability::Nullable,
            (Nullability::Unknown, _) | (_, Nullability::Unknown) => Nullability::Unknown,
            _ => Nullability::NotNull,
        }
    }
}

impl std::fmt::Display for Nullability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Nullability::NotNull => write!(f, "NOT NULL"),
            Nullability::Nullable => write!(f, "NULLABLE"),
            Nullability::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// A column with type and provenance information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedColumn {
    /// Column name
    pub name: String,
    /// Source table this column came from (for qualified wildcard expansion)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_table: Option<String>,
    /// Inferred or declared SQL type
    pub sql_type: SqlType,
    /// Nullability state
    pub nullability: Nullability,
    /// Where this column originated
    pub provenance: Vec<ColumnProvenance>,
}

/// Tracks where a column value originated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProvenance {
    /// Source table or model name
    pub source_table: String,
    /// Source column name
    pub source_column: String,
    /// Whether this is a direct pass-through (vs computed/transformed)
    pub is_direct: bool,
}

/// Parse a raw bit-width integer into an IntBitWidth, if valid
fn parse_int_bit_width(bits: u16) -> Option<IntBitWidth> {
    match bits {
        8 => Some(IntBitWidth::I8),
        16 => Some(IntBitWidth::I16),
        32 => Some(IntBitWidth::I32),
        64 => Some(IntBitWidth::I64),
        _ => None,
    }
}

/// Parse a raw bit-width integer into a FloatBitWidth, if valid
fn parse_float_bit_width(bits: u16) -> Option<FloatBitWidth> {
    match bits {
        32 => Some(FloatBitWidth::F32),
        64 => Some(FloatBitWidth::F64),
        _ => None,
    }
}

/// Parse a SQL type string (from YAML `data_type` or sqlparser) into SqlType
pub fn parse_sql_type(s: &str) -> SqlType {
    let upper = s.trim().to_uppercase();
    let upper = upper.as_str();

    match upper {
        "BOOL" | "BOOLEAN" => SqlType::Boolean,

        "TINYINT" | "INT1" => SqlType::Integer {
            bits: IntBitWidth::I8,
        },
        "SMALLINT" | "INT2" => SqlType::Integer {
            bits: IntBitWidth::I16,
        },
        "INT" | "INTEGER" | "INT4" => SqlType::Integer {
            bits: IntBitWidth::I32,
        },
        "BIGINT" | "INT8" | "LONG" => SqlType::Integer {
            bits: IntBitWidth::I64,
        },
        "HUGEINT" | "INT16" | "INT128" => SqlType::HugeInt,

        "FLOAT" | "REAL" | "FLOAT4" => SqlType::Float {
            bits: FloatBitWidth::F32,
        },
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => SqlType::Float {
            bits: FloatBitWidth::F64,
        },

        "DECIMAL" | "NUMERIC" => SqlType::Decimal {
            precision: None,
            scale: None,
        },

        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "CHARACTER VARYING" => {
            SqlType::String { max_length: None }
        }

        "DATE" => SqlType::Date,
        "TIME" => SqlType::Time,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => SqlType::Timestamp,
        "INTERVAL" => SqlType::Interval,
        "BLOB" | "BINARY" | "BYTEA" | "VARBINARY" => SqlType::Binary,

        "JSON" | "JSONB" => SqlType::Json,
        "UUID" => SqlType::Uuid,

        _ => {
            if let Some(inner) = try_parse_parameterized(s) {
                return inner;
            }
            SqlType::Unknown(s.to_string())
        }
    }
}

/// Try to parse parameterized type strings like `VARCHAR(255)`, `DECIMAL(10,2)`,
/// `INTEGER[]`, `STRUCT(name VARCHAR, age INT)`, `MAP(VARCHAR, INTEGER)`
fn try_parse_parameterized(s: &str) -> Option<SqlType> {
    let trimmed = s.trim();
    let upper = trimmed.to_uppercase();

    if upper.ends_with("[]") {
        let inner_str = &trimmed[..trimmed.len() - 2];
        let inner_type = parse_sql_type(inner_str);
        return Some(SqlType::Array(Box::new(inner_type)));
    }

    if upper.starts_with("STRUCT(") && upper.ends_with(')') {
        let inner = &trimmed[7..trimmed.len() - 1]; // between STRUCT( and )
        return parse_struct_fields(inner);
    }

    if upper.starts_with("MAP(") && upper.ends_with(')') {
        let inner = &trimmed[4..trimmed.len() - 1]; // between MAP( and )
        let parts = split_top_level(inner, ',');
        if parts.len() == 2 {
            let key_type = parse_sql_type(parts[0].trim());
            let value_type = parse_sql_type(parts[1].trim());
            return Some(SqlType::Map {
                key: Box::new(key_type),
                value: Box::new(value_type),
            });
        }
    }

    let open = upper.find('(')?;
    let close = upper.rfind(')')?;
    let base = upper[..open].trim();
    let params = &upper[open + 1..close];

    match base {
        "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "TEXT" => {
            let len: u32 = params.trim().parse().ok()?;
            Some(SqlType::String {
                max_length: Some(len),
            })
        }
        "DECIMAL" | "NUMERIC" => {
            let parts: Vec<&str> = params.split(',').collect();
            let precision: u16 = parts.first()?.trim().parse().ok()?;
            let scale: Option<u16> = parts.get(1).and_then(|s| s.trim().parse().ok());
            Some(SqlType::Decimal {
                precision: Some(precision),
                scale,
            })
        }
        "INT" | "INTEGER" => {
            let raw_bits: u16 = params.trim().parse().ok()?;
            let bits = parse_int_bit_width(raw_bits)?;
            Some(SqlType::Integer { bits })
        }
        "FLOAT" => {
            let raw_bits: u16 = params.trim().parse().ok()?;
            let bits = parse_float_bit_width(raw_bits)?;
            Some(SqlType::Float { bits })
        }
        _ => None,
    }
}

/// Parse STRUCT fields like "name VARCHAR, age INT"
fn parse_struct_fields(s: &str) -> Option<SqlType> {
    let parts = split_top_level(s, ',');
    let mut fields = Vec::new();
    for part in parts {
        let part = part.trim();
        // Split on first whitespace to get "name TYPE"
        let space_pos = part.find(|c: char| c.is_ascii_whitespace())?;
        let name = part[..space_pos].trim().to_string();
        let type_str = part[space_pos..].trim();
        let sql_type = parse_sql_type(type_str);
        fields.push((name, sql_type));
    }
    if fields.is_empty() {
        return None;
    }
    Some(SqlType::Struct(fields))
}

/// Split a string on a delimiter, but only at the top level (not inside parentheses)
fn split_top_level(s: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth: usize = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    log::warn!("Unbalanced closing parenthesis in type string: {:?}", s);
                } else {
                    depth -= 1;
                }
            }
            c if c == delimiter && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

#[cfg(test)]
mod tests {
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
}
