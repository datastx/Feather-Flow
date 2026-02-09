//! Core type system for the SQL IR

use serde::{Deserialize, Serialize};

/// SQL data types normalized from sqlparser's DataType
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlType {
    /// Boolean (BOOL, BOOLEAN)
    Boolean,
    /// Integer types: TINYINT(8), SMALLINT(16), INT(32), BIGINT(64)
    Integer { bits: u16 },
    /// Floating-point: FLOAT(32), DOUBLE(64)
    Float { bits: u16 },
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
    /// Type could not be determined; carries a reason
    Unknown(String),
}

impl SqlType {
    /// Returns true if this is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            SqlType::Integer { .. } | SqlType::Float { .. } | SqlType::Decimal { .. }
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

    /// Human-readable display name
    pub fn display_name(&self) -> String {
        match self {
            SqlType::Boolean => "BOOLEAN".into(),
            SqlType::Integer { bits: 8 } => "TINYINT".into(),
            SqlType::Integer { bits: 16 } => "SMALLINT".into(),
            SqlType::Integer { bits: 32 } => "INTEGER".into(),
            SqlType::Integer { bits: 64 } => "BIGINT".into(),
            SqlType::Integer { bits } => format!("INTEGER({bits})"),
            SqlType::Float { bits: 32 } => "FLOAT".into(),
            SqlType::Float { bits: 64 } => "DOUBLE".into(),
            SqlType::Float { bits } => format!("FLOAT({bits})"),
            SqlType::Decimal {
                precision: Some(p),
                scale: Some(s),
            } => format!("DECIMAL({p},{s})"),
            SqlType::Decimal {
                precision: Some(p), ..
            } => format!("DECIMAL({p})"),
            SqlType::Decimal { .. } => "DECIMAL".into(),
            SqlType::String {
                max_length: Some(n),
            } => format!("VARCHAR({n})"),
            SqlType::String { .. } => "VARCHAR".into(),
            SqlType::Date => "DATE".into(),
            SqlType::Time => "TIME".into(),
            SqlType::Timestamp => "TIMESTAMP".into(),
            SqlType::Interval => "INTERVAL".into(),
            SqlType::Binary => "BINARY".into(),
            SqlType::Unknown(reason) => format!("UNKNOWN({reason})"),
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

/// Parse a SQL type string (from YAML `data_type` or sqlparser) into SqlType
pub fn parse_sql_type(s: &str) -> SqlType {
    let upper = s.trim().to_uppercase();
    let upper = upper.as_str();

    match upper {
        "BOOL" | "BOOLEAN" => SqlType::Boolean,

        "TINYINT" | "INT1" => SqlType::Integer { bits: 8 },
        "SMALLINT" | "INT2" => SqlType::Integer { bits: 16 },
        "INT" | "INTEGER" | "INT4" => SqlType::Integer { bits: 32 },
        "BIGINT" | "INT8" | "LONG" => SqlType::Integer { bits: 64 },
        "HUGEINT" | "INT16" => SqlType::Integer { bits: 128 },

        "FLOAT" | "REAL" | "FLOAT4" => SqlType::Float { bits: 32 },
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => SqlType::Float { bits: 64 },

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

        _ => {
            // Try to parse parameterized types like VARCHAR(255), DECIMAL(10,2)
            if let Some(inner) = try_parse_parameterized(s) {
                return inner;
            }
            SqlType::Unknown(s.to_string())
        }
    }
}

/// Try to parse parameterized type strings like `VARCHAR(255)`, `DECIMAL(10,2)`
fn try_parse_parameterized(s: &str) -> Option<SqlType> {
    let upper = s.trim().to_uppercase();
    let open = upper.find('(')?;
    let close = upper.find(')')?;
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
            let bits: u16 = params.trim().parse().ok()?;
            Some(SqlType::Integer { bits })
        }
        "FLOAT" => {
            let bits: u16 = params.trim().parse().ok()?;
            Some(SqlType::Float { bits })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_types() {
        assert_eq!(parse_sql_type("boolean"), SqlType::Boolean);
        assert_eq!(parse_sql_type("INT"), SqlType::Integer { bits: 32 });
        assert_eq!(parse_sql_type("bigint"), SqlType::Integer { bits: 64 });
        assert_eq!(parse_sql_type("FLOAT"), SqlType::Float { bits: 32 });
        assert_eq!(parse_sql_type("DOUBLE"), SqlType::Float { bits: 64 });
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
        let result = parse_sql_type("JSONB");
        assert!(matches!(result, SqlType::Unknown(_)));
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
        assert_eq!(SqlType::Integer { bits: 32 }.display_name(), "INTEGER");
        assert_eq!(SqlType::Float { bits: 64 }.display_name(), "DOUBLE");
        assert_eq!(
            SqlType::String {
                max_length: Some(100)
            }
            .display_name(),
            "VARCHAR(100)"
        );
    }
}
