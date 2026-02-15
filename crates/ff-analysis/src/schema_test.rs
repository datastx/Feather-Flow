use super::*;
use crate::types::{IntBitWidth, Nullability, SqlType};

fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
    TypedColumn {
        name: name.to_string(),
        source_table: None,
        sql_type: ty,
        nullability: null,
        provenance: vec![],
    }
}

#[test]
fn test_find_column() {
    let schema = RelSchema::new(vec![
        make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        ),
        make_col(
            "name",
            SqlType::String { max_length: None },
            Nullability::Nullable,
        ),
    ]);

    assert!(schema.find_column("id").is_some());
    assert!(schema.find_column("ID").is_some()); // case-insensitive
    assert!(schema.find_column("missing").is_none());
}

#[test]
fn test_merge_schemas() {
    let left = RelSchema::new(vec![make_col(
        "a",
        SqlType::Integer {
            bits: IntBitWidth::I32,
        },
        Nullability::NotNull,
    )]);
    let right = RelSchema::new(vec![make_col(
        "b",
        SqlType::String { max_length: None },
        Nullability::Nullable,
    )]);
    let merged = RelSchema::merge(&left, &right);
    assert_eq!(merged.len(), 2);
    assert!(merged.find_column("a").is_some());
    assert!(merged.find_column("b").is_some());
}

#[test]
fn test_with_nullability() {
    let schema = RelSchema::new(vec![make_col(
        "id",
        SqlType::Integer {
            bits: IntBitWidth::I32,
        },
        Nullability::NotNull,
    )]);
    let nullable = schema.with_nullability(Nullability::Nullable);
    assert_eq!(nullable.columns[0].nullability, Nullability::Nullable);
}
