//! Relation schema — ordered list of typed columns per operator node

use super::types::{Nullability, TypedColumn};
use serde::{Deserialize, Serialize};

/// Schema of a relational operator's output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelSchema {
    /// Ordered list of output columns
    pub columns: Vec<TypedColumn>,
}

impl RelSchema {
    /// Create an empty schema
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Create a schema from a list of typed columns
    pub fn new(columns: Vec<TypedColumn>) -> Self {
        Self { columns }
    }

    /// Find a column by name (case-insensitive)
    pub fn find_column(&self, name: &str) -> Option<&TypedColumn> {
        let lower = name.to_lowercase();
        self.columns.iter().find(|c| c.name.to_lowercase() == lower)
    }

    /// Find a column by qualified name (table.column), case-insensitive
    pub fn find_qualified(&self, _table: &str, column: &str) -> Option<&TypedColumn> {
        // In our IR, columns are flattened — we look up by column name
        // Table qualification is used for disambiguation in the lowering phase
        self.find_column(column)
    }

    /// Merge two schemas (e.g. for JOIN output)
    pub fn merge(left: &RelSchema, right: &RelSchema) -> Self {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.iter().cloned());
        Self { columns }
    }

    /// Return a new schema with all columns set to the given nullability
    pub fn with_nullability(&self, nullability: Nullability) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .map(|c| TypedColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type.clone(),
                    nullability,
                    provenance: c.provenance.clone(),
                })
                .collect(),
        }
    }

    /// Number of columns
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the schema has no columns
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get column names as a vec
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

impl Default for RelSchema {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::types::{Nullability, SqlType};

    fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            sql_type: ty,
            nullability: null,
            provenance: vec![],
        }
    }

    #[test]
    fn test_find_column() {
        let schema = RelSchema::new(vec![
            make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
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
            SqlType::Integer { bits: 32 },
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
            SqlType::Integer { bits: 32 },
            Nullability::NotNull,
        )]);
        let nullable = schema.with_nullability(Nullability::Nullable);
        assert_eq!(nullable.columns[0].nullability, Nullability::Nullable);
    }
}
