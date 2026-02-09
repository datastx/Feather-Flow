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

    /// Find a column by qualified name (table.column), case-insensitive.
    ///
    /// If `source_table` metadata is available, filters by it first.
    /// Falls back to column-name-only lookup when table info is missing.
    pub fn find_qualified(&self, table: &str, column: &str) -> Option<&TypedColumn> {
        let lower_table = table.to_lowercase();
        let lower_col = column.to_lowercase();

        // Try to find a column that matches both table and name
        let qualified_match = self.columns.iter().find(|c| {
            c.name.to_lowercase() == lower_col
                && c.source_table
                    .as_ref()
                    .is_some_and(|t| t.to_lowercase() == lower_table)
        });

        // Fall back to column-name-only if no qualified match
        qualified_match.or_else(|| self.find_column(column))
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
                    source_table: c.source_table.clone(),
                    sql_type: c.sql_type.clone(),
                    nullability,
                    provenance: c.provenance.clone(),
                })
                .collect(),
        }
    }

    /// Return a new schema with all columns tagged with the given source table name.
    ///
    /// Existing `source_table` values are preserved; only `None` values are filled.
    pub fn with_source_table(&self, table: &str) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .map(|c| TypedColumn {
                    name: c.name.clone(),
                    source_table: c.source_table.clone().or_else(|| Some(table.to_string())),
                    sql_type: c.sql_type.clone(),
                    nullability: c.nullability,
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
    use crate::ir::types::{IntBitWidth, Nullability, SqlType};
    use crate::test_utils::make_col;

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
}
