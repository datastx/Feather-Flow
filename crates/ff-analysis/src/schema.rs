//! Relation schema — ordered list of typed columns per operator node
//!
//! Moved from `ir::schema` — used by the DataFusion bridge and analysis
//! passes, independent of the deleted custom IR.

use crate::types::{Nullability, TypedColumn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Schema catalog: maps table/model names to known schemas.
///
/// Values are `Arc`-wrapped so that cloning the catalog (e.g. during
/// schema propagation) is cheap — only the reference count is bumped,
/// not the column vectors inside each `RelSchema`.
pub type SchemaCatalog = HashMap<String, Arc<RelSchema>>;

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
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Find a column by qualified name (table.column), case-insensitive.
    ///
    /// If `source_table` metadata is available, filters by it first.
    /// Falls back to column-name-only lookup when table info is missing.
    pub fn find_qualified(&self, table: &str, column: &str) -> Option<&TypedColumn> {
        // Try to find a column that matches both table and name
        let qualified_match = self.columns.iter().find(|c| {
            c.name.eq_ignore_ascii_case(column)
                && c.source_table
                    .as_ref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(table))
        });

        // Fall back to column-name-only if no qualified match
        qualified_match.or_else(|| self.find_column(column))
    }

    /// Merge two schemas (e.g. for JOIN output)
    pub fn merge(left: &RelSchema, right: &RelSchema) -> Self {
        let mut columns = Vec::with_capacity(left.columns.len() + right.columns.len());
        columns.extend(left.columns.iter().cloned());
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
                    nullability,
                    ..c.clone()
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
                    source_table: c.source_table.clone().or_else(|| Some(table.to_string())),
                    ..c.clone()
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
#[path = "schema_test.rs"]
mod tests;
