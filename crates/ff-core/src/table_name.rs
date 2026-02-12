//! Strongly-typed table name wrapper.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// Strongly-typed wrapper for table names (potentially schema-qualified like "schema.table").
    ///
    /// Prevents accidental mixing of table names with model names, column names,
    /// or other string types. Guaranteed non-empty after construction.
    pub struct TableName;
}

#[cfg(test)]
#[path = "table_name_test.rs"]
mod tests;
