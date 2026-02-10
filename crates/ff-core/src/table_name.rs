//! Strongly-typed table name wrapper.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

/// Strongly-typed wrapper for table names (potentially schema-qualified like "schema.table").
///
/// Prevents accidental mixing of table names with model names, column names,
/// or other string types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableName(String);

impl TableName {
    /// Create a new `TableName`, panicking in debug builds if the name is empty.
    ///
    /// Prefer [`try_new`](Self::try_new) when handling untrusted input.
    pub fn new(name: impl Into<String>) -> Self {
        let s = name.into();
        debug_assert!(!s.is_empty(), "TableName must not be empty");
        Self(s)
    }

    /// Try to create a new `TableName`, returning `None` if the name is empty.
    pub fn try_new(name: impl Into<String>) -> Option<Self> {
        let s = name.into();
        if s.is_empty() {
            None
        } else {
            Some(Self(s))
        }
    }

    /// Return the underlying name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the wrapper and return the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for TableName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for TableName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<String> for TableName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TableName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for TableName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for TableName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_creation() {
        let name = TableName::new("raw.orders");
        assert_eq!(name.as_str(), "raw.orders");
    }

    #[test]
    fn test_table_name_display() {
        let name = TableName::new("raw.orders");
        assert_eq!(format!("{}", name), "raw.orders");
    }

    #[test]
    fn test_table_name_deref() {
        let name = TableName::new("raw.orders");
        assert_eq!(&*name, "raw.orders");
        // Can call str methods via Deref
        assert!(name.starts_with("raw."));
    }

    #[test]
    fn test_table_name_equality() {
        let name = TableName::new("raw.orders");
        assert_eq!(name, "raw.orders");
        assert_eq!(name, *"raw.orders");
        assert_eq!(name, "raw.orders".to_string());
    }

    #[test]
    fn test_table_name_from_string() {
        let name: TableName = "raw.orders".to_string().into();
        assert_eq!(name.as_str(), "raw.orders");
    }

    #[test]
    fn test_table_name_from_str() {
        let name: TableName = "raw.orders".into();
        assert_eq!(name.as_str(), "raw.orders");
    }

    #[test]
    fn test_table_name_into_inner() {
        let name = TableName::new("raw.orders");
        let s: String = name.into_inner();
        assert_eq!(s, "raw.orders");
    }

    #[test]
    fn test_table_name_clone() {
        let name = TableName::new("raw.orders");
        let cloned = name.clone();
        assert_eq!(name, cloned);
    }

    #[test]
    fn test_table_name_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TableName::new("a"));
        set.insert(TableName::new("b"));
        set.insert(TableName::new("a")); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_table_name_ord() {
        let a = TableName::new("alpha");
        let b = TableName::new("beta");
        assert!(a < b);
    }

    #[test]
    fn test_table_name_serde_roundtrip() {
        let name = TableName::new("raw.orders");
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, r#""raw.orders""#);
        let deserialized: TableName = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, name);
    }

    #[test]
    fn test_table_name_borrow() {
        use std::collections::HashMap;
        let mut map: HashMap<TableName, i32> = HashMap::new();
        map.insert(TableName::new("test"), 42);
        // Can look up by &str thanks to Borrow<str>
        assert_eq!(map.get("test"), Some(&42));
    }

    #[test]
    fn test_table_name_schema_qualified() {
        let name = TableName::new("schema.table");
        assert!(name.contains('.'));
        assert_eq!(name.split('.').count(), 2);
    }
}
