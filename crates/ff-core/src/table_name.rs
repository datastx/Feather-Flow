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
    fn test_table_name_try_from_string() {
        let name: TableName = "raw.orders".to_string().try_into().unwrap();
        assert_eq!(name.as_str(), "raw.orders");
    }

    #[test]
    fn test_table_name_try_from_str() {
        let name: TableName = "raw.orders".try_into().unwrap();
        assert_eq!(name.as_str(), "raw.orders");
    }

    #[test]
    fn test_table_name_try_from_empty_fails() {
        let result: Result<TableName, _> = "".try_into();
        assert!(result.is_err());
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
        assert_eq!(map.get("test"), Some(&42));
    }

    #[test]
    fn test_table_name_schema_qualified() {
        let name = TableName::new("schema.table");
        assert!(name.contains('.'));
        assert_eq!(name.split('.').count(), 2);
    }
}
