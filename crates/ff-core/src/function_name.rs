//! Strongly-typed function name wrapper.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// Strongly-typed wrapper for user-defined function names.
    ///
    /// Prevents accidental mixing of function names with model names, table names,
    /// or other string types. Guaranteed non-empty after construction.
    pub struct FunctionName;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_name_creation() {
        let name = FunctionName::new("safe_divide");
        assert_eq!(name.as_str(), "safe_divide");
    }

    #[test]
    fn test_function_name_display() {
        let name = FunctionName::new("safe_divide");
        assert_eq!(format!("{}", name), "safe_divide");
    }

    #[test]
    fn test_function_name_deref() {
        let name = FunctionName::new("safe_divide");
        assert_eq!(&*name, "safe_divide");
        assert!(name.starts_with("safe_"));
    }

    #[test]
    fn test_function_name_equality() {
        let name = FunctionName::new("safe_divide");
        assert_eq!(name, "safe_divide");
        assert_eq!(name, *"safe_divide");
        assert_eq!(name, "safe_divide".to_string());
    }

    #[test]
    fn test_function_name_try_from_string() {
        let name: FunctionName = "safe_divide".to_string().try_into().unwrap();
        assert_eq!(name.as_str(), "safe_divide");
    }

    #[test]
    fn test_function_name_try_from_str() {
        let name: FunctionName = "safe_divide".try_into().unwrap();
        assert_eq!(name.as_str(), "safe_divide");
    }

    #[test]
    fn test_function_name_try_from_empty_fails() {
        let result: Result<FunctionName, _> = "".try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_function_name_into_inner() {
        let name = FunctionName::new("safe_divide");
        let s: String = name.into_inner();
        assert_eq!(s, "safe_divide");
    }

    #[test]
    fn test_function_name_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(FunctionName::new("a"));
        set.insert(FunctionName::new("b"));
        set.insert(FunctionName::new("a")); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_function_name_ord() {
        let a = FunctionName::new("alpha");
        let b = FunctionName::new("beta");
        assert!(a < b);
    }

    #[test]
    fn test_function_name_serde_roundtrip() {
        let name = FunctionName::new("safe_divide");
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, r#""safe_divide""#);
        let deserialized: FunctionName = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, name);
    }

    #[test]
    fn test_function_name_borrow() {
        use std::collections::HashMap;
        let mut map: HashMap<FunctionName, i32> = HashMap::new();
        map.insert(FunctionName::new("test"), 42);
        assert_eq!(map.get("test"), Some(&42));
    }
}
