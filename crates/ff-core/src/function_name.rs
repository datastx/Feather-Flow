//! Strongly-typed function name wrapper.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

/// Strongly-typed wrapper for user-defined function names.
///
/// Prevents accidental mixing of function names with model names, table names,
/// or other string types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FunctionName(String);

impl FunctionName {
    /// Create a new `FunctionName` from any type that can be converted into a `String`.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
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

impl fmt::Display for FunctionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for FunctionName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for FunctionName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for FunctionName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<String> for FunctionName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for FunctionName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl PartialEq<str> for FunctionName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for FunctionName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for FunctionName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
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
    fn test_function_name_from_string() {
        let name: FunctionName = "safe_divide".to_string().into();
        assert_eq!(name.as_str(), "safe_divide");
    }

    #[test]
    fn test_function_name_from_str() {
        let name: FunctionName = "safe_divide".into();
        assert_eq!(name.as_str(), "safe_divide");
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
