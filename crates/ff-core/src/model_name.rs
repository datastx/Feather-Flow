//! Strongly-typed model name wrapper.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

/// Strongly-typed wrapper for model names.
///
/// Prevents accidental mixing of model names with table names, column names,
/// or other string types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelName(String);

impl ModelName {
    /// Create a new `ModelName`, panicking in debug builds if the name is empty.
    ///
    /// Prefer [`try_new`](Self::try_new) when handling untrusted input.
    pub fn new(name: impl Into<String>) -> Self {
        let s = name.into();
        debug_assert!(!s.is_empty(), "ModelName must not be empty");
        Self(s)
    }

    /// Try to create a new `ModelName`, returning `None` if the name is empty.
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

impl fmt::Display for ModelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ModelName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for ModelName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for ModelName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<String> for ModelName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ModelName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl PartialEq<str> for ModelName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for ModelName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for ModelName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_name_creation() {
        let name = ModelName::new("my_model");
        assert_eq!(name.as_str(), "my_model");
    }

    #[test]
    fn test_model_name_display() {
        let name = ModelName::new("my_model");
        assert_eq!(format!("{}", name), "my_model");
    }

    #[test]
    fn test_model_name_deref() {
        let name = ModelName::new("my_model");
        assert_eq!(&*name, "my_model");
        // Can call str methods via Deref
        assert!(name.starts_with("my_"));
    }

    #[test]
    fn test_model_name_equality() {
        let name = ModelName::new("my_model");
        assert_eq!(name, "my_model");
        assert_eq!(name, *"my_model");
        assert_eq!(name, "my_model".to_string());
    }

    #[test]
    fn test_model_name_from_string() {
        let name: ModelName = "my_model".to_string().into();
        assert_eq!(name.as_str(), "my_model");
    }

    #[test]
    fn test_model_name_from_str() {
        let name: ModelName = "my_model".into();
        assert_eq!(name.as_str(), "my_model");
    }

    #[test]
    fn test_model_name_into_inner() {
        let name = ModelName::new("my_model");
        let s: String = name.into_inner();
        assert_eq!(s, "my_model");
    }

    #[test]
    fn test_model_name_clone() {
        let name = ModelName::new("my_model");
        let cloned = name.clone();
        assert_eq!(name, cloned);
    }

    #[test]
    fn test_model_name_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ModelName::new("a"));
        set.insert(ModelName::new("b"));
        set.insert(ModelName::new("a")); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_model_name_ord() {
        let a = ModelName::new("alpha");
        let b = ModelName::new("beta");
        assert!(a < b);
    }

    #[test]
    fn test_model_name_serde_roundtrip() {
        let name = ModelName::new("my_model");
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, r#""my_model""#);
        let deserialized: ModelName = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, name);
    }

    #[test]
    fn test_model_name_borrow() {
        use std::collections::HashMap;
        let mut map: HashMap<ModelName, i32> = HashMap::new();
        map.insert(ModelName::new("test"), 42);
        // Can look up by &str thanks to Borrow<str>
        assert_eq!(map.get("test"), Some(&42));
    }
}
