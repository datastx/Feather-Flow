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
fn test_model_name_try_from_string() {
    let name: ModelName = "my_model".to_string().try_into().unwrap();
    assert_eq!(name.as_str(), "my_model");
}

#[test]
fn test_model_name_try_from_str() {
    let name: ModelName = "my_model".try_into().unwrap();
    assert_eq!(name.as_str(), "my_model");
}

#[test]
fn test_model_name_try_from_empty_fails() {
    let result: Result<ModelName, _> = "".try_into();
    assert!(result.is_err());
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
    assert_eq!(map.get("test"), Some(&42));
}
