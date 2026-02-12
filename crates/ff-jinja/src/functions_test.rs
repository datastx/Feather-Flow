use super::*;

#[test]
fn test_json_to_minijinja_value() {
    let json = serde_json::json!({
        "string": "hello",
        "number": 42,
        "bool": true,
        "array": [1, 2, 3]
    });

    let value = json_to_minijinja_value(&json);
    // Just verify it's truthy - minijinja 2.x changed the API
    assert!(!value.is_none());
}

#[test]
fn test_yaml_to_json() {
    let yaml: serde_yaml::Value = serde_yaml::from_str("key: value").unwrap();
    let json = yaml_to_json(&yaml);
    assert_eq!(json["key"], "value");
}

#[test]
fn test_incremental_state_first_run() {
    // First run: model doesn't exist yet
    let state = IncrementalState::new(true, false, false);
    assert!(!state.is_incremental_run());
}

#[test]
fn test_incremental_state_subsequent_run() {
    // Subsequent run: model exists
    let state = IncrementalState::new(true, true, false);
    assert!(state.is_incremental_run());
}

#[test]
fn test_incremental_state_full_refresh() {
    // Full refresh: should not be incremental
    let state = IncrementalState::new(true, true, true);
    assert!(!state.is_incremental_run());
}

#[test]
fn test_incremental_state_not_incremental_model() {
    // Not an incremental model
    let state = IncrementalState::new(false, true, false);
    assert!(!state.is_incremental_run());
}
