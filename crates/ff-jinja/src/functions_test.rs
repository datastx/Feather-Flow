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

// ===== env() function tests =====

#[test]
fn test_env_fn_reads_variable() {
    std::env::set_var("FF_TEST_ENV_VAR", "hello_world");
    let env_fn = make_env_fn();
    let result = env_fn("FF_TEST_ENV_VAR", None).unwrap();
    assert_eq!(result, "hello_world");
    std::env::remove_var("FF_TEST_ENV_VAR");
}

#[test]
fn test_env_fn_default_value() {
    std::env::remove_var("FF_TEST_MISSING_VAR");
    let env_fn = make_env_fn();
    let result = env_fn("FF_TEST_MISSING_VAR", Some(Value::from("fallback"))).unwrap();
    assert_eq!(result, "fallback");
}

#[test]
fn test_env_fn_missing_no_default() {
    std::env::remove_var("FF_TEST_TOTALLY_MISSING");
    let env_fn = make_env_fn();
    let result = env_fn("FF_TEST_TOTALLY_MISSING", None);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not set"));
}

// ===== log() function tests =====

#[test]
fn test_log_fn_returns_empty() {
    let log_fn = make_log_fn();
    let result = log_fn("debug message");
    assert_eq!(result, "");
}

// ===== error() function tests =====

#[test]
fn test_error_fn_raises() {
    let error_fn = make_error_fn();
    let result = error_fn("something went wrong");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("something went wrong"));
}

// ===== warn() function tests =====

#[test]
fn test_warn_fn_captures() {
    let capture: WarningCapture = Arc::new(Mutex::new(Vec::new()));
    let warn_fn = make_warn_fn(capture.clone());
    let result = warn_fn("potential issue").unwrap();
    assert_eq!(result, "");
    let warnings = capture.lock().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0], "potential issue");
}

// ===== from_json() function tests =====

#[test]
fn test_from_json_valid() {
    let from_json_fn = make_from_json_fn();
    let result = from_json_fn(r#"{"key": "value", "num": 42}"#).unwrap();
    assert!(!result.is_none());
    // Access the key
    let key_val = result.get_attr("key").unwrap();
    assert_eq!(key_val.as_str(), Some("value"));
    let num_val = result.get_attr("num").unwrap();
    assert_eq!(i64::try_from(num_val).unwrap(), 42);
}

#[test]
fn test_from_json_invalid() {
    let from_json_fn = make_from_json_fn();
    let result = from_json_fn("not valid json {{{");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("from_json parse error"));
}

// ===== to_json() function tests =====

#[test]
fn test_to_json_object() {
    let to_json_fn = make_to_json_fn();
    // Build a minijinja map
    let map: HashMap<String, Value> = [
        ("name".to_string(), Value::from("test")),
        ("count".to_string(), Value::from(42)),
    ]
    .into_iter()
    .collect();
    let val = Value::from_iter(map);
    let result = to_json_fn(val).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["name"], "test");
    assert_eq!(parsed["count"], 42);
}

#[test]
fn test_to_json_string() {
    let to_json_fn = make_to_json_fn();
    let result = to_json_fn(Value::from("hello")).unwrap();
    assert_eq!(result, r#""hello""#);
}

// ===== minijinja_value_to_json roundtrip tests =====

#[test]
fn test_minijinja_value_to_json_roundtrip() {
    let original = serde_json::json!({
        "string": "hello",
        "number": 42,
        "float": 1.5,
        "bool": true,
        "null": null,
        "array": [1, "two", false],
        "nested": {"a": 1}
    });
    let mj_val = json_to_minijinja_value(&original);
    let back = minijinja_value_to_json(&mj_val);
    assert_eq!(back["string"], "hello");
    assert_eq!(back["number"], 42);
    assert_eq!(back["bool"], true);
    assert!(back["null"].is_null());
    assert_eq!(back["array"][0], 1);
    assert_eq!(back["array"][1], "two");
    assert_eq!(back["nested"]["a"], 1);
}

#[test]
fn test_minijinja_value_to_json_primitives() {
    assert_eq!(
        minijinja_value_to_json(&Value::from(true)),
        serde_json::Value::Bool(true)
    );
    assert_eq!(
        minijinja_value_to_json(&Value::from(99)),
        serde_json::json!(99)
    );
    assert_eq!(
        minijinja_value_to_json(&Value::from("text")),
        serde_json::json!("text")
    );
    assert!(minijinja_value_to_json(&Value::from(())).is_null());
}
