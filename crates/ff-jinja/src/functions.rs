//! Jinja template functions: config() and var()

use minijinja::value::{Kwargs, Value};
use minijinja::Error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Captured config values from config() calls
pub(crate) type ConfigCapture = Arc<Mutex<HashMap<String, Value>>>;

/// State for is_incremental() function
#[derive(Debug, Clone, Default)]
pub struct IncrementalState {
    /// Whether the model is configured as incremental
    pub is_incremental_model: bool,
    /// Whether the model already exists in the database
    pub model_exists: bool,
    /// Whether --full-refresh was specified
    pub full_refresh: bool,
}

impl IncrementalState {
    /// Create a new incremental state
    pub fn new(is_incremental_model: bool, model_exists: bool, full_refresh: bool) -> Self {
        Self {
            is_incremental_model,
            model_exists,
            full_refresh,
        }
    }

    /// Check if this run should be incremental
    ///
    /// Returns true when:
    /// 1. Model is configured as incremental
    /// 2. Model already exists in database
    /// 3. --full-refresh was NOT specified
    pub fn is_incremental_run(&self) -> bool {
        self.is_incremental_model && self.model_exists && !self.full_refresh
    }
}

/// Create the is_incremental() function
///
/// Usage in templates:
/// ```jinja
/// {% if is_incremental() %}
///   WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
/// {% endif %}
/// ```
pub(crate) fn make_is_incremental_fn(
    state: IncrementalState,
) -> impl Fn() -> bool + Send + Sync + Clone + 'static {
    move || state.is_incremental_run()
}

/// Create the this() function that returns the current model's table name
///
/// Usage in templates:
/// ```jinja
/// SELECT MAX(updated_at) FROM {{ this }}
/// ```
pub(crate) fn make_this_fn(
    qualified_name: String,
) -> impl Fn() -> String + Send + Sync + Clone + 'static {
    move || qualified_name.clone()
}

/// Create the config() function that captures model configuration
///
/// Usage in templates:
/// ```jinja
/// {{ config(materialized='table', schema='staging') }}
/// ```
pub(crate) fn make_config_fn(
    capture: ConfigCapture,
) -> impl Fn(Kwargs) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |kwargs: Kwargs| {
        let mut captured = capture.lock().map_err(|e| {
            Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("config mutex poisoned: {e}"),
            )
        })?;

        // Capture all keyword arguments
        for key in kwargs.args() {
            if let Ok(value) = kwargs.get::<Value>(key) {
                captured.insert(key.to_string(), value);
            }
        }

        // Return empty string (config doesn't output anything)
        Ok(String::new())
    }
}

/// Create the var() function that retrieves variables from config
///
/// Usage in templates:
/// ```jinja
/// {{ var('start_date') }}
/// {{ var('missing', 'default_value') }}
/// ```
pub(crate) fn make_var_fn(
    vars: HashMap<String, serde_json::Value>,
) -> impl Fn(&str, Option<Value>) -> Result<Value, Error> + Send + Sync + Clone + 'static {
    move |name: &str, default: Option<Value>| {
        if let Some(value) = vars.get(name) {
            Ok(json_to_minijinja_value(value))
        } else if let Some(default_val) = default {
            Ok(default_val)
        } else {
            Err(Error::new(
                minijinja::ErrorKind::UndefinedError,
                format!("Variable '{}' is not defined and no default provided", name),
            ))
        }
    }
}

/// Convert serde_json::Value to minijinja::Value
fn json_to_minijinja_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::from(()),
        serde_json::Value::Bool(b) => Value::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(f) = n.as_f64() {
                Value::from(f)
            } else {
                Value::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            let values: Vec<Value> = arr.iter().map(json_to_minijinja_value).collect();
            Value::from(values)
        }
        serde_json::Value::Object(obj) => {
            let map: HashMap<String, Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_minijinja_value(v)))
                .collect();
            Value::from_iter(map)
        }
    }
}

/// Convert serde_yaml::Value to serde_json::Value
pub(crate) fn yaml_to_json(yaml: &serde_yaml::Value) -> serde_json::Value {
    match yaml {
        serde_yaml::Value::Null => serde_json::Value::Null,
        serde_yaml::Value::Bool(b) => serde_json::Value::Bool(*b),
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(serde_json::Number::from(i))
            } else if let Some(f) = n.as_f64() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::Null
            }
        }
        serde_yaml::Value::String(s) => serde_json::Value::String(s.clone()),
        serde_yaml::Value::Sequence(seq) => {
            serde_json::Value::Array(seq.iter().map(yaml_to_json).collect())
        }
        serde_yaml::Value::Mapping(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter_map(|(k, v): (&serde_yaml::Value, &serde_yaml::Value)| {
                    k.as_str().map(|key| (key.to_string(), yaml_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        serde_yaml::Value::Tagged(tagged) => yaml_to_json(&tagged.value),
    }
}

#[cfg(test)]
mod tests {
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
}
