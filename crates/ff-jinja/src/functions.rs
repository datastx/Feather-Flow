//! Jinja template functions: config(), var(), env(), log(), error(), warn(),
//! from_json(), and to_json().

use minijinja::value::{Kwargs, Value};
use minijinja::Error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Captured warnings from warn() calls
pub(crate) type WarningCapture = Arc<Mutex<Vec<String>>>;

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
    let shared: Arc<str> = qualified_name.into();
    move || shared.to_string()
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
            let value = kwargs.get::<Value>(key).map_err(|e| {
                Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!("failed to get config kwarg '{}': {}", key, e),
                )
            })?;
            captured.insert(key.to_string(), value);
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
pub(crate) fn json_to_minijinja_value(json: &serde_json::Value) -> Value {
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
        serde_json::Value::String(s) => Value::from(s.as_str()),
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
        serde_yaml::Value::Number(n) => convert_yaml_number(n),
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

/// Convert a minijinja Value to a serde_json::Value.
///
/// This is the inverse of [`json_to_minijinja_value`] and is used by `to_json`.
pub(crate) fn minijinja_value_to_json(val: &Value) -> serde_json::Value {
    use minijinja::value::ValueKind;
    match val.kind() {
        ValueKind::Undefined | ValueKind::None => serde_json::Value::Null,
        ValueKind::Bool => serde_json::Value::Bool(val.is_true()),
        ValueKind::Number => {
            let owned = val.clone();
            if let Ok(i) = i64::try_from(owned.clone()) {
                serde_json::Value::Number(i.into())
            } else if let Ok(f) = f64::try_from(owned) {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::Null
            }
        }
        ValueKind::String => {
            serde_json::Value::String(val.as_str().unwrap_or_default().to_string())
        }
        ValueKind::Seq => {
            let items: Vec<serde_json::Value> = val
                .try_iter()
                .map(|iter| iter.map(|v| minijinja_value_to_json(&v)).collect())
                .unwrap_or_default();
            serde_json::Value::Array(items)
        }
        ValueKind::Map => build_json_map(val),
        _ => serde_json::Value::String(val.to_string()),
    }
}

/// Convert a YAML number to a JSON value, handling NaN/Infinity gracefully.
fn convert_yaml_number(n: &serde_yaml::Number) -> serde_json::Value {
    if let Some(i) = n.as_i64() {
        return serde_json::Value::Number(serde_json::Number::from(i));
    }
    if let Some(f) = n.as_f64() {
        return match serde_json::Number::from_f64(f) {
            Some(num) => serde_json::Value::Number(num),
            None => {
                log::warn!(
                    "YAML number {} is NaN or Infinity; converting to JSON null",
                    f
                );
                serde_json::Value::Null
            }
        };
    }
    serde_json::Value::Null
}

/// Convert a minijinja map value to a JSON object.
fn build_json_map(val: &Value) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    if let Ok(keys) = val.try_iter() {
        for key in keys {
            let key_str = key.as_str().unwrap_or_default().to_string();
            if let Ok(v) = val.get_item(&key) {
                map.insert(key_str, minijinja_value_to_json(&v));
            }
        }
    }
    serde_json::Value::Object(map)
}

/// Create the `env(name, default?)` function to read environment variables.
///
/// Usage in templates:
/// ```jinja
/// {{ env("DATABASE_URL") }}
/// {{ env("MISSING_VAR", "fallback") }}
/// ```
pub(crate) fn make_env_fn(
) -> impl Fn(&str, Option<Value>) -> Result<String, Error> + Send + Sync + Clone + 'static {
    |name: &str, default: Option<Value>| match std::env::var(name) {
        Ok(val) => Ok(val),
        Err(_) => {
            if let Some(d) = default {
                Ok(d.to_string())
            } else {
                Err(Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!(
                        "Environment variable '{}' is not set and no default provided",
                        name
                    ),
                ))
            }
        }
    }
}

/// Create the `log(msg)` function for template debugging.
///
/// Prints the message to stderr and returns an empty string so it
/// does not affect the rendered SQL output.
pub(crate) fn make_log_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    |msg: &str| {
        eprintln!("[jinja:log] {}", msg);
        String::new()
    }
}

/// Create the `error(msg)` function that raises a compilation error.
///
/// Usage in templates:
/// ```jinja
/// {% if var("env") == "prod" %}{{ error("Cannot run in prod!") }}{% endif %}
/// ```
pub(crate) fn make_error_fn(
) -> impl Fn(&str) -> Result<String, Error> + Send + Sync + Clone + 'static {
    |msg: &str| {
        Err(Error::new(
            minijinja::ErrorKind::InvalidOperation,
            msg.to_string(),
        ))
    }
}

/// Create the `warn(msg)` function that emits a warning.
///
/// Captures the warning message for later retrieval and prints it to stderr.
/// Returns an empty string.
pub(crate) fn make_warn_fn(
    capture: WarningCapture,
) -> impl Fn(&str) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |msg: &str| {
        eprintln!("[jinja:warn] {}", msg);
        let mut warnings = capture.lock().map_err(|e| {
            Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("warning mutex poisoned: {e}"),
            )
        })?;
        warnings.push(msg.to_string());
        Ok(String::new())
    }
}

/// Create the `from_json(str)` function to parse a JSON string.
///
/// Usage in templates:
/// ```jinja
/// {% set data = from_json('{"key": "value"}') %}
/// {{ data.key }}
/// ```
pub(crate) fn make_from_json_fn(
) -> impl Fn(&str) -> Result<Value, Error> + Send + Sync + Clone + 'static {
    |s: &str| {
        let parsed: serde_json::Value = serde_json::from_str(s).map_err(|e| {
            Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("from_json parse error: {}", e),
            )
        })?;
        Ok(json_to_minijinja_value(&parsed))
    }
}

/// Create the `to_json(value)` function to serialize a value to JSON.
///
/// Also registered as a filter so `{{ value | to_json }}` works.
pub(crate) fn make_to_json_fn(
) -> impl Fn(Value) -> Result<String, Error> + Send + Sync + Clone + 'static {
    |val: Value| {
        let json_val = minijinja_value_to_json(&val);
        serde_json::to_string(&json_val).map_err(|e| {
            Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("to_json serialization error: {}", e),
            )
        })
    }
}

#[cfg(test)]
#[path = "functions_test.rs"]
mod tests;
