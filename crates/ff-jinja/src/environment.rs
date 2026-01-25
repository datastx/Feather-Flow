//! Jinja environment setup for Featherflow

use crate::error::{JinjaError, JinjaResult};
use crate::functions::{make_config_fn, make_var_fn, yaml_to_json, ConfigCapture};
use minijinja::{Environment, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Jinja templating environment for Featherflow
pub struct JinjaEnvironment<'a> {
    env: Environment<'a>,
    config_capture: ConfigCapture,
}

impl<'a> JinjaEnvironment<'a> {
    /// Create a new Jinja environment with variables from config
    pub fn new(vars: &HashMap<String, serde_yaml::Value>) -> Self {
        let mut env = Environment::new();
        let config_capture: ConfigCapture = Arc::new(Mutex::new(HashMap::new()));

        // Convert YAML vars to JSON for the var function
        let json_vars: HashMap<String, serde_json::Value> = vars
            .iter()
            .map(|(k, v): (&String, &serde_yaml::Value)| (k.clone(), yaml_to_json(v)))
            .collect();

        // Register config() function
        let config_fn = make_config_fn(config_capture.clone());
        env.add_function("config", config_fn);

        // Register var() function
        let var_fn = make_var_fn(json_vars);
        env.add_function("var", var_fn);

        Self {
            env,
            config_capture,
        }
    }

    /// Render a template string
    pub fn render(&self, template: &str) -> JinjaResult<String> {
        // Clear previous config captures
        self.config_capture.lock().unwrap().clear();

        // Render the template
        let result = self
            .env
            .render_str(template, ())
            .map_err(JinjaError::from)?;

        Ok(result)
    }

    /// Render a template and return both the result and captured config
    pub fn render_with_config(
        &self,
        template: &str,
    ) -> JinjaResult<(String, HashMap<String, Value>)> {
        let rendered = self.render(template)?;
        let config = self.config_capture.lock().unwrap().clone();
        Ok((rendered, config))
    }

    /// Get the captured config values from the last render
    pub fn get_captured_config(&self) -> HashMap<String, Value> {
        self.config_capture.lock().unwrap().clone()
    }

    /// Extract materialization from captured config
    pub fn get_materialization(&self) -> Option<String> {
        self.config_capture
            .lock()
            .unwrap()
            .get("materialized")
            .and_then(|v| v.as_str().map(String::from))
    }

    /// Extract schema from captured config
    pub fn get_schema(&self) -> Option<String> {
        self.config_capture
            .lock()
            .unwrap()
            .get("schema")
            .and_then(|v| v.as_str().map(String::from))
    }

    /// Extract tags from captured config
    pub fn get_tags(&self) -> Vec<String> {
        self.config_capture
            .lock()
            .unwrap()
            .get("tags")
            .and_then(|v| {
                // Try to iterate over the value if it's a sequence
                v.try_iter().ok().map(|iter| {
                    iter.filter_map(|item| item.as_str().map(String::from))
                        .collect()
                })
            })
            .unwrap_or_default()
    }
}

impl Default for JinjaEnvironment<'_> {
    fn default() -> Self {
        Self::new(&HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_simple() {
        let env = JinjaEnvironment::default();
        let result = env.render("SELECT * FROM users").unwrap();
        assert_eq!(result, "SELECT * FROM users");
    }

    #[test]
    fn test_render_with_var() {
        let mut vars = HashMap::new();
        vars.insert(
            "start_date".to_string(),
            serde_yaml::Value::String("2024-01-01".to_string()),
        );

        let env = JinjaEnvironment::new(&vars);
        let result = env
            .render("SELECT * FROM orders WHERE created_at >= '{{ var(\"start_date\") }}'")
            .unwrap();

        assert_eq!(
            result,
            "SELECT * FROM orders WHERE created_at >= '2024-01-01'"
        );
    }

    #[test]
    fn test_render_with_var_default() {
        let env = JinjaEnvironment::default();
        let result = env
            .render("{{ var(\"missing\", \"default_value\") }}")
            .unwrap();
        assert_eq!(result, "default_value");
    }

    #[test]
    fn test_render_with_config() {
        let env = JinjaEnvironment::default();
        let (result, config) = env
            .render_with_config("{{ config(materialized='table', schema='staging') }}SELECT 1")
            .unwrap();

        assert_eq!(result, "SELECT 1");
        assert_eq!(config.get("materialized").unwrap().as_str(), Some("table"));
        assert_eq!(config.get("schema").unwrap().as_str(), Some("staging"));
    }

    #[test]
    fn test_get_materialization() {
        let env = JinjaEnvironment::default();
        env.render("{{ config(materialized='table') }}").unwrap();

        assert_eq!(env.get_materialization(), Some("table".to_string()));
    }

    #[test]
    fn test_var_missing_no_default() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ var(\"missing\") }}");
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_template() {
        let mut vars = HashMap::new();
        vars.insert(
            "start_date".to_string(),
            serde_yaml::Value::String("2024-01-01".to_string()),
        );
        vars.insert(
            "environment".to_string(),
            serde_yaml::Value::String("dev".to_string()),
        );

        let env = JinjaEnvironment::new(&vars);
        let template = r#"{{ config(materialized='view', schema='staging') }}
SELECT
    id AS order_id,
    user_id AS customer_id,
    created_at AS order_date,
    amount
FROM raw.orders
WHERE created_at >= '{{ var("start_date") }}'
"#;

        let (result, config) = env.render_with_config(template).unwrap();

        assert!(result.contains("WHERE created_at >= '2024-01-01'"));
        assert_eq!(config.get("materialized").unwrap().as_str(), Some("view"));
        assert_eq!(config.get("schema").unwrap().as_str(), Some("staging"));
    }
}
