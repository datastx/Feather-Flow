//! Jinja environment setup for Featherflow

use crate::builtins::{
    make_bool_or_fn, make_clean_string_fn, make_coalesce_columns_fn, make_date_add_fn,
    make_date_diff_fn, make_date_spine_fn, make_date_trunc_fn, make_hash_fn, make_limit_zero_fn,
    make_not_null_fn, make_percent_of_fn, make_round_money_fn, make_safe_divide_fn,
    make_slugify_fn, make_split_part_fn, make_surrogate_key_fn,
};
use crate::error::{JinjaError, JinjaResult};
use crate::functions::{
    make_config_fn, make_is_incremental_fn, make_this_fn, make_var_fn, yaml_to_json, ConfigCapture,
    IncrementalState,
};
use minijinja::{path_loader, Environment, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Jinja templating environment for Featherflow
pub struct JinjaEnvironment<'a> {
    env: Environment<'a>,
    config_capture: ConfigCapture,
}

impl<'a> JinjaEnvironment<'a> {
    /// Create a new Jinja environment with variables from config
    pub fn new(vars: &HashMap<String, serde_yaml::Value>) -> Self {
        Self::with_macros(vars, &[])
    }

    /// Create a new Jinja environment with variables and macro paths
    ///
    /// This enables loading macros using `{% from "file.sql" import macro_name %}` syntax.
    /// Macro files are loaded from the specified directories.
    pub fn with_macros(vars: &HashMap<String, serde_yaml::Value>, macro_paths: &[PathBuf]) -> Self {
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

        // Register built-in macros
        register_builtins(&mut env);

        // Set up path loader for macros from the first valid macro path
        // Minijinja's path_loader only supports a single path, so we use the first one
        for macro_path in macro_paths {
            if macro_path.exists() && macro_path.is_dir() {
                env.set_loader(path_loader(macro_path.clone()));
                break;
            }
        }

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

impl JinjaEnvironment<'_> {
    /// Create a new Jinja environment with incremental model context
    ///
    /// This adds the `is_incremental()` and `this` functions for incremental models.
    pub fn with_incremental_context(
        vars: &HashMap<String, serde_yaml::Value>,
        macro_paths: &[PathBuf],
        incremental_state: IncrementalState,
        qualified_name: &str,
    ) -> Self {
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

        // Register is_incremental() function
        let is_incremental_fn = make_is_incremental_fn(incremental_state);
        env.add_function("is_incremental", is_incremental_fn);

        // Register this variable (the current model's qualified name)
        let this_fn = make_this_fn(qualified_name.to_string());
        env.add_function("this", this_fn);

        // Register built-in macros
        register_builtins(&mut env);

        // Set up path loader for macros
        for macro_path in macro_paths {
            if macro_path.exists() && macro_path.is_dir() {
                env.set_loader(path_loader(macro_path.clone()));
                break;
            }
        }

        Self {
            env,
            config_capture,
        }
    }
}

impl Default for JinjaEnvironment<'_> {
    fn default() -> Self {
        Self::new(&HashMap::new())
    }
}

/// Register all built-in macros with the Jinja environment
fn register_builtins(env: &mut Environment<'_>) {
    // Date/Time macros
    env.add_function("date_spine", make_date_spine_fn());
    env.add_function("date_trunc", make_date_trunc_fn());
    env.add_function("date_add", make_date_add_fn());
    env.add_function("date_diff", make_date_diff_fn());

    // String macros
    env.add_function("slugify", make_slugify_fn());
    env.add_function("clean_string", make_clean_string_fn());
    env.add_function("split_part", make_split_part_fn());

    // Math macros
    env.add_function("safe_divide", make_safe_divide_fn());
    env.add_function("round_money", make_round_money_fn());
    env.add_function("percent_of", make_percent_of_fn());

    // Cross-DB macros
    env.add_function("limit_zero", make_limit_zero_fn());
    env.add_function("bool_or", make_bool_or_fn());
    env.add_function("hash", make_hash_fn());

    // Utility macros
    env.add_function("surrogate_key", make_surrogate_key_fn());
    env.add_function("coalesce_columns", make_coalesce_columns_fn());
    env.add_function("not_null", make_not_null_fn());
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

    #[test]
    fn test_macro_loading() {
        use std::fs;
        use tempfile::TempDir;

        // Create temp directory with macro file
        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        // Create a macro file
        fs::write(
            macros_dir.join("utils.sql"),
            r#"{% macro cents_to_dollars(column_name) %}({{ column_name }} / 100.0){% endmacro %}
{% macro safe_divide(num, denom) %}CASE WHEN {{ denom }} = 0 THEN 0 ELSE {{ num }} / {{ denom }} END{% endmacro %}"#,
        )
        .unwrap();

        // Create environment with macro path
        let env = JinjaEnvironment::with_macros(&HashMap::new(), std::slice::from_ref(&macros_dir));

        // Test using the macro
        let template = r#"{% from "utils.sql" import cents_to_dollars %}
SELECT {{ cents_to_dollars("amount_cents") }} AS amount_dollars FROM orders"#;

        let result = env.render(template).unwrap();
        assert!(result.contains("(amount_cents / 100.0) AS amount_dollars"));
    }

    #[test]
    fn test_macro_with_multiple_imports() {
        use std::fs;
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        fs::write(
            macros_dir.join("utils.sql"),
            r#"{% macro cents_to_dollars(col) %}({{ col }} / 100.0){% endmacro %}
{% macro safe_divide(num, denom) %}CASE WHEN {{ denom }} = 0 THEN 0 ELSE {{ num }} / {{ denom }} END{% endmacro %}"#,
        )
        .unwrap();

        let env = JinjaEnvironment::with_macros(&HashMap::new(), &[macros_dir]);

        let template = r#"{% from "utils.sql" import cents_to_dollars, safe_divide %}
SELECT
  {{ cents_to_dollars("price") }} AS price_dollars,
  {{ safe_divide("revenue", "count") }} AS avg_revenue
FROM sales"#;

        let result = env.render(template).unwrap();
        assert!(result.contains("(price / 100.0) AS price_dollars"));
        assert!(
            result.contains("CASE WHEN count = 0 THEN 0 ELSE revenue / count END AS avg_revenue")
        );
    }

    #[test]
    fn test_macro_with_import_as() {
        use std::fs;
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        fs::write(
            macros_dir.join("utils.sql"),
            r#"{% macro cents_to_dollars(col) %}({{ col }} / 100.0){% endmacro %}"#,
        )
        .unwrap();

        let env = JinjaEnvironment::with_macros(&HashMap::new(), &[macros_dir]);

        let template = r#"{% import "utils.sql" as utils %}
SELECT {{ utils.cents_to_dollars("amount") }} AS amount_dollars FROM orders"#;

        let result = env.render(template).unwrap();
        assert!(result.contains("(amount / 100.0) AS amount_dollars"));
    }

    #[test]
    fn test_macros_with_vars() {
        use std::fs;
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        fs::write(
            macros_dir.join("filters.sql"),
            r#"{% macro date_filter(column, start, end) %}{{ column }} BETWEEN '{{ start }}' AND '{{ end }}'{% endmacro %}"#,
        )
        .unwrap();

        let mut vars = HashMap::new();
        vars.insert(
            "start_date".to_string(),
            serde_yaml::Value::String("2024-01-01".to_string()),
        );
        vars.insert(
            "end_date".to_string(),
            serde_yaml::Value::String("2024-12-31".to_string()),
        );

        let env = JinjaEnvironment::with_macros(&vars, &[macros_dir]);

        let template = r#"{% from "filters.sql" import date_filter %}
SELECT * FROM orders WHERE {{ date_filter("created_at", var("start_date"), var("end_date")) }}"#;

        let result = env.render(template).unwrap();
        assert!(result.contains("created_at BETWEEN '2024-01-01' AND '2024-12-31'"));
    }

    // ===== Built-in Macro Integration Tests =====

    #[test]
    fn test_builtin_date_spine() {
        let env = JinjaEnvironment::default();
        let result = env
            .render("{{ date_spine('2024-01-01', '2024-01-31') }}")
            .unwrap();
        assert!(result.contains("generate_series"));
        assert!(result.contains("2024-01-01"));
        assert!(result.contains("2024-01-31"));
    }

    #[test]
    fn test_builtin_date_trunc() {
        let env = JinjaEnvironment::default();
        let result = env
            .render("{{ date_trunc('month', 'created_at') }}")
            .unwrap();
        assert_eq!(result, "DATE_TRUNC('month', created_at)");
    }

    #[test]
    fn test_builtin_date_add() {
        let env = JinjaEnvironment::default();
        let result = env
            .render("{{ date_add('order_date', 7, 'day') }}")
            .unwrap();
        assert_eq!(result, "order_date + INTERVAL '7 day'");
    }

    #[test]
    fn test_builtin_date_diff() {
        let env = JinjaEnvironment::default();
        let result = env
            .render("{{ date_diff('day', 'start_date', 'end_date') }}")
            .unwrap();
        assert_eq!(result, "DATE_DIFF('day', start_date, end_date)");
    }

    #[test]
    fn test_builtin_safe_divide() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ safe_divide('revenue', 'count') }}").unwrap();
        assert!(result.contains("CASE WHEN"));
        assert!(result.contains("IS NULL"));
    }

    #[test]
    fn test_builtin_round_money() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ round_money('amount') }}").unwrap();
        assert_eq!(result, "ROUND(CAST(amount AS DOUBLE), 2)");
    }

    #[test]
    fn test_builtin_percent_of() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ percent_of('sales', 'total') }}").unwrap();
        assert!(result.contains("100.0"));
        assert!(result.contains("sales"));
        assert!(result.contains("total"));
    }

    #[test]
    fn test_builtin_slugify() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ slugify('title') }}").unwrap();
        assert!(result.contains("LOWER"));
        assert!(result.contains("REGEXP_REPLACE"));
    }

    #[test]
    fn test_builtin_clean_string() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ clean_string('name') }}").unwrap();
        assert!(result.contains("TRIM"));
        assert!(result.contains("REGEXP_REPLACE"));
    }

    #[test]
    fn test_builtin_split_part() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ split_part('email', '@', 2) }}").unwrap();
        assert_eq!(result, "SPLIT_PART(email, '@', 2)");
    }

    #[test]
    fn test_builtin_limit_zero() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ limit_zero() }}").unwrap();
        assert_eq!(result, "LIMIT 0");
    }

    #[test]
    fn test_builtin_bool_or() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ bool_or('is_active') }}").unwrap();
        assert_eq!(result, "BOOL_OR(is_active)");
    }

    #[test]
    fn test_builtin_hash() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ hash('user_id') }}").unwrap();
        assert_eq!(result, "MD5(CAST(user_id AS VARCHAR))");
    }

    #[test]
    fn test_builtin_not_null() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ not_null('email') }}").unwrap();
        assert_eq!(result, "email IS NOT NULL");
    }

    #[test]
    fn test_builtin_surrogate_key() {
        let env = JinjaEnvironment::default();
        let result = env.render("{{ surrogate_key(['col1', 'col2']) }}").unwrap();
        assert!(result.contains("MD5"));
        assert!(result.contains("col1"));
        assert!(result.contains("col2"));
    }

    #[test]
    fn test_builtin_coalesce_columns() {
        let env = JinjaEnvironment::default();
        let result = env
            .render("{{ coalesce_columns(['a', 'b', 'c']) }}")
            .unwrap();
        assert_eq!(result, "COALESCE(a, b, c)");
    }

    #[test]
    fn test_builtin_in_select() {
        let env = JinjaEnvironment::default();
        let template = r#"SELECT
    id,
    {{ round_money('price') }} AS price_rounded,
    {{ safe_divide('revenue', 'orders') }} AS avg_order_value,
    {{ date_trunc('month', 'created_at') }} AS month
FROM sales
WHERE {{ not_null('customer_id') }}"#;

        let result = env.render(template).unwrap();
        assert!(result.contains("ROUND(CAST(price AS DOUBLE), 2) AS price_rounded"));
        assert!(result.contains("CASE WHEN orders = 0 OR orders IS NULL THEN NULL"));
        assert!(result.contains("DATE_TRUNC('month', created_at) AS month"));
        assert!(result.contains("WHERE customer_id IS NOT NULL"));
    }
}
