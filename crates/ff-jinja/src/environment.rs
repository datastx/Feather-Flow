//! Jinja environment setup for Featherflow

use crate::builtins::{
    make_bool_or_fn, make_clean_string_fn, make_coalesce_columns_fn, make_date_add_fn,
    make_date_diff_fn, make_date_spine_fn, make_date_trunc_fn, make_hash_columns_fn, make_hash_fn,
    make_limit_zero_fn, make_not_null_fn, make_percent_of_fn, make_round_money_fn,
    make_safe_divide_fn, make_slugify_fn, make_split_part_fn, make_surrogate_key_fn,
};
use crate::error::{JinjaError, JinjaResult};
use crate::functions::{
    make_config_fn, make_is_incremental_fn, make_this_fn, make_var_fn, yaml_to_json, ConfigCapture,
    IncrementalState,
};
use minijinja::{Environment, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Jinja templating environment for Featherflow
pub struct JinjaEnvironment<'a> {
    env: Environment<'a>,
    config_capture: ConfigCapture,
}

impl std::fmt::Debug for JinjaEnvironment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JinjaEnvironment")
            .field("env", &"<Environment>")
            .finish()
    }
}

impl<'a> JinjaEnvironment<'a> {
    /// Create a new Jinja environment with variables from config
    pub fn new(vars: &HashMap<String, serde_yaml::Value>) -> Self {
        Self::with_macros(vars, &[])
    }

    /// Shared initialization: creates the environment, registers config/var
    /// functions, built-in macros, and sets up the macro path loader.
    ///
    /// Returns the partially-built `(Environment, ConfigCapture)` so that
    /// callers can register additional functions before finalising.
    fn init_common(
        vars: &HashMap<String, serde_yaml::Value>,
        macro_paths: &[PathBuf],
    ) -> (Environment<'a>, ConfigCapture) {
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

        // Set up a multi-path loader that searches all configured macro paths.
        // Minijinja's built-in path_loader only supports a single directory,
        // so we use a custom closure that iterates over all valid paths.
        let valid_paths: Vec<PathBuf> = macro_paths
            .iter()
            .filter(|p| p.exists() && p.is_dir())
            .cloned()
            .collect();
        if !valid_paths.is_empty() {
            env.set_loader(move |name: &str| load_macro_from_paths(name, &valid_paths));
        }

        (env, config_capture)
    }

    /// Create a new Jinja environment with variables and macro paths
    ///
    /// This enables loading macros using `{% from "file.sql" import macro_name %}` syntax.
    /// Macro files are loaded from the specified directories.
    pub fn with_macros(vars: &HashMap<String, serde_yaml::Value>, macro_paths: &[PathBuf]) -> Self {
        let (env, config_capture) = Self::init_common(vars, macro_paths);
        Self {
            env,
            config_capture,
        }
    }

    /// Render a template string
    pub fn render(&self, template: &str) -> JinjaResult<String> {
        // Clear previous config captures
        self.config_capture
            .lock()
            .map_err(|e| JinjaError::Internal(format!("config mutex poisoned: {e}")))?
            .clear();

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
        let config = self
            .config_capture
            .lock()
            .map_err(|e| JinjaError::Internal(format!("config mutex poisoned: {e}")))?
            .clone();
        Ok((rendered, config))
    }

    /// Acquire the config capture lock, recovering from poison.
    ///
    /// Poisoned mutex recovery: these getters read non-critical config
    /// metadata. If a panic poisoned the lock, we still want to return
    /// the inner data rather than propagate the poison.
    fn read_config(&self) -> std::sync::MutexGuard<'_, HashMap<String, Value>> {
        self.config_capture
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Get the captured config values from the last render
    pub fn get_captured_config(&self) -> HashMap<String, Value> {
        self.read_config().clone()
    }

    /// Extract materialization from captured config
    pub fn get_materialization(&self) -> Option<String> {
        self.read_config()
            .get("materialized")
            .and_then(|v| v.as_str().map(String::from))
    }

    /// Extract schema from captured config
    pub fn get_schema(&self) -> Option<String> {
        self.read_config()
            .get("schema")
            .and_then(|v| v.as_str().map(String::from))
    }

    /// Extract tags from captured config
    pub fn get_tags(&self) -> Vec<String> {
        self.read_config()
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
        let (mut env, config_capture) = Self::init_common(vars, macro_paths);

        // Register is_incremental() function
        let is_incremental_fn = make_is_incremental_fn(incremental_state);
        env.add_function("is_incremental", is_incremental_fn);

        // Register this variable (the current model's qualified name)
        let this_fn = make_this_fn(qualified_name.to_string());
        env.add_function("this", this_fn);

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

fn load_macro_from_paths(
    name: &str,
    paths: &[PathBuf],
) -> Result<Option<String>, minijinja::Error> {
    for base in paths {
        let full = base.join(name);
        if !full.is_file() {
            continue;
        }
        match std::fs::read_to_string(&full) {
            Ok(contents) => return Ok(Some(contents)),
            Err(e) => {
                log::warn!("Failed to read macro file {}: {}", full.display(), e);
                continue;
            }
        }
    }
    Ok(None)
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
    env.add_function("hash_columns", make_hash_columns_fn());
    env.add_function("surrogate_key", make_surrogate_key_fn());
    env.add_function("coalesce_columns", make_coalesce_columns_fn());
    env.add_function("not_null", make_not_null_fn());
}

#[cfg(test)]
#[path = "environment_test.rs"]
mod tests;
