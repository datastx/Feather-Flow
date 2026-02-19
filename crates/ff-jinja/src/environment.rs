//! Jinja environment setup for Featherflow

use crate::builtins::{
    make_bool_or_fn, make_clean_string_fn, make_coalesce_columns_fn, make_date_add_fn,
    make_date_diff_fn, make_date_spine_fn, make_date_trunc_fn, make_hash_columns_fn, make_hash_fn,
    make_limit_zero_fn, make_not_null_fn, make_percent_of_fn, make_round_money_fn,
    make_safe_divide_fn, make_slugify_fn, make_split_part_fn, make_surrogate_key_fn,
};
use crate::context::{ModelContext, TemplateContext};
use crate::error::{JinjaError, JinjaResult};
use crate::functions::{
    make_config_fn, make_env_fn, make_error_fn, make_from_json_fn, make_is_incremental_fn,
    make_log_fn, make_this_fn, make_to_json_fn, make_var_fn, make_warn_fn, yaml_to_json,
    ConfigCapture, IncrementalState, WarningCapture,
};
use minijinja::{Environment, Value};
use regex::Regex;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

/// Private render context passed to `render_str`.
///
/// Contains optional per-model context. When `model` is `None`, the template
/// variable `{{ model }}` is simply absent (undefined).
#[derive(Debug, Serialize)]
struct RenderContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<ModelContext>,
}

/// Jinja templating environment for Featherflow
pub struct JinjaEnvironment<'a> {
    env: Environment<'a>,
    config_capture: ConfigCapture,
    warning_capture: WarningCapture,
    /// Auto-generated `{% from "file.sql" import macro1, macro2 %}` lines
    /// prepended to every template so user macros are globally available.
    macro_preamble: String,
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

    /// Shared initialization: creates the environment, registers all
    /// functions, built-in macros, and sets up the macro path loader.
    ///
    /// If `template_context` is provided, static context variables
    /// (`project_name`, `target`, `run_id`, etc.) are set as globals.
    fn init_common(
        vars: &HashMap<String, serde_yaml::Value>,
        macro_paths: &[PathBuf],
        template_context: Option<&TemplateContext>,
    ) -> (Environment<'a>, ConfigCapture, WarningCapture, String) {
        let mut env = Environment::new();
        let config_capture: ConfigCapture = Arc::new(Mutex::new(HashMap::new()));
        let warning_capture: WarningCapture = Arc::new(Mutex::new(Vec::new()));

        let json_vars: HashMap<String, serde_json::Value> = vars
            .iter()
            .map(|(k, v): (&String, &serde_yaml::Value)| (k.clone(), yaml_to_json(v)))
            .collect();

        env.add_function("config", make_config_fn(config_capture.clone()));
        env.add_function("var", make_var_fn(json_vars));

        env.add_function("env", make_env_fn());
        env.add_function("log", make_log_fn());
        env.add_function("error", make_error_fn());
        env.add_function("warn", make_warn_fn(warning_capture.clone()));
        env.add_function("from_json", make_from_json_fn());
        let to_json_fn = make_to_json_fn();
        env.add_function("to_json", to_json_fn.clone());
        env.add_filter("to_json", to_json_fn);

        register_builtins(&mut env);

        if let Some(ctx) = template_context {
            env.add_global("project_name", Value::from(ctx.project_name.clone()));
            env.add_global("target", Value::from_serialize(&ctx.target));
            env.add_global("run_id", Value::from(ctx.run_id.clone()));
            env.add_global("run_started_at", Value::from(ctx.run_started_at.clone()));
            env.add_global("ff_version", Value::from(ctx.ff_version.clone()));
            env.add_global("executing", Value::from(ctx.executing));
        }

        // Set up a multi-path loader that searches all configured macro paths.
        let valid_paths: Vec<PathBuf> = macro_paths
            .iter()
            .filter(|p| p.exists() && p.is_dir())
            .cloned()
            .collect();
        let macro_preamble = build_macro_preamble(&valid_paths);
        if !valid_paths.is_empty() {
            env.set_loader(move |name: &str| load_macro_from_paths(name, &valid_paths));
        }

        (env, config_capture, warning_capture, macro_preamble)
    }

    /// Create a new Jinja environment with variables and macro paths
    ///
    /// This enables loading macros using `{% from "file.sql" import macro_name %}` syntax.
    /// Macro files are loaded from the specified directories.
    pub fn with_macros(vars: &HashMap<String, serde_yaml::Value>, macro_paths: &[PathBuf]) -> Self {
        let (env, config_capture, warning_capture, macro_preamble) =
            Self::init_common(vars, macro_paths, None);
        Self {
            env,
            config_capture,
            warning_capture,
            macro_preamble,
        }
    }

    /// Create a new Jinja environment with variables, macro paths, and template context.
    ///
    /// The template context provides static globals like `project_name`, `target`,
    /// `run_id`, `run_started_at`, `ff_version`, and `executing`.
    pub fn with_context(
        vars: &HashMap<String, serde_yaml::Value>,
        macro_paths: &[PathBuf],
        context: &TemplateContext,
    ) -> Self {
        let (env, config_capture, warning_capture, macro_preamble) =
            Self::init_common(vars, macro_paths, Some(context));
        Self {
            env,
            config_capture,
            warning_capture,
            macro_preamble,
        }
    }

    /// Render a template string
    pub fn render(&self, template: &str) -> JinjaResult<String> {
        self.render_with_model_context(template, None)
    }

    /// Render a template with per-model context.
    ///
    /// The model context is available in the template as `{{ model.name }}`,
    /// `{{ model.materialized }}`, etc.
    pub fn render_with_model(&self, template: &str, model: &ModelContext) -> JinjaResult<String> {
        self.render_with_model_context(template, Some(model))
    }

    /// Render a template and return both the result and captured config,
    /// optionally with per-model context.
    pub fn render_with_config_and_model(
        &self,
        template: &str,
        model: Option<&ModelContext>,
    ) -> JinjaResult<(String, HashMap<String, Value>)> {
        let rendered = self.render_with_model_context(template, model)?;
        let config = self
            .config_capture
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        Ok((rendered, config))
    }

    /// Render a template and return both the result and captured config
    pub fn render_with_config(
        &self,
        template: &str,
    ) -> JinjaResult<(String, HashMap<String, Value>)> {
        self.render_with_config_and_model(template, None)
    }

    /// Internal render method that handles both with and without model context.
    fn render_with_model_context(
        &self,
        template: &str,
        model: Option<&ModelContext>,
    ) -> JinjaResult<String> {
        // Clear previous captures (recover from poisoning — these are process-local
        // and the next lines clear stale data anyway)
        self.config_capture
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        self.read_warnings().clear();

        let ctx = RenderContext {
            model: model.cloned(),
        };

        // Prepend auto-generated import lines so user macros are globally available.
        let full_template;
        let source = if self.macro_preamble.is_empty() {
            template
        } else {
            full_template = format!("{}{}", self.macro_preamble, template);
            &full_template
        };

        let result = self.env.render_str(source, ctx).map_err(JinjaError::from)?;

        Ok(result)
    }

    /// Acquire the config capture lock, recovering from poison.
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
                v.try_iter().ok().map(|iter| {
                    iter.filter_map(|item| item.as_str().map(String::from))
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    /// Acquire the warning capture lock, recovering from poison.
    fn read_warnings(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
        self.warning_capture
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Get warnings captured during the last render.
    pub fn get_captured_warnings(&self) -> Vec<String> {
        self.read_warnings().clone()
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
        let (mut env, config_capture, warning_capture, macro_preamble) =
            Self::init_common(vars, macro_paths, None);

        let is_incremental_fn = make_is_incremental_fn(incremental_state);
        env.add_function("is_incremental", is_incremental_fn);

        let this_fn = make_this_fn(qualified_name.to_string());
        env.add_function("this", this_fn);

        Self {
            env,
            config_capture,
            warning_capture,
            macro_preamble,
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

/// Scan macro files in `paths`, extract `{% macro name( %}` definitions,
/// and build `{% from "file.sql" import macro1, macro2 %}` lines so every
/// user macro is available globally without explicit imports.
/// Regex for extracting macro names from Jinja template files.
static MACRO_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{%-?\s*macro\s+(\w+)\s*\(").expect("valid regex literal"));

fn build_macro_preamble(paths: &[PathBuf]) -> String {
    let re = &*MACRO_RE;

    // BTreeMap keeps files sorted for deterministic output.
    let mut file_macros: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for dir in paths {
        if !dir.is_dir() {
            continue;
        }
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("sql") {
                continue;
            }
            let contents = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let names: Vec<String> = re
                .captures_iter(&contents)
                .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
                .collect();
            if !names.is_empty() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                file_macros.insert(file_name, names);
            }
        }
    }

    file_macros
        .iter()
        .map(|(file, names)| format!("{{%- from \"{}\" import {} -%}}", file, names.join(", ")))
        .collect()
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
