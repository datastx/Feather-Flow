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
    make_env_fn, make_error_fn, make_from_json_fn, make_is_exists_fn, make_is_incremental_fn,
    make_log_fn, make_this_fn, make_to_json_fn, make_var_fn, make_warn_fn, yaml_to_json,
    DeprecationCapture, IncrementalState, WarningCapture,
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
    ) -> (Environment<'a>, WarningCapture, String) {
        let mut env = Environment::new();
        let warning_capture: WarningCapture = Arc::new(Mutex::new(Vec::new()));

        let json_vars: HashMap<String, serde_json::Value> = vars
            .iter()
            .map(|(k, v): (&String, &serde_yaml::Value)| (k.clone(), yaml_to_json(v)))
            .collect();

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

        // Default is_exists() and is_incremental() to false. These will be
        // overridden by with_is_exists() or with_incremental_context() when
        // compiling/running incremental models.
        env.add_function("is_exists", || false);
        env.add_function("is_incremental", || -> bool {
            eprintln!(
                "[jinja:deprecation] is_incremental() is deprecated, use is_exists() instead"
            );
            false
        });

        if let Some(ctx) = template_context {
            env.add_global("project_name", Value::from(ctx.project_name.as_str()));
            env.add_global("target", Value::from_serialize(&ctx.target));
            env.add_global("run_id", Value::from(ctx.run_id.as_str()));
            env.add_global("run_started_at", Value::from(ctx.run_started_at.as_str()));
            env.add_global("ff_version", Value::from(ctx.ff_version.as_str()));
            env.add_global("executing", Value::from(ctx.executing));
        }

        let valid_paths: Vec<PathBuf> = macro_paths
            .iter()
            .filter(|p| p.exists() && p.is_dir())
            .cloned()
            .collect();
        let macro_preamble = build_macro_preamble(&valid_paths);
        if !valid_paths.is_empty() {
            env.set_loader(move |name: &str| load_macro_from_paths(name, &valid_paths));
        }

        (env, warning_capture, macro_preamble)
    }

    /// Create a new Jinja environment with variables and macro paths
    ///
    /// This enables loading macros using `{% from "file.sql" import macro_name %}` syntax.
    /// Macro files are loaded from the specified directories.
    pub fn with_macros(vars: &HashMap<String, serde_yaml::Value>, macro_paths: &[PathBuf]) -> Self {
        let (env, warning_capture, macro_preamble) = Self::init_common(vars, macro_paths, None);
        Self {
            env,
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
        let (env, warning_capture, macro_preamble) =
            Self::init_common(vars, macro_paths, Some(context));
        Self {
            env,
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

    /// Internal render method that handles both with and without model context.
    fn render_with_model_context(
        &self,
        template: &str,
        model: Option<&ModelContext>,
    ) -> JinjaResult<String> {
        self.read_warnings().clear();

        let ctx = RenderContext {
            model: model.cloned(),
        };

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
    /// Create a new Jinja environment with incremental model context.
    ///
    /// Registers both `is_exists()` (preferred) and `is_incremental()` (deprecated
    /// alias that emits a warning), plus the `this` function.
    pub fn with_incremental_context(
        vars: &HashMap<String, serde_yaml::Value>,
        macro_paths: &[PathBuf],
        incremental_state: IncrementalState,
        qualified_name: &str,
    ) -> Self {
        let (mut env, warning_capture, macro_preamble) = Self::init_common(vars, macro_paths, None);

        let is_exists_fn = make_is_exists_fn(incremental_state.clone());
        env.add_function("is_exists", is_exists_fn);

        let deprecation_capture: DeprecationCapture = Arc::new(Mutex::new(Vec::new()));
        let is_incremental_fn = make_is_incremental_fn(incremental_state, deprecation_capture);
        env.add_function("is_incremental", is_incremental_fn);

        let this_fn = make_this_fn(qualified_name.to_string());
        env.add_function("this", this_fn);

        Self {
            env,
            warning_capture,
            macro_preamble,
        }
    }

    /// Create a Jinja environment for compile-time dual-path rendering.
    ///
    /// Sets `is_exists()` to the provided boolean value. Also registers
    /// `is_incremental()` as a deprecated alias. Used to compile incremental
    /// models twice: once with `is_exists=false` (full path) and once with
    /// `is_exists=true` (incremental path).
    pub fn with_is_exists(
        vars: &HashMap<String, serde_yaml::Value>,
        macro_paths: &[PathBuf],
        template_context: Option<&TemplateContext>,
        is_exists_value: bool,
    ) -> Self {
        let (mut env, warning_capture, macro_preamble) =
            Self::init_common(vars, macro_paths, template_context);

        let val = is_exists_value;
        env.add_function("is_exists", move || val);

        let deprecation_capture: DeprecationCapture = Arc::new(Mutex::new(Vec::new()));
        let dep_cap = deprecation_capture;
        env.add_function("is_incremental", move || -> bool {
            let msg = "is_incremental() is deprecated, use is_exists() instead".to_string();
            eprintln!("[jinja:deprecation] {}", msg);
            if let Ok(mut warnings) = dep_cap.lock() {
                warnings.push(msg);
            }
            val
        });

        Self {
            env,
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
            collect_macros_from_entry(&entry, re, &mut file_macros);
        }
    }

    file_macros
        .iter()
        .map(|(file, names)| format!("{{%- from \"{}\" import {} -%}}", file, names.join(", ")))
        .collect()
}

/// Extract macro names from a single directory entry and add them to the map.
fn collect_macros_from_entry(
    entry: &std::fs::DirEntry,
    re: &Regex,
    file_macros: &mut BTreeMap<String, Vec<String>>,
) {
    let path = entry.path();
    if path.extension().and_then(|e| e.to_str()) != Some("sql") {
        return;
    }
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return,
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

/// Register all built-in macros with the Jinja environment
fn register_builtins(env: &mut Environment<'_>) {
    env.add_function("date_spine", make_date_spine_fn());
    env.add_function("date_trunc", make_date_trunc_fn());
    env.add_function("date_add", make_date_add_fn());
    env.add_function("date_diff", make_date_diff_fn());

    env.add_function("slugify", make_slugify_fn());
    env.add_function("clean_string", make_clean_string_fn());
    env.add_function("split_part", make_split_part_fn());

    env.add_function("safe_divide", make_safe_divide_fn());
    env.add_function("round_money", make_round_money_fn());
    env.add_function("percent_of", make_percent_of_fn());

    env.add_function("limit_zero", make_limit_zero_fn());
    env.add_function("bool_or", make_bool_or_fn());
    env.add_function("hash", make_hash_fn());

    env.add_function("hash_columns", make_hash_columns_fn());
    env.add_function("surrogate_key", make_surrogate_key_fn());
    env.add_function("coalesce_columns", make_coalesce_columns_fn());
    env.add_function("not_null", make_not_null_fn());
}

#[cfg(test)]
#[path = "environment_test.rs"]
mod tests;
