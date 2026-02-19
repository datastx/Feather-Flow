//! Custom test macro discovery and execution
//!
//! This module provides functionality to discover and execute custom test macros
//! defined by users. Custom test macros follow the naming convention `test_<name>`
//! and generate SQL that returns failing rows (if any).

use crate::error::{JinjaError, JinjaResult};
use minijinja::Environment;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Metadata for a discovered custom test macro
#[derive(Debug, Clone, Serialize)]
pub struct CustomTestMacro {
    /// Name of the test (without the test_ prefix)
    pub name: String,
    /// Full macro name (with test_ prefix)
    pub macro_name: String,
    /// Source file path
    pub source_file: String,
}

/// Discover custom test macros from macro files.
///
/// Scans the given macro paths for files containing macros with the `test_` prefix.
/// Returns a list of discovered custom test macros, or an error if any I/O operation fails.
pub fn discover_custom_test_macros(
    macro_paths: &[impl AsRef<Path>],
) -> JinjaResult<Vec<CustomTestMacro>> {
    let mut discovered = Vec::new();

    for macro_path in macro_paths {
        let path = macro_path.as_ref();
        if !path.exists() || !path.is_dir() {
            continue;
        }

        // Scan top-level .sql files in the directory (non-recursive by design)
        let entries = fs::read_dir(path).map_err(|e| {
            JinjaError::Internal(format!(
                "failed to read macro directory {}: {}",
                path.display(),
                e
            ))
        })?;

        for entry_result in entries {
            let entry = entry_result.map_err(|e| {
                JinjaError::Internal(format!(
                    "failed to read directory entry in {}: {}",
                    path.display(),
                    e
                ))
            })?;
            let file_path = entry.path();
            if file_path.extension().is_some_and(|e| e == "sql") {
                let file_macros = process_macro_file(&file_path)?;
                discovered.extend(file_macros);
            }
        }
    }

    Ok(discovered)
}

/// Read a single macro file and extract any test macros from it.
fn process_macro_file(file_path: &std::path::PathBuf) -> JinjaResult<Vec<CustomTestMacro>> {
    let content = fs::read_to_string(file_path).map_err(|e| {
        JinjaError::Internal(format!(
            "failed to read macro file {}: {}",
            file_path.display(),
            e
        ))
    })?;
    Ok(extract_test_macros_from_content(
        &content,
        file_path.to_string_lossy().as_ref(),
    ))
}

/// Extract test macro names from file content
///
/// Looks for patterns like `{% macro test_<name>(...) %}` in the content.
fn extract_test_macros_from_content(content: &str, source_file: &str) -> Vec<CustomTestMacro> {
    use std::sync::LazyLock;

    let mut macros = Vec::new();

    static MACRO_PATTERN: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"\{%-?\s*macro\s+(test_(\w+))\s*\("#).expect("valid regex literal")
    });
    let macro_pattern = &*MACRO_PATTERN;

    for captures in macro_pattern.captures_iter(content) {
        if let (Some(full_name), Some(test_name)) = (captures.get(1), captures.get(2)) {
            macros.push(CustomTestMacro {
                name: test_name.as_str().to_string(),
                macro_name: full_name.as_str().to_string(),
                source_file: source_file.to_string(),
            });
        }
    }

    macros
}

/// Generate SQL for a custom test macro
///
/// Invokes the test macro with the given model, column, and kwargs to generate SQL.
/// The macro is expected to return SQL that selects failing rows.
pub fn generate_custom_test_sql(
    env: &Environment<'_>,
    macro_source_file: &str,
    macro_name: &str,
    model: &str,
    column: &str,
    kwargs: &HashMap<String, serde_json::Value>,
) -> JinjaResult<String> {
    // Build the template that imports and calls the macro
    let relative_file = Path::new(macro_source_file)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| macro_source_file.to_string());

    // Convert kwargs to Jinja syntax
    let kwargs_str = if kwargs.is_empty() {
        String::new()
    } else {
        // Validate kwarg keys are safe identifiers to prevent Jinja injection
        for k in kwargs.keys() {
            if !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(JinjaError::RenderError(format!(
                    "invalid kwarg key '{}': must contain only alphanumeric characters and underscores",
                    k
                )));
            }
        }
        let pairs: Vec<String> = kwargs
            .iter()
            .map(|(k, v)| format!("{}={}", k, json_value_to_jinja(v)))
            .collect();
        format!(", {}", pairs.join(", "))
    };

    // Escape model and column for safe embedding in Jinja string literals.
    // Without this, a model/column name containing `"` or `{{` could break
    // out of the string literal and inject template code.
    let safe_model = model.replace('\\', "\\\\").replace('"', "\\\"");
    let safe_column = column.replace('\\', "\\\\").replace('"', "\\\"");

    // Escape relative_file for safe embedding in the Jinja import string literal.
    let safe_file = relative_file.replace('\\', "\\\\").replace('"', "\\\"");

    // Validate macro_name contains only safe identifier characters to prevent
    // Jinja template injection via crafted macro names.
    if !macro_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(JinjaError::RenderError(format!(
            "invalid macro name '{}': must contain only alphanumeric characters and underscores",
            macro_name
        )));
    }

    // Create template that imports and calls the macro
    let template = format!(
        r#"{{% from "{}" import {} %}}
{{{{ {}("{}", "{}"{}) }}}}"#,
        safe_file, macro_name, macro_name, safe_model, safe_column, kwargs_str
    );

    // Render the template
    let result = env.render_str(&template, ()).map_err(JinjaError::from)?;

    Ok(result.trim().to_string())
}

/// Convert a JSON value to Jinja syntax
fn json_value_to_jinja(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "none".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_value_to_jinja).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            let pairs: Vec<String> = obj
                .iter()
                .map(|(k, v)| {
                    let escaped_key = k.replace('\\', "\\\\").replace('"', "\\\"");
                    format!("\"{}\": {}", escaped_key, json_value_to_jinja(v))
                })
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

/// Registry of custom test macros
#[derive(Debug, Clone, Default)]
pub struct CustomTestRegistry {
    /// Map of test name (without prefix) to macro info
    macros: HashMap<String, CustomTestMacro>,
}

impl CustomTestRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            macros: HashMap::new(),
        }
    }

    /// Discover and register custom test macros from paths.
    pub fn discover(&mut self, macro_paths: &[impl AsRef<Path>]) -> JinjaResult<()> {
        let discovered = discover_custom_test_macros(macro_paths)?;
        for macro_info in discovered {
            self.macros.insert(macro_info.name.clone(), macro_info);
        }
        Ok(())
    }

    /// Check if a test type is a registered custom test
    pub fn is_custom_test(&self, name: &str) -> bool {
        self.macros.contains_key(name)
    }

    /// Get info for a custom test
    pub fn get(&self, name: &str) -> Option<&CustomTestMacro> {
        self.macros.get(name)
    }

    /// Get all registered custom tests
    pub fn list(&self) -> Vec<&CustomTestMacro> {
        self.macros.values().collect()
    }

    /// Get the number of registered custom tests
    pub fn len(&self) -> usize {
        self.macros.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.macros.is_empty()
    }
}

#[cfg(test)]
#[path = "custom_tests_test.rs"]
mod tests;
