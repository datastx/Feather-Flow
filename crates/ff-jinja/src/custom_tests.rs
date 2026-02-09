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

/// Discover custom test macros from macro files
///
/// Scans the given macro paths for files containing macros with the `test_` prefix.
/// Returns a list of discovered custom test macros.
pub fn discover_custom_test_macros(macro_paths: &[impl AsRef<Path>]) -> Vec<CustomTestMacro> {
    let mut discovered = Vec::new();

    for macro_path in macro_paths {
        let path = macro_path.as_ref();
        if !path.exists() || !path.is_dir() {
            continue;
        }

        // Scan all .sql files in the directory
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let file_path = entry.path();
                if file_path.extension().is_some_and(|e| e == "sql") {
                    if let Ok(content) = fs::read_to_string(&file_path) {
                        let file_macros = extract_test_macros_from_content(
                            &content,
                            file_path.to_string_lossy().as_ref(),
                        );
                        discovered.extend(file_macros);
                    }
                }
            }
        }
    }

    discovered
}

/// Extract test macro names from file content
///
/// Looks for patterns like `{% macro test_<name>(...) %}` in the content.
fn extract_test_macros_from_content(content: &str, source_file: &str) -> Vec<CustomTestMacro> {
    use std::sync::OnceLock;

    let mut macros = Vec::new();

    // Look for macro definitions with test_ prefix
    // Pattern: {% macro test_<name>( or {%- macro test_<name>(
    static MACRO_PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    let macro_pattern = MACRO_PATTERN.get_or_init(|| {
        regex::Regex::new(r#"\{%-?\s*macro\s+(test_(\w+))\s*\("#).expect("valid regex literal")
    });

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

    // Create template that imports and calls the macro
    let template = format!(
        r#"{{% from "{}" import {} %}}
{{{{ {}("{}", "{}"{}) }}}}"#,
        relative_file, macro_name, macro_name, safe_model, safe_column, kwargs_str
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

    /// Discover and register custom test macros from paths
    pub fn discover(&mut self, macro_paths: &[impl AsRef<Path>]) {
        let discovered = discover_custom_test_macros(macro_paths);
        for macro_info in discovered {
            self.macros.insert(macro_info.name.clone(), macro_info);
        }
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
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_extract_test_macros_from_content() {
        let content = r#"
{% macro test_valid_email(model, column) %}
SELECT * FROM {{ model }} WHERE {{ column }} NOT LIKE '%@%.%'
{% endmacro %}

{% macro test_positive_value(model, column) %}
SELECT * FROM {{ model }} WHERE {{ column }} <= 0
{% endmacro %}

{% macro some_other_macro(x) %}
SELECT {{ x }}
{% endmacro %}
"#;

        let macros = extract_test_macros_from_content(content, "tests.sql");
        assert_eq!(macros.len(), 2);
        assert_eq!(macros[0].name, "valid_email");
        assert_eq!(macros[0].macro_name, "test_valid_email");
        assert_eq!(macros[1].name, "positive_value");
        assert_eq!(macros[1].macro_name, "test_positive_value");
    }

    #[test]
    fn test_extract_test_macros_with_whitespace() {
        let content = r#"
{%- macro test_no_duplicates(model, column) -%}
SELECT {{ column }} FROM {{ model }} GROUP BY {{ column }} HAVING COUNT(*) > 1
{%- endmacro -%}
"#;

        let macros = extract_test_macros_from_content(content, "tests.sql");
        assert_eq!(macros.len(), 1);
        assert_eq!(macros[0].name, "no_duplicates");
    }

    #[test]
    fn test_discover_custom_test_macros() {
        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        fs::write(
            macros_dir.join("custom_tests.sql"),
            r#"
{% macro test_valid_email(model, column) %}
SELECT * FROM {{ model }} WHERE {{ column }} NOT LIKE '%@%.%'
{% endmacro %}
"#,
        )
        .unwrap();

        let macros = discover_custom_test_macros(&[&macros_dir]);
        assert_eq!(macros.len(), 1);
        assert_eq!(macros[0].name, "valid_email");
    }

    #[test]
    fn test_custom_test_registry() {
        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        fs::write(
            macros_dir.join("tests.sql"),
            r#"
{% macro test_in_range(model, column, min_val, max_val) %}
SELECT * FROM {{ model }} WHERE {{ column }} < {{ min_val }} OR {{ column }} > {{ max_val }}
{% endmacro %}

{% macro test_valid_status(model, column) %}
SELECT * FROM {{ model }} WHERE {{ column }} NOT IN ('active', 'inactive', 'pending')
{% endmacro %}
"#,
        )
        .unwrap();

        let mut registry = CustomTestRegistry::new();
        registry.discover(&[&macros_dir]);

        assert_eq!(registry.len(), 2);
        assert!(registry.is_custom_test("in_range"));
        assert!(registry.is_custom_test("valid_status"));
        assert!(!registry.is_custom_test("unknown"));

        let in_range = registry.get("in_range").unwrap();
        assert_eq!(in_range.macro_name, "test_in_range");
    }

    #[test]
    fn test_json_value_to_jinja() {
        assert_eq!(json_value_to_jinja(&serde_json::json!(null)), "none");
        assert_eq!(json_value_to_jinja(&serde_json::json!(true)), "true");
        assert_eq!(json_value_to_jinja(&serde_json::json!(false)), "false");
        assert_eq!(json_value_to_jinja(&serde_json::json!(42)), "42");
        assert_eq!(json_value_to_jinja(&serde_json::json!(3.15)), "3.15");
        assert_eq!(
            json_value_to_jinja(&serde_json::json!("hello")),
            "\"hello\""
        );
        assert_eq!(
            json_value_to_jinja(&serde_json::json!([1, 2, 3])),
            "[1, 2, 3]"
        );
    }

    #[test]
    fn test_generate_custom_test_sql() {
        use minijinja::path_loader;

        let temp = TempDir::new().unwrap();
        let macros_dir = temp.path().join("macros");
        fs::create_dir(&macros_dir).unwrap();

        fs::write(
            macros_dir.join("tests.sql"),
            r#"{% macro test_in_range(model, column, min_val, max_val) %}
SELECT * FROM {{ model }} WHERE {{ column }} < {{ min_val }} OR {{ column }} > {{ max_val }}
{% endmacro %}"#,
        )
        .unwrap();

        let mut env = Environment::new();
        env.set_loader(path_loader(&macros_dir));

        let mut kwargs = HashMap::new();
        kwargs.insert("min_val".to_string(), serde_json::json!(0));
        kwargs.insert("max_val".to_string(), serde_json::json!(100));

        let sql = generate_custom_test_sql(
            &env,
            "tests.sql",
            "test_in_range",
            "orders",
            "amount",
            &kwargs,
        )
        .unwrap();

        assert!(sql.contains("SELECT * FROM orders"));
        assert!(sql.contains("amount < 0"));
        assert!(sql.contains("amount > 100"));
    }
}
