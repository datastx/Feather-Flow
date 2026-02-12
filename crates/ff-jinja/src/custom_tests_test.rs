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

    let macros = discover_custom_test_macros(&[&macros_dir]).unwrap();
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
    registry.discover(&[&macros_dir]).unwrap();

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
