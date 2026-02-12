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
    assert!(result.contains("CASE WHEN count = 0 THEN 0 ELSE revenue / count END AS avg_revenue"));
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
    assert_eq!(result, "DATE_TRUNC('month', \"created_at\")");
}

#[test]
fn test_builtin_date_add() {
    let env = JinjaEnvironment::default();
    let result = env
        .render("{{ date_add('order_date', 7, 'day') }}")
        .unwrap();
    assert_eq!(result, "\"order_date\" + INTERVAL '7 day'");
}

#[test]
fn test_builtin_date_diff() {
    let env = JinjaEnvironment::default();
    let result = env
        .render("{{ date_diff('day', 'start_date', 'end_date') }}")
        .unwrap();
    assert_eq!(result, "DATE_DIFF('day', \"start_date\", \"end_date\")");
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
    assert_eq!(result, "ROUND(CAST(\"amount\" AS DOUBLE), 2)");
}

#[test]
fn test_builtin_percent_of() {
    let env = JinjaEnvironment::default();
    let result = env.render("{{ percent_of('sales', 'total') }}").unwrap();
    assert!(result.contains("100.0"));
    assert!(result.contains("\"sales\""));
    assert!(result.contains("\"total\""));
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
    assert_eq!(result, "SPLIT_PART(\"email\", '@', 2)");
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
    assert_eq!(result, "BOOL_OR(\"is_active\")");
}

#[test]
fn test_builtin_hash() {
    let env = JinjaEnvironment::default();
    let result = env.render("{{ hash('user_id') }}").unwrap();
    assert_eq!(result, "MD5(CAST(\"user_id\" AS VARCHAR))");
}

#[test]
fn test_builtin_not_null() {
    let env = JinjaEnvironment::default();
    let result = env.render("{{ not_null('email') }}").unwrap();
    assert_eq!(result, "\"email\" IS NOT NULL");
}

#[test]
fn test_builtin_surrogate_key() {
    let env = JinjaEnvironment::default();
    let result = env.render("{{ surrogate_key(['col1', 'col2']) }}").unwrap();
    assert!(result.contains("MD5"));
    assert!(result.contains("\"col1\""));
    assert!(result.contains("\"col2\""));
}

#[test]
fn test_builtin_coalesce_columns() {
    let env = JinjaEnvironment::default();
    let result = env
        .render("{{ coalesce_columns(['a', 'b', 'c']) }}")
        .unwrap();
    assert_eq!(result, "COALESCE(\"a\", \"b\", \"c\")");
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
    assert!(result.contains(r#"ROUND(CAST("price" AS DOUBLE), 2) AS price_rounded"#));
    assert!(result.contains(r#"CASE WHEN "orders" = 0 OR "orders" IS NULL THEN NULL"#));
    assert!(result.contains(r#"DATE_TRUNC('month', "created_at") AS month"#));
    assert!(result.contains(r#"WHERE "customer_id" IS NOT NULL"#));
}
