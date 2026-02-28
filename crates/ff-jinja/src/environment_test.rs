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
    let template = r#"SELECT
    id AS order_id,
    user_id AS customer_id,
    created_at AS order_date,
    amount
FROM raw.orders
WHERE created_at >= '{{ var("start_date") }}'
"#;

    let result = env.render(template).unwrap();

    assert!(result.contains("WHERE created_at >= '2024-01-01'"));
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

    // Create environment with macro path — macros auto-registered, no import needed
    let env = JinjaEnvironment::with_macros(&HashMap::new(), std::slice::from_ref(&macros_dir));

    let template = r#"SELECT {{ cents_to_dollars("amount_cents") }} AS amount_dollars FROM orders"#;

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

    // No explicit import — both macros auto-registered
    let template = r#"SELECT
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

    // No explicit import — auto-registered from filters.sql
    let template = r#"SELECT * FROM orders WHERE {{ date_filter("created_at", var("start_date"), var("end_date")) }}"#;

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

// ===== Context Variable Tests =====

#[test]
fn test_static_context_variables() {
    use crate::context::{TargetContext, TemplateContext};

    let ctx = TemplateContext::new(
        "my_project".to_string(),
        TargetContext {
            name: "dev".to_string(),
            schema: Some("analytics".to_string()),
            database_type: "duckdb".to_string(),
        },
        false,
    );

    let env = JinjaEnvironment::with_context(&HashMap::new(), &[], &ctx);

    let result = env.render("{{ project_name }}").unwrap();
    assert_eq!(result, "my_project");

    let result = env.render("{{ ff_version }}").unwrap();
    assert!(!result.is_empty());

    // run_id should be a valid UUID
    let result = env.render("{{ run_id }}").unwrap();
    assert_eq!(result.len(), 36); // UUID format: 8-4-4-4-12
    assert!(result.contains('-'));
}

#[test]
fn test_target_context_object() {
    use crate::context::{TargetContext, TemplateContext};

    let ctx = TemplateContext::new(
        "project".to_string(),
        TargetContext {
            name: "prod".to_string(),
            schema: Some("public".to_string()),
            database_type: "duckdb".to_string(),
        },
        true,
    );

    let env = JinjaEnvironment::with_context(&HashMap::new(), &[], &ctx);

    assert_eq!(env.render("{{ target.name }}").unwrap(), "prod");
    assert_eq!(env.render("{{ target.database_type }}").unwrap(), "duckdb");
    assert_eq!(env.render("{{ target.schema }}").unwrap(), "public");
}

#[test]
fn test_model_context() {
    use crate::context::ModelContext;

    let env = JinjaEnvironment::default();
    let model = ModelContext {
        name: "stg_orders".to_string(),
        schema: Some("staging".to_string()),
        materialized: "table".to_string(),
        tags: vec!["staging".to_string(), "orders".to_string()],
        path: "models/stg_orders/stg_orders.sql".to_string(),
    };

    let result = env.render_with_model("{{ model.name }}", &model).unwrap();
    assert_eq!(result, "stg_orders");

    let result = env
        .render_with_model("{{ model.materialized }}", &model)
        .unwrap();
    assert_eq!(result, "table");

    let result = env.render_with_model("{{ model.path }}", &model).unwrap();
    assert_eq!(result, "models/stg_orders/stg_orders.sql");
}

#[test]
fn test_model_context_optional() {
    // Rendering without model context should work — model just won't be available
    let env = JinjaEnvironment::default();
    let result = env.render("SELECT 1").unwrap();
    assert_eq!(result, "SELECT 1");
}

#[test]
fn test_executing_flag() {
    use crate::context::{TargetContext, TemplateContext};

    // executing = false (compile/validate)
    let ctx = TemplateContext::new("proj".to_string(), TargetContext::default(), false);
    let env = JinjaEnvironment::with_context(&HashMap::new(), &[], &ctx);
    assert_eq!(env.render("{{ executing }}").unwrap(), "false");

    // executing = true (run)
    let ctx = TemplateContext::new("proj".to_string(), TargetContext::default(), true);
    let env = JinjaEnvironment::with_context(&HashMap::new(), &[], &ctx);
    assert_eq!(env.render("{{ executing }}").unwrap(), "true");
}

#[test]
fn test_warn_capture_and_clear() {
    let env = JinjaEnvironment::default();

    // First render with a warning
    let result = env.render("{{ warn('first warning') }}SELECT 1").unwrap();
    assert_eq!(result, "SELECT 1");
    let warnings = env.get_captured_warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0], "first warning");

    // Second render should clear previous warnings
    let result = env.render("{{ warn('second warning') }}SELECT 2").unwrap();
    assert_eq!(result, "SELECT 2");
    let warnings = env.get_captured_warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0], "second warning");

    // Render without warnings should have empty capture
    let _ = env.render("SELECT 3").unwrap();
    assert!(env.get_captured_warnings().is_empty());
}

#[test]
fn test_backward_compatibility() {
    // JinjaEnvironment::new() and with_macros() should still work
    let env1 = JinjaEnvironment::new(&HashMap::new());
    assert_eq!(env1.render("SELECT 1").unwrap(), "SELECT 1");

    let env2 = JinjaEnvironment::with_macros(&HashMap::new(), &[]);
    assert_eq!(env2.render("SELECT 2").unwrap(), "SELECT 2");

    let env3 = JinjaEnvironment::default();
    assert_eq!(env3.render("SELECT 3").unwrap(), "SELECT 3");
}

// ===== Integration tests for new functions via render =====

#[test]
fn test_env_fn_via_render() {
    std::env::set_var("FF_JINJA_TEST_DB", "mydb");
    let env = JinjaEnvironment::default();
    let result = env.render("{{ env('FF_JINJA_TEST_DB') }}").unwrap();
    assert_eq!(result, "mydb");
    std::env::remove_var("FF_JINJA_TEST_DB");
}

#[test]
fn test_env_fn_default_via_render() {
    std::env::remove_var("FF_JINJA_TEST_NONEXIST");
    let env = JinjaEnvironment::default();
    let result = env
        .render("{{ env('FF_JINJA_TEST_NONEXIST', 'fallback') }}")
        .unwrap();
    assert_eq!(result, "fallback");
}

#[test]
fn test_log_fn_via_render() {
    let env = JinjaEnvironment::default();
    let result = env.render("before{{ log('debug msg') }}after").unwrap();
    assert_eq!(result, "beforeafter");
}

#[test]
fn test_error_fn_via_render() {
    let env = JinjaEnvironment::default();
    let result = env.render("{{ error('bad thing') }}");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("bad thing"));
}

#[test]
fn test_from_json_via_render() {
    let env = JinjaEnvironment::default();
    let template = r#"{% set data = from_json('{"name": "alice"}') %}{{ data.name }}"#;
    let result = env.render(template).unwrap();
    assert_eq!(result, "alice");
}

#[test]
fn test_to_json_function_via_render() {
    let env = JinjaEnvironment::default();
    // to_json on a string
    let result = env.render("{{ to_json('hello') }}").unwrap();
    assert_eq!(result, r#""hello""#);
}

#[test]
fn test_to_json_as_filter() {
    let env = JinjaEnvironment::default();
    let result = env.render("{{ 42 | to_json }}").unwrap();
    assert_eq!(result, "42");
}

#[test]
fn test_render_with_model() {
    use crate::context::ModelContext;

    let env = JinjaEnvironment::default();
    let model = ModelContext {
        name: "test_model".to_string(),
        materialized: "view".to_string(),
        ..Default::default()
    };

    let rendered = env.render_with_model("{{ model.name }}", &model).unwrap();
    assert_eq!(rendered, "test_model");
}

// ===== Auto-registration tests =====

#[test]
fn test_macro_auto_registration() {
    use std::fs;
    use tempfile::TempDir;

    let temp = TempDir::new().unwrap();
    let macros_dir = temp.path().join("macros");
    fs::create_dir(&macros_dir).unwrap();

    fs::write(
        macros_dir.join("helpers.sql"),
        r#"{% macro greet(name) %}Hello, {{ name }}!{% endmacro %}
{% macro farewell(name) %}Goodbye, {{ name }}!{% endmacro %}"#,
    )
    .unwrap();

    let env = JinjaEnvironment::with_macros(&HashMap::new(), &[macros_dir]);

    // Both macros should be available without any import statement
    let result = env
        .render(r#"{{ greet("world") }} {{ farewell("world") }}"#)
        .unwrap();
    assert_eq!(result, "Hello, world! Goodbye, world!");
}

#[test]
fn test_macro_auto_registration_multiple_files() {
    use std::fs;
    use tempfile::TempDir;

    let temp = TempDir::new().unwrap();
    let macros_dir = temp.path().join("macros");
    fs::create_dir(&macros_dir).unwrap();

    fs::write(
        macros_dir.join("math.sql"),
        r#"{% macro double(x) %}({{ x }} * 2){% endmacro %}"#,
    )
    .unwrap();

    fs::write(
        macros_dir.join("strings.sql"),
        r#"{% macro upper(col) %}UPPER({{ col }}){% endmacro %}"#,
    )
    .unwrap();

    let env = JinjaEnvironment::with_macros(&HashMap::new(), &[macros_dir]);

    // Macros from different files should all be available
    let result = env
        .render(r#"SELECT {{ double("price") }}, {{ upper("name") }}"#)
        .unwrap();
    assert!(result.contains("(price * 2)"));
    assert!(result.contains("UPPER(name)"));
}

#[test]
fn test_macro_explicit_import_still_works() {
    use std::fs;
    use tempfile::TempDir;

    let temp = TempDir::new().unwrap();
    let macros_dir = temp.path().join("macros");
    fs::create_dir(&macros_dir).unwrap();

    fs::write(
        macros_dir.join("utils.sql"),
        r#"{% macro greet(name) %}Hi, {{ name }}{% endmacro %}"#,
    )
    .unwrap();

    let env = JinjaEnvironment::with_macros(&HashMap::new(), &[macros_dir]);

    // Explicit import should still work (duplicate import is fine in minijinja)
    let template = r#"{% from "utils.sql" import greet %}{{ greet("Alice") }}"#;
    let result = env.render(template).unwrap();
    assert_eq!(result, "Hi, Alice");
}

// ===== is_exists() / is_incremental() integration tests =====

#[test]
fn test_is_exists_true_via_render() {
    let env = JinjaEnvironment::with_is_exists(&HashMap::new(), &[], None, true);
    let result = env
        .render("{% if is_exists() %}WHERE updated_at > '2024-01-01'{% endif %}")
        .unwrap();
    assert_eq!(result, "WHERE updated_at > '2024-01-01'");
}

#[test]
fn test_is_exists_false_via_render() {
    let env = JinjaEnvironment::with_is_exists(&HashMap::new(), &[], None, false);
    let result = env
        .render("{% if is_exists() %}WHERE updated_at > '2024-01-01'{% endif %}")
        .unwrap();
    assert_eq!(result, "");
}

#[test]
fn test_is_incremental_deprecated_via_render() {
    let env = JinjaEnvironment::with_is_exists(&HashMap::new(), &[], None, true);
    // is_incremental() should still work as a deprecated alias
    let result = env
        .render("{% if is_incremental() %}WHERE updated_at > '2024-01-01'{% endif %}")
        .unwrap();
    assert_eq!(result, "WHERE updated_at > '2024-01-01'");
}

#[test]
fn test_dual_path_full_and_incremental() {
    let template = r#"SELECT id, name FROM source
{% if is_exists() %}WHERE updated_at > (SELECT MAX(updated_at) FROM target){% endif %}"#;

    // Full path
    let env_full = JinjaEnvironment::with_is_exists(&HashMap::new(), &[], None, false);
    let full_result = env_full.render(template).unwrap();
    assert_eq!(full_result.trim(), "SELECT id, name FROM source");

    // Incremental path
    let env_inc = JinjaEnvironment::with_is_exists(&HashMap::new(), &[], None, true);
    let inc_result = env_inc.render(template).unwrap();
    assert!(inc_result.contains("WHERE updated_at > (SELECT MAX(updated_at) FROM target)"));
}

#[test]
fn test_with_incremental_context_registers_both_functions() {
    use crate::functions::IncrementalState;

    let state = IncrementalState::new(true, true, false);
    let env = JinjaEnvironment::with_incremental_context(
        &HashMap::new(),
        &[],
        state,
        "main.public.my_model",
    );

    // is_exists() should return true
    let result = env.render("{{ is_exists() }}").unwrap();
    assert_eq!(result, "true");

    // is_incremental() (deprecated) should also return true
    let result = env.render("{{ is_incremental() }}").unwrap();
    assert_eq!(result, "true");

    // this() should return the qualified name
    let result = env.render("{{ this() }}").unwrap();
    assert_eq!(result, "main.public.my_model");
}
