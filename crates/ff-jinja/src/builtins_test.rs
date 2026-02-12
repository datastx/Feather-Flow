use super::*;

#[test]
fn test_date_spine() {
    let result = date_spine("2024-01-01", "2024-01-31").unwrap();
    assert!(result.contains("generate_series"));
    assert!(result.contains("2024-01-01"));
    assert!(result.contains("2024-01-31"));
}

#[test]
fn test_date_spine_rejects_bad_start_date() {
    let err = date_spine("not-a-date", "2024-01-31").unwrap_err();
    assert!(err.to_string().contains("YYYY-MM-DD"));
}

#[test]
fn test_date_spine_rejects_bad_end_date() {
    let err = date_spine("2024-01-01", "01/31/2024").unwrap_err();
    assert!(err.to_string().contains("YYYY-MM-DD"));
}

#[test]
fn test_date_trunc() {
    let result = date_trunc("month", "created_at");
    assert_eq!(result, "DATE_TRUNC('month', \"created_at\")");
}

#[test]
fn test_date_add() {
    let result = date_add("order_date", 7, "day");
    assert_eq!(result, "\"order_date\" + INTERVAL '7 day'");
}

#[test]
fn test_date_diff() {
    let result = date_diff("day", "start_date", "end_date");
    assert_eq!(result, "DATE_DIFF('day', \"start_date\", \"end_date\")");
}

#[test]
fn test_slugify() {
    let result = slugify("title");
    assert!(result.contains("LOWER"));
    assert!(result.contains("REGEXP_REPLACE"));
    assert!(result.contains("\"title\""));
}

#[test]
fn test_clean_string() {
    let result = clean_string("name");
    assert!(result.contains("TRIM"));
    assert!(result.contains("REGEXP_REPLACE"));
    assert!(result.contains("\"name\""));
}

#[test]
fn test_split_part() {
    let result = split_part("email", "@", 2);
    assert_eq!(result, "SPLIT_PART(\"email\", '@', 2)");
}

#[test]
fn test_safe_divide() {
    let result = safe_divide("revenue", "count");
    assert!(result.contains("CASE WHEN"));
    assert!(result.contains("IS NULL"));
    assert!(result.contains("\"revenue\""));
    assert!(result.contains("\"count\""));
}

#[test]
fn test_round_money() {
    let result = round_money("amount");
    assert_eq!(result, "ROUND(CAST(\"amount\" AS DOUBLE), 2)");
}

#[test]
fn test_percent_of() {
    let result = percent_of("sales", "total_sales");
    assert!(result.contains("100.0"));
    assert!(result.contains("\"sales\""));
    assert!(result.contains("\"total_sales\""));
}

#[test]
fn test_limit_zero() {
    assert_eq!(limit_zero(), "LIMIT 0");
}

#[test]
fn test_bool_or() {
    let result = bool_or("is_active");
    assert_eq!(result, "BOOL_OR(\"is_active\")");
}

#[test]
fn test_hash() {
    let result = hash("user_id");
    assert_eq!(result, "MD5(CAST(\"user_id\" AS VARCHAR))");
}

#[test]
fn test_hash_columns() {
    let result = hash_columns(&["col1".to_string(), "col2".to_string()]);
    assert!(result.contains("MD5"));
    assert!(result.contains("COALESCE"));
    assert!(result.contains("\"col1\""));
    assert!(result.contains("\"col2\""));
}

#[test]
fn test_surrogate_key() {
    let result = surrogate_key(&["id".to_string(), "type".to_string()]);
    assert!(result.contains("MD5"));
}

#[test]
fn test_coalesce_columns() {
    let result = coalesce_columns(&["a".to_string(), "b".to_string(), "c".to_string()]);
    assert_eq!(result, "COALESCE(\"a\", \"b\", \"c\")");
}

#[test]
fn test_not_null() {
    let result = not_null("email");
    assert_eq!(result, "\"email\" IS NOT NULL");
}

// ===== Macro Metadata Tests =====

#[test]
fn test_get_builtin_macros_count() {
    let macros = get_builtin_macros();
    assert_eq!(macros.len(), 17, "Expected 17 built-in macros");
}

#[test]
fn test_get_builtin_macros_all_have_required_fields() {
    for m in get_builtin_macros() {
        assert!(!m.name.is_empty(), "Macro name should not be empty");
        assert!(!m.category.is_empty(), "Macro category should not be empty");
        assert!(
            !m.description.is_empty(),
            "Macro description should not be empty"
        );
        assert!(!m.example.is_empty(), "Macro example should not be empty");
        assert!(
            !m.example_output.is_empty(),
            "Macro example_output should not be empty"
        );
        // Verify example uses the macro
        assert!(
            m.example.contains(m.name),
            "Example '{}' should contain macro name '{}'",
            m.example,
            m.name
        );
    }
}

#[test]
fn test_get_macro_by_name() {
    let date_trunc = get_macro_by_name("date_trunc");
    assert!(date_trunc.is_some());
    let dt = date_trunc.unwrap();
    assert_eq!(dt.name, "date_trunc");
    assert_eq!(dt.category, "date");
    assert_eq!(dt.params.len(), 2);

    let not_found = get_macro_by_name("nonexistent_macro");
    assert!(not_found.is_none());
}

#[test]
fn test_get_macros_by_category() {
    let date_macros = get_macros_by_category("date");
    assert_eq!(date_macros.len(), 4);
    for m in &date_macros {
        assert_eq!(m.category, "date");
    }

    let string_macros = get_macros_by_category("string");
    assert_eq!(string_macros.len(), 3);

    let math_macros = get_macros_by_category("math");
    assert_eq!(math_macros.len(), 3);

    let utility_macros = get_macros_by_category("utility");
    assert_eq!(utility_macros.len(), 4);

    let cross_db_macros = get_macros_by_category("cross_db");
    assert_eq!(cross_db_macros.len(), 3);
}

#[test]
fn test_get_macro_categories() {
    let categories = get_macro_categories();
    assert_eq!(categories.len(), 5);
    assert!(categories.contains(&"date"));
    assert!(categories.contains(&"string"));
    assert!(categories.contains(&"math"));
    assert!(categories.contains(&"utility"));
    assert!(categories.contains(&"cross_db"));
}

#[test]
fn test_macro_param_creation() {
    let required_param = MacroParam::required("column", "string", "A column name");
    assert!(required_param.required);
    assert_eq!(required_param.name, "column");
    assert_eq!(required_param.param_type, "string");

    let optional_param = MacroParam::optional("limit", "integer", "Maximum rows");
    assert!(!optional_param.required);
    assert_eq!(optional_param.name, "limit");
}

#[test]
fn test_macro_metadata_serializable() {
    let macros = get_builtin_macros();
    // Should be serializable to JSON
    let json = serde_json::to_string(&macros).expect("Failed to serialize macros");
    assert!(json.contains("date_trunc"));
    assert!(json.contains("surrogate_key"));
}
