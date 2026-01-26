//! Built-in macros for Featherflow
//!
//! This module provides built-in macros that are available in all templates
//! without needing to import them from macro files.

use minijinja::value::Value;
use minijinja::Error;

// ===== Date/Time Macros =====

/// Generate a date spine (range of dates)
///
/// Usage: `{{ date_spine('2024-01-01', '2024-12-31') }}`
/// Returns SQL that generates a series of dates
pub fn date_spine(start_date: &str, end_date: &str) -> String {
    // DuckDB-specific syntax for generating date series
    format!(
        "SELECT CAST(unnest AS DATE) AS date_day FROM unnest(generate_series(DATE '{}', DATE '{}', INTERVAL '1 day'))",
        start_date, end_date
    )
}

/// Truncate a date to a specific part
///
/// Usage: `{{ date_trunc('month', 'created_at') }}`
pub fn date_trunc(date_part: &str, column: &str) -> String {
    format!("DATE_TRUNC('{}', {})", date_part, column)
}

/// Add an interval to a date
///
/// Usage: `{{ date_add('created_at', 7, 'day') }}`
pub fn date_add(column: &str, amount: i64, unit: &str) -> String {
    format!("{} + INTERVAL '{} {}'", column, amount, unit)
}

/// Calculate the difference between two dates
///
/// Usage: `{{ date_diff('day', 'start_date', 'end_date') }}`
pub fn date_diff(unit: &str, start_col: &str, end_col: &str) -> String {
    format!("DATE_DIFF('{}', {}, {})", unit, start_col, end_col)
}

// ===== String Macros =====

/// Convert a string to a URL-friendly slug
///
/// Usage: `{{ slugify('column_name') }}`
pub fn slugify(column: &str) -> String {
    format!(
        "LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM({}), '[^a-zA-Z0-9]+', '-', 'g'), '^-|-$', '', 'g'))",
        column
    )
}

/// Clean a string by removing special characters
///
/// Usage: `{{ clean_string('column_name') }}`
pub fn clean_string(column: &str) -> String {
    format!("TRIM(REGEXP_REPLACE({}, '[^a-zA-Z0-9 ]', '', 'g'))", column)
}

/// Get a specific part of a split string
///
/// Usage: `{{ split_part('column_name', '-', 1) }}`
pub fn split_part(column: &str, delimiter: &str, part: i64) -> String {
    format!("SPLIT_PART({}, '{}', {})", column, delimiter, part)
}

// ===== Math Macros =====

/// Safely divide two numbers, returning NULL if denominator is 0
///
/// Usage: `{{ safe_divide('numerator', 'denominator') }}`
pub fn safe_divide(numerator: &str, denominator: &str) -> String {
    format!(
        "CASE WHEN {} = 0 OR {} IS NULL THEN NULL ELSE CAST({} AS DOUBLE) / {} END",
        denominator, denominator, numerator, denominator
    )
}

/// Round a number to 2 decimal places (for money)
///
/// Usage: `{{ round_money('amount') }}`
pub fn round_money(column: &str) -> String {
    format!("ROUND(CAST({} AS DOUBLE), 2)", column)
}

/// Calculate percentage of a value relative to a total
///
/// Usage: `{{ percent_of('value', 'total') }}`
pub fn percent_of(value: &str, total: &str) -> String {
    format!(
        "CASE WHEN {} = 0 OR {} IS NULL THEN 0.0 ELSE ROUND(100.0 * {} / {}, 2) END",
        total, total, value, total
    )
}

// ===== Cross-DB Macros =====

/// Generate a LIMIT 0 clause for schema validation
///
/// Usage: `{{ limit_zero() }}`
pub fn limit_zero() -> String {
    "LIMIT 0".to_string()
}

/// Boolean OR aggregation (works across dialects)
///
/// Usage: `{{ bool_or('is_active') }}`
pub fn bool_or(column: &str) -> String {
    format!("BOOL_OR({})", column)
}

/// Hash a column using a consistent algorithm
///
/// Usage: `{{ hash('column_name') }}`
pub fn hash(column: &str) -> String {
    format!("MD5(CAST({} AS VARCHAR))", column)
}

/// Hash multiple columns into a single value
///
/// Usage: `{{ hash_columns(['col1', 'col2', 'col3']) }}`
pub fn hash_columns(columns: Vec<String>) -> String {
    let concat_expr = columns
        .iter()
        .map(|c| format!("COALESCE(CAST({} AS VARCHAR), '')", c))
        .collect::<Vec<_>>()
        .join(" || '|' || ");
    format!("MD5({})", concat_expr)
}

// ===== Utility Macros =====

/// Generate a surrogate key from multiple columns
///
/// Usage: `{{ surrogate_key(['col1', 'col2']) }}`
pub fn surrogate_key(columns: Vec<String>) -> String {
    hash_columns(columns)
}

/// Coalesce multiple columns
///
/// Usage: `{{ coalesce_columns(['col1', 'col2', 'col3']) }}`
pub fn coalesce_columns(columns: Vec<String>) -> String {
    format!("COALESCE({})", columns.join(", "))
}

/// Generate a not-null check expression
///
/// Usage: `{{ not_null('column_name') }}`
pub fn not_null(column: &str) -> String {
    format!("{} IS NOT NULL", column)
}

// ===== Minijinja Function Wrappers =====

/// Wrapper for date_spine as a minijinja function
pub fn make_date_spine_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static {
    move |start: &str, end: &str| date_spine(start, end)
}

/// Wrapper for date_trunc as a minijinja function
pub fn make_date_trunc_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static {
    move |part: &str, col: &str| date_trunc(part, col)
}

/// Wrapper for date_add as a minijinja function
pub fn make_date_add_fn() -> impl Fn(&str, i64, &str) -> String + Send + Sync + Clone + 'static {
    move |col: &str, amount: i64, unit: &str| date_add(col, amount, unit)
}

/// Wrapper for date_diff as a minijinja function
pub fn make_date_diff_fn() -> impl Fn(&str, &str, &str) -> String + Send + Sync + Clone + 'static {
    move |unit: &str, start: &str, end: &str| date_diff(unit, start, end)
}

/// Wrapper for slugify as a minijinja function
pub fn make_slugify_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| slugify(col)
}

/// Wrapper for clean_string as a minijinja function
pub fn make_clean_string_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| clean_string(col)
}

/// Wrapper for split_part as a minijinja function
pub fn make_split_part_fn() -> impl Fn(&str, &str, i64) -> String + Send + Sync + Clone + 'static {
    move |col: &str, delim: &str, part: i64| split_part(col, delim, part)
}

/// Wrapper for safe_divide as a minijinja function
pub fn make_safe_divide_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static {
    move |num: &str, denom: &str| safe_divide(num, denom)
}

/// Wrapper for round_money as a minijinja function
pub fn make_round_money_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| round_money(col)
}

/// Wrapper for percent_of as a minijinja function
pub fn make_percent_of_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static {
    move |value: &str, total: &str| percent_of(value, total)
}

/// Wrapper for limit_zero as a minijinja function
pub fn make_limit_zero_fn() -> impl Fn() -> String + Send + Sync + Clone + 'static {
    || limit_zero()
}

/// Wrapper for bool_or as a minijinja function
pub fn make_bool_or_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| bool_or(col)
}

/// Wrapper for hash as a minijinja function
pub fn make_hash_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| hash(col)
}

/// Wrapper for not_null as a minijinja function
pub fn make_not_null_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| not_null(col)
}

/// Wrapper for surrogate_key that accepts a Value array
pub fn make_surrogate_key_fn(
) -> impl Fn(Value) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |columns: Value| {
        let cols: Vec<String> = columns
            .try_iter()
            .map_err(|_| {
                Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "surrogate_key requires an array of column names",
                )
            })?
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        Ok(surrogate_key(cols))
    }
}

/// Wrapper for coalesce_columns that accepts a Value array
pub fn make_coalesce_columns_fn(
) -> impl Fn(Value) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |columns: Value| {
        let cols: Vec<String> = columns
            .try_iter()
            .map_err(|_| {
                Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "coalesce_columns requires an array of column names",
                )
            })?
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        Ok(coalesce_columns(cols))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_spine() {
        let result = date_spine("2024-01-01", "2024-01-31");
        assert!(result.contains("generate_series"));
        assert!(result.contains("2024-01-01"));
        assert!(result.contains("2024-01-31"));
    }

    #[test]
    fn test_date_trunc() {
        let result = date_trunc("month", "created_at");
        assert_eq!(result, "DATE_TRUNC('month', created_at)");
    }

    #[test]
    fn test_date_add() {
        let result = date_add("order_date", 7, "day");
        assert_eq!(result, "order_date + INTERVAL '7 day'");
    }

    #[test]
    fn test_date_diff() {
        let result = date_diff("day", "start_date", "end_date");
        assert_eq!(result, "DATE_DIFF('day', start_date, end_date)");
    }

    #[test]
    fn test_slugify() {
        let result = slugify("title");
        assert!(result.contains("LOWER"));
        assert!(result.contains("REGEXP_REPLACE"));
        assert!(result.contains("title"));
    }

    #[test]
    fn test_clean_string() {
        let result = clean_string("name");
        assert!(result.contains("TRIM"));
        assert!(result.contains("REGEXP_REPLACE"));
    }

    #[test]
    fn test_split_part() {
        let result = split_part("email", "@", 2);
        assert_eq!(result, "SPLIT_PART(email, '@', 2)");
    }

    #[test]
    fn test_safe_divide() {
        let result = safe_divide("revenue", "count");
        assert!(result.contains("CASE WHEN"));
        assert!(result.contains("IS NULL"));
        assert!(result.contains("revenue"));
        assert!(result.contains("count"));
    }

    #[test]
    fn test_round_money() {
        let result = round_money("amount");
        assert_eq!(result, "ROUND(CAST(amount AS DOUBLE), 2)");
    }

    #[test]
    fn test_percent_of() {
        let result = percent_of("sales", "total_sales");
        assert!(result.contains("100.0"));
        assert!(result.contains("sales"));
        assert!(result.contains("total_sales"));
    }

    #[test]
    fn test_limit_zero() {
        assert_eq!(limit_zero(), "LIMIT 0");
    }

    #[test]
    fn test_bool_or() {
        let result = bool_or("is_active");
        assert_eq!(result, "BOOL_OR(is_active)");
    }

    #[test]
    fn test_hash() {
        let result = hash("user_id");
        assert_eq!(result, "MD5(CAST(user_id AS VARCHAR))");
    }

    #[test]
    fn test_hash_columns() {
        let result = hash_columns(vec!["col1".to_string(), "col2".to_string()]);
        assert!(result.contains("MD5"));
        assert!(result.contains("COALESCE"));
        assert!(result.contains("col1"));
        assert!(result.contains("col2"));
    }

    #[test]
    fn test_surrogate_key() {
        let result = surrogate_key(vec!["id".to_string(), "type".to_string()]);
        assert!(result.contains("MD5"));
    }

    #[test]
    fn test_coalesce_columns() {
        let result = coalesce_columns(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(result, "COALESCE(a, b, c)");
    }

    #[test]
    fn test_not_null() {
        let result = not_null("email");
        assert_eq!(result, "email IS NOT NULL");
    }
}
