//! Built-in macros for Featherflow
//!
//! This module provides built-in macros that are available in all templates
//! without needing to import them from macro files.

use std::collections::BTreeSet;

use ff_core::sql_utils::{escape_sql_string, quote_ident};
use minijinja::value::Value;
use minijinja::Error;
use regex::Regex;
use serde::Serialize;
use std::sync::OnceLock;

// ===== Macro Metadata =====

/// Parameter definition for a macro
#[derive(Debug, Clone, Serialize)]
pub struct MacroParam {
    /// Parameter name
    pub name: &'static str,
    /// Parameter type (string, integer, array)
    pub param_type: &'static str,
    /// Whether this parameter is required
    pub required: bool,
    /// Description of the parameter
    pub description: &'static str,
}

impl MacroParam {
    /// Create a new required parameter
    pub fn required(
        name: &'static str,
        param_type: &'static str,
        description: &'static str,
    ) -> Self {
        Self {
            name,
            param_type,
            required: true,
            description,
        }
    }

    /// Create a new optional parameter
    pub fn optional(
        name: &'static str,
        param_type: &'static str,
        description: &'static str,
    ) -> Self {
        Self {
            name,
            param_type,
            required: false,
            description,
        }
    }
}

/// Metadata describing a built-in macro
#[derive(Debug, Clone, Serialize)]
pub struct MacroMetadata {
    /// Macro name as used in templates
    pub name: &'static str,
    /// Category of the macro (date, string, math, utility, cross_db)
    pub category: &'static str,
    /// Brief description of what the macro does
    pub description: &'static str,
    /// Parameters accepted by the macro
    pub params: Vec<MacroParam>,
    /// Example usage in a template
    pub example: &'static str,
    /// Expected output from the example
    pub example_output: &'static str,
}

impl MacroMetadata {
    /// Create new macro metadata
    pub fn new(
        name: &'static str,
        category: &'static str,
        description: &'static str,
        params: Vec<MacroParam>,
        example: &'static str,
        example_output: &'static str,
    ) -> Self {
        Self {
            name,
            category,
            description,
            params,
            example,
            example_output,
        }
    }
}

/// Get metadata for all built-in macros
///
/// Returns a vector of metadata for all 16 built-in macros,
/// organized by category and including usage examples.
pub fn get_builtin_macros() -> Vec<MacroMetadata> {
    vec![
        // Date/Time Macros
        MacroMetadata::new(
            "date_spine",
            "date",
            "Generate a date spine (range of dates) as SQL",
            vec![
                MacroParam::required("start_date", "string", "Start date in YYYY-MM-DD format"),
                MacroParam::required("end_date", "string", "End date in YYYY-MM-DD format"),
            ],
            "{{ date_spine('2024-01-01', '2024-01-31') }}",
            "SELECT CAST(unnest AS DATE) AS date_day FROM unnest(generate_series(DATE '2024-01-01', DATE '2024-01-31', INTERVAL '1 day'))",
        ),
        MacroMetadata::new(
            "date_trunc",
            "date",
            "Truncate a date to a specific part (year, month, day, etc.)",
            vec![
                MacroParam::required("date_part", "string", "Part to truncate to (year, month, day, hour, etc.)"),
                MacroParam::required("column", "string", "Column or expression to truncate"),
            ],
            "{{ date_trunc('month', 'created_at') }}",
            "DATE_TRUNC('month', created_at)",
        ),
        MacroMetadata::new(
            "date_add",
            "date",
            "Add an interval to a date column",
            vec![
                MacroParam::required("column", "string", "Column or expression to add to"),
                MacroParam::required("amount", "integer", "Number of units to add"),
                MacroParam::required("unit", "string", "Unit of time (day, week, month, year)"),
            ],
            "{{ date_add('order_date', 7, 'day') }}",
            "order_date + INTERVAL '7 day'",
        ),
        MacroMetadata::new(
            "date_diff",
            "date",
            "Calculate the difference between two dates",
            vec![
                MacroParam::required("unit", "string", "Unit for the result (day, week, month, year)"),
                MacroParam::required("start_col", "string", "Start date column or expression"),
                MacroParam::required("end_col", "string", "End date column or expression"),
            ],
            "{{ date_diff('day', 'start_date', 'end_date') }}",
            "DATE_DIFF('day', start_date, end_date)",
        ),
        // String Macros
        MacroMetadata::new(
            "slugify",
            "string",
            "Convert a string column to a URL-friendly slug",
            vec![
                MacroParam::required("column", "string", "Column or expression to slugify"),
            ],
            "{{ slugify('title') }}",
            "LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(title), '[^a-zA-Z0-9]+', '-', 'g'), '^-|-$', '', 'g'))",
        ),
        MacroMetadata::new(
            "clean_string",
            "string",
            "Remove special characters from a string column",
            vec![
                MacroParam::required("column", "string", "Column or expression to clean"),
            ],
            "{{ clean_string('name') }}",
            "TRIM(REGEXP_REPLACE(name, '[^a-zA-Z0-9 ]', '', 'g'))",
        ),
        MacroMetadata::new(
            "split_part",
            "string",
            "Extract a specific part from a delimited string",
            vec![
                MacroParam::required("column", "string", "Column or expression containing the string"),
                MacroParam::required("delimiter", "string", "Delimiter character or string"),
                MacroParam::required("part", "integer", "1-based index of the part to extract"),
            ],
            "{{ split_part('email', '@', 2) }}",
            "SPLIT_PART(email, '@', 2)",
        ),
        // Math Macros
        MacroMetadata::new(
            "safe_divide",
            "math",
            "Safely divide two numbers, returning NULL if denominator is 0 or NULL",
            vec![
                MacroParam::required("numerator", "string", "Numerator column or expression"),
                MacroParam::required("denominator", "string", "Denominator column or expression"),
            ],
            "{{ safe_divide('revenue', 'count') }}",
            "CASE WHEN count = 0 OR count IS NULL THEN NULL ELSE CAST(revenue AS DOUBLE) / count END",
        ),
        MacroMetadata::new(
            "round_money",
            "math",
            "Round a number to 2 decimal places (for monetary values)",
            vec![
                MacroParam::required("column", "string", "Column or expression to round"),
            ],
            "{{ round_money('amount') }}",
            "ROUND(CAST(amount AS DOUBLE), 2)",
        ),
        MacroMetadata::new(
            "percent_of",
            "math",
            "Calculate a value as a percentage of a total",
            vec![
                MacroParam::required("value", "string", "Value column or expression"),
                MacroParam::required("total", "string", "Total column or expression"),
            ],
            "{{ percent_of('sales', 'total_sales') }}",
            "CASE WHEN total_sales = 0 OR total_sales IS NULL THEN 0.0 ELSE ROUND(100.0 * sales / total_sales, 2) END",
        ),
        // Cross-DB Macros
        MacroMetadata::new(
            "limit_zero",
            "cross_db",
            "Generate a LIMIT 0 clause (useful for schema validation)",
            vec![],
            "{{ limit_zero() }}",
            "LIMIT 0",
        ),
        MacroMetadata::new(
            "bool_or",
            "cross_db",
            "Boolean OR aggregation that works across dialects",
            vec![
                MacroParam::required("column", "string", "Boolean column to aggregate"),
            ],
            "{{ bool_or('is_active') }}",
            "BOOL_OR(is_active)",
        ),
        MacroMetadata::new(
            "hash",
            "cross_db",
            "Hash a column value using MD5",
            vec![
                MacroParam::required("column", "string", "Column or expression to hash"),
            ],
            "{{ hash('user_id') }}",
            "MD5(CAST(user_id AS VARCHAR))",
        ),
        MacroMetadata::new(
            "hash_columns",
            "utility",
            "Hash multiple columns into a single value",
            vec![
                MacroParam::required("columns", "array", "Array of column names to hash"),
            ],
            "{{ hash_columns(['col1', 'col2', 'col3']) }}",
            "MD5(COALESCE(CAST(col1 AS VARCHAR), '') || '|' || COALESCE(CAST(col2 AS VARCHAR), '') || '|' || COALESCE(CAST(col3 AS VARCHAR), ''))",
        ),
        // Utility Macros
        MacroMetadata::new(
            "surrogate_key",
            "utility",
            "Generate a surrogate key from multiple columns (alias for hash_columns)",
            vec![
                MacroParam::required("columns", "array", "Array of column names to combine into a key"),
            ],
            "{{ surrogate_key(['id', 'type']) }}",
            "MD5(COALESCE(CAST(id AS VARCHAR), '') || '|' || COALESCE(CAST(type AS VARCHAR), ''))",
        ),
        MacroMetadata::new(
            "coalesce_columns",
            "utility",
            "Return the first non-NULL value from multiple columns",
            vec![
                MacroParam::required("columns", "array", "Array of column names to coalesce"),
            ],
            "{{ coalesce_columns(['col1', 'col2', 'col3']) }}",
            "COALESCE(col1, col2, col3)",
        ),
        MacroMetadata::new(
            "not_null",
            "utility",
            "Generate a NOT NULL check expression",
            vec![
                MacroParam::required("column", "string", "Column to check"),
            ],
            "{{ not_null('email') }}",
            "email IS NOT NULL",
        ),
    ]
}

/// Cached builtin macros — built once, reused on every call
static BUILTIN_MACROS: OnceLock<Vec<MacroMetadata>> = OnceLock::new();

/// Get the cached builtin macros slice
fn cached_builtin_macros() -> &'static [MacroMetadata] {
    BUILTIN_MACROS.get_or_init(get_builtin_macros)
}

/// Get metadata for a specific macro by name
pub fn get_macro_by_name(name: &str) -> Option<&'static MacroMetadata> {
    cached_builtin_macros().iter().find(|m| m.name == name)
}

/// Get all macros in a specific category
pub fn get_macros_by_category(category: &str) -> Vec<&'static MacroMetadata> {
    cached_builtin_macros()
        .iter()
        .filter(|m| m.category == category)
        .collect()
}

/// Get all available macro categories
pub fn get_macro_categories() -> Vec<&'static str> {
    cached_builtin_macros()
        .iter()
        .map(|m| m.category)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

// ===== Date/Time Macros =====

/// Regex pattern for validating YYYY-MM-DD date format
static DATE_FORMAT_RE: OnceLock<Regex> = OnceLock::new();

/// Get the compiled date-format regex (built once, reused)
fn date_format_regex() -> &'static Regex {
    DATE_FORMAT_RE.get_or_init(|| Regex::new(r"^\d{4}-\d{2}-\d{2}$").expect("valid regex"))
}

/// Generate a date spine (range of dates)
///
/// Usage: `{{ date_spine('2024-01-01', '2024-12-31') }}`
/// Returns SQL that generates a series of dates.
///
/// Both `start_date` and `end_date` must be in `YYYY-MM-DD` format.
pub(crate) fn date_spine(start_date: &str, end_date: &str) -> Result<String, Error> {
    let re = date_format_regex();
    if !re.is_match(start_date) {
        return Err(Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!(
                "date_spine: start_date '{}' is not in YYYY-MM-DD format",
                start_date
            ),
        ));
    }
    if !re.is_match(end_date) {
        return Err(Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!(
                "date_spine: end_date '{}' is not in YYYY-MM-DD format",
                end_date
            ),
        ));
    }

    // DuckDB-specific syntax for generating date series
    let escaped_start = escape_sql_string(start_date);
    let escaped_end = escape_sql_string(end_date);
    Ok(format!(
        "SELECT CAST(unnest AS DATE) AS date_day FROM unnest(generate_series(DATE '{}', DATE '{}', INTERVAL '1 day'))",
        escaped_start, escaped_end
    ))
}

/// Truncate a date to a specific part
///
/// Usage: `{{ date_trunc('month', 'created_at') }}`
pub(crate) fn date_trunc(date_part: &str, column: &str) -> String {
    format!(
        "DATE_TRUNC('{}', {})",
        escape_sql_string(date_part),
        quote_ident(column)
    )
}

/// Add an interval to a date
///
/// Usage: `{{ date_add('created_at', 7, 'day') }}`
pub(crate) fn date_add(column: &str, amount: i64, unit: &str) -> String {
    format!(
        "{} + INTERVAL '{} {}'",
        quote_ident(column),
        amount,
        escape_sql_string(unit)
    )
}

/// Calculate the difference between two dates
///
/// Usage: `{{ date_diff('day', 'start_date', 'end_date') }}`
pub(crate) fn date_diff(unit: &str, start_col: &str, end_col: &str) -> String {
    format!(
        "DATE_DIFF('{}', {}, {})",
        escape_sql_string(unit),
        quote_ident(start_col),
        quote_ident(end_col)
    )
}

// ===== String Macros =====

/// Convert a string to a URL-friendly slug
///
/// Usage: `{{ slugify('column_name') }}`
pub(crate) fn slugify(column: &str) -> String {
    format!(
        "LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM({}), '[^a-zA-Z0-9]+', '-', 'g'), '^-|-$', '', 'g'))",
        quote_ident(column)
    )
}

/// Clean a string by removing special characters
///
/// Usage: `{{ clean_string('column_name') }}`
pub(crate) fn clean_string(column: &str) -> String {
    format!(
        "TRIM(REGEXP_REPLACE({}, '[^a-zA-Z0-9 ]', '', 'g'))",
        quote_ident(column)
    )
}

/// Get a specific part of a split string
///
/// Usage: `{{ split_part('column_name', '-', 1) }}`
pub(crate) fn split_part(column: &str, delimiter: &str, part: i64) -> String {
    format!(
        "SPLIT_PART({}, '{}', {})",
        quote_ident(column),
        escape_sql_string(delimiter),
        part
    )
}

// ===== Math Macros =====

/// Safely divide two numbers, returning NULL if denominator is 0
///
/// Usage: `{{ safe_divide('numerator', 'denominator') }}`
pub(crate) fn safe_divide(numerator: &str, denominator: &str) -> String {
    let num = quote_ident(numerator);
    let denom = quote_ident(denominator);
    format!(
        "CASE WHEN {denom} = 0 OR {denom} IS NULL THEN NULL ELSE CAST({num} AS DOUBLE) / {denom} END"
    )
}

/// Round a number to 2 decimal places (for money)
///
/// Usage: `{{ round_money('amount') }}`
pub(crate) fn round_money(column: &str) -> String {
    format!("ROUND(CAST({} AS DOUBLE), 2)", quote_ident(column))
}

/// Calculate percentage of a value relative to a total
///
/// Usage: `{{ percent_of('value', 'total') }}`
pub(crate) fn percent_of(value: &str, total: &str) -> String {
    let val = quote_ident(value);
    let tot = quote_ident(total);
    format!(
        "CASE WHEN {tot} = 0 OR {tot} IS NULL THEN 0.0 ELSE ROUND(100.0 * {val} / {tot}, 2) END"
    )
}

// ===== Cross-DB Macros =====

/// Generate a LIMIT 0 clause for schema validation
///
/// Usage: `{{ limit_zero() }}`
pub(crate) fn limit_zero() -> String {
    "LIMIT 0".to_string()
}

/// Boolean OR aggregation (works across dialects)
///
/// Usage: `{{ bool_or('is_active') }}`
pub(crate) fn bool_or(column: &str) -> String {
    format!("BOOL_OR({})", quote_ident(column))
}

/// Hash a column using a consistent algorithm
///
/// Usage: `{{ hash('column_name') }}`
pub(crate) fn hash(column: &str) -> String {
    format!("MD5(CAST({} AS VARCHAR))", quote_ident(column))
}

/// Hash multiple columns into a single value
///
/// Usage: `{{ hash_columns(['col1', 'col2', 'col3']) }}`
pub(crate) fn hash_columns(columns: &[String]) -> String {
    let concat_expr = columns
        .iter()
        .map(|c| format!("COALESCE(CAST({} AS VARCHAR), '')", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(" || '|' || ");
    format!("MD5({})", concat_expr)
}

// ===== Utility Macros =====

/// Generate a surrogate key from multiple columns
///
/// Usage: `{{ surrogate_key(['col1', 'col2']) }}`
pub(crate) fn surrogate_key(columns: &[String]) -> String {
    hash_columns(columns)
}

/// Coalesce multiple columns
///
/// Usage: `{{ coalesce_columns(['col1', 'col2', 'col3']) }}`
pub(crate) fn coalesce_columns(columns: &[String]) -> String {
    let quoted: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    format!("COALESCE({})", quoted.join(", "))
}

/// Generate a not-null check expression
///
/// Usage: `{{ not_null('column_name') }}`
pub(crate) fn not_null(column: &str) -> String {
    format!("{} IS NOT NULL", quote_ident(column))
}

// ===== Minijinja Function Wrappers =====

/// Wrapper for date_spine as a minijinja function
pub(crate) fn make_date_spine_fn(
) -> impl Fn(&str, &str) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |start: &str, end: &str| date_spine(start, end)
}

/// Wrapper for date_trunc as a minijinja function
pub(crate) fn make_date_trunc_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static
{
    move |part: &str, col: &str| date_trunc(part, col)
}

/// Wrapper for date_add as a minijinja function
pub(crate) fn make_date_add_fn(
) -> impl Fn(&str, i64, &str) -> String + Send + Sync + Clone + 'static {
    move |col: &str, amount: i64, unit: &str| date_add(col, amount, unit)
}

/// Wrapper for date_diff as a minijinja function
pub(crate) fn make_date_diff_fn(
) -> impl Fn(&str, &str, &str) -> String + Send + Sync + Clone + 'static {
    move |unit: &str, start: &str, end: &str| date_diff(unit, start, end)
}

/// Wrapper for slugify as a minijinja function
pub(crate) fn make_slugify_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| slugify(col)
}

/// Wrapper for clean_string as a minijinja function
pub(crate) fn make_clean_string_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| clean_string(col)
}

/// Wrapper for split_part as a minijinja function
pub(crate) fn make_split_part_fn(
) -> impl Fn(&str, &str, i64) -> String + Send + Sync + Clone + 'static {
    move |col: &str, delim: &str, part: i64| split_part(col, delim, part)
}

/// Wrapper for safe_divide as a minijinja function
pub(crate) fn make_safe_divide_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static
{
    move |num: &str, denom: &str| safe_divide(num, denom)
}

/// Wrapper for round_money as a minijinja function
pub(crate) fn make_round_money_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| round_money(col)
}

/// Wrapper for percent_of as a minijinja function
pub(crate) fn make_percent_of_fn() -> impl Fn(&str, &str) -> String + Send + Sync + Clone + 'static
{
    move |value: &str, total: &str| percent_of(value, total)
}

/// Wrapper for limit_zero as a minijinja function
pub(crate) fn make_limit_zero_fn() -> impl Fn() -> String + Send + Sync + Clone + 'static {
    || limit_zero()
}

/// Wrapper for bool_or as a minijinja function
pub(crate) fn make_bool_or_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| bool_or(col)
}

/// Wrapper for hash as a minijinja function
pub(crate) fn make_hash_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| hash(col)
}

/// Wrapper for not_null as a minijinja function
pub(crate) fn make_not_null_fn() -> impl Fn(&str) -> String + Send + Sync + Clone + 'static {
    move |col: &str| not_null(col)
}

/// Wrapper for hash_columns that accepts a Value array
/// Extract a non-empty `Vec<String>` from a minijinja `Value` array.
///
/// Shared validation logic for column-array wrapper functions
/// (`hash_columns`, `surrogate_key`, `coalesce_columns`).
fn extract_column_array(value: Value, fn_name: &str) -> Result<Vec<String>, Error> {
    let cols: Vec<String> = value
        .try_iter()
        .map_err(|_| {
            Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("{fn_name} requires an array of column names"),
            )
        })?
        .map(|v| {
            v.as_str().map(String::from).ok_or_else(|| {
                Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!("{fn_name}: expected string element, got {}", v.kind()),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if cols.is_empty() {
        return Err(Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!("{fn_name} requires a non-empty array of column names"),
        ));
    }
    Ok(cols)
}

pub(crate) fn make_hash_columns_fn(
) -> impl Fn(Value) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |columns: Value| {
        let cols = extract_column_array(columns, "hash_columns")?;
        Ok(hash_columns(&cols))
    }
}

/// Wrapper for surrogate_key that accepts a Value array
pub(crate) fn make_surrogate_key_fn(
) -> impl Fn(Value) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |columns: Value| {
        let cols = extract_column_array(columns, "surrogate_key")?;
        Ok(surrogate_key(&cols))
    }
}

/// Wrapper for coalesce_columns that accepts a Value array
pub(crate) fn make_coalesce_columns_fn(
) -> impl Fn(Value) -> Result<String, Error> + Send + Sync + Clone + 'static {
    move |columns: Value| {
        let cols = extract_column_array(columns, "coalesce_columns")?;
        Ok(coalesce_columns(&cols))
    }
}

#[cfg(test)]
#[path = "builtins_test.rs"]
mod tests;
