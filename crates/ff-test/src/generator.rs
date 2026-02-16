//! Test SQL generation

use ff_core::model::{SchemaTest, TestSeverity, TestType};
use ff_core::sql_utils::{escape_sql_string, quote_ident, quote_qualified};
use regex::Regex;
use std::sync::LazyLock;
use thiserror::Error;

/// Word-boundary regex matching dangerous SQL keywords.
///
/// Uses `\b` to avoid false positives on column names like `created_at`
/// (which contains "CREATE" as a substring) or `updated_at` ("UPDATE").
static DANGEROUS_KEYWORDS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(UNION|INSERT|UPDATE|DELETE|DROP|ALTER|CREATE)\b")
        .expect("hardcoded regex is valid")
});

/// Error type for test SQL generation.
///
/// Currently has a single variant for invalid thresholds. Structured as an enum
/// to allow extending with additional error kinds (e.g. custom test resolution
/// failures) without breaking the public API.
#[derive(Error, Debug)]
pub enum TestGenError {
    /// Invalid threshold value (NaN or Infinity)
    #[error("invalid threshold value: {0}")]
    InvalidThreshold(String),
}

/// Result type alias for test generation
pub type TestGenResult<T> = Result<T, TestGenError>;

/// Generate SQL for a unique test
///
/// Returns rows that violate the unique constraint (duplicates).
pub fn generate_unique_test(table: &str, column: &str) -> String {
    let qt = quote_qualified(table);
    let qc = quote_ident(column);
    format!("SELECT {qc}, COUNT(*) as cnt\nFROM {qt}\nGROUP BY {qc}\nHAVING COUNT(*) > 1")
}

/// Generate SQL for a not_null test
///
/// Returns rows where the column is NULL.
pub fn generate_not_null_test(table: &str, column: &str) -> String {
    format!(
        "SELECT * FROM {} WHERE {} IS NULL",
        quote_qualified(table),
        quote_ident(column)
    )
}

/// Generate SQL for a positive test
///
/// Returns rows where the column value is <= 0.
pub fn generate_positive_test(table: &str, column: &str) -> String {
    format!(
        "SELECT * FROM {} WHERE {} <= 0",
        quote_qualified(table),
        quote_ident(column)
    )
}

/// Generate SQL for a non_negative test
///
/// Returns rows where the column value is < 0.
pub fn generate_non_negative_test(table: &str, column: &str) -> String {
    format!(
        "SELECT * FROM {} WHERE {} < 0",
        quote_qualified(table),
        quote_ident(column)
    )
}

/// Generate SQL for an accepted_values test
///
/// Returns rows where the column value is not in the allowed list or is NULL.
pub fn generate_accepted_values_test(
    table: &str,
    column: &str,
    values: &[String],
    quote: bool,
) -> String {
    if values.is_empty() {
        // Empty values list means every row fails — return all rows
        return format!("SELECT * FROM {}", quote_qualified(table));
    }
    let formatted_values: Vec<String> = if quote {
        values
            .iter()
            .map(|v| format!("'{}'", escape_sql_string(v)))
            .collect()
    } else {
        // When unquoted, values are intended to be numeric literals.
        // Always quote them to prevent SQL injection from non-numeric values.
        values
            .iter()
            .map(|v| {
                // If the value parses as a number, emit it unquoted; otherwise quote it.
                if v.parse::<f64>().is_ok() || v.parse::<i64>().is_ok() {
                    v.clone()
                } else {
                    format!("'{}'", escape_sql_string(v))
                }
            })
            .collect()
    };
    let values_list = formatted_values.join(", ");
    let qt = quote_qualified(table);
    let qc = quote_ident(column);

    format!("SELECT * FROM {qt} WHERE {qc} NOT IN ({values_list}) OR {qc} IS NULL")
}

/// Generate SQL for a min_value test
///
/// Returns rows where the column value is less than the threshold.
/// Returns `Err` if the threshold is NaN or Infinity.
pub fn generate_min_value_test(table: &str, column: &str, min: f64) -> TestGenResult<String> {
    if !min.is_finite() {
        return Err(TestGenError::InvalidThreshold(format!(
            "min_value must be finite, got {}",
            min
        )));
    }
    Ok(format!(
        "SELECT * FROM {} WHERE {} < {}",
        quote_qualified(table),
        quote_ident(column),
        min
    ))
}

/// Generate SQL for a max_value test
///
/// Returns rows where the column value is greater than the threshold.
/// Returns `Err` if the threshold is NaN or Infinity.
pub fn generate_max_value_test(table: &str, column: &str, max: f64) -> TestGenResult<String> {
    if !max.is_finite() {
        return Err(TestGenError::InvalidThreshold(format!(
            "max_value must be finite, got {}",
            max
        )));
    }
    Ok(format!(
        "SELECT * FROM {} WHERE {} > {}",
        quote_qualified(table),
        quote_ident(column),
        max
    ))
}

/// Generate SQL for a regex test
///
/// Returns rows where the column value does not match the pattern.
pub fn generate_regex_test(table: &str, column: &str, pattern: &str) -> String {
    format!(
        "SELECT * FROM {} WHERE NOT regexp_matches({}, '{}')",
        quote_qualified(table),
        quote_ident(column),
        escape_sql_string(pattern)
    )
}

/// Generate SQL for a relationship test (foreign key validation)
///
/// Returns rows where the column value does not exist in the referenced table.
/// This validates referential integrity - every value in `table.column` must
/// exist in `ref_table.ref_column`.
pub fn generate_relationship_test(
    table: &str,
    column: &str,
    ref_table: &str,
    ref_column: &str,
) -> String {
    let qt = quote_qualified(table);
    let qc = quote_ident(column);
    let qrt = quote_qualified(ref_table);
    let qrc = quote_ident(ref_column);
    format!(
        "SELECT src.{qc}\n\
         FROM {qt} AS src\n\
         WHERE src.{qc} IS NOT NULL\n\
         \x20 AND NOT EXISTS (\n\
         \x20   SELECT 1 FROM {qrt} AS ref_tbl\n\
         \x20   WHERE ref_tbl.{qrc} = src.{qc}\n\
         \x20 )"
    )
}

/// Generate SQL for a `TestType` against a given table and column.
///
/// This is the single authoritative `TestType` -> SQL mapping. All entry
/// points (`generate_test_sql`, `GeneratedTest::from_schema_test_qualified`,
/// etc.) delegate here.
///
/// `ref_table_resolver` is called only for `Relationship` tests to
/// qualify the referenced table name. Pass `None` to use the raw
/// `to` value from the variant.
fn generate_sql_for_test_type(
    test_type: &TestType,
    table: &str,
    column: &str,
    ref_table_resolver: Option<&dyn Fn(&str) -> String>,
) -> String {
    match test_type {
        TestType::Unique => generate_unique_test(table, column),
        TestType::NotNull => generate_not_null_test(table, column),
        TestType::Positive => generate_positive_test(table, column),
        TestType::NonNegative => generate_non_negative_test(table, column),
        TestType::AcceptedValues { values, quote } => {
            generate_accepted_values_test(table, column, values, *quote)
        }
        TestType::MinValue { value } => generate_min_value_test(table, column, *value)
            .unwrap_or_else(|e| {
                log::warn!(
                    "min_value test for {}.{} has invalid threshold: {}",
                    table,
                    column,
                    e
                );
                // Return SQL that yields one row so the test FAILS instead of silently passing
                format!(
                    "SELECT '{}' AS error",
                    escape_sql_string(&format!("ERROR: invalid threshold value: {}", e))
                )
            }),
        TestType::MaxValue { value } => generate_max_value_test(table, column, *value)
            .unwrap_or_else(|e| {
                log::warn!(
                    "max_value test for {}.{} has invalid threshold: {}",
                    table,
                    column,
                    e
                );
                format!(
                    "SELECT '{}' AS error",
                    escape_sql_string(&format!("ERROR: invalid threshold value: {}", e))
                )
            }),
        TestType::Regex { pattern } => generate_regex_test(table, column, pattern),
        TestType::Relationship { to, field } => {
            let ref_column = field.as_deref().unwrap_or(column);
            let resolved_ref = ref_table_resolver
                .map(|resolve| resolve(to))
                .unwrap_or_else(|| to.clone());
            generate_relationship_test(table, column, &resolved_ref, ref_column)
        }
        TestType::Custom { name, kwargs: _ } => {
            // Custom tests require the Jinja environment to render.
            // Return SQL that fails with a descriptive message so users
            // know the custom test macro was not resolved.
            format!(
                "SELECT '{}' AS unresolved_custom_test",
                escape_sql_string(&format!(
                    "Custom test '{}' for {}.{} was not resolved — ensure the test macro is registered",
                    name, table, column
                ))
            )
        }
    }
}

/// Generate SQL for a schema test.
///
/// Applies `TestConfig.where_clause` (wrapping the base query with an additional filter)
/// and `TestConfig.limit` to cap the number of failing rows returned.
pub fn generate_test_sql(test: &SchemaTest) -> String {
    let base_sql = generate_sql_for_test_type(&test.test_type, &test.model, &test.column, None);
    apply_test_config(&base_sql, &test.config)
}

/// Wrap a base test SQL with `TestConfig` options (where_clause, limit).
///
/// The `where_clause` comes from YAML config authored by project developers.
/// We wrap it in parentheses and reject semicolons as defense-in-depth.
fn apply_test_config(base_sql: &str, config: &ff_core::model::TestConfig) -> String {
    let mut sql = base_sql.to_string();
    if let Some(ref where_clause) = config.where_clause {
        // Reject semicolons to prevent multi-statement injection
        let sanitized = where_clause.replace(';', "");
        // Reject dangerous SQL keywords as defense-in-depth (word-boundary match
        // to avoid false positives on column names like `created_at` or `updated_at`)
        if DANGEROUS_KEYWORDS_RE.is_match(&sanitized) {
            return "SELECT 'ERROR: where_clause contains disallowed keyword' AS error".to_string();
        }
        sql = format!(
            "SELECT * FROM ({}) AS _test_base WHERE ({})",
            sql, sanitized
        );
    }
    if let Some(limit) = config.limit {
        sql = format!("{} LIMIT {}", sql, limit);
    }
    sql
}

/// Test SQL with metadata
#[derive(Debug, Clone)]
pub struct GeneratedTest {
    /// Model being tested
    pub model: String,

    /// Column being tested
    pub column: String,

    /// Test type
    pub test_type: TestType,

    /// Generated SQL
    pub sql: String,

    /// Human-readable test name
    pub name: String,

    /// Test severity propagated from schema test config
    pub severity: TestSeverity,
}

impl GeneratedTest {
    /// Build a `GeneratedTest` from common `SchemaTest` fields and a pre-built SQL string
    fn build(test: &SchemaTest, sql: String) -> Self {
        Self {
            name: format!("{}_{}__{}", test.test_type, test.model, test.column),
            model: test.model.clone(),
            column: test.column.clone(),
            test_type: test.test_type.clone(),
            severity: test.config.severity,
            sql,
        }
    }

    /// Create a generated test from a schema test (applies `TestConfig` options)
    pub fn from_schema_test(test: &SchemaTest) -> Self {
        let sql = generate_test_sql(test);
        Self::build(test, sql)
    }

    /// Create a generated test with custom SQL (for custom test macros)
    pub fn with_custom_sql(test: &SchemaTest, sql: String) -> Self {
        Self::build(test, sql)
    }

    /// Create a generated test with a qualified model name (schema.model)
    pub fn from_schema_test_qualified(test: &SchemaTest, qualified_name: &str) -> Self {
        let base_sql =
            generate_sql_for_test_type(&test.test_type, qualified_name, &test.column, None);
        let sql = apply_test_config(&base_sql, &test.config);
        Self::build(test, sql)
    }

    /// Create a generated test with qualified names for both source and referenced tables
    ///
    /// This is used for relationship tests where both the source table and the
    /// referenced table need to be qualified with their schemas.
    pub fn from_schema_test_qualified_with_refs(
        test: &SchemaTest,
        qualified_name: &str,
        ref_table_resolver: impl Fn(&str) -> String,
    ) -> Self {
        let base_sql = generate_sql_for_test_type(
            &test.test_type,
            qualified_name,
            &test.column,
            Some(&ref_table_resolver),
        );
        let sql = apply_test_config(&base_sql, &test.config);
        Self::build(test, sql)
    }
}

#[cfg(test)]
#[path = "generator_test.rs"]
mod tests;
