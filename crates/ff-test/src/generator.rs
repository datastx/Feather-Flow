//! Test SQL generation

use ff_core::model::{SchemaTest, TestType};

/// Generate SQL for a unique test
///
/// Returns rows that violate the unique constraint (duplicates).
pub fn generate_unique_test(table: &str, column: &str) -> String {
    format!(
        r#"SELECT {column}, COUNT(*) as cnt
FROM {table}
GROUP BY {column}
HAVING COUNT(*) > 1"#,
        table = table,
        column = column
    )
}

/// Generate SQL for a not_null test
///
/// Returns rows where the column is NULL.
pub fn generate_not_null_test(table: &str, column: &str) -> String {
    format!(
        "SELECT * FROM {table} WHERE {column} IS NULL",
        table = table,
        column = column
    )
}

/// Generate SQL for a positive test
///
/// Returns rows where the column value is <= 0.
pub fn generate_positive_test(table: &str, column: &str) -> String {
    format!(
        "SELECT * FROM {table} WHERE {column} <= 0",
        table = table,
        column = column
    )
}

/// Generate SQL for a non_negative test
///
/// Returns rows where the column value is < 0.
pub fn generate_non_negative_test(table: &str, column: &str) -> String {
    format!(
        "SELECT * FROM {table} WHERE {column} < 0",
        table = table,
        column = column
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
    let formatted_values: Vec<String> = if quote {
        values.iter().map(|v| format!("'{}'", v)).collect()
    } else {
        values.to_vec()
    };
    let values_list = formatted_values.join(", ");

    format!(
        "SELECT * FROM {table} WHERE {column} NOT IN ({values_list}) OR {column} IS NULL",
        table = table,
        column = column,
        values_list = values_list
    )
}

/// Generate SQL for a min_value test
///
/// Returns rows where the column value is less than the threshold.
pub fn generate_min_value_test(table: &str, column: &str, min: f64) -> String {
    format!(
        "SELECT * FROM {table} WHERE {column} < {min}",
        table = table,
        column = column,
        min = min
    )
}

/// Generate SQL for a max_value test
///
/// Returns rows where the column value is greater than the threshold.
pub fn generate_max_value_test(table: &str, column: &str, max: f64) -> String {
    format!(
        "SELECT * FROM {table} WHERE {column} > {max}",
        table = table,
        column = column,
        max = max
    )
}

/// Generate SQL for a regex test
///
/// Returns rows where the column value does not match the pattern.
pub fn generate_regex_test(table: &str, column: &str, pattern: &str) -> String {
    format!(
        "SELECT * FROM {table} WHERE NOT regexp_matches({column}, '{pattern}')",
        table = table,
        column = column,
        pattern = pattern
    )
}

/// Generate SQL for a schema test
pub fn generate_test_sql(test: &SchemaTest) -> String {
    match &test.test_type {
        TestType::Unique => generate_unique_test(&test.model, &test.column),
        TestType::NotNull => generate_not_null_test(&test.model, &test.column),
        TestType::Positive => generate_positive_test(&test.model, &test.column),
        TestType::NonNegative => generate_non_negative_test(&test.model, &test.column),
        TestType::AcceptedValues { values, quote } => {
            generate_accepted_values_test(&test.model, &test.column, values, *quote)
        }
        TestType::MinValue { value } => generate_min_value_test(&test.model, &test.column, *value),
        TestType::MaxValue { value } => generate_max_value_test(&test.model, &test.column, *value),
        TestType::Regex { pattern } => generate_regex_test(&test.model, &test.column, pattern),
    }
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
}

impl GeneratedTest {
    /// Create a generated test from a schema test
    pub fn from_schema_test(test: &SchemaTest) -> Self {
        let sql = generate_test_sql(test);
        let name = format!("{}_{}__{}", test.test_type, test.model, test.column);

        Self {
            model: test.model.clone(),
            column: test.column.clone(),
            test_type: test.test_type.clone(),
            sql,
            name,
        }
    }

    /// Create a generated test with a qualified model name (schema.model)
    pub fn from_schema_test_qualified(test: &SchemaTest, qualified_name: &str) -> Self {
        let sql = match &test.test_type {
            TestType::Unique => generate_unique_test(qualified_name, &test.column),
            TestType::NotNull => generate_not_null_test(qualified_name, &test.column),
            TestType::Positive => generate_positive_test(qualified_name, &test.column),
            TestType::NonNegative => generate_non_negative_test(qualified_name, &test.column),
            TestType::AcceptedValues { values, quote } => {
                generate_accepted_values_test(qualified_name, &test.column, values, *quote)
            }
            TestType::MinValue { value } => {
                generate_min_value_test(qualified_name, &test.column, *value)
            }
            TestType::MaxValue { value } => {
                generate_max_value_test(qualified_name, &test.column, *value)
            }
            TestType::Regex { pattern } => {
                generate_regex_test(qualified_name, &test.column, pattern)
            }
        };
        let name = format!("{}_{}__{}", test.test_type, test.model, test.column);

        Self {
            model: test.model.clone(),
            column: test.column.clone(),
            test_type: test.test_type.clone(),
            sql,
            name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_unique_test() {
        let sql = generate_unique_test("users", "id");
        assert!(sql.contains("GROUP BY id"));
        assert!(sql.contains("HAVING COUNT(*) > 1"));
    }

    #[test]
    fn test_generate_not_null_test() {
        let sql = generate_not_null_test("users", "email");
        assert!(sql.contains("IS NULL"));
        assert!(sql.contains("email"));
    }

    #[test]
    fn test_generate_positive_test() {
        let sql = generate_positive_test("orders", "amount");
        assert!(sql.contains("amount <= 0"));
        assert!(sql.contains("FROM orders"));
    }

    #[test]
    fn test_generate_non_negative_test() {
        let sql = generate_non_negative_test("orders", "quantity");
        assert!(sql.contains("quantity < 0"));
        assert!(sql.contains("FROM orders"));
    }

    #[test]
    fn test_generate_accepted_values_test_quoted() {
        let values = vec!["pending".to_string(), "completed".to_string()];
        let sql = generate_accepted_values_test("orders", "status", &values, true);
        assert!(sql.contains("NOT IN ('pending', 'completed')"));
        assert!(sql.contains("status IS NULL"));
    }

    #[test]
    fn test_generate_accepted_values_test_unquoted() {
        let values = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        let sql = generate_accepted_values_test("orders", "priority", &values, false);
        assert!(sql.contains("NOT IN (1, 2, 3)"));
    }

    #[test]
    fn test_generate_min_value_test() {
        let sql = generate_min_value_test("products", "price", 0.0);
        assert!(sql.contains("price < 0"));
        assert!(sql.contains("FROM products"));
    }

    #[test]
    fn test_generate_max_value_test() {
        let sql = generate_max_value_test("products", "discount", 100.0);
        assert!(sql.contains("discount > 100"));
        assert!(sql.contains("FROM products"));
    }

    #[test]
    fn test_generate_regex_test() {
        let sql = generate_regex_test(
            "users",
            "email",
            r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        );
        assert!(sql.contains("regexp_matches(email,"));
        assert!(sql.contains("NOT"));
    }

    #[test]
    fn test_generated_test_name() {
        let schema_test = SchemaTest {
            test_type: TestType::Unique,
            column: "order_id".to_string(),
            model: "stg_orders".to_string(),
        };

        let generated = GeneratedTest::from_schema_test(&schema_test);
        assert_eq!(generated.name, "unique_stg_orders__order_id");
    }

    #[test]
    fn test_generated_test_accepted_values() {
        let schema_test = SchemaTest {
            test_type: TestType::AcceptedValues {
                values: vec!["a".to_string(), "b".to_string()],
                quote: true,
            },
            column: "status".to_string(),
            model: "orders".to_string(),
        };

        let generated = GeneratedTest::from_schema_test(&schema_test);
        assert_eq!(generated.name, "accepted_values_orders__status");
        assert!(generated.sql.contains("NOT IN ('a', 'b')"));
    }
}
