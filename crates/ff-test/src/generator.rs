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
    // Escape single quotes to prevent SQL injection
    let escaped_pattern = pattern.replace('\'', "''");
    format!(
        "SELECT * FROM {table} WHERE NOT regexp_matches({column}, '{escaped_pattern}')",
        table = table,
        column = column,
        escaped_pattern = escaped_pattern
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
    format!(
        r#"SELECT src.{column}
FROM {table} AS src
WHERE src.{column} IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM {ref_table} AS ref
    WHERE ref.{ref_column} = src.{column}
  )"#,
        table = table,
        column = column,
        ref_table = ref_table,
        ref_column = ref_column
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
        TestType::Relationship { to, field } => {
            let ref_column = field.as_deref().unwrap_or(&test.column);
            generate_relationship_test(&test.model, &test.column, to, ref_column)
        }
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
            TestType::Relationship { to, field } => {
                let ref_column = field.as_deref().unwrap_or(&test.column);
                generate_relationship_test(qualified_name, &test.column, to, ref_column)
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

    /// Create a generated test with qualified names for both source and referenced tables
    ///
    /// This is used for relationship tests where both the source table and the
    /// referenced table need to be qualified with their schemas.
    pub fn from_schema_test_qualified_with_refs(
        test: &SchemaTest,
        qualified_name: &str,
        ref_table_resolver: impl Fn(&str) -> String,
    ) -> Self {
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
            TestType::Relationship { to, field } => {
                let ref_column = field.as_deref().unwrap_or(&test.column);
                let qualified_ref = ref_table_resolver(to);
                generate_relationship_test(qualified_name, &test.column, &qualified_ref, ref_column)
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
    fn test_generate_regex_test_escapes_quotes() {
        // Verify that single quotes in patterns are escaped to prevent SQL injection
        let sql = generate_regex_test("users", "name", "O'Brien");
        assert!(sql.contains("O''Brien"), "Single quotes should be escaped");
        assert!(
            !sql.contains("O'Brien' OR"),
            "SQL injection should be prevented"
        );
    }

    #[test]
    fn test_generated_test_name() {
        let schema_test = SchemaTest {
            test_type: TestType::Unique,
            column: "order_id".to_string(),
            model: "stg_orders".to_string(),
            config: Default::default(),
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
            config: Default::default(),
        };

        let generated = GeneratedTest::from_schema_test(&schema_test);
        assert_eq!(generated.name, "accepted_values_orders__status");
        assert!(generated.sql.contains("NOT IN ('a', 'b')"));
    }

    #[test]
    fn test_generate_relationship_test() {
        let sql = generate_relationship_test("orders", "customer_id", "customers", "id");
        assert!(sql.contains("FROM orders AS src"));
        assert!(sql.contains("FROM customers AS ref"));
        assert!(sql.contains("ref.id = src.customer_id"));
        assert!(sql.contains("src.customer_id IS NOT NULL"));
        assert!(sql.contains("NOT EXISTS"));
    }

    #[test]
    fn test_generate_relationship_test_same_column() {
        let sql = generate_relationship_test("orders", "customer_id", "customers", "customer_id");
        assert!(sql.contains("ref.customer_id = src.customer_id"));
    }

    #[test]
    fn test_generated_test_relationship() {
        let schema_test = SchemaTest {
            test_type: TestType::Relationship {
                to: "customers".to_string(),
                field: Some("id".to_string()),
            },
            column: "customer_id".to_string(),
            model: "orders".to_string(),
            config: Default::default(),
        };

        let generated = GeneratedTest::from_schema_test(&schema_test);
        assert_eq!(generated.name, "relationship_orders__customer_id");
        assert!(generated.sql.contains("FROM orders AS src"));
        assert!(generated.sql.contains("FROM customers AS ref"));
        assert!(generated.sql.contains("ref.id = src.customer_id"));
    }

    #[test]
    fn test_generated_test_relationship_default_field() {
        // When field is not specified, it should default to the same column name
        let schema_test = SchemaTest {
            test_type: TestType::Relationship {
                to: "users".to_string(),
                field: None,
            },
            column: "user_id".to_string(),
            model: "posts".to_string(),
            config: Default::default(),
        };

        let generated = GeneratedTest::from_schema_test(&schema_test);
        assert!(generated.sql.contains("ref.user_id = src.user_id"));
    }

    #[test]
    fn test_generated_test_relationship_qualified() {
        let schema_test = SchemaTest {
            test_type: TestType::Relationship {
                to: "dim_customers".to_string(),
                field: Some("customer_id".to_string()),
            },
            column: "customer_id".to_string(),
            model: "fct_orders".to_string(),
            config: Default::default(),
        };

        let generated =
            GeneratedTest::from_schema_test_qualified(&schema_test, "analytics.fct_orders");
        assert!(generated.sql.contains("FROM analytics.fct_orders AS src"));
        // Referenced table uses unqualified name by default
        assert!(generated.sql.contains("FROM dim_customers AS ref"));
    }

    #[test]
    fn test_generated_test_relationship_qualified_with_refs() {
        let schema_test = SchemaTest {
            test_type: TestType::Relationship {
                to: "dim_customers".to_string(),
                field: Some("customer_id".to_string()),
            },
            column: "customer_id".to_string(),
            model: "fct_orders".to_string(),
            config: Default::default(),
        };

        // Resolver that qualifies referenced tables
        let resolver = |name: &str| format!("analytics.{}", name);

        let generated = GeneratedTest::from_schema_test_qualified_with_refs(
            &schema_test,
            "analytics.fct_orders",
            resolver,
        );
        assert!(generated.sql.contains("FROM analytics.fct_orders AS src"));
        assert!(generated
            .sql
            .contains("FROM analytics.dim_customers AS ref"));
    }
}
