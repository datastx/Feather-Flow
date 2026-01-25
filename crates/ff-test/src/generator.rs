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

/// Generate SQL for a schema test
pub fn generate_test_sql(test: &SchemaTest) -> String {
    match test.test_type {
        TestType::Unique => generate_unique_test(&test.model, &test.column),
        TestType::NotNull => generate_not_null_test(&test.model, &test.column),
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
            test_type: test.test_type,
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
    fn test_generated_test_name() {
        let schema_test = SchemaTest {
            test_type: TestType::Unique,
            column: "order_id".to_string(),
            model: "stg_orders".to_string(),
        };

        let generated = GeneratedTest::from_schema_test(&schema_test);
        assert_eq!(generated.name, "unique_stg_orders__order_id");
    }
}
