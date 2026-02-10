//! Test execution

use crate::generator::GeneratedTest;
use ff_core::model::{SchemaTest, TestSeverity, TestType};
use ff_db::Database;
use std::time::{Duration, Instant};

/// Result of a single test execution
#[derive(Debug, Clone)]
pub struct TestResult {
    /// Test name
    pub name: String,

    /// Model tested
    pub model: String,

    /// Column tested
    pub column: String,

    /// Test type
    pub test_type: TestType,

    /// Whether the test passed
    pub passed: bool,

    /// Number of failing rows (0 if passed)
    pub failure_count: usize,

    /// Sample failing rows (up to 5 rows as formatted strings)
    pub sample_failures: Vec<String>,

    /// Execution time
    pub duration: Duration,

    /// Error message if execution failed
    pub error: Option<String>,

    /// Test severity (error or warn)
    pub severity: TestSeverity,
}

impl TestResult {
    /// Create a passed test result
    pub fn pass(test: &GeneratedTest, duration: Duration) -> Self {
        Self {
            name: test.name.clone(),
            model: test.model.clone(),
            column: test.column.clone(),
            test_type: test.test_type.clone(),
            passed: true,
            failure_count: 0,
            sample_failures: Vec::new(),
            duration,
            error: None,
            severity: TestSeverity::default(),
        }
    }

    /// Create a failed test result
    pub fn fail(
        test: &GeneratedTest,
        failure_count: usize,
        sample_failures: Vec<String>,
        duration: Duration,
    ) -> Self {
        Self {
            name: test.name.clone(),
            model: test.model.clone(),
            column: test.column.clone(),
            test_type: test.test_type.clone(),
            passed: false,
            failure_count,
            sample_failures,
            duration,
            error: None,
            severity: TestSeverity::default(),
        }
    }

    /// Create an error test result
    pub fn error(test: &GeneratedTest, error: String, duration: Duration) -> Self {
        Self {
            name: test.name.clone(),
            model: test.model.clone(),
            column: test.column.clone(),
            test_type: test.test_type.clone(),
            passed: false,
            failure_count: 0,
            sample_failures: Vec::new(),
            duration,
            error: Some(error),
            severity: TestSeverity::default(),
        }
    }
}

/// Summary of test run
#[derive(Debug, Clone)]
pub struct TestSummary {
    /// Total tests run
    pub total: usize,

    /// Tests passed
    pub passed: usize,

    /// Tests failed
    pub failed: usize,

    /// Tests with errors
    pub errors: usize,

    /// Total execution time
    pub duration: Duration,
}

impl TestSummary {
    /// Create a summary from test results
    pub fn from_results(results: &[TestResult], duration: Duration) -> Self {
        let total = results.len();
        let passed = results.iter().filter(|r| r.passed).count();
        let errors = results.iter().filter(|r| r.error.is_some()).count();
        let failed = results
            .iter()
            .filter(|r| !r.passed && r.error.is_none())
            .count();

        Self {
            total,
            passed,
            failed,
            errors,
            duration,
        }
    }

    /// Check if all tests passed
    pub fn all_passed(&self) -> bool {
        self.failed == 0 && self.errors == 0
    }
}

/// Test runner for executing schema tests
pub struct TestRunner<'a> {
    db: &'a dyn Database,
}

impl<'a> TestRunner<'a> {
    /// Create a new test runner
    pub fn new(db: &'a dyn Database) -> Self {
        Self { db }
    }

    /// Run a single generated test
    pub async fn run_test(&self, test: &GeneratedTest) -> TestResult {
        let start = Instant::now();

        match self.db.query_count(&test.sql).await {
            Ok(count) => {
                let duration = start.elapsed();
                if count == 0 {
                    TestResult::pass(test, duration)
                } else {
                    // Fetch sample failing rows (up to 5)
                    let sample_failures = self
                        .db
                        .query_sample_rows(&test.sql, 5)
                        .await
                        .unwrap_or_default();
                    TestResult::fail(test, count, sample_failures, duration)
                }
            }
            Err(e) => {
                let duration = start.elapsed();
                TestResult::error(test, e.to_string(), duration)
            }
        }
    }

    /// Run multiple tests
    pub async fn run_tests(&self, tests: &[GeneratedTest]) -> Vec<TestResult> {
        let mut results = Vec::with_capacity(tests.len());

        for test in tests {
            let result = self.run_test(test).await;
            results.push(result);
        }

        results
    }

    /// Run tests from schema test definitions
    pub async fn run_schema_tests(&self, tests: &[SchemaTest]) -> Vec<TestResult> {
        let generated: Vec<GeneratedTest> =
            tests.iter().map(GeneratedTest::from_schema_test).collect();

        self.run_tests(&generated).await
    }

    /// Run all tests and return summary
    pub async fn run_all(&self, tests: &[SchemaTest]) -> (Vec<TestResult>, TestSummary) {
        let start = Instant::now();
        let results = self.run_schema_tests(tests).await;
        let duration = start.elapsed();
        let summary = TestSummary::from_results(&results, duration);

        (results, summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ff_db::{DatabaseCore, DuckDbBackend};

    #[tokio::test]
    async fn test_unique_pass() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE test_table (id INT); INSERT INTO test_table VALUES (1), (2), (3);",
        )
        .await
        .unwrap();

        let test = GeneratedTest::from_schema_test(&SchemaTest {
            test_type: TestType::Unique,
            column: "id".to_string(),
            model: "test_table".to_string(),
            config: Default::default(),
        });

        let runner = TestRunner::new(&db);
        let result = runner.run_test(&test).await;

        assert!(result.passed);
        assert_eq!(result.failure_count, 0);
    }

    #[tokio::test]
    async fn test_unique_fail() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE test_table (id INT); INSERT INTO test_table VALUES (1), (1), (2);",
        )
        .await
        .unwrap();

        let test = GeneratedTest::from_schema_test(&SchemaTest {
            test_type: TestType::Unique,
            column: "id".to_string(),
            model: "test_table".to_string(),
            config: Default::default(),
        });

        let runner = TestRunner::new(&db);
        let result = runner.run_test(&test).await;

        assert!(!result.passed);
        assert_eq!(result.failure_count, 1); // One duplicate value (1)
        assert!(!result.sample_failures.is_empty()); // Should have sample failures
    }

    #[tokio::test]
    async fn test_not_null_pass() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE test_table (name VARCHAR); INSERT INTO test_table VALUES ('a'), ('b');",
        )
        .await
        .unwrap();

        let test = GeneratedTest::from_schema_test(&SchemaTest {
            test_type: TestType::NotNull,
            column: "name".to_string(),
            model: "test_table".to_string(),
            config: Default::default(),
        });

        let runner = TestRunner::new(&db);
        let result = runner.run_test(&test).await;

        assert!(result.passed);
    }

    #[tokio::test]
    async fn test_not_null_fail() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE test_table (name VARCHAR); INSERT INTO test_table VALUES ('a'), (NULL);",
        )
        .await
        .unwrap();

        let test = GeneratedTest::from_schema_test(&SchemaTest {
            test_type: TestType::NotNull,
            column: "name".to_string(),
            model: "test_table".to_string(),
            config: Default::default(),
        });

        let runner = TestRunner::new(&db);
        let result = runner.run_test(&test).await;

        assert!(!result.passed);
        assert_eq!(result.failure_count, 1);
        assert!(!result.sample_failures.is_empty()); // Should have sample failures
    }

    #[tokio::test]
    async fn test_summary() {
        let db = DuckDbBackend::in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE t (id INT, name VARCHAR); INSERT INTO t VALUES (1, 'a'), (2, NULL);",
        )
        .await
        .unwrap();

        let tests = vec![
            SchemaTest {
                test_type: TestType::Unique,
                column: "id".to_string(),
                model: "t".to_string(),
                config: Default::default(),
            },
            SchemaTest {
                test_type: TestType::NotNull,
                column: "name".to_string(),
                model: "t".to_string(),
                config: Default::default(),
            },
        ];

        let runner = TestRunner::new(&db);
        let (_results, summary) = runner.run_all(&tests).await;

        assert_eq!(summary.total, 2);
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 1);
    }
}
