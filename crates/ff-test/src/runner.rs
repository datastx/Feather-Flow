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
            severity: test.severity,
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
            severity: test.severity,
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
            severity: test.severity,
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
    ///
    /// Returns `false` when no tests were run (`total == 0`) to avoid
    /// falsely reporting success on an empty test suite.
    pub fn all_passed(&self) -> bool {
        self.total > 0 && self.failed == 0 && self.errors == 0
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

        let count = match self.db.query_count(&test.sql).await {
            Ok(c) => c,
            Err(e) => {
                return TestResult::error(test, e.to_string(), start.elapsed());
            }
        };

        let duration = start.elapsed();
        if count == 0 {
            return TestResult::pass(test, duration);
        }

        let sample_failures = self
            .db
            .query_sample_rows(&test.sql, 5)
            .await
            .unwrap_or_else(|e| {
                log::warn!(
                    "Failed to fetch sample failures for test '{}': {}",
                    test.name,
                    e
                );
                Vec::new()
            });
        TestResult::fail(test, count, sample_failures, duration)
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
#[path = "runner_test.rs"]
mod tests;
