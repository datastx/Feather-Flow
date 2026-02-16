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
        model: ff_core::model_name::ModelName::new("test_table"),
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
        model: ff_core::model_name::ModelName::new("test_table"),
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
        model: ff_core::model_name::ModelName::new("test_table"),
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
        model: ff_core::model_name::ModelName::new("test_table"),
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
            model: ff_core::model_name::ModelName::new("t"),
            config: Default::default(),
        },
        SchemaTest {
            test_type: TestType::NotNull,
            column: "name".to_string(),
            model: ff_core::model_name::ModelName::new("t"),
            config: Default::default(),
        },
    ];

    let runner = TestRunner::new(&db);
    let (_results, summary) = runner.run_all(&tests).await;

    assert_eq!(summary.total, 2);
    assert_eq!(summary.passed, 1);
    assert_eq!(summary.failed, 1);
}
