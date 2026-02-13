use super::*;

#[tokio::test]
async fn test_in_memory() {
    let db = DuckDbBackend::in_memory().unwrap();
    assert_eq!(db.db_type(), "duckdb");
}

#[tokio::test]
async fn test_create_table_as() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.create_table_as("test_table", "SELECT 1 AS id, 'hello' AS name", false)
        .await
        .unwrap();

    assert!(db.relation_exists("test_table").await.unwrap());
}

#[tokio::test]
async fn test_create_view_as() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.create_view_as("test_view", "SELECT 1 AS id", false)
        .await
        .unwrap();

    assert!(db.relation_exists("test_view").await.unwrap());
}

#[tokio::test]
async fn test_query_count() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.execute_batch("CREATE TABLE nums AS SELECT * FROM range(10) t(n)")
        .await
        .unwrap();

    let count = db.query_count("SELECT * FROM nums").await.unwrap();
    assert_eq!(count, 10);
}

#[tokio::test]
async fn test_execute_batch() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT); INSERT INTO t1 VALUES (1);",
    )
    .await
    .unwrap();

    assert!(db.relation_exists("t1").await.unwrap());
    assert!(db.relation_exists("t2").await.unwrap());
}

#[tokio::test]
async fn test_relation_not_exists() {
    let db = DuckDbBackend::in_memory().unwrap();
    assert!(!db.relation_exists("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_drop_if_exists() {
    let db = DuckDbBackend::in_memory().unwrap();
    db.create_table_as("to_drop", "SELECT 1 AS id", false)
        .await
        .unwrap();

    assert!(db.relation_exists("to_drop").await.unwrap());

    db.drop_if_exists("to_drop").await.unwrap();

    assert!(!db.relation_exists("to_drop").await.unwrap());
}

#[tokio::test]
async fn test_create_schema_if_not_exists() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create a schema
    db.create_schema_if_not_exists("staging").await.unwrap();

    // Create a table in the schema
    db.create_table_as("staging.test_table", "SELECT 1 AS id", false)
        .await
        .unwrap();

    // Verify the table exists in the schema
    assert!(db.relation_exists("staging.test_table").await.unwrap());

    // Creating the same schema again should not fail (IF NOT EXISTS)
    db.create_schema_if_not_exists("staging").await.unwrap();
}

#[tokio::test]
async fn test_merge_into() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create target table with initial data
    db.execute_batch(
        "CREATE TABLE users (id INT, name VARCHAR, updated_at VARCHAR);
         INSERT INTO users VALUES (1, 'Alice', '2024-01-01'), (2, 'Bob', '2024-01-01');",
    )
    .await
    .unwrap();

    // Merge in new/updated data
    let source_sql =
        "SELECT 2 AS id, 'Bobby' AS name, '2024-01-02' AS updated_at UNION ALL SELECT 3, 'Charlie', '2024-01-02'";

    db.merge_into("users", source_sql, &["id".to_string()])
        .await
        .unwrap();

    // Verify: id=1 unchanged, id=2 updated, id=3 inserted
    let count = db.query_count("SELECT * FROM users").await.unwrap();
    assert_eq!(count, 3);

    // Verify Bob was updated to Bobby
    let name = db
        .query_one("SELECT name FROM users WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(name, Some("Bobby".to_string()));

    // Verify Alice unchanged
    let name = db
        .query_one("SELECT name FROM users WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(name, Some("Alice".to_string()));

    // Verify Charlie was inserted
    let name = db
        .query_one("SELECT name FROM users WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(name, Some("Charlie".to_string()));
}

#[tokio::test]
async fn test_delete_insert() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create target table with initial data
    db.execute_batch(
        "CREATE TABLE orders (order_id INT, customer_id INT, amount INT);
         INSERT INTO orders VALUES (1, 100, 50), (2, 100, 75), (3, 200, 100);",
    )
    .await
    .unwrap();

    // Delete+insert: delete matching order_ids and insert new versions
    // Source has order_id 1 (updated) and order_id 4 (new)
    let source_sql =
        "SELECT 1 AS order_id, 100 AS customer_id, 60 AS amount UNION ALL SELECT 4, 100, 80";

    db.delete_insert("orders", source_sql, &["order_id".to_string()])
        .await
        .unwrap();

    // Verify: order 1 replaced (only matched row deleted), order 2 unchanged, order 3 unchanged, order 4 inserted
    let count = db.query_count("SELECT * FROM orders").await.unwrap();
    assert_eq!(count, 4); // orders 1, 2, 3, 4

    // Verify order 1 amount updated
    let amount = db
        .query_one("SELECT amount FROM orders WHERE order_id = 1")
        .await
        .unwrap();
    assert_eq!(amount, Some("60".to_string()));

    // Verify order 2 unchanged (not in source, so not deleted)
    let amount = db
        .query_one("SELECT amount FROM orders WHERE order_id = 2")
        .await
        .unwrap();
    assert_eq!(amount, Some("75".to_string()));

    // Verify order 3 unchanged
    let amount = db
        .query_one("SELECT amount FROM orders WHERE order_id = 3")
        .await
        .unwrap();
    assert_eq!(amount, Some("100".to_string()));

    // Verify order 4 inserted
    let amount = db
        .query_one("SELECT amount FROM orders WHERE order_id = 4")
        .await
        .unwrap();
    assert_eq!(amount, Some("80".to_string()));
}

#[tokio::test]
async fn test_merge_into_composite_key() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create target table with composite key
    db.execute_batch(
        "CREATE TABLE inventory (warehouse VARCHAR, product VARCHAR, qty INT);
         INSERT INTO inventory VALUES ('A', 'widget', 10), ('A', 'gadget', 5), ('B', 'widget', 20);",
    )
    .await
    .unwrap();

    // Merge with composite key
    let source_sql =
        "SELECT 'A' AS warehouse, 'widget' AS product, 15 AS qty UNION ALL SELECT 'C', 'widget', 30";

    db.merge_into(
        "inventory",
        source_sql,
        &["warehouse".to_string(), "product".to_string()],
    )
    .await
    .unwrap();

    // Verify counts
    let count = db.query_count("SELECT * FROM inventory").await.unwrap();
    assert_eq!(count, 4); // A-widget updated, A-gadget unchanged, B-widget unchanged, C-widget inserted

    // Verify A-widget updated
    let qty = db
        .query_one("SELECT qty FROM inventory WHERE warehouse = 'A' AND product = 'widget'")
        .await
        .unwrap();
    assert_eq!(qty, Some("15".to_string()));
}

#[tokio::test]
async fn test_get_table_schema() {
    let db = DuckDbBackend::in_memory().unwrap();

    db.execute_batch(
        "CREATE TABLE test_schema (id INT, name VARCHAR, amount DOUBLE, created_at TIMESTAMP)",
    )
    .await
    .unwrap();

    let schema = db.get_table_schema("test_schema").await.unwrap();

    assert_eq!(schema.len(), 4);
    assert_eq!(schema[0].0, "id");
    assert_eq!(schema[1].0, "name");
    assert_eq!(schema[2].0, "amount");
    assert_eq!(schema[3].0, "created_at");
}

#[tokio::test]
async fn test_describe_query() {
    let db = DuckDbBackend::in_memory().unwrap();

    let schema = db
        .describe_query("SELECT 1 AS id, 'hello' AS name, 3.14 AS value")
        .await
        .unwrap();

    assert_eq!(schema.len(), 3);
    assert_eq!(schema[0].0, "id");
    assert_eq!(schema[1].0, "name");
    assert_eq!(schema[2].0, "value");
}

#[tokio::test]
async fn test_add_columns() {
    let db = DuckDbBackend::in_memory().unwrap();

    db.execute_batch("CREATE TABLE test_add (id INT)")
        .await
        .unwrap();

    // Add new columns
    db.add_columns(
        "test_add",
        &[
            ("name".to_string(), "VARCHAR".to_string()),
            ("created_at".to_string(), "TIMESTAMP".to_string()),
        ],
    )
    .await
    .unwrap();

    // Verify columns exist
    let schema = db.get_table_schema("test_add").await.unwrap();
    assert_eq!(schema.len(), 3);
    assert_eq!(schema[1].0, "name");
    assert_eq!(schema[2].0, "created_at");
}
