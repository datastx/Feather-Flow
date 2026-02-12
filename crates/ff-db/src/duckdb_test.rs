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

// ===== Snapshot Tests =====

#[tokio::test]
async fn test_snapshot_initial_load() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create source table
    db.execute_batch(
        "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
         INSERT INTO customers VALUES
           (1, 'Alice', '2024-01-01'::TIMESTAMP),
           (2, 'Bob', '2024-01-01'::TIMESTAMP);",
    )
    .await
    .unwrap();

    // Execute initial snapshot
    let result = db
        .execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

    // Verify initial load
    assert_eq!(result.new_records, 2);
    assert_eq!(result.updated_records, 0);
    assert_eq!(result.deleted_records, 0);

    // Verify snapshot table has SCD columns
    let schema = db.get_table_schema("customers_snapshot").await.unwrap();
    let col_names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
    assert!(col_names.contains(&"dbt_scd_id"));
    assert!(col_names.contains(&"dbt_updated_at"));
    assert!(col_names.contains(&"dbt_valid_from"));
    assert!(col_names.contains(&"dbt_valid_to"));

    // All records should have dbt_valid_to = NULL (active)
    let active_count = db
        .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(active_count, 2);
}

#[tokio::test]
async fn test_snapshot_insert_new_records() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create source table
    db.execute_batch(
        "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
         INSERT INTO customers VALUES (1, 'Alice', '2024-01-01'::TIMESTAMP);",
    )
    .await
    .unwrap();

    // Initial snapshot
    let initial_result = db
        .execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

    // Check initial snapshot loaded correctly
    assert_eq!(initial_result.new_records, 1);

    // Add new record to source
    db.execute_batch("INSERT INTO customers VALUES (2, 'Bob', '2024-01-02'::TIMESTAMP);")
        .await
        .unwrap();

    // Execute snapshot again
    let result = db
        .execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

    // Only new record should be inserted
    assert_eq!(result.new_records, 1);
    assert_eq!(result.updated_records, 0);
    assert_eq!(result.deleted_records, 0);

    // Total active records should be 2
    let active_count = db
        .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(active_count, 2);
}

#[tokio::test]
async fn test_snapshot_update_changed_timestamp() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create source table
    db.execute_batch(
        "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
         INSERT INTO customers VALUES (1, 'Alice', '2024-01-01 00:00:00'::TIMESTAMP);",
    )
    .await
    .unwrap();

    // Initial snapshot
    db.execute_snapshot(
        "customers_snapshot",
        "customers",
        &["id".to_string()],
        Some("updated_at"),
        None,
        false,
    )
    .await
    .unwrap();

    // Update record in source with newer timestamp
    db.execute_batch(
        "UPDATE customers SET name = 'Alice Smith', updated_at = '2024-01-02 00:00:00'::TIMESTAMP WHERE id = 1;",
    )
    .await
    .unwrap();

    // Execute snapshot again
    let result = db
        .execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

    // Record should be updated (old version invalidated, new version inserted)
    assert_eq!(result.new_records, 0);
    assert_eq!(result.updated_records, 1);
    assert_eq!(result.deleted_records, 0);

    // Total records should be 2 (one active, one historical)
    let total_count = db
        .query_count("SELECT * FROM customers_snapshot")
        .await
        .unwrap();
    assert_eq!(total_count, 2);

    // Active records should be 1
    let active_count = db
        .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(active_count, 1);

    // Active record should have new name
    let name = db
        .query_one("SELECT name FROM customers_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(name, Some("Alice Smith".to_string()));
}

#[tokio::test]
async fn test_snapshot_hard_deletes() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create source table
    db.execute_batch(
        "CREATE TABLE customers (id INT, name VARCHAR, updated_at TIMESTAMP);
         INSERT INTO customers VALUES
           (1, 'Alice', '2024-01-01'::TIMESTAMP),
           (2, 'Bob', '2024-01-01'::TIMESTAMP);",
    )
    .await
    .unwrap();

    // Initial snapshot
    db.execute_snapshot(
        "customers_snapshot",
        "customers",
        &["id".to_string()],
        Some("updated_at"),
        None,
        true, // invalidate_hard_deletes = true
    )
    .await
    .unwrap();

    // Delete Bob from source
    db.execute_batch("DELETE FROM customers WHERE id = 2;")
        .await
        .unwrap();

    // Execute snapshot with hard delete tracking
    let result = db
        .execute_snapshot(
            "customers_snapshot",
            "customers",
            &["id".to_string()],
            Some("updated_at"),
            None,
            true,
        )
        .await
        .unwrap();

    // Bob should be invalidated
    assert_eq!(result.new_records, 0);
    assert_eq!(result.updated_records, 0);
    assert_eq!(result.deleted_records, 1);

    // Only Alice should be active
    let active_count = db
        .query_count("SELECT * FROM customers_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(active_count, 1);

    let name = db
        .query_one("SELECT name FROM customers_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(name, Some("Alice".to_string()));
}

#[tokio::test]
async fn test_snapshot_check_strategy() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create source table (no timestamp column)
    db.execute_batch(
        "CREATE TABLE products (id INT, name VARCHAR, price DECIMAL);
         INSERT INTO products VALUES (1, 'Widget', 10.00);",
    )
    .await
    .unwrap();

    // Initial snapshot with check strategy
    db.execute_snapshot(
        "products_snapshot",
        "products",
        &["id".to_string()],
        None,
        Some(&["name".to_string(), "price".to_string()]),
        false,
    )
    .await
    .unwrap();

    // Update price
    db.execute_batch("UPDATE products SET price = 15.00 WHERE id = 1;")
        .await
        .unwrap();

    // Execute snapshot again
    let result = db
        .execute_snapshot(
            "products_snapshot",
            "products",
            &["id".to_string()],
            None,
            Some(&["name".to_string(), "price".to_string()]),
            false,
        )
        .await
        .unwrap();

    // Record should be updated due to price change
    assert_eq!(result.updated_records, 1);

    // Active record should have new price
    let price = db
        .query_one("SELECT price FROM products_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    // DuckDB formats DECIMAL differently, just check it starts with 15
    assert!(price.unwrap().starts_with("15"));
}

#[tokio::test]
async fn test_snapshot_composite_key() {
    let db = DuckDbBackend::in_memory().unwrap();

    // Create source table with composite key
    db.execute_batch(
        "CREATE TABLE inventory (warehouse VARCHAR, product VARCHAR, qty INT, updated_at TIMESTAMP);
         INSERT INTO inventory VALUES
           ('A', 'widget', 10, '2024-01-01'::TIMESTAMP),
           ('A', 'gadget', 5, '2024-01-01'::TIMESTAMP);",
    )
    .await
    .unwrap();

    // Initial snapshot
    let result = db
        .execute_snapshot(
            "inventory_snapshot",
            "inventory",
            &["warehouse".to_string(), "product".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

    assert_eq!(result.new_records, 2);

    // Add new product
    db.execute_batch("INSERT INTO inventory VALUES ('B', 'widget', 20, '2024-01-02'::TIMESTAMP);")
        .await
        .unwrap();

    let result = db
        .execute_snapshot(
            "inventory_snapshot",
            "inventory",
            &["warehouse".to_string(), "product".to_string()],
            Some("updated_at"),
            None,
            false,
        )
        .await
        .unwrap();

    assert_eq!(result.new_records, 1);

    let active_count = db
        .query_count("SELECT * FROM inventory_snapshot WHERE dbt_valid_to IS NULL")
        .await
        .unwrap();
    assert_eq!(active_count, 3);
}
