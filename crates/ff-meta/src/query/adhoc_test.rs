use super::*;
use crate::MetaDb;

fn open_meta() -> MetaDb {
    MetaDb::open_memory().unwrap()
}

#[test]
fn execute_simple_query() {
    let meta = open_meta();
    let result = execute_query(meta.conn(), "SELECT 42 AS answer, 'hello' AS greeting").unwrap();

    assert_eq!(result.columns, vec!["answer", "greeting"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec!["42", "hello"]);
}

#[test]
fn execute_empty_result() {
    let meta = open_meta();
    let result = execute_query(meta.conn(), "SELECT 1 WHERE false").unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn execute_invalid_sql_returns_error() {
    let meta = open_meta();
    let result = execute_query(meta.conn(), "SELECTTTT garbage");

    assert!(result.is_err());
}

#[test]
fn list_tables_returns_meta_tables() {
    let meta = open_meta();
    let tables = list_tables(meta.conn()).unwrap();

    assert!(!tables.is_empty());
    assert!(tables.contains(&"models".to_string()));
    assert!(tables.contains(&"projects".to_string()));
}

#[test]
fn table_row_count_returns_zero_for_empty_table() {
    let meta = open_meta();
    let count = table_row_count(meta.conn(), "models").unwrap();

    assert_eq!(count, 0);
}

#[test]
fn query_meta_tables_after_population() {
    let meta = open_meta();
    let conn = meta.conn();

    conn.execute(
        "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('test', '/tmp', '/tmp/dev.duckdb')",
        [],
    )
    .unwrap();

    let result = execute_query(conn, "SELECT name FROM ff_meta.projects").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "test");

    let count = table_row_count(conn, "projects").unwrap();
    assert_eq!(count, 1);
}
