use super::*;
use std::collections::HashMap;

/// Helper to build a map with 2-part (default db) entries.
fn make_map_2part(entries: &[(&str, &str, &str)]) -> HashMap<String, QualifiedRef> {
    entries
        .iter()
        .map(|(bare, schema, table)| {
            (
                bare.to_string(),
                QualifiedRef {
                    database: None,
                    schema: schema.to_string(),
                    table: table.to_string(),
                },
            )
        })
        .collect()
}

/// Helper to build a map with 3-part (cross-database) entries.
fn make_map_3part(entries: &[(&str, &str, &str, &str)]) -> HashMap<String, QualifiedRef> {
    entries
        .iter()
        .map(|(bare, db, schema, table)| {
            (
                bare.to_string(),
                QualifiedRef {
                    database: Some(db.to_string()),
                    schema: schema.to_string(),
                    table: table.to_string(),
                },
            )
        })
        .collect()
}

#[test]
fn test_qualify_bare_name_2part() {
    let sql = "SELECT id, name FROM stg_customers";
    let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("analytics.stg_customers"),
        "Expected 2-part name, got: {}",
        result
    );
    assert!(
        !result.contains("main.analytics"),
        "Should not have 3-part name for default db, got: {}",
        result
    );
}

#[test]
fn test_qualify_join_2part() {
    let sql = "SELECT c.id FROM stg_customers c INNER JOIN stg_orders o ON c.id = o.customer_id";
    let map = make_map_2part(&[
        ("stg_customers", "analytics", "stg_customers"),
        ("stg_orders", "analytics", "stg_orders"),
    ]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("analytics.stg_customers"),
        "Expected qualified stg_customers, got: {}",
        result
    );
    assert!(
        result.contains("analytics.stg_orders"),
        "Expected qualified stg_orders, got: {}",
        result
    );
}

#[test]
fn test_already_qualified_unchanged() {
    let sql = "SELECT id FROM staging.stg_customers";
    let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        !result.contains("analytics.stg_customers"),
        "Should not re-qualify already-qualified name, got: {}",
        result
    );
}

#[test]
fn test_unknown_table_unchanged() {
    let sql = "SELECT id FROM unknown_table";
    let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("unknown_table"),
        "Unknown table should be unchanged, got: {}",
        result
    );
}

#[test]
fn test_case_insensitive_matching() {
    let sql = "SELECT id FROM STG_CUSTOMERS";
    let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("analytics.stg_customers"),
        "Case-insensitive match should work, got: {}",
        result
    );
}

#[test]
fn test_empty_map_returns_original() {
    let sql = "SELECT id FROM stg_customers";
    let map = HashMap::new();
    let result = qualify_table_references(sql, &map).unwrap();
    assert_eq!(result, sql);
}

#[test]
fn test_qualify_with_different_schemas() {
    let sql = "SELECT c.id FROM stg_customers c INNER JOIN raw_orders r ON c.id = r.customer_id";
    let map = make_map_2part(&[
        ("stg_customers", "staging", "stg_customers"),
        ("raw_orders", "analytics", "raw_orders"),
    ]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("staging.stg_customers"),
        "Expected staging schema, got: {}",
        result
    );
    assert!(
        result.contains("analytics.raw_orders"),
        "Expected analytics schema, got: {}",
        result
    );
}

#[test]
fn test_qualify_with_cross_database() {
    let sql = "SELECT id FROM external_table";
    let map = make_map_3part(&[("external_table", "ext_db", "raw", "external_table")]);
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("ext_db.raw.external_table"),
        "Expected 3-part name for cross-database, got: {}",
        result
    );
}

#[test]
fn test_mixed_default_and_cross_database() {
    let sql = "SELECT a.id FROM local_table a JOIN remote_table b ON a.id = b.id";
    let mut map = make_map_2part(&[("local_table", "analytics", "local_table")]);
    map.extend(make_map_3part(&[(
        "remote_table",
        "ext_db",
        "raw",
        "remote_table",
    )]));
    let result = qualify_table_references(sql, &map).unwrap();
    assert!(
        result.contains("analytics.local_table"),
        "Expected 2-part for default db, got: {}",
        result
    );
    assert!(
        result.contains("ext_db.raw.remote_table"),
        "Expected 3-part for cross-db, got: {}",
        result
    );
}
