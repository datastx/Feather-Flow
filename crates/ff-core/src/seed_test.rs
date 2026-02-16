use super::*;
use crate::model::schema::{ModelKind, ModelSchema};
use tempfile::TempDir;

/// Helper to build a minimal ModelSchema with kind: seed
fn seed_schema() -> ModelSchema {
    ModelSchema {
        kind: ModelKind::Seed,
        ..Default::default()
    }
}

#[test]
fn test_seed_from_schema_basic() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("customers.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob").unwrap();

    let schema = seed_schema();
    let seed = Seed::from_schema(csv_path, &schema).unwrap();
    assert_eq!(seed.name, "customers");
    assert!(seed.is_enabled());
    assert_eq!(seed.delimiter(), ',');
}

#[test]
fn test_seed_from_schema_with_overrides() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("orders.csv");
    std::fs::write(&csv_path, "id,amount\n1,100.50").unwrap();

    let mut schema = seed_schema();
    schema.description = Some("Order data".to_string());
    schema.schema = Some("raw".to_string());
    schema
        .column_types
        .insert("id".to_string(), "INTEGER".to_string());
    schema
        .column_types
        .insert("amount".to_string(), "DECIMAL(10,2)".to_string());

    let seed = Seed::from_schema(csv_path, &schema).unwrap();
    assert_eq!(seed.name, "orders");
    assert_eq!(seed.description, Some("Order data".to_string()));
    assert_eq!(seed.schema, Some("raw".to_string()));
    assert_eq!(seed.column_types.get("id"), Some(&"INTEGER".to_string()));
    assert_eq!(
        seed.column_types.get("amount"),
        Some(&"DECIMAL(10,2)".to_string())
    );
}

#[test]
fn test_seed_disabled() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("disabled.csv");
    std::fs::write(&csv_path, "id\n1").unwrap();

    let mut schema = seed_schema();
    schema.enabled = false;

    let seed = Seed::from_schema(csv_path, &schema).unwrap();
    assert!(!seed.is_enabled());
}

#[test]
fn test_seed_qualified_name() {
    let dir = TempDir::new().unwrap();

    // Without schema override
    let csv_path = dir.path().join("customers.csv");
    std::fs::write(&csv_path, "id\n1").unwrap();
    let schema = seed_schema();
    let seed = Seed::from_schema(csv_path, &schema).unwrap();
    assert_eq!(seed.qualified_name(None), "customers");
    assert_eq!(seed.qualified_name(Some("default")), "default.customers");

    // With schema override in seed config
    let csv_path2 = dir.path().join("orders.csv");
    std::fs::write(&csv_path2, "id\n1").unwrap();
    let mut schema2 = seed_schema();
    schema2.schema = Some("raw".to_string());
    let seed2 = Seed::from_schema(csv_path2, &schema2).unwrap();
    // Config schema overrides default
    assert_eq!(seed2.qualified_name(Some("default")), "raw.orders");
}

#[test]
fn test_seed_defaults() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id\n1").unwrap();

    let schema = seed_schema();
    let seed = Seed::from_schema(csv_path, &schema).unwrap();
    assert_eq!(seed.delimiter, ',');
    assert!(seed.enabled);
    assert!(!seed.quote_columns);
    assert!(seed.column_types.is_empty());
}
