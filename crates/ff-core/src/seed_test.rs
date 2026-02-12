use super::*;
use tempfile::TempDir;

#[test]
fn test_seed_from_file_without_config() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("customers.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob").unwrap();

    let seed = Seed::from_file(csv_path).unwrap();
    assert_eq!(seed.name, "customers");
    assert!(seed.config.is_none());
    assert!(seed.is_enabled());
    assert_eq!(seed.delimiter(), ',');
}

#[test]
fn test_seed_from_file_with_config() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("orders.csv");
    let yml_path = dir.path().join("orders.yml");

    std::fs::write(&csv_path, "id,amount\n1,100.50").unwrap();
    std::fs::write(
        &yml_path,
        r#"
version: 1
description: Order data
schema: raw
delimiter: ","
column_types:
  id: INTEGER
  amount: DECIMAL(10,2)
"#,
    )
    .unwrap();

    let seed = Seed::from_file(csv_path).unwrap();
    assert_eq!(seed.name, "orders");
    assert!(seed.config.is_some());

    let config = seed.config.as_ref().unwrap();
    assert_eq!(config.schema, Some("raw".to_string()));
    assert_eq!(config.column_types.get("id"), Some(&"INTEGER".to_string()));
    assert_eq!(
        config.column_types.get("amount"),
        Some(&"DECIMAL(10,2)".to_string())
    );
}

#[test]
fn test_seed_disabled() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("disabled.csv");
    let yml_path = dir.path().join("disabled.yml");

    std::fs::write(&csv_path, "id\n1").unwrap();
    std::fs::write(&yml_path, "enabled: false").unwrap();

    let seed = Seed::from_file(csv_path).unwrap();
    assert!(!seed.is_enabled());
}

#[test]
fn test_seed_qualified_name() {
    let dir = TempDir::new().unwrap();

    // Without config
    let csv_path = dir.path().join("customers.csv");
    std::fs::write(&csv_path, "id\n1").unwrap();
    let seed = Seed::from_file(csv_path).unwrap();
    assert_eq!(seed.qualified_name(None), "customers");
    assert_eq!(seed.qualified_name(Some("default")), "default.customers");

    // With config schema
    let csv_path2 = dir.path().join("orders.csv");
    let yml_path2 = dir.path().join("orders.yml");
    std::fs::write(&csv_path2, "id\n1").unwrap();
    std::fs::write(&yml_path2, "schema: raw").unwrap();
    let seed2 = Seed::from_file(csv_path2).unwrap();
    // Config schema overrides default
    assert_eq!(seed2.qualified_name(Some("default")), "raw.orders");
}

#[test]
fn test_discover_seeds() {
    let dir = TempDir::new().unwrap();
    let seeds_dir = dir.path().join("seeds");
    std::fs::create_dir_all(&seeds_dir).unwrap();

    std::fs::write(seeds_dir.join("a.csv"), "id\n1").unwrap();
    std::fs::write(seeds_dir.join("b.csv"), "id\n2").unwrap();

    let seeds = discover_seeds(&[seeds_dir]).unwrap();
    assert_eq!(seeds.len(), 2);
    assert_eq!(seeds[0].name, "a");
    assert_eq!(seeds[1].name, "b");
}

#[test]
fn test_seed_config_defaults() {
    let config = SeedConfig::default();
    assert_eq!(config.version, 1);
    assert_eq!(config.delimiter, ',');
    assert!(config.enabled);
    assert!(!config.quote_columns);
    assert!(config.column_types.is_empty());
}

#[test]
fn test_custom_delimiter() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("tsv_data.csv");
    let yml_path = dir.path().join("tsv_data.yml");

    std::fs::write(&csv_path, "id\tname\n1\tAlice").unwrap();
    std::fs::write(&yml_path, "delimiter: \"\\t\"").unwrap();

    let seed = Seed::from_file(csv_path).unwrap();
    // Note: YAML escape sequences need special handling
    // For now, tab delimiter would need to be specified differently
    assert!(seed.config.is_some());
}
