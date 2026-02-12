use super::*;
use std::io::Write;
use tempfile::TempDir;

#[test]
fn test_snapshot_config_validation_timestamp() {
    let config = SnapshotConfig {
        name: "test_snapshot".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Timestamp,
        updated_at: Some("updated_at".to_string()),
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    assert!(config.validate().is_ok());
}

#[test]
fn test_snapshot_config_validation_timestamp_missing_updated_at() {
    let config = SnapshotConfig {
        name: "test_snapshot".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Timestamp,
        updated_at: None,
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_snapshot_config_validation_check() {
    let config = SnapshotConfig {
        name: "test_snapshot".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Check,
        updated_at: None,
        check_cols: vec!["name".to_string(), "address".to_string()],
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    assert!(config.validate().is_ok());
}

#[test]
fn test_snapshot_config_validation_check_missing_cols() {
    let config = SnapshotConfig {
        name: "test_snapshot".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Check,
        updated_at: None,
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_snapshot_config_validation_empty_unique_key() {
    let config = SnapshotConfig {
        name: "test_snapshot".to_string(),
        source: "raw.customers".to_string(),
        unique_key: Vec::new(),
        strategy: SnapshotStrategy::Timestamp,
        updated_at: Some("updated_at".to_string()),
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_snapshot_qualified_name() {
    let config = SnapshotConfig {
        name: "customer_history".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Timestamp,
        updated_at: Some("updated_at".to_string()),
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: Some("snapshots".to_string()),
        description: None,
        tags: Vec::new(),
    };

    assert_eq!(config.qualified_name(), "snapshots.customer_history");
}

#[test]
fn test_snapshot_load_yaml() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("customers.yml");

    let yaml_content = r#"
version: 1
snapshots:
  - name: customer_history
    source: raw.customers
    unique_key:
      - id
    strategy: timestamp
    updated_at: updated_at
    invalidate_hard_deletes: true
    description: Track customer changes
"#;

    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(yaml_content.as_bytes()).unwrap();

    let snapshot_file = SnapshotFile::load(&path).unwrap();
    assert_eq!(snapshot_file.snapshots.len(), 1);

    let snapshot = &snapshot_file.snapshots[0];
    assert_eq!(snapshot.name, "customer_history");
    assert_eq!(snapshot.source, "raw.customers");
    assert_eq!(snapshot.unique_key, vec!["id"]);
    assert_eq!(snapshot.strategy, SnapshotStrategy::Timestamp);
    assert_eq!(snapshot.updated_at, Some("updated_at".to_string()));
    assert!(snapshot.invalidate_hard_deletes);
}

#[test]
fn test_snapshot_create_table_sql() {
    let config = SnapshotConfig {
        name: "customer_history".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Timestamp,
        updated_at: Some("updated_at".to_string()),
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    let snapshot = Snapshot::new(config, std::path::PathBuf::from("test.yml"));
    let source_columns = vec![
        ("id".to_string(), "INTEGER".to_string()),
        ("name".to_string(), "VARCHAR".to_string()),
    ];

    let sql = snapshot.create_table_sql(&source_columns);
    assert!(sql.contains(r#"CREATE TABLE IF NOT EXISTS "customer_history""#));
    assert!(sql.contains(r#""id" INTEGER"#));
    assert!(sql.contains(r#""name" VARCHAR"#));
    assert!(sql.contains(r#""dbt_scd_id" VARCHAR"#));
    assert!(sql.contains(r#""dbt_valid_from" TIMESTAMP"#));
    assert!(sql.contains(r#""dbt_valid_to" TIMESTAMP"#));
}

#[test]
fn test_discover_snapshots() {
    let temp = TempDir::new().unwrap();
    let snapshot_dir = temp.path().join("snapshots");
    std::fs::create_dir(&snapshot_dir).unwrap();

    let yaml_content = r#"
version: 1
snapshots:
  - name: customer_history
    source: raw.customers
    unique_key:
      - id
    strategy: timestamp
    updated_at: updated_at
"#;

    let path = snapshot_dir.join("customers.yml");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(yaml_content.as_bytes()).unwrap();

    let snapshots = discover_snapshots(temp.path(), &["snapshots".to_string()]).unwrap();

    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].name, "customer_history");
}

#[test]
fn test_changed_records_sql_timestamp_missing_updated_at_returns_error() {
    let config = SnapshotConfig {
        name: "bad_snapshot".to_string(),
        source: "raw.customers".to_string(),
        unique_key: vec!["id".to_string()],
        strategy: SnapshotStrategy::Timestamp,
        updated_at: None, // Missing updated_at
        check_cols: Vec::new(),
        invalidate_hard_deletes: false,
        schema: None,
        description: None,
        tags: Vec::new(),
    };

    let snapshot = Snapshot::new(config, std::path::PathBuf::from("test.yml"));
    let result = snapshot.changed_records_sql("src", "snap");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("missing 'updated_at'"), "Got: {}", err_msg);
}
