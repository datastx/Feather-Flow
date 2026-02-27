use super::*;
use tempfile::TempDir;

fn create_source_file(dir: &Path, name: &str, content: &str) {
    std::fs::write(dir.join(name), content).unwrap();
}

#[test]
fn test_parse_source_file() {
    let yaml = r#"
kind: sources
version: 1
name: raw_ecommerce
description: "Raw e-commerce data"
schema: ecommerce

tables:
  - name: orders
    description: "One record per order"
    columns:
      - name: id
        type: INTEGER
        tests:
          - unique
          - not_null
      - name: amount
        type: DECIMAL(10,2)
"#;

    let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(source.name, "raw_ecommerce");
    assert_eq!(source.schema, "ecommerce");
    assert_eq!(source.tables.len(), 1);
    assert_eq!(source.tables[0].name, "orders");
    assert_eq!(source.tables[0].columns.len(), 2);
}

#[test]
fn test_source_kind_validation() {
    let temp = TempDir::new().unwrap();
    let sources_dir = temp.path().join("sources");
    std::fs::create_dir(&sources_dir).unwrap();

    // Invalid kind
    create_source_file(
        &sources_dir,
        "invalid.yml",
        r#"
kind: models
name: test
schema: raw
tables:
  - name: test
"#,
    );

    let result = SourceFile::load(&sources_dir.join("invalid.yml"));
    assert!(result.is_err());
}

#[test]
fn test_source_empty_tables_validation() {
    let temp = TempDir::new().unwrap();
    let sources_dir = temp.path().join("sources");
    std::fs::create_dir(&sources_dir).unwrap();

    create_source_file(
        &sources_dir,
        "empty.yml",
        r#"
kind: sources
name: test
schema: raw
tables: []
"#,
    );

    let result = SourceFile::load(&sources_dir.join("empty.yml"));
    assert!(result.is_err());
}

#[test]
fn test_discover_sources() {
    let temp = TempDir::new().unwrap();
    let sources_dir = temp.path().join("sources");
    std::fs::create_dir(&sources_dir).unwrap();

    create_source_file(
        &sources_dir,
        "raw_data.yml",
        r#"
kind: sources
name: raw_data
schema: raw
tables:
  - name: orders
  - name: customers
"#,
    );

    create_source_file(
        &sources_dir,
        "external_api.yml",
        r#"
kind: sources
name: external_api
schema: api
tables:
  - name: users
"#,
    );

    let sources = discover_sources(std::slice::from_ref(&sources_dir)).unwrap();
    assert_eq!(sources.len(), 2);
}

#[test]
fn test_build_source_lookup() {
    let yaml = r#"
kind: sources
name: raw
schema: ecommerce
tables:
  - name: orders
    identifier: api_orders
  - name: customers
"#;

    let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
    let lookup = build_source_lookup(&[source]);

    assert!(lookup.contains("orders"));
    assert!(lookup.contains("ecommerce.orders"));
    assert!(lookup.contains("api_orders"));
    assert!(lookup.contains("ecommerce.api_orders"));
    assert!(lookup.contains("customers"));
    assert!(lookup.contains("ecommerce.customers"));
}

#[test]
fn test_get_qualified_name() {
    let yaml = r#"
kind: sources
name: raw
schema: ecommerce
tables:
  - name: orders
    identifier: api_orders
"#;

    let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
    let qualified = source.get_qualified_name(&source.tables[0]);
    assert_eq!(qualified, "ecommerce.api_orders");
}

#[test]
fn test_source_duplicate_table_validation() {
    let temp = TempDir::new().unwrap();
    let sources_dir = temp.path().join("sources");
    std::fs::create_dir(&sources_dir).unwrap();

    // Duplicate table name
    create_source_file(
        &sources_dir,
        "duplicate.yml",
        r#"
kind: sources
name: test
schema: raw
tables:
  - name: orders
  - name: orders
"#,
    );

    let result = SourceFile::load(&sources_dir.join("duplicate.yml"));
    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("SRC007"),
        "Expected SRC007 error code, got: {}",
        err_str
    );
    assert!(
        err_str.contains("orders"),
        "Expected table name in error, got: {}",
        err_str
    );
}

#[test]
fn test_source_error_codes() {
    let temp = TempDir::new().unwrap();
    let sources_dir = temp.path().join("sources");
    std::fs::create_dir(&sources_dir).unwrap();

    // SRC002: Invalid kind
    create_source_file(
        &sources_dir,
        "invalid_kind.yml",
        r#"
kind: models
name: test
schema: raw
tables:
  - name: test
"#,
    );

    let result = SourceFile::load(&sources_dir.join("invalid_kind.yml"));
    assert!(result.is_err());
    // Note: serde_yaml will fail to parse because SourceKind::Sources is required
    // So this becomes a SRC005 parse error, not SRC002

    // SRC004: Empty tables
    create_source_file(
        &sources_dir,
        "empty_tables.yml",
        r#"
kind: sources
name: empty_source
schema: raw
tables: []
"#,
    );

    let result = SourceFile::load(&sources_dir.join("empty_tables.yml"));
    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("SRC004"),
        "Expected SRC004 error code, got: {}",
        err_str
    );

    // SRC005: Parse error
    create_source_file(&sources_dir, "parse_error.yml", "invalid: yaml: content: [");

    let result = SourceFile::load(&sources_dir.join("parse_error.yml"));
    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("SRC005"),
        "Expected SRC005 error code, got: {}",
        err_str
    );
}

#[test]
fn test_source_description_ai_generated_flag() {
    let yaml = r#"
kind: sources
version: 1
name: raw_data
description: "AI-generated source description"
description_ai_generated: true
schema: raw

tables:
  - name: orders
    description: "AI-generated table description"
    description_ai_generated: true
    columns:
      - name: id
        type: INTEGER
        description: "AI-generated column description"
        description_ai_generated: true
      - name: amount
        type: DECIMAL(10,2)
        description: "Human-written description"
"#;

    let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(source.description_ai_generated, Some(true));
    assert_eq!(source.tables[0].description_ai_generated, Some(true));
    assert_eq!(source.tables[0].columns[0].description_ai_generated, Some(true));
    // Defaults to None when omitted
    assert_eq!(source.tables[0].columns[1].description_ai_generated, None);
}

#[test]
fn test_source_description_ai_generated_defaults_to_none() {
    let yaml = r#"
kind: sources
version: 1
name: raw_data
description: "Description with unknown provenance"
schema: raw

tables:
  - name: orders
    description: "Table description with unknown provenance"
    columns:
      - name: id
        type: INTEGER
"#;

    let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(source.description_ai_generated, None);
    assert_eq!(source.tables[0].description_ai_generated, None);
}
