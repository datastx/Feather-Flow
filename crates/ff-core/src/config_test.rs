use super::*;

#[test]
fn test_parse_minimal_config() {
    let yaml = r#"
name: test_project
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.name, "test_project");
    // Paths are hardcoded now — verify via accessor methods
    let root = std::path::PathBuf::from("/tmp/test");
    assert_eq!(config.node_paths_absolute(&root), vec![root.join("nodes")]);
    assert_eq!(
        config.macro_paths_absolute(&root),
        vec![root.join("macros")]
    );
    assert_eq!(config.target_path_absolute(&root), root.join("target"));
}

#[test]
fn test_parse_full_config() {
    let yaml = r#"
name: my_analytics_project
version: "1.0.0"
materialization: view
dialect: duckdb
database:
  default:
    type: duckdb
    path: "./warehouse.duckdb"
    schema: analytics
external_tables:
  - raw.orders
  - raw.customers
vars:
  start_date: "2024-01-01"
  environment: dev
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.name, "my_analytics_project");
    assert_eq!(config.external_tables.len(), 2);
    assert!(config.is_external_table("raw.orders"));
    assert!(!config.is_external_table("stg_orders"));
    assert_eq!(config.get_schema(None), Some("analytics"));
}

#[test]
fn test_materialization_default() {
    let config: Config = serde_yaml::from_str("name: test").unwrap();
    assert_eq!(config.materialization, Materialization::View);
}

#[test]
fn test_materialization_ephemeral() {
    let config: Config = serde_yaml::from_str("name: test\nmaterialization: ephemeral").unwrap();
    assert_eq!(config.materialization, Materialization::Ephemeral);
    assert!(config.materialization.is_ephemeral());
}

#[test]
fn test_dialect_default() {
    let config: Config = serde_yaml::from_str("name: test").unwrap();
    assert_eq!(config.dialect, Dialect::DuckDb);
}

#[test]
fn test_run_hooks_default() {
    let config: Config = serde_yaml::from_str("name: test").unwrap();
    assert!(config.on_run_start.is_empty());
    assert!(config.on_run_end.is_empty());
}

#[test]
fn test_run_hooks_parsing() {
    let yaml = r#"
name: test_project
on_run_start:
  - "CREATE SCHEMA IF NOT EXISTS staging"
  - "SET timezone = 'UTC'"
on_run_end:
  - "ANALYZE staging.final_table"
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.on_run_start.len(), 2);
    assert_eq!(
        config.on_run_start[0],
        "CREATE SCHEMA IF NOT EXISTS staging"
    );
    assert_eq!(config.on_run_start[1], "SET timezone = 'UTC'");
    assert_eq!(config.on_run_end.len(), 1);
    assert_eq!(config.on_run_end[0], "ANALYZE staging.final_table");
}

#[test]
fn test_named_database_connections() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./dev.duckdb
    schema: dev_schema
  prod:
    type: duckdb
    path: ./prod.duckdb
    schema: prod_schema
  staging:
    type: duckdb
    path: ./staging.duckdb
    schema: staging_schema
vars:
  environment: dev
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let dbs = config.available_databases();
    assert_eq!(dbs.len(), 3);
    assert!(dbs.contains(&"default"));
    assert!(dbs.contains(&"prod"));
    assert!(dbs.contains(&"staging"));

    let prod = config.get_database_config(Some("prod")).unwrap();
    assert_eq!(prod.path, "./prod.duckdb");
    assert_eq!(prod.schema.as_deref(), Some("prod_schema"));
}

#[test]
fn test_database_requires_named_connections() {
    // Legacy single-object format is no longer supported — must use named connections
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./base.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let db = config.get_database_config(None).unwrap();
    assert_eq!(db.path, "./base.duckdb");
}

#[test]
fn test_get_database_config_base() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./base.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let db = config.get_database_config(None).unwrap();
    assert_eq!(db.path, "./base.duckdb");
}

#[test]
fn test_get_database_config_by_name() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./base.duckdb
  prod:
    type: duckdb
    path: ./prod.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let db = config.get_database_config(Some("prod")).unwrap();
    assert_eq!(db.path, "./prod.duckdb");
}

#[test]
fn test_get_database_config_invalid_name() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./base.duckdb
  prod:
    type: duckdb
    path: ./prod.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let result = config.get_database_config(Some("nonexistent"));
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("nonexistent"));
    assert!(err.contains("prod"));
}

#[test]
fn test_get_schema_with_named_connection() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./dev.duckdb
    schema: base_schema
  prod:
    type: duckdb
    path: ./prod.duckdb
    schema: prod_schema
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.get_schema(None), Some("base_schema"));
    assert_eq!(config.get_schema(Some("prod")), Some("prod_schema"));
}

#[test]
fn test_get_merged_vars() {
    let yaml = r#"
name: test_project
vars:
  environment: dev
  debug: true
  common_key: base_value
database:
  default:
    type: duckdb
    path: ./dev.duckdb
    vars:
      extra_default: from_default
  prod:
    type: duckdb
    path: ./prod.duckdb
    vars:
      environment: prod
      debug: false
      extra_key: prod_only
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();

    // Base vars merged with default connection vars
    let base_vars = config.get_merged_vars(None);
    assert_eq!(base_vars.get("environment").unwrap().as_str(), Some("dev"));
    assert_eq!(base_vars.get("debug").unwrap().as_bool(), Some(true));

    // Merged vars with named connection
    let merged = config.get_merged_vars(Some("prod"));
    assert_eq!(merged.get("environment").unwrap().as_str(), Some("prod"));
    assert_eq!(merged.get("debug").unwrap().as_bool(), Some(false));
    assert_eq!(
        merged.get("common_key").unwrap().as_str(),
        Some("base_value")
    );
    assert_eq!(merged.get("extra_key").unwrap().as_str(), Some("prod_only"));
}

#[test]
fn test_available_databases() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./dev.duckdb
  staging:
    type: duckdb
    path: ./staging.duckdb
  prod:
    type: duckdb
    path: ./prod.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let dbs = config.available_databases();
    assert_eq!(dbs.len(), 3);
    assert!(dbs.contains(&"default"));
    assert!(dbs.contains(&"staging"));
    assert!(dbs.contains(&"prod"));
}

// These tests modify environment variables and must run serially
use serial_test::serial;

#[test]
#[serial]
fn test_resolve_database_cli_takes_precedence() {
    let original = std::env::var("FF_DATABASE").ok();
    std::env::set_var("FF_DATABASE", "staging");
    let result = Config::resolve_database(Some("prod"));
    assert_eq!(result, Some("prod".to_string()));
    match original {
        Some(v) => std::env::set_var("FF_DATABASE", v),
        None => std::env::remove_var("FF_DATABASE"),
    }
}

#[test]
#[serial]
fn test_resolve_database_uses_env_var() {
    let original = std::env::var("FF_DATABASE").ok();
    std::env::set_var("FF_DATABASE", "staging");
    let result = Config::resolve_database(None);
    assert_eq!(result, Some("staging".to_string()));
    match original {
        Some(v) => std::env::set_var("FF_DATABASE", v),
        None => std::env::remove_var("FF_DATABASE"),
    }
}

#[test]
#[serial]
fn test_resolve_database_none_when_not_set() {
    let original = std::env::var("FF_DATABASE").ok();
    std::env::remove_var("FF_DATABASE");
    let result = Config::resolve_database(None);
    assert_eq!(result, None);
    if let Some(v) = original {
        std::env::set_var("FF_DATABASE", v);
    }
}

#[test]
fn test_project_level_pre_hook_post_hook_rejected() {
    let yaml = r#"
name: test_project
pre_hook:
  - "CREATE SCHEMA IF NOT EXISTS staging"
"#;
    let result: Result<Config, _> = serde_yaml::from_str(yaml);
    assert!(
        result.is_err(),
        "pre_hook should be rejected at project level"
    );

    let yaml = r#"
name: test_project
post_hook:
  - "ANALYZE {{ this }}"
"#;
    let result: Result<Config, _> = serde_yaml::from_str(yaml);
    assert!(
        result.is_err(),
        "post_hook should be rejected at project level"
    );
}

#[test]
fn test_analysis_severity_overrides_default_empty() {
    let config: Config = serde_yaml::from_str("name: test").unwrap();
    assert!(config.analysis.severity_overrides.is_empty());
}

#[test]
fn test_analysis_severity_overrides_parsing() {
    let yaml = r#"
name: test_project
analysis:
  severity_overrides:
    A020: warning
    A032: off
    SA02: error
    A010: info
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.analysis.severity_overrides.len(), 4);
    assert_eq!(
        config.analysis.severity_overrides["A020"],
        ConfigSeverity::Warning
    );
    assert_eq!(
        config.analysis.severity_overrides["A032"],
        ConfigSeverity::Off
    );
    assert_eq!(
        config.analysis.severity_overrides["SA02"],
        ConfigSeverity::Error
    );
    assert_eq!(
        config.analysis.severity_overrides["A010"],
        ConfigSeverity::Info
    );
}

#[test]
fn test_analysis_severity_overrides_rejects_unknown_code() {
    let yaml = r#"
name: test_project
analysis:
  severity_overrides:
    BOGUS: warning
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let result = config.validate();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unknown diagnostic code 'BOGUS'"));
    assert!(err.contains("Valid codes:"));
}

#[test]
fn test_analysis_severity_overrides_all_valid_codes_accepted() {
    let yaml = r#"
name: test_project
analysis:
  severity_overrides:
    A002: info
    A003: warning
    A004: error
    A005: off
    A010: info
    A011: warning
    A012: error
    A020: off
    A030: info
    A032: warning
    A033: error
    A040: off
    A041: info
    SA01: warning
    SA02: error
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert!(config.validate().is_ok());
    assert_eq!(config.analysis.severity_overrides.len(), 15);
}

#[test]
fn test_unknown_fields_rejected() {
    let yaml = r#"
name: test_project
bogus_field: true
"#;
    let result: Result<Config, _> = serde_yaml::from_str(yaml);
    assert!(result.is_err(), "Unknown fields should be rejected");
}

#[test]
fn test_format_config_default() {
    let config: Config = serde_yaml::from_str("name: test").unwrap();
    assert_eq!(config.format.line_length, 88);
    assert!(!config.format.no_jinjafmt);
}

#[test]
fn test_format_config_custom() {
    let yaml = r#"
name: test_project
format:
  line_length: 120
  no_jinjafmt: true
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.format.line_length, 120);
    assert!(config.format.no_jinjafmt);
}

#[test]
fn test_format_config_partial() {
    let yaml = r#"
name: test_project
format:
  line_length: 100
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.format.line_length, 100);
    assert!(!config.format.no_jinjafmt); // default
}

#[test]
fn test_legacy_fields_rejected() {
    // Legacy fields like schema, targets, model_paths should now be rejected
    let yaml = r#"
name: test_project
schema: dev_schema
"#;
    let result: Result<Config, _> = serde_yaml::from_str(yaml);
    assert!(result.is_err(), "Legacy 'schema' field should be rejected");

    let yaml = r#"
name: test_project
targets:
  prod:
    schema: prod_schema
"#;
    let result: Result<Config, _> = serde_yaml::from_str(yaml);
    assert!(result.is_err(), "Legacy 'targets' field should be rejected");

    let yaml = r#"
name: test_project
model_paths: ["models"]
"#;
    let result: Result<Config, _> = serde_yaml::from_str(yaml);
    assert!(
        result.is_err(),
        "Legacy 'model_paths' field should be rejected"
    );
}

#[test]
fn test_wap_schema_from_connection() {
    let yaml = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ./dev.duckdb
    wap_schema: wap_staging
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.get_wap_schema(None), Some("wap_staging"));
}
