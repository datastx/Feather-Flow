use super::*;

#[test]
fn test_parse_minimal_config() {
    let yaml = r#"
name: test_project
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.name, "test_project");
    assert_eq!(config.model_paths, vec!["models"]);
    assert_eq!(config.macro_paths, vec!["macros"]);
    assert_eq!(config.source_paths, vec!["sources"]);
    assert_eq!(config.target_path, "target");
}

#[test]
fn test_parse_full_config() {
    let yaml = r#"
name: my_analytics_project
version: "1.0.0"
model_paths: ["models"]
macro_paths: ["macros", "shared_macros"]
source_paths: ["sources"]
target_path: "target"
materialization: view
schema: analytics
dialect: duckdb
database:
  type: duckdb
  path: "./warehouse.duckdb"
external_tables:
  - raw.orders
  - raw.customers
vars:
  start_date: "2024-01-01"
  environment: dev
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.name, "my_analytics_project");
    assert_eq!(config.macro_paths, vec!["macros", "shared_macros"]);
    assert_eq!(config.source_paths, vec!["sources"]);
    assert_eq!(config.external_tables.len(), 2);
    assert!(config.is_external_table("raw.orders"));
    assert!(!config.is_external_table("stg_orders"));
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
fn test_targets_parsing() {
    let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./dev.duckdb
schema: dev_schema
vars:
  environment: dev
targets:
  prod:
    database:
      type: duckdb
      path: ./prod.duckdb
    schema: prod_schema
    vars:
      environment: prod
      debug_mode: false
  staging:
    database:
      type: duckdb
      path: ./staging.duckdb
    schema: staging_schema
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.targets.len(), 2);
    assert!(config.targets.contains_key("prod"));
    assert!(config.targets.contains_key("staging"));

    let prod = config.targets.get("prod").unwrap();
    assert_eq!(prod.database.as_ref().unwrap().path, "./prod.duckdb");
    assert_eq!(prod.schema.as_ref().unwrap(), "prod_schema");
    assert!(prod.vars.contains_key("environment"));
}

#[test]
fn test_get_database_config_base() {
    let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let db = config.get_database_config(None).unwrap();
    assert_eq!(db.path, "./base.duckdb");
}

#[test]
fn test_get_database_config_with_target() {
    let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
targets:
  prod:
    database:
      type: duckdb
      path: ./prod.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let db = config.get_database_config(Some("prod")).unwrap();
    assert_eq!(db.path, "./prod.duckdb");
}

#[test]
fn test_get_database_config_target_without_db_override() {
    let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
targets:
  prod:
    schema: prod_schema
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    // Target exists but has no database override, should use base
    let db = config.get_database_config(Some("prod")).unwrap();
    assert_eq!(db.path, "./base.duckdb");
}

#[test]
fn test_get_database_config_invalid_target() {
    let yaml = r#"
name: test_project
database:
  type: duckdb
  path: ./base.duckdb
targets:
  prod:
    database:
      path: ./prod.duckdb
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let result = config.get_database_config(Some("nonexistent"));
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Target 'nonexistent' not found"));
    assert!(err.contains("prod"));
}

#[test]
fn test_get_schema_with_target_override() {
    let yaml = r#"
name: test_project
schema: base_schema
targets:
  prod:
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
targets:
  prod:
    vars:
      environment: prod
      debug: false
      extra_key: prod_only
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();

    // Base vars
    let base_vars = config.get_merged_vars(None);
    assert_eq!(base_vars.get("environment").unwrap().as_str(), Some("dev"));
    assert_eq!(base_vars.get("debug").unwrap().as_bool(), Some(true));

    // Merged vars with target
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
fn test_available_targets() {
    let yaml = r#"
name: test_project
targets:
  dev: {}
  staging: {}
  prod: {}
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let targets = config.available_targets();
    assert_eq!(targets.len(), 3);
    assert!(targets.contains(&"dev"));
    assert!(targets.contains(&"staging"));
    assert!(targets.contains(&"prod"));
}

// These tests modify environment variables and must run serially
use serial_test::serial;

#[test]
#[serial]
fn test_resolve_target_cli_takes_precedence() {
    // CLI flag should take precedence over env var
    let original = std::env::var("FF_TARGET").ok();
    std::env::set_var("FF_TARGET", "staging");
    let result = Config::resolve_target(Some("prod"));
    assert_eq!(result, Some("prod".to_string()));
    // Restore original state
    match original {
        Some(v) => std::env::set_var("FF_TARGET", v),
        None => std::env::remove_var("FF_TARGET"),
    }
}

#[test]
#[serial]
fn test_resolve_target_uses_env_var() {
    let original = std::env::var("FF_TARGET").ok();
    std::env::set_var("FF_TARGET", "staging");
    let result = Config::resolve_target(None);
    assert_eq!(result, Some("staging".to_string()));
    // Restore original state
    match original {
        Some(v) => std::env::set_var("FF_TARGET", v),
        None => std::env::remove_var("FF_TARGET"),
    }
}

#[test]
#[serial]
fn test_resolve_target_none_when_not_set() {
    let original = std::env::var("FF_TARGET").ok();
    std::env::remove_var("FF_TARGET");
    let result = Config::resolve_target(None);
    assert_eq!(result, None);
    // Restore original state
    if let Some(v) = original {
        std::env::set_var("FF_TARGET", v);
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
    // validate() is called inside Config::load, but we can call it explicitly
    // by going through from_str + validate
    assert!(config.validate().is_ok());
    assert_eq!(config.analysis.severity_overrides.len(), 15);
}
