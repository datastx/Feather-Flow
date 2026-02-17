use super::*;

#[test]
fn parse_rule_file_with_full_header() {
    let content = "-- rule: no_select_star\n-- severity: error\n-- description: Models must not use SELECT *\nSELECT name FROM ff_meta.models WHERE raw_sql LIKE '%SELECT *%'";
    let path = PathBuf::from("rules/no_select_star.sql");
    let rule = parse_rule_file(&path, content, RuleSeverity::Warn);

    assert_eq!(rule.name, "no_select_star");
    assert_eq!(rule.severity, RuleSeverity::Error);
    assert_eq!(
        rule.description.as_deref(),
        Some("Models must not use SELECT *")
    );
    assert!(rule.sql.starts_with("SELECT name"));
}

#[test]
fn parse_rule_file_without_header() {
    let content = "SELECT name FROM ff_meta.models WHERE materialization = 'view'";
    let path = PathBuf::from("rules/check_views.sql");
    let rule = parse_rule_file(&path, content, RuleSeverity::Error);

    assert_eq!(rule.name, "check_views");
    assert_eq!(rule.severity, RuleSeverity::Error);
    assert!(rule.description.is_none());
    assert!(rule.sql.starts_with("SELECT name"));
}

#[test]
fn parse_rule_file_default_severity_applies() {
    let content = "-- rule: my_rule\nSELECT 1";
    let path = PathBuf::from("rules/my_rule.sql");
    let rule = parse_rule_file(&path, content, RuleSeverity::Warn);

    assert_eq!(rule.severity, RuleSeverity::Warn);
}

#[test]
fn parse_rule_file_severity_warn_variant() {
    let content = "-- rule: my_rule\n-- severity: warning\nSELECT 1";
    let path = PathBuf::from("rules/my_rule.sql");
    let rule = parse_rule_file(&path, content, RuleSeverity::Error);

    assert_eq!(rule.severity, RuleSeverity::Warn);
}

#[test]
fn discover_rules_from_directory() {
    let dir = tempfile::TempDir::new().unwrap();
    let rules_dir = dir.path().join("rules");
    std::fs::create_dir_all(&rules_dir).unwrap();

    std::fs::write(
        rules_dir.join("rule_a.sql"),
        "-- rule: alpha\nSELECT 1 WHERE false",
    )
    .unwrap();
    std::fs::write(
        rules_dir.join("rule_b.sql"),
        "-- rule: beta\n-- severity: warn\nSELECT 1 WHERE false",
    )
    .unwrap();
    std::fs::write(rules_dir.join("readme.md"), "Not a rule").unwrap();

    let rules = discover_rules(&[rules_dir], RuleSeverity::Error).unwrap();

    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].name, "alpha");
    assert_eq!(rules[0].severity, RuleSeverity::Error);
    assert_eq!(rules[1].name, "beta");
    assert_eq!(rules[1].severity, RuleSeverity::Warn);
}

#[test]
fn discover_rules_skips_missing_directory() {
    let rules = discover_rules(&[PathBuf::from("/nonexistent/path")], RuleSeverity::Error).unwrap();
    assert!(rules.is_empty());
}

#[test]
fn rules_config_deserializes() {
    let yaml = r#"
paths:
  - rules/
severity: warn
on_failure: fail
"#;
    let config: RulesConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.paths, vec!["rules/"]);
    assert_eq!(config.severity, RuleSeverity::Warn);
    assert_eq!(config.on_failure, OnRuleFailure::Fail);
}

#[test]
fn rules_config_defaults() {
    let yaml = "paths: []";
    let config: RulesConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.severity, RuleSeverity::Error);
    assert_eq!(config.on_failure, OnRuleFailure::Fail);
}

#[test]
fn resolve_rule_paths_joins_with_root() {
    let root = PathBuf::from("/project");
    let paths = vec!["rules/".to_string(), "custom_rules/".to_string()];
    let resolved = resolve_rule_paths(&paths, &root);
    assert_eq!(resolved[0], PathBuf::from("/project/rules/"));
    assert_eq!(resolved[1], PathBuf::from("/project/custom_rules/"));
}
