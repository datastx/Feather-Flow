use super::*;
use std::collections::HashMap;

fn make_diagnostic(code: DiagnosticCode, severity: Severity) -> Diagnostic {
    Diagnostic {
        code,
        severity,
        message: format!("test diagnostic {:?}", code),
        model: "test_model".to_string(),
        column: None,
        hint: None,
        pass_name: "test_pass".into(),
    }
}

#[test]
fn empty_overrides_is_noop() {
    let overrides = SeverityOverrides::default();
    let diags = vec![
        make_diagnostic(DiagnosticCode::A020, Severity::Info),
        make_diagnostic(DiagnosticCode::A032, Severity::Warning),
    ];
    let result = apply_severity_overrides(diags, &overrides);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].severity, Severity::Info);
    assert_eq!(result[1].severity, Severity::Warning);
}

#[test]
fn promote_info_to_warning() {
    let mut map = HashMap::new();
    map.insert("A020".to_string(), ConfigSeverity::Warning);
    let overrides = SeverityOverrides::from_config(&map);

    let diags = vec![make_diagnostic(DiagnosticCode::A020, Severity::Info)];
    let result = apply_severity_overrides(diags, &overrides);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].severity, Severity::Warning);
}

#[test]
fn demote_error_to_info() {
    let mut map = HashMap::new();
    map.insert("A002".to_string(), ConfigSeverity::Info);
    let overrides = SeverityOverrides::from_config(&map);

    let diags = vec![make_diagnostic(DiagnosticCode::A002, Severity::Error)];
    let result = apply_severity_overrides(diags, &overrides);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].severity, Severity::Info);
}

#[test]
fn off_removes_diagnostic() {
    let mut map = HashMap::new();
    map.insert("A032".to_string(), ConfigSeverity::Off);
    let overrides = SeverityOverrides::from_config(&map);

    let diags = vec![
        make_diagnostic(DiagnosticCode::A020, Severity::Info),
        make_diagnostic(DiagnosticCode::A032, Severity::Warning),
        make_diagnostic(DiagnosticCode::A002, Severity::Error),
    ];
    let result = apply_severity_overrides(diags, &overrides);
    assert_eq!(result.len(), 2);
    assert!(result.iter().all(|d| d.code != DiagnosticCode::A032));
}

#[test]
fn get_for_code_lookup() {
    let mut map = HashMap::new();
    map.insert("A020".to_string(), ConfigSeverity::Warning);
    let overrides = SeverityOverrides::from_config(&map);

    assert_eq!(
        overrides.get_for_code(DiagnosticCode::A020),
        Some(OverriddenSeverity::Level(Severity::Warning))
    );
    assert_eq!(overrides.get_for_code(DiagnosticCode::A032), None);
}

#[test]
fn get_for_sa_lookup() {
    let mut map = HashMap::new();
    map.insert("SA01".to_string(), ConfigSeverity::Off);
    map.insert("SA02".to_string(), ConfigSeverity::Error);
    let overrides = SeverityOverrides::from_config(&map);

    assert_eq!(overrides.get_for_sa("SA01"), Some(OverriddenSeverity::Off));
    assert_eq!(
        overrides.get_for_sa("SA02"),
        Some(OverriddenSeverity::Level(Severity::Error))
    );
    assert_eq!(overrides.get_for_sa("SA99"), None);
}

#[test]
fn mixed_overrides() {
    let mut map = HashMap::new();
    map.insert("A020".to_string(), ConfigSeverity::Warning);
    map.insert("A032".to_string(), ConfigSeverity::Off);
    map.insert("A002".to_string(), ConfigSeverity::Info);
    let overrides = SeverityOverrides::from_config(&map);

    let diags = vec![
        make_diagnostic(DiagnosticCode::A020, Severity::Info), // promote to warning
        make_diagnostic(DiagnosticCode::A032, Severity::Warning), // suppress
        make_diagnostic(DiagnosticCode::A002, Severity::Error), // demote to info
        make_diagnostic(DiagnosticCode::A010, Severity::Warning), // unchanged
    ];
    let result = apply_severity_overrides(diags, &overrides);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].code, DiagnosticCode::A020);
    assert_eq!(result[0].severity, Severity::Warning);
    assert_eq!(result[1].code, DiagnosticCode::A002);
    assert_eq!(result[1].severity, Severity::Info);
    assert_eq!(result[2].code, DiagnosticCode::A010);
    assert_eq!(result[2].severity, Severity::Warning);
}
