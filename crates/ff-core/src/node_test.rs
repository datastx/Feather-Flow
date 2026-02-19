use super::*;

#[test]
fn normalize_legacy_kinds() {
    assert_eq!(NodeKind::Model.normalize(), NodeKind::Sql);
    assert_eq!(NodeKind::Sources.normalize(), NodeKind::Source);
    assert_eq!(NodeKind::Functions.normalize(), NodeKind::Function);
}

#[test]
fn normalize_modern_kinds_are_identity() {
    assert_eq!(NodeKind::Sql.normalize(), NodeKind::Sql);
    assert_eq!(NodeKind::Seed.normalize(), NodeKind::Seed);
    assert_eq!(NodeKind::Source.normalize(), NodeKind::Source);
    assert_eq!(NodeKind::Function.normalize(), NodeKind::Function);
    assert_eq!(NodeKind::Python.normalize(), NodeKind::Python);
}

#[test]
fn display_uses_modern_names() {
    assert_eq!(NodeKind::Model.to_string(), "sql");
    assert_eq!(NodeKind::Sources.to_string(), "source");
    assert_eq!(NodeKind::Functions.to_string(), "function");
    assert_eq!(NodeKind::Sql.to_string(), "sql");
}

#[test]
fn expected_extensions() {
    assert_eq!(NodeKind::Sql.expected_extension(), Some("sql"));
    assert_eq!(NodeKind::Seed.expected_extension(), Some("csv"));
    assert_eq!(NodeKind::Source.expected_extension(), None);
    assert_eq!(NodeKind::Function.expected_extension(), Some("sql"));
    assert_eq!(NodeKind::Python.expected_extension(), Some("py"));
}

#[test]
fn deserialize_modern_kinds() {
    let probe: NodeKindProbe = serde_yaml::from_str("kind: sql").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Sql);

    let probe: NodeKindProbe = serde_yaml::from_str("kind: seed").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Seed);

    let probe: NodeKindProbe = serde_yaml::from_str("kind: source").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Source);

    let probe: NodeKindProbe = serde_yaml::from_str("kind: function").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Function);
}

#[test]
fn deserialize_legacy_kinds() {
    let probe: NodeKindProbe = serde_yaml::from_str("kind: model").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Model);

    let probe: NodeKindProbe = serde_yaml::from_str("kind: sources").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Sources);

    let probe: NodeKindProbe = serde_yaml::from_str("kind: functions").unwrap();
    assert_eq!(probe.kind.unwrap(), NodeKind::Functions);
}

#[test]
fn probe_missing_kind() {
    let probe: NodeKindProbe = serde_yaml::from_str("version: 1").unwrap();
    assert!(probe.kind.is_none());
}
