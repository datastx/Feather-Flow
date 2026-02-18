use super::*;

#[test]
fn test_classification_rank_ordering() {
    assert!(
        classification_rank(&DataClassification::Pii)
            > classification_rank(&DataClassification::Sensitive)
    );
    assert!(
        classification_rank(&DataClassification::Sensitive)
            > classification_rank(&DataClassification::Internal)
    );
    assert!(
        classification_rank(&DataClassification::Internal)
            > classification_rank(&DataClassification::Public)
    );
}

#[test]
fn test_parse_classification() {
    assert_eq!(parse_classification("pii"), Some(DataClassification::Pii));
    assert_eq!(parse_classification("PII"), Some(DataClassification::Pii));
    assert_eq!(
        parse_classification("sensitive"),
        Some(DataClassification::Sensitive)
    );
    assert_eq!(
        parse_classification("internal"),
        Some(DataClassification::Internal)
    );
    assert_eq!(
        parse_classification("public"),
        Some(DataClassification::Public)
    );
    assert_eq!(parse_classification("unknown"), None);
}

#[test]
fn test_propagate_classifications_simple_chain() {
    // A -> B -> C, where A.email is declared PII
    let edges = vec![
        ClassificationEdge {
            source_model: "A".into(),
            source_column: "email".into(),
            target_model: "B".into(),
            target_column: "email".into(),
            is_direct: true,
        },
        ClassificationEdge {
            source_model: "B".into(),
            source_column: "email".into(),
            target_model: "C".into(),
            target_column: "user_email".into(),
            is_direct: true,
        },
    ];
    let topo = vec!["A".into(), "B".into(), "C".into()];
    let mut declared: HashMap<String, HashMap<String, String>> = HashMap::new();
    declared
        .entry("A".into())
        .or_default()
        .insert("email".into(), "pii".into());

    let effective = propagate_classifications_topo(&topo, &edges, &declared);

    assert_eq!(effective["A"]["email"], "pii");
    assert_eq!(effective["B"]["email"], "pii");
    assert_eq!(effective["C"]["user_email"], "pii");
}

#[test]
fn test_propagate_classifications_max_wins() {
    // B selects from A (pii) and C (internal) — pii wins
    let edges = vec![
        ClassificationEdge {
            source_model: "A".into(),
            source_column: "ssn".into(),
            target_model: "B".into(),
            target_column: "combined".into(),
            is_direct: false,
        },
        ClassificationEdge {
            source_model: "C".into(),
            source_column: "code".into(),
            target_model: "B".into(),
            target_column: "combined".into(),
            is_direct: false,
        },
    ];
    let topo = vec!["A".into(), "C".into(), "B".into()];
    let mut declared: HashMap<String, HashMap<String, String>> = HashMap::new();
    declared
        .entry("A".into())
        .or_default()
        .insert("ssn".into(), "pii".into());
    declared
        .entry("C".into())
        .or_default()
        .insert("code".into(), "internal".into());

    let effective = propagate_classifications_topo(&topo, &edges, &declared);

    assert_eq!(effective["B"]["combined"], "pii");
}

#[test]
fn test_propagate_declared_overrides_lower_propagated() {
    // A.col is public, but B.col is declared sensitive — B keeps sensitive
    let edges = vec![ClassificationEdge {
        source_model: "A".into(),
        source_column: "col".into(),
        target_model: "B".into(),
        target_column: "col".into(),
        is_direct: true,
    }];
    let topo = vec!["A".into(), "B".into()];
    let mut declared: HashMap<String, HashMap<String, String>> = HashMap::new();
    declared
        .entry("A".into())
        .or_default()
        .insert("col".into(), "public".into());
    declared
        .entry("B".into())
        .or_default()
        .insert("col".into(), "sensitive".into());

    let effective = propagate_classifications_topo(&topo, &edges, &declared);

    assert_eq!(effective["B"]["col"], "sensitive");
}

#[test]
fn test_propagate_no_classification_stays_empty() {
    // A -> B but no classification on A
    let edges = vec![ClassificationEdge {
        source_model: "A".into(),
        source_column: "id".into(),
        target_model: "B".into(),
        target_column: "id".into(),
        is_direct: true,
    }];
    let topo = vec!["A".into(), "B".into()];
    let declared: HashMap<String, HashMap<String, String>> = HashMap::new();

    let effective = propagate_classifications_topo(&topo, &edges, &declared);

    assert!(!effective.contains_key("B") || !effective["B"].contains_key("id"));
}
