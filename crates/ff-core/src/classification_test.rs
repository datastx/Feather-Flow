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
