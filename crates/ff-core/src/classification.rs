//! Data classification utilities for governance and compliance
//!
//! Provides helpers for building classification lookups from project schemas
//! and ranking classification sensitivity levels.

use crate::model::DataClassification;
use crate::Project;
use std::collections::HashMap;

/// Build a lookup of model → column → classification from project schemas
///
/// Returns a nested map: `{ model_name: { column_name: classification_string } }`
pub fn build_classification_lookup(project: &Project) -> HashMap<String, HashMap<String, String>> {
    let mut lookup = HashMap::new();

    for (name, model) in &project.models {
        if let Some(schema) = &model.schema {
            let mut col_map = HashMap::new();
            for col in &schema.columns {
                if let Some(cls) = &col.classification {
                    col_map.insert(col.name.clone(), cls.to_string());
                }
            }
            if !col_map.is_empty() {
                lookup.insert(name.clone(), col_map);
            }
        }
    }

    lookup
}

/// Return a numeric rank for a classification level (higher = more sensitive)
///
/// - `pii` = 4 (highest)
/// - `sensitive` = 3
/// - `internal` = 2
/// - `public` = 1 (lowest)
pub fn classification_rank(cls: &DataClassification) -> u8 {
    match cls {
        DataClassification::Pii => 4,
        DataClassification::Sensitive => 3,
        DataClassification::Internal => 2,
        DataClassification::Public => 1,
    }
}

/// Parse a classification string into the enum, returning None for unknown values
pub fn parse_classification(s: &str) -> Option<DataClassification> {
    match s.to_lowercase().as_str() {
        "pii" => Some(DataClassification::Pii),
        "sensitive" => Some(DataClassification::Sensitive),
        "internal" => Some(DataClassification::Internal),
        "public" => Some(DataClassification::Public),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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
}
