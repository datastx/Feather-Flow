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
        let Some(schema) = &model.schema else {
            continue;
        };

        let col_map: HashMap<String, String> = schema
            .columns
            .iter()
            .filter_map(|col| {
                col.classification
                    .as_ref()
                    .map(|cls| (col.name.clone(), cls.to_string()))
            })
            .collect();

        if !col_map.is_empty() {
            lookup.insert(name.to_string(), col_map);
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
#[path = "classification_test.rs"]
mod tests;
