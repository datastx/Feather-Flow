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
pub(crate) fn classification_rank(cls: &DataClassification) -> u8 {
    match cls {
        DataClassification::Pii => 4,
        DataClassification::Sensitive => 3,
        DataClassification::Internal => 2,
        DataClassification::Public => 1,
    }
}

/// Parse a classification string into the enum, returning None for unknown values
pub(crate) fn parse_classification(s: &str) -> Option<DataClassification> {
    match s.to_lowercase().as_str() {
        "pii" => Some(DataClassification::Pii),
        "sensitive" => Some(DataClassification::Sensitive),
        "internal" => Some(DataClassification::Internal),
        "public" => Some(DataClassification::Public),
        _ => None,
    }
}

/// Return a numeric rank for a classification string (higher = more sensitive)
///
/// Delegates to [`parse_classification`] + [`classification_rank`].
/// Returns 0 for unrecognized values.
fn rank_str(cls: &str) -> u8 {
    parse_classification(cls)
        .map(|c| classification_rank(&c))
        .unwrap_or(0)
}

/// A column-level lineage edge used for classification propagation.
///
/// This is a simplified representation that decouples the propagation logic
/// from the SQL-level lineage types in `ff-sql`.
#[derive(Debug, Clone)]
pub struct ClassificationEdge {
    /// Source model name
    pub source_model: String,
    /// Source column name
    pub source_column: String,
    /// Target model name
    pub target_model: String,
    /// Target column name
    pub target_column: String,
    /// Whether this is a direct pass-through (copy) or a transform.
    /// Inspect edges (WHERE/JOIN) should be excluded before calling propagation.
    pub is_direct: bool,
}

/// Update the effective classification for a single column in a model.
///
/// Compares the best upstream classification (from incoming lineage edges)
/// against the column's current classification and upgrades it if the
/// upstream is more sensitive.
fn update_column_classification(
    model: &str,
    column: &str,
    target_index: &HashMap<(&str, &str), Vec<&ClassificationEdge>>,
    effective_cls: &mut HashMap<String, HashMap<String, String>>,
) {
    let Some(incoming) = target_index.get(&(model, column)) else {
        return;
    };

    let Some(best_cls) = find_best_upstream_classification(effective_cls, incoming) else {
        return;
    };

    let current_rank = effective_cls
        .get(model)
        .and_then(|cols| cols.get(column))
        .map(|c| rank_str(c))
        .unwrap_or(0);

    if rank_str(&best_cls) > current_rank {
        effective_cls
            .entry(model.to_string())
            .or_default()
            .insert(column.to_string(), best_cls);
    }
}

/// Find the highest-ranked upstream classification among a set of incoming edges.
///
/// Returns `None` if no upstream edge carries a classification.
fn find_best_upstream_classification(
    effective_cls: &HashMap<String, HashMap<String, String>>,
    incoming: &[&ClassificationEdge],
) -> Option<String> {
    let mut best_rank: u8 = 0;
    let mut best_cls: Option<String> = None;

    for edge in incoming {
        let upstream = effective_cls
            .get(&edge.source_model)
            .and_then(|cols| cols.get(&edge.source_column));

        if let Some(cls) = upstream {
            let r = rank_str(cls);
            if r > best_rank {
                best_rank = r;
                best_cls = Some(cls.clone());
            }
        }
    }

    best_cls
}

/// Propagate data classifications through column lineage in topological order.
///
/// Starting from declared classifications (from YAML schemas), walks models in
/// topological order. For each output column, computes the effective
/// classification as `max(declared, max(upstream effective via copy/transform edges))`.
///
/// Only `copy` and `transform` edges propagate classifications — `inspect` edges
/// (WHERE/JOIN conditions) do not carry data into the output and should be
/// filtered out before calling this function.
///
/// Returns a map of `model → column → effective_classification` for all columns
/// that have an effective classification (declared or propagated).
pub fn propagate_classifications_topo(
    topo_order: &[String],
    edges: &[ClassificationEdge],
    declared: &HashMap<String, HashMap<String, String>>,
) -> HashMap<String, HashMap<String, String>> {
    // Build an index: (target_model, target_column) → list of edges
    let mut target_index: HashMap<(&str, &str), Vec<&ClassificationEdge>> = HashMap::new();
    for edge in edges {
        target_index
            .entry((edge.target_model.as_str(), edge.target_column.as_str()))
            .or_default()
            .push(edge);
    }

    // Collect all target columns per model so we know what to process
    let mut model_columns: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in edges {
        let cols = model_columns.entry(edge.target_model.as_str()).or_default();
        if !cols.contains(&edge.target_column.as_str()) {
            cols.push(edge.target_column.as_str());
        }
    }

    // effective_cls tracks the running effective classification per model/column.
    // Initialize with declared classifications.
    let mut effective_cls: HashMap<String, HashMap<String, String>> = declared.clone();

    for model in topo_order {
        let Some(columns) = model_columns.get(model.as_str()) else {
            continue;
        };

        for &column in columns {
            update_column_classification(model, column, &target_index, &mut effective_cls);
        }
    }

    effective_cls
}

#[cfg(test)]
#[path = "classification_test.rs"]
mod tests;
