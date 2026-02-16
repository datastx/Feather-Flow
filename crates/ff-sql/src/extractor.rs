//! Table dependency extraction from SQL AST

use sqlparser::ast::{visit_relations, Query, Statement, With};
use std::collections::{HashMap, HashSet};

/// Extract CTE names from a WITH clause
fn extract_cte_names(with: &With) -> HashSet<String> {
    with.cte_tables
        .iter()
        .map(|cte| cte.alias.name.value.clone())
        .collect()
}

/// Extract CTE names from a statement
fn get_cte_names(stmt: &Statement) -> HashSet<String> {
    match stmt {
        Statement::Query(query) => get_cte_names_from_query(query),
        _ => HashSet::new(),
    }
}

/// Extract CTE names from a query (including nested queries)
fn get_cte_names_from_query(query: &Query) -> HashSet<String> {
    let mut cte_names = HashSet::new();
    if let Some(with) = &query.with {
        cte_names.extend(extract_cte_names(with));
    }
    cte_names
}

/// Extract all table references from SQL statements
///
/// Uses `visit_relations` to walk the AST and collect all `ObjectName` references
/// from FROM clauses, JOINs, and subqueries.
///
/// CTE names defined in WITH clauses are automatically filtered out.
pub fn extract_dependencies(statements: &[Statement]) -> HashSet<String> {
    let all_cte_names: HashSet<String> = statements.iter().flat_map(get_cte_names).collect();

    let mut deps = HashSet::new();
    for stmt in statements {
        let _ = visit_relations(stmt, |relation| {
            deps.insert(crate::object_name_to_string(relation));
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    deps.retain(|dep: &String| {
        // Safety: str::split() always yields at least one element
        let normalized = dep.split('.').next_back().unwrap_or(dep);
        !all_cte_names.contains(normalized)
    });

    deps
}

/// Categorize dependencies into models vs external tables
///
/// Returns (model_deps, external_deps)
/// Note: Model matching is case-insensitive (DuckDB is case-insensitive by default)
pub fn categorize_dependencies(
    deps: HashSet<String>,
    known_models: &HashSet<&str>,
    external_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let (model_deps, external_deps, _unknown) =
        categorize_dependencies_with_unknown(deps, known_models, external_tables);
    (model_deps, external_deps)
}

/// Categorize dependencies into models, external tables, and unknown
///
/// Returns (model_deps, external_deps, unknown_deps)
/// Note: Model matching is case-insensitive (DuckDB is case-insensitive by default)
pub fn categorize_dependencies_with_unknown(
    deps: HashSet<String>,
    known_models: &HashSet<&str>,
    external_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut model_deps = Vec::new();
    let mut external_deps = Vec::new();
    let mut unknown_deps = Vec::new();

    // Build lowercase -> original-case map for O(1) case-insensitive lookup
    let known_models_map: HashMap<String, &str> = known_models
        .iter()
        .map(|s| (s.to_lowercase(), *s))
        .collect();

    // Pre-compute lowercased external table set for case-insensitive matching
    let external_lower: HashSet<String> =
        external_tables.iter().map(|t| t.to_lowercase()).collect();

    for dep in deps {
        // Normalize the dependency name for comparison
        let dep_normalized = normalize_table_name(&dep);
        let dep_lower = dep_normalized.to_lowercase();

        // Check for case-insensitive model match
        if let Some(original_name) = known_models_map.get(&dep_lower) {
            model_deps.push(original_name.to_string());
        } else if external_lower.contains(&dep.to_lowercase())
            || external_lower.contains(&dep_lower)
        {
            external_deps.push(dep);
        } else {
            // Unknown tables — not in models or declared external_tables.
            // They are added to external_deps (in addition to unknown_deps) for
            // backward compatibility: callers that only inspect model_deps vs
            // external_deps still see them, which avoids false "missing
            // dependency" errors for tables managed outside the project
            // (e.g. system tables, information_schema, temp tables).
            unknown_deps.push(dep.clone());
            external_deps.push(dep);
        }
    }

    model_deps.sort();
    external_deps.sort();
    unknown_deps.sort();

    (model_deps, external_deps, unknown_deps)
}

/// Normalize a table name by taking only the last component
/// This handles cases like "schema.table" -> "table" for model matching
fn normalize_table_name(name: &str) -> &str {
    // Safety: str::split() always yields at least one element
    name.split('.').next_back().unwrap_or(name)
}

#[cfg(test)]
#[path = "extractor_test.rs"]
mod tests;
