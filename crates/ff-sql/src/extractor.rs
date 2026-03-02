//! Table dependency extraction from SQL AST

use sqlparser::ast::{visit_relations, Query, Statement, With};
use std::collections::{HashMap, HashSet};

use crate::dialect::{ResolvedIdent, SqlDialect};

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
    let cap = deps.len();
    let mut model_deps = Vec::with_capacity(cap);
    let mut external_deps = Vec::with_capacity(cap);
    let mut unknown_deps = Vec::with_capacity(cap);

    let known_models_map: HashMap<String, &str> = known_models
        .iter()
        .map(|s| (s.to_lowercase(), *s))
        .collect();

    let external_lower: HashSet<String> =
        external_tables.iter().map(|t| t.to_lowercase()).collect();

    for dep in deps {
        let dep_normalized = normalize_table_name(&dep);
        let dep_lower = dep_normalized.to_lowercase();

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

/// Extract all table references from SQL statements with dialect-aware case
/// resolution.
///
/// Like [`extract_dependencies`] but each returned [`ResolvedIdent`] carries
/// per-part `CaseSensitivity` metadata and has its identifier values folded
/// according to the dialect's rules (e.g. Snowflake folds unquoted idents to
/// UPPER CASE, PostgreSQL to lower case, DuckDB preserves as-is).
///
/// Quoted identifiers (e.g. `"MyTable"`) are always marked case-sensitive and
/// their values are preserved exactly.
pub fn extract_dependencies_resolved(
    statements: &[Statement],
    dialect: &dyn SqlDialect,
) -> Vec<ResolvedIdent> {
    let all_cte_names: HashSet<String> = statements
        .iter()
        .flat_map(get_cte_names)
        .map(|n| n.to_lowercase())
        .collect();

    let mut deps: Vec<ResolvedIdent> = Vec::new();
    let mut seen = HashSet::new();

    for stmt in statements {
        let _ = visit_relations(stmt, |relation| {
            let resolved = dialect.resolve_object_name(relation);

            let table_part_lower = resolved.table_part().value.to_lowercase();
            if all_cte_names.contains(&table_part_lower) {
                return std::ops::ControlFlow::<()>::Continue(());
            }

            if seen.insert(resolved.name.clone()) {
                deps.push(resolved);
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    deps
}

/// Categorize resolved dependencies into models, external tables, and unknown.
///
/// Like [`categorize_dependencies_with_unknown`] but respects case sensitivity
/// from quoted identifiers. Case-sensitive (quoted) identifiers require exact
/// matches; case-insensitive (unquoted) identifiers match via lowercased
/// comparison as before.
///
/// Returns `(model_deps, external_deps, unknown_deps)`.
pub fn categorize_dependencies_resolved(
    deps: Vec<ResolvedIdent>,
    known_models: &HashSet<&str>,
    external_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let lookups = DependencyLookups {
        known_models_lower: known_models
            .iter()
            .map(|s| (s.to_lowercase(), *s))
            .collect(),
        known_models_exact: known_models.iter().copied().collect(),
        external_tables,
        external_lower: external_tables.iter().map(|t| t.to_lowercase()).collect(),
    };

    let mut out = ClassifiedDeps::default();

    for dep in deps {
        classify_single_dependency(&dep, &lookups, &mut out);
    }

    out.model_deps.sort();
    out.external_deps.sort();
    out.unknown_deps.sort();

    (out.model_deps, out.external_deps, out.unknown_deps)
}

/// Pre-built lookup tables for dependency classification.
struct DependencyLookups<'a> {
    known_models_lower: HashMap<String, &'a str>,
    known_models_exact: HashSet<&'a str>,
    external_tables: &'a HashSet<String>,
    external_lower: HashSet<String>,
}

/// Accumulator for classified dependencies.
#[derive(Default)]
struct ClassifiedDeps {
    model_deps: Vec<String>,
    external_deps: Vec<String>,
    unknown_deps: Vec<String>,
}

/// Classify a single resolved dependency into model, external, or unknown.
fn classify_single_dependency(
    dep: &ResolvedIdent,
    lookups: &DependencyLookups<'_>,
    out: &mut ClassifiedDeps,
) {
    let table_part = dep.table_part();

    if dep.is_case_sensitive {
        if lookups
            .known_models_exact
            .contains(table_part.value.as_str())
        {
            out.model_deps.push(table_part.value.clone());
        } else if lookups.external_tables.contains(&dep.name) {
            out.external_deps.push(dep.name.clone());
        } else {
            out.unknown_deps.push(dep.name.clone());
            out.external_deps.push(dep.name.clone());
        }
    } else {
        let dep_lower = table_part.value.to_lowercase();

        if let Some(original_name) = lookups.known_models_lower.get(&dep_lower) {
            out.model_deps.push(original_name.to_string());
        } else if lookups.external_lower.contains(&dep.name.to_lowercase())
            || lookups.external_lower.contains(&dep_lower)
        {
            out.external_deps.push(dep.name.clone());
        } else {
            out.unknown_deps.push(dep.name.clone());
            out.external_deps.push(dep.name.clone());
        }
    }
}

#[cfg(test)]
#[path = "extractor_test.rs"]
mod tests;
