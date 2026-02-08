//! Ephemeral model inlining
//!
//! This module provides functionality to inline ephemeral models as CTEs
//! in downstream SQL queries.

use std::collections::{HashMap, HashSet};

/// Inline ephemeral model SQL as CTEs in the target SQL.
///
/// Given a SQL query and a map of ephemeral model names to their compiled SQL,
/// this function prepends CTE definitions for all ephemeral dependencies.
///
/// # Arguments
///
/// * `sql` - The original SQL query
/// * `ephemeral_deps` - Map of ephemeral model name to compiled SQL
/// * `model_deps` - List of model dependencies (to preserve CTE order)
///
/// # Returns
///
/// The SQL query with ephemeral models inlined as CTEs
///
/// # Example
///
/// ```
/// use ff_sql::inline::inline_ephemeral_ctes;
/// use std::collections::HashMap;
///
/// let sql = "SELECT * FROM stg_orders";
/// let mut ephemeral_deps = HashMap::new();
/// ephemeral_deps.insert("stg_orders".to_string(), "SELECT id, amount FROM raw_orders".to_string());
///
/// let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &["stg_orders".to_string()]);
/// assert!(result.contains("WITH stg_orders AS"));
/// ```
pub fn inline_ephemeral_ctes(
    sql: &str,
    ephemeral_deps: &HashMap<String, String>,
    model_deps: &[String],
) -> String {
    if ephemeral_deps.is_empty() {
        return sql.to_string();
    }

    // Build CTEs in dependency order (from model_deps)
    let mut ctes: Vec<String> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    for dep in model_deps {
        if let Some(ephemeral_sql) = ephemeral_deps.get(dep) {
            if !seen.contains(dep.as_str()) {
                seen.insert(dep);
                // Clean up the ephemeral SQL (remove trailing semicolons)
                let clean_sql = ephemeral_sql.trim().trim_end_matches(';').trim();
                ctes.push(format!("{} AS (\n{}\n)", dep, clean_sql));
            }
        }
    }

    if ctes.is_empty() {
        return sql.to_string();
    }

    // Check if original SQL already has a WITH clause
    let trimmed_sql = sql.trim();
    let upper_sql = trimmed_sql.to_uppercase();

    if upper_sql.starts_with("WITH ") {
        // Merge with existing WITH clause
        // Find the position after "WITH " and insert our CTEs before the existing ones
        let with_end = "WITH".len();
        let rest = &trimmed_sql[with_end..].trim_start();
        format!("WITH {},\n{}", ctes.join(",\n"), rest)
    } else {
        // Prepend new WITH clause
        format!("WITH {}\n{}", ctes.join(",\n"), trimmed_sql)
    }
}

/// Resolve all ephemeral dependencies for a model, including nested ephemeral models.
///
/// This performs a topological traversal to collect all ephemeral models
/// that need to be inlined, in the correct order.
///
/// # Arguments
///
/// * `model_name` - The target model name
/// * `model_deps` - Map of model name to its dependencies
/// * `is_ephemeral` - Function to check if a model is ephemeral
/// * `get_compiled_sql` - Function to get compiled SQL for a model
///
/// # Returns
///
/// A tuple of:
/// - HashMap of ephemeral model name to compiled SQL
/// - Vec of model names in dependency order
pub fn collect_ephemeral_dependencies<F, G>(
    model_name: &str,
    model_deps: &HashMap<String, Vec<String>>,
    is_ephemeral: F,
    get_compiled_sql: G,
) -> (HashMap<String, String>, Vec<String>)
where
    F: Fn(&str) -> bool,
    G: Fn(&str) -> Option<String>,
{
    let mut ephemeral_sql: HashMap<String, String> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();

    // Get direct dependencies
    if let Some(deps) = model_deps.get(model_name) {
        collect_ephemeral_recursive(
            deps,
            model_deps,
            &is_ephemeral,
            &get_compiled_sql,
            &mut ephemeral_sql,
            &mut order,
            &mut visited,
        );
    }

    (ephemeral_sql, order)
}

/// Recursively collect ephemeral dependencies
fn collect_ephemeral_recursive<F, G>(
    deps: &[String],
    model_deps: &HashMap<String, Vec<String>>,
    is_ephemeral: &F,
    get_compiled_sql: &G,
    ephemeral_sql: &mut HashMap<String, String>,
    order: &mut Vec<String>,
    visited: &mut HashSet<String>,
) where
    F: Fn(&str) -> bool,
    G: Fn(&str) -> Option<String>,
{
    for dep in deps {
        if visited.contains(dep) {
            continue;
        }
        visited.insert(dep.clone());

        if is_ephemeral(dep) {
            // First, recursively process this ephemeral model's dependencies
            if let Some(nested_deps) = model_deps.get(dep) {
                collect_ephemeral_recursive(
                    nested_deps,
                    model_deps,
                    is_ephemeral,
                    get_compiled_sql,
                    ephemeral_sql,
                    order,
                    visited,
                );
            }

            // Then add this ephemeral model
            if let Some(sql) = get_compiled_sql(dep) {
                ephemeral_sql.insert(dep.clone(), sql);
                order.push(dep.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_single_ephemeral() {
        let sql = "SELECT * FROM stg_orders";
        let mut ephemeral_deps = HashMap::new();
        ephemeral_deps.insert(
            "stg_orders".to_string(),
            "SELECT id, amount FROM raw_orders".to_string(),
        );
        let order = vec!["stg_orders".to_string()];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order);
        assert!(result.contains("WITH stg_orders AS"));
        assert!(result.contains("SELECT id, amount FROM raw_orders"));
        assert!(result.contains("SELECT * FROM stg_orders"));
    }

    #[test]
    fn test_inline_multiple_ephemerals() {
        let sql =
            "SELECT o.*, c.name FROM stg_orders o JOIN stg_customers c ON o.customer_id = c.id";
        let mut ephemeral_deps = HashMap::new();
        ephemeral_deps.insert(
            "stg_orders".to_string(),
            "SELECT id, customer_id, amount FROM raw_orders".to_string(),
        );
        ephemeral_deps.insert(
            "stg_customers".to_string(),
            "SELECT id, name FROM raw_customers".to_string(),
        );
        let order = vec!["stg_orders".to_string(), "stg_customers".to_string()];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order);
        assert!(result.contains("WITH stg_orders AS"));
        assert!(result.contains("stg_customers AS"));
        // Both CTEs should be present
        assert!(result.contains("FROM raw_orders"));
        assert!(result.contains("FROM raw_customers"));
    }

    #[test]
    fn test_inline_with_existing_cte() {
        let sql = "WITH my_cte AS (SELECT 1) SELECT * FROM my_cte, stg_orders";
        let mut ephemeral_deps = HashMap::new();
        ephemeral_deps.insert(
            "stg_orders".to_string(),
            "SELECT id FROM raw_orders".to_string(),
        );
        let order = vec!["stg_orders".to_string()];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order);
        // Should merge CTEs
        assert!(result.contains("WITH stg_orders AS"));
        assert!(result.contains("my_cte AS"));
    }

    #[test]
    fn test_inline_no_ephemerals() {
        let sql = "SELECT * FROM orders";
        let ephemeral_deps = HashMap::new();
        let order: Vec<String> = vec![];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order);
        assert_eq!(result, "SELECT * FROM orders");
    }

    #[test]
    fn test_inline_removes_trailing_semicolon() {
        let sql = "SELECT * FROM stg_orders";
        let mut ephemeral_deps = HashMap::new();
        ephemeral_deps.insert(
            "stg_orders".to_string(),
            "SELECT id FROM raw_orders;".to_string(),
        );
        let order = vec!["stg_orders".to_string()];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order);
        // The inner SQL should not have a trailing semicolon
        assert!(result.contains("FROM raw_orders\n)"));
        assert!(!result.contains("FROM raw_orders;"));
    }

    #[test]
    fn test_collect_ephemeral_dependencies() {
        let mut model_deps: HashMap<String, Vec<String>> = HashMap::new();
        model_deps.insert(
            "fct_orders".to_string(),
            vec!["stg_orders".to_string(), "dim_customers".to_string()],
        );
        model_deps.insert("stg_orders".to_string(), vec!["raw_orders".to_string()]);
        model_deps.insert("dim_customers".to_string(), vec![]);

        let is_ephemeral = |name: &str| name == "stg_orders";
        let get_sql = |name: &str| {
            if name == "stg_orders" {
                Some("SELECT id FROM raw_orders".to_string())
            } else {
                None
            }
        };

        let (ephemeral_sql, order) =
            collect_ephemeral_dependencies("fct_orders", &model_deps, is_ephemeral, get_sql);

        assert_eq!(ephemeral_sql.len(), 1);
        assert!(ephemeral_sql.contains_key("stg_orders"));
        assert_eq!(order, vec!["stg_orders".to_string()]);
    }

    #[test]
    fn test_collect_nested_ephemeral_dependencies() {
        // stg_orders (ephemeral) -> stg_raw (ephemeral) -> raw_orders
        let mut model_deps: HashMap<String, Vec<String>> = HashMap::new();
        model_deps.insert("fct_orders".to_string(), vec!["stg_orders".to_string()]);
        model_deps.insert("stg_orders".to_string(), vec!["stg_raw".to_string()]);
        model_deps.insert("stg_raw".to_string(), vec!["raw_orders".to_string()]);

        let is_ephemeral = |name: &str| name == "stg_orders" || name == "stg_raw";
        let get_sql = |name: &str| match name {
            "stg_orders" => Some("SELECT id FROM stg_raw".to_string()),
            "stg_raw" => Some("SELECT id FROM raw_orders".to_string()),
            _ => None,
        };

        let (ephemeral_sql, order) =
            collect_ephemeral_dependencies("fct_orders", &model_deps, is_ephemeral, get_sql);

        assert_eq!(ephemeral_sql.len(), 2);
        assert!(ephemeral_sql.contains_key("stg_orders"));
        assert!(ephemeral_sql.contains_key("stg_raw"));
        // stg_raw should come before stg_orders (dependency order)
        assert_eq!(order, vec!["stg_raw".to_string(), "stg_orders".to_string()]);
    }

    #[test]
    fn test_cte_order_preserved() {
        // When inlining, CTEs should appear in the correct dependency order
        let sql = "SELECT * FROM a";
        let mut ephemeral_deps = HashMap::new();
        ephemeral_deps.insert("a".to_string(), "SELECT * FROM b".to_string());
        ephemeral_deps.insert("b".to_string(), "SELECT * FROM c".to_string());
        ephemeral_deps.insert("c".to_string(), "SELECT 1 AS x".to_string());
        // Order matters: c must come before b, b before a
        let order = vec!["c".to_string(), "b".to_string(), "a".to_string()];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order);

        // Find positions of each CTE in the result
        let pos_c = result.find("c AS").unwrap();
        let pos_b = result.find("b AS").unwrap();
        let pos_a = result.find("a AS").unwrap();

        assert!(pos_c < pos_b, "c should come before b");
        assert!(pos_b < pos_a, "b should come before a");
    }
}
