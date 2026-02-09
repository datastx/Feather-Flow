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
    let mut deps = HashSet::new();
    let mut all_cte_names = HashSet::new();

    // First, collect all CTE names
    for stmt in statements {
        all_cte_names.extend(get_cte_names(stmt));
    }

    // Then extract all table references
    for stmt in statements {
        let _ = visit_relations(stmt, |relation| {
            deps.insert(crate::object_name_to_string(relation));
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    // Filter out CTE names from dependencies
    deps.retain(|dep: &String| {
        // Normalize the name for comparison (take last component)
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
    known_models: &HashSet<String>,
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
    known_models: &HashSet<String>,
    external_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut model_deps = Vec::new();
    let mut external_deps = Vec::new();
    let mut unknown_deps = Vec::new();

    // Build lowercase -> original-case map for O(1) case-insensitive lookup
    let known_models_map: HashMap<String, &String> =
        known_models.iter().map(|s| (s.to_lowercase(), s)).collect();

    for dep in deps {
        // Normalize the dependency name for comparison
        let dep_normalized = normalize_table_name(&dep);
        let dep_lower = dep_normalized.to_lowercase();

        // Check for case-insensitive model match
        if let Some(original_name) = known_models_map.get(&dep_lower) {
            model_deps.push((*original_name).clone());
        } else if external_tables.contains(&dep) || external_tables.contains(&dep_normalized) {
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
fn normalize_table_name(name: &str) -> String {
    // Safety: str::split() always yields at least one element
    name.split('.').next_back().unwrap_or(name).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn parse_and_extract(sql: &str) -> HashSet<String> {
        let parser = SqlParser::duckdb();
        let stmts = parser.parse(sql).unwrap();
        extract_dependencies(&stmts)
    }

    #[test]
    fn test_extract_from_simple_select() {
        let deps = parse_and_extract("SELECT * FROM users");
        assert!(deps.contains("users"));
        assert_eq!(deps.len(), 1);
    }

    #[test]
    fn test_extract_from_join() {
        let deps =
            parse_and_extract("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id");
        assert!(deps.contains("orders"));
        assert!(deps.contains("customers"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_extract_from_subquery() {
        let deps = parse_and_extract(
            "SELECT * FROM (SELECT * FROM raw_data) AS sub JOIN other_table ON sub.id = other_table.id",
        );
        assert!(deps.contains("raw_data"));
        assert!(deps.contains("other_table"));
    }

    #[test]
    fn test_extract_schema_qualified() {
        let deps = parse_and_extract("SELECT * FROM raw.orders");
        assert!(deps.contains("raw.orders"));
    }

    #[test]
    fn test_extract_from_cte() {
        let deps = parse_and_extract(
            r#"
            WITH staged AS (
                SELECT * FROM raw_orders
            )
            SELECT * FROM staged
            JOIN customers ON staged.customer_id = customers.id
            "#,
        );
        assert!(deps.contains("raw_orders"));
        assert!(deps.contains("customers"));
        // CTEs should NOT appear in dependencies
        assert!(
            !deps.contains("staged"),
            "CTE 'staged' should not be in dependencies"
        );
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_extract_from_multiple_ctes() {
        let deps = parse_and_extract(
            r#"
            WITH
                orders_cte AS (SELECT * FROM raw_orders),
                customers_cte AS (SELECT * FROM raw_customers)
            SELECT * FROM orders_cte
            JOIN customers_cte ON orders_cte.customer_id = customers_cte.id
            JOIN products ON orders_cte.product_id = products.id
            "#,
        );
        assert!(deps.contains("raw_orders"));
        assert!(deps.contains("raw_customers"));
        assert!(deps.contains("products"));
        // CTEs should NOT appear in dependencies
        assert!(
            !deps.contains("orders_cte"),
            "CTE 'orders_cte' should not be in dependencies"
        );
        assert!(
            !deps.contains("customers_cte"),
            "CTE 'customers_cte' should not be in dependencies"
        );
        assert_eq!(deps.len(), 3);
    }

    #[test]
    fn test_recursive_cte_not_in_deps() {
        let deps = parse_and_extract(
            r#"
            WITH RECURSIVE emp_tree AS (
                SELECT * FROM employees WHERE manager_id IS NULL
                UNION ALL
                SELECT e.* FROM employees e
                JOIN emp_tree t ON e.manager_id = t.id
            )
            SELECT * FROM emp_tree
            "#,
        );
        assert!(deps.contains("employees"));
        // Recursive CTE should NOT appear in dependencies
        assert!(
            !deps.contains("emp_tree"),
            "Recursive CTE 'emp_tree' should not be in dependencies"
        );
        assert_eq!(deps.len(), 1);
    }

    #[test]
    fn test_extract_from_union() {
        let deps = parse_and_extract("SELECT * FROM table1 UNION ALL SELECT * FROM table2");
        assert!(deps.contains("table1"));
        assert!(deps.contains("table2"));
    }

    #[test]
    fn test_categorize_dependencies() {
        let deps = HashSet::from([
            "stg_orders".to_string(),
            "raw.orders".to_string(),
            "unknown_table".to_string(),
        ]);

        let known_models = HashSet::from(["stg_orders".to_string()]);
        let external_tables = HashSet::from(["raw.orders".to_string()]);

        let (model_deps, external_deps) =
            categorize_dependencies(deps, &known_models, &external_tables);

        assert_eq!(model_deps, vec!["stg_orders"]);
        assert!(external_deps.contains(&"raw.orders".to_string()));
        assert!(external_deps.contains(&"unknown_table".to_string()));
    }

    #[test]
    fn test_normalize_table_name() {
        assert_eq!(normalize_table_name("users"), "users");
        assert_eq!(normalize_table_name("schema.users"), "users");
        assert_eq!(normalize_table_name("db.schema.users"), "users");
    }

    #[test]
    fn test_case_insensitive_model_matching() {
        // SQL references "STG_ORDERS" but model is "stg_orders"
        let deps = HashSet::from(["STG_ORDERS".to_string(), "RAW_DATA".to_string()]);
        let known_models = HashSet::from(["stg_orders".to_string()]);
        let external_tables = HashSet::from(["raw_data".to_string()]);

        let (model_deps, external_deps) =
            categorize_dependencies(deps, &known_models, &external_tables);

        // Should match case-insensitively and preserve original model name
        assert_eq!(model_deps, vec!["stg_orders"]);
        // External tables should also match the reference (uppercase in this case)
        assert!(external_deps.contains(&"RAW_DATA".to_string()));
    }

    #[test]
    fn test_extract_left_join() {
        let deps = parse_and_extract(
            "SELECT * FROM orders LEFT JOIN customers ON orders.customer_id = customers.id",
        );
        assert!(deps.contains("orders"));
        assert!(deps.contains("customers"));
    }

    #[test]
    fn test_extract_multiple_joins() {
        let deps = parse_and_extract(
            r#"
            SELECT o.*, c.name, p.product_name
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.id
            LEFT JOIN products p ON o.product_id = p.id
            "#,
        );
        assert!(deps.contains("orders"));
        assert!(deps.contains("customers"));
        assert!(deps.contains("products"));
        assert_eq!(deps.len(), 3);
    }
}
