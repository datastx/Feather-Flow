//! Table dependency extraction from SQL AST

use sqlparser::ast::{visit_relations, Statement};
use std::collections::HashSet;

/// Extract all table references from SQL statements
///
/// Uses `visit_relations` to walk the AST and collect all `ObjectName` references
/// from FROM clauses, JOINs, and subqueries.
pub fn extract_dependencies(statements: &[Statement]) -> HashSet<String> {
    let mut deps = HashSet::new();

    for stmt in statements {
        let _ = visit_relations(stmt, |relation| {
            let table_name = relation
                .0
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            deps.insert(table_name);
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    deps
}

/// Extract dependencies from a single statement
pub fn extract_dependencies_single(statement: &Statement) -> HashSet<String> {
    extract_dependencies(std::slice::from_ref(statement))
}

/// Categorize dependencies into models vs external tables
///
/// Returns (model_deps, external_deps)
pub fn categorize_dependencies(
    deps: HashSet<String>,
    known_models: &HashSet<String>,
    external_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut model_deps = Vec::new();
    let mut external_deps = Vec::new();

    for dep in deps {
        // Normalize the dependency name for comparison
        let dep_normalized = normalize_table_name(&dep);

        if known_models.contains(&dep_normalized) {
            model_deps.push(dep_normalized);
        } else if external_tables.contains(&dep) || external_tables.contains(&dep_normalized) {
            external_deps.push(dep);
        } else {
            // Unknown tables default to external
            external_deps.push(dep);
        }
    }

    model_deps.sort();
    external_deps.sort();

    (model_deps, external_deps)
}

/// Normalize a table name by taking only the last component
/// This handles cases like "schema.table" -> "table" for model matching
fn normalize_table_name(name: &str) -> String {
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
        // CTEs themselves are not external dependencies
        // Note: visit_relations may include the CTE name depending on implementation
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
