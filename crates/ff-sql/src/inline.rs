//! Ephemeral model inlining
//!
//! This module provides functionality to inline ephemeral models as CTEs
//! in downstream SQL queries. CTE injection is performed via AST manipulation
//! using `sqlparser`, ensuring correctness even when CTE names are SQL
//! reserved words.

use std::collections::{HashMap, HashSet};

use crate::error::{SqlError, SqlResult};
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{Cte, Ident, Statement, TableAlias, With};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

/// Inline ephemeral model SQL as CTEs in the target SQL.
///
/// Given a SQL query and a map of ephemeral model names to their compiled SQL,
/// this function prepends CTE definitions for all ephemeral dependencies.
///
/// Both the target SQL and each ephemeral SQL fragment are parsed into AST
/// nodes.  The ephemeral queries are injected into the target statement's
/// `WITH` clause (creating one when absent), and the result is emitted via
/// `Statement::to_string()`.  CTE names are always double-quoted so that SQL
/// reserved words work correctly.
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
/// assert!(result.unwrap().contains(r#""stg_orders" AS"#));
/// ```
pub fn inline_ephemeral_ctes(
    sql: &str,
    ephemeral_deps: &HashMap<String, String>,
    model_deps: &[String],
) -> SqlResult<String> {
    if ephemeral_deps.is_empty() {
        return Ok(sql.to_string());
    }

    let dialect = DuckDbDialect {};

    // ---- Build CTE AST nodes in dependency order ----
    let mut new_ctes: Vec<Cte> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    for dep in model_deps {
        if let Some(ephemeral_sql) = ephemeral_deps.get(dep) {
            if seen.contains(dep.as_str()) {
                continue;
            }
            seen.insert(dep);

            // Clean trailing semicolons before parsing
            let clean_sql = ephemeral_sql.trim().trim_end_matches(';').trim();

            // Parse the ephemeral SQL into a Query AST node
            let ephemeral_query = match Parser::parse_sql(&dialect, clean_sql) {
                Ok(stmts) => match stmts.into_iter().next() {
                    Some(Statement::Query(q)) => q,
                    Some(other) => {
                        return Err(SqlError::InlineError {
                            model_name: dep.clone(),
                            reason: format!(
                                "expected a SELECT query, got {}",
                                statement_kind(&other)
                            ),
                        });
                    }
                    None => {
                        return Err(SqlError::InlineError {
                            model_name: dep.clone(),
                            reason: "SQL parsed to empty statement list".to_string(),
                        });
                    }
                },
                Err(e) => {
                    return Err(SqlError::InlineError {
                        model_name: dep.clone(),
                        reason: format!("parse error: {e}"),
                    });
                }
            };

            new_ctes.push(Cte {
                alias: TableAlias {
                    name: Ident::with_quote('"', dep),
                    columns: vec![],
                    explicit: false,
                },
                query: ephemeral_query,
                from: None,
                materialized: None,
                closing_paren_token: AttachedToken::empty(),
            });
        }
    }

    if new_ctes.is_empty() {
        return Ok(sql.to_string());
    }

    // ---- Parse the target SQL ----
    let trimmed_sql = sql.trim().trim_end_matches(';').trim();
    let mut stmts =
        Parser::parse_sql(&dialect, trimmed_sql).map_err(|e| SqlError::InlineError {
            model_name: String::new(),
            reason: format!("failed to parse target SQL: {e}"),
        })?;

    let Some(Statement::Query(ref mut query)) = stmts.first_mut() else {
        return Err(SqlError::InlineError {
            model_name: String::new(),
            reason: "target SQL is not a SELECT query".to_string(),
        });
    };

    // Inject CTEs into the query's WITH clause
    match query.with.as_mut() {
        Some(with) => {
            // Prepend new CTEs before existing ones
            new_ctes.append(&mut with.cte_tables);
            with.cte_tables = new_ctes;
        }
        None => {
            query.with = Some(With {
                with_token: AttachedToken::empty(),
                recursive: false,
                cte_tables: new_ctes,
            });
        }
    }

    Ok(stmts[0].to_string())
}

/// Return a human-readable label for a SQL statement kind
fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert { .. } => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::Drop { .. } => "DROP",
        _ => "non-SELECT statement",
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

            // Then add this ephemeral model — warn if compiled SQL is missing
            if let Some(sql) = get_compiled_sql(dep) {
                ephemeral_sql.insert(dep.clone(), sql);
                order.push(dep.clone());
            } else {
                log::warn!(
                    "Ephemeral model '{}' has no compiled SQL — it will not be inlined",
                    dep
                );
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

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
        assert!(
            result.contains(r#""stg_orders" AS"#),
            "expected quoted CTE name, got: {result}"
        );
        assert!(
            result.contains("raw_orders"),
            "expected raw_orders in CTE body, got: {result}"
        );
        assert!(
            result.contains("stg_orders"),
            "expected stg_orders in final SELECT, got: {result}"
        );
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

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
        assert!(
            result.contains(r#""stg_orders" AS"#),
            "expected quoted stg_orders CTE, got: {result}"
        );
        assert!(
            result.contains(r#""stg_customers" AS"#),
            "expected quoted stg_customers CTE, got: {result}"
        );
        // Both CTEs should be present
        assert!(
            result.contains("raw_orders"),
            "expected raw_orders, got: {result}"
        );
        assert!(
            result.contains("raw_customers"),
            "expected raw_customers, got: {result}"
        );
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

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
        // Should merge CTEs — new ephemeral CTE is prepended before existing ones
        assert!(
            result.contains(r#""stg_orders" AS"#),
            "expected quoted stg_orders CTE, got: {result}"
        );
        assert!(
            result.contains("my_cte AS"),
            "expected existing my_cte CTE, got: {result}"
        );
    }

    #[test]
    fn test_inline_no_ephemerals() {
        let sql = "SELECT * FROM orders";
        let ephemeral_deps = HashMap::new();
        let order: Vec<String> = vec![];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
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

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
        // The inner SQL should not have a trailing semicolon
        assert!(
            result.contains("raw_orders"),
            "expected raw_orders in CTE body, got: {result}"
        );
        assert!(
            !result.contains("raw_orders;"),
            "semicolon should have been stripped, got: {result}"
        );
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

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();

        // Find positions of each CTE in the result (names are double-quoted)
        let pos_c = result
            .find(r#""c" AS"#)
            .unwrap_or_else(|| panic!("expected \"c\" AS in: {result}"));
        let pos_b = result
            .find(r#""b" AS"#)
            .unwrap_or_else(|| panic!("expected \"b\" AS in: {result}"));
        let pos_a = result
            .find(r#""a" AS"#)
            .unwrap_or_else(|| panic!("expected \"a\" AS in: {result}"));

        assert!(pos_c < pos_b, "c should come before b in: {result}");
        assert!(pos_b < pos_a, "b should come before a in: {result}");
    }

    #[test]
    fn test_inline_reserved_word_cte_name() {
        // CTE names that are SQL reserved words should be properly quoted
        let sql = "SELECT * FROM \"select\"";
        let mut ephemeral_deps = HashMap::new();
        ephemeral_deps.insert(
            "select".to_string(),
            "SELECT id, name FROM raw_data".to_string(),
        );
        let order = vec!["select".to_string()];

        let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
        // The reserved word must be double-quoted in the CTE definition
        assert!(
            result.contains(r#""select" AS"#),
            "expected quoted reserved-word CTE name, got: {result}"
        );
        assert!(
            result.contains("raw_data"),
            "expected CTE body, got: {result}"
        );

        // Also test with another reserved word: "order"
        let sql2 = "SELECT * FROM \"order\"";
        let mut ephemeral_deps2 = HashMap::new();
        ephemeral_deps2.insert(
            "order".to_string(),
            "SELECT id, total FROM raw_orders".to_string(),
        );
        let order2 = vec!["order".to_string()];

        let result2 = inline_ephemeral_ctes(sql2, &ephemeral_deps2, &order2).unwrap();
        assert!(
            result2.contains(r#""order" AS"#),
            "expected quoted reserved-word CTE name, got: {result2}"
        );
        assert!(
            result2.contains("raw_orders"),
            "expected CTE body, got: {result2}"
        );
    }
}
