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

    let mut new_ctes: Vec<Cte> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    for dep in model_deps {
        let Some(ephemeral_sql) = ephemeral_deps.get(dep) else {
            continue;
        };
        if !seen.insert(dep.as_str()) {
            continue;
        }

        let clean_sql = ephemeral_sql.trim().trim_end_matches(';').trim();
        let stmts = Parser::parse_sql(&dialect, clean_sql).map_err(|e| SqlError::InlineError {
            model_name: dep.clone(),
            reason: format!("parse error: {e}"),
        })?;
        let Some(first_stmt) = stmts.into_iter().next() else {
            return Err(SqlError::InlineError {
                model_name: dep.clone(),
                reason: "SQL parsed to empty statement list".to_string(),
            });
        };
        let Statement::Query(ephemeral_query) = first_stmt else {
            return Err(SqlError::InlineError {
                model_name: dep.clone(),
                reason: format!(
                    "expected a SELECT query, got {}",
                    statement_kind(&first_stmt)
                ),
            });
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

    if new_ctes.is_empty() {
        return Ok(sql.to_string());
    }

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

    match query.with.as_mut() {
        Some(with) => {
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
        if !visited.insert(dep.clone()) {
            continue;
        }
        if !is_ephemeral(dep) {
            continue;
        }

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

        let Some(sql) = get_compiled_sql(dep) else {
            log::warn!(
                "Ephemeral model '{}' has no compiled SQL — it will not be inlined",
                dep
            );
            continue;
        };
        let dep = dep.to_string();
        ephemeral_sql.insert(dep.clone(), sql);
        order.push(dep);
    }
}

#[cfg(test)]
#[path = "inline_test.rs"]
mod tests;
