//! SQL validation utilities

use crate::error::{SqlError, SqlResult};
use sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins};

/// Validate that SQL contains no CTEs (WITH clauses)
///
/// Every transform should be its own model — CTEs violate the
/// directory-per-model architecture.
fn validate_no_ctes(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        let Statement::Query(query) = stmt else {
            continue;
        };
        let Some(with) = &query.with else {
            continue;
        };
        let cte_names: Vec<String> = with
            .cte_tables
            .iter()
            .map(|c| c.alias.name.value.clone())
            .collect();
        return Err(SqlError::CteNotAllowed { cte_names });
    }
    Ok(())
}

/// Validate that SQL contains no derived tables (subqueries in FROM clause)
///
/// Scalar subqueries in SELECT/WHERE/HAVING are still allowed —
/// only FROM-clause derived tables are rejected.
fn validate_no_derived_tables(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        if let Statement::Query(query) = stmt {
            check_query_for_derived_tables(query)?;
        }
    }
    Ok(())
}

/// Recursively check a query's FROM clause for derived tables
fn check_query_for_derived_tables(query: &Query) -> SqlResult<()> {
    check_set_expr_for_derived_tables(query.body.as_ref())
}

/// Recursively check a SetExpr for derived tables in FROM clauses
fn check_set_expr_for_derived_tables(expr: &SetExpr) -> SqlResult<()> {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                check_table_with_joins_for_derived(table)?;
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr_for_derived_tables(left)?;
            check_set_expr_for_derived_tables(right)?;
        }
        _ => {}
    }
    Ok(())
}

/// Check a table reference (and its joins) for derived tables
fn check_table_with_joins_for_derived(table: &TableWithJoins) -> SqlResult<()> {
    check_table_factor_for_derived(&table.relation)?;
    for join in &table.joins {
        check_table_factor_for_derived(&join.relation)?;
    }
    Ok(())
}

/// Check a single table factor for derived tables
fn check_table_factor_for_derived(factor: &TableFactor) -> SqlResult<()> {
    match factor {
        TableFactor::Derived { .. } => Err(SqlError::DerivedTableNotAllowed),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => check_table_with_joins_for_derived(table_with_joins),
        _ => Ok(()),
    }
}

/// Validate that SQL contains no CTEs and no derived tables
///
/// This is the combined check that should be called during validation and compilation.
/// Scalar subqueries in SELECT/WHERE/HAVING remain allowed.
pub fn validate_no_complex_queries(statements: &[Statement]) -> SqlResult<()> {
    validate_no_ctes(statements)?;
    validate_no_derived_tables(statements)?;
    Ok(())
}

#[cfg(test)]
#[path = "validator_test.rs"]
mod tests;
