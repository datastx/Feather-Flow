//! SQL validation utilities

use crate::error::{SqlError, SqlResult};
use sqlparser::ast::{Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins};

/// Validate that SQL contains only supported statements
#[cfg(test)]
fn validate_statements(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        validate_statement(stmt)?;
    }
    Ok(())
}

/// Validate a single SQL statement
#[cfg(test)]
fn validate_statement(statement: &Statement) -> SqlResult<()> {
    match statement {
        // Supported statements for models
        Statement::Query(_) => Ok(()),

        // Unsupported statements
        Statement::Insert(_) => Err(SqlError::UnsupportedStatement(
            "INSERT statements are not allowed in models".to_string(),
        )),
        Statement::Update { .. } => Err(SqlError::UnsupportedStatement(
            "UPDATE statements are not allowed in models".to_string(),
        )),
        Statement::Delete(_) => Err(SqlError::UnsupportedStatement(
            "DELETE statements are not allowed in models".to_string(),
        )),
        Statement::Drop { .. } => Err(SqlError::UnsupportedStatement(
            "DROP statements are not allowed in models".to_string(),
        )),
        Statement::Truncate { .. } => Err(SqlError::UnsupportedStatement(
            "TRUNCATE statements are not allowed in models".to_string(),
        )),

        // Other statements - allow for now (CREATE, etc. might be used in edge cases)
        _ => Ok(()),
    }
}

/// Check if SQL is a SELECT statement
#[cfg(test)]
fn is_select_statement(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_))
}

/// Validate that SQL contains no CTEs (WITH clauses)
///
/// Every transform should be its own model — CTEs violate the
/// directory-per-model architecture.
fn validate_no_ctes(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        if let Statement::Query(query) = stmt {
            if let Some(with) = &query.with {
                let cte_names: Vec<String> = with
                    .cte_tables
                    .iter()
                    .map(|c| c.alias.name.value.clone())
                    .collect();
                return Err(SqlError::CteNotAllowed { cte_names });
            }
        }
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
    if let SetExpr::Select(select) = query.body.as_ref() {
        for table in &select.from {
            check_table_with_joins_for_derived(table)?;
        }
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

/// Validate that SQL contains no SELECT * or table.* wildcards
///
/// Every model must explicitly list its columns — wildcards hide schema
/// changes and break the contract between models.
fn validate_no_select_star(statements: &[Statement]) -> SqlResult<()> {
    for stmt in statements {
        if let Statement::Query(query) = stmt {
            check_query_for_select_star(query)?;
        }
    }
    Ok(())
}

/// Recursively check a query for SELECT * and table.* wildcards
fn check_query_for_select_star(query: &Query) -> SqlResult<()> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            for item in &select.projection {
                if matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                ) {
                    return Err(SqlError::SelectStarNotAllowed);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr_for_select_star(left)?;
            check_set_expr_for_select_star(right)?;
        }
        _ => {}
    }
    Ok(())
}

/// Check a SetExpr node for SELECT * wildcards
fn check_set_expr_for_select_star(expr: &SetExpr) -> SqlResult<()> {
    match expr {
        SetExpr::Select(select) => {
            for item in &select.projection {
                if matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                ) {
                    return Err(SqlError::SelectStarNotAllowed);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr_for_select_star(left)?;
            check_set_expr_for_select_star(right)?;
        }
        _ => {}
    }
    Ok(())
}

/// Validate that SQL contains no CTEs, no derived tables, and no SELECT *
///
/// This is the combined check that should be called during validation and compilation.
/// Scalar subqueries in SELECT/WHERE/HAVING remain allowed.
pub fn validate_no_complex_queries(statements: &[Statement]) -> SqlResult<()> {
    validate_no_ctes(statements)?;
    validate_no_derived_tables(statements)?;
    validate_no_select_star(statements)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn validate_sql(sql: &str) -> SqlResult<()> {
        let parser = SqlParser::duckdb();
        let stmts = parser.parse(sql)?;
        validate_statements(&stmts)
    }

    fn validate_no_complex(sql: &str) -> SqlResult<()> {
        let parser = SqlParser::duckdb();
        let stmts = parser.parse(sql)?;
        validate_no_complex_queries(&stmts)
    }

    #[test]
    fn test_validate_select() {
        assert!(validate_sql("SELECT id FROM users").is_ok());
    }

    #[test]
    fn test_validate_insert_fails() {
        assert!(validate_sql("INSERT INTO users VALUES (1)").is_err());
    }

    #[test]
    fn test_validate_update_fails() {
        assert!(validate_sql("UPDATE users SET name = 'test'").is_err());
    }

    #[test]
    fn test_validate_delete_fails() {
        assert!(validate_sql("DELETE FROM users").is_err());
    }

    #[test]
    fn test_is_select() {
        let parser = SqlParser::duckdb();
        let stmt = parser.parse_single("SELECT 1").unwrap();
        assert!(is_select_statement(&stmt));
    }

    #[test]
    fn test_cte_rejected() {
        let result =
            validate_no_complex("WITH staged AS (SELECT id FROM raw_users) SELECT id FROM staged");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SqlError::CteNotAllowed { ref cte_names } if cte_names == &["staged"]),
            "Expected CteNotAllowed, got: {:?}",
            err
        );
    }

    #[test]
    fn test_multiple_ctes_rejected() {
        let result = validate_no_complex(
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT a.id FROM a JOIN b ON true",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SqlError::CteNotAllowed { ref cte_names } if cte_names.contains(&"a".to_string()) && cte_names.contains(&"b".to_string())),
        );
    }

    #[test]
    fn test_derived_table_rejected() {
        let result = validate_no_complex("SELECT id FROM (SELECT id, name FROM users) AS sub");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SqlError::DerivedTableNotAllowed
        ));
    }

    #[test]
    fn test_derived_table_in_join_rejected() {
        let result = validate_no_complex(
            "SELECT o.id FROM orders o JOIN (SELECT id FROM customers) AS c ON o.customer_id = c.id",
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SqlError::DerivedTableNotAllowed
        ));
    }

    #[test]
    fn test_scalar_subquery_allowed() {
        let result = validate_no_complex(
            "SELECT id, (SELECT MAX(amount) FROM orders) AS max_amount FROM users",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_where_subquery_allowed() {
        let result =
            validate_no_complex("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_select_passes() {
        let result = validate_no_complex("SELECT id, name FROM users WHERE active = true");
        assert!(result.is_ok());
    }

    #[test]
    fn test_join_without_derived_passes() {
        let result = validate_no_complex(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_star_rejected() {
        let result = validate_no_complex("SELECT * FROM users");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SqlError::SelectStarNotAllowed
        ));
    }

    #[test]
    fn test_qualified_wildcard_rejected() {
        let result = validate_no_complex("SELECT t.* FROM users t");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SqlError::SelectStarNotAllowed
        ));
    }

    #[test]
    fn test_select_star_in_union_rejected() {
        let result = validate_no_complex("SELECT * FROM users UNION ALL SELECT * FROM customers");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SqlError::SelectStarNotAllowed
        ));
    }

    #[test]
    fn test_explicit_columns_allowed() {
        let result = validate_no_complex("SELECT id, name FROM users");
        assert!(result.is_ok());
    }
}
