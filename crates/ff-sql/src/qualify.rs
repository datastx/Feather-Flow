//! Table reference qualification for compiled SQL
//!
//! Rewrites bare table references in SQL to qualified names using AST
//! manipulation via `visit_relations_mut`. Produces 2-part (`schema.table`)
//! names for the default database and 3-part (`database.schema.table`) names
//! only for cross-database (attached) references. Only single-part (bare)
//! names are qualified; already-qualified references are left unchanged.

use sqlparser::ast::{visit_relations_mut, Ident, ObjectName, ObjectNamePart};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::error::{SqlError, SqlResult};

/// A qualified table reference.
///
/// When `database` is `None`, the reference targets the current/default
/// database and is emitted as a 2-part name (`schema.table`). When
/// `database` is `Some`, it targets an attached database and is emitted
/// as a 3-part name (`database.schema.table`).
#[derive(Debug, Clone)]
pub struct QualifiedRef {
    /// Database/catalog name — `None` for the default database
    pub database: Option<String>,
    /// Schema name (e.g., "analytics")
    pub schema: String,
    /// Table name (e.g., "stg_customers")
    pub table: String,
}

/// Rewrite bare table references in SQL to qualified names.
///
/// Takes rendered SQL and a map of `lowercase_bare_name → QualifiedRef`.
/// Produces 2-part names (`schema.table`) for the default database and
/// 3-part names (`database.schema.table`) for cross-database references.
///
/// Only single-part names (`name.0.len() == 1`) are qualified. If a reference
/// is already multi-part (e.g., `schema.table`), it is left unchanged.
pub fn qualify_table_references(
    sql: &str,
    qualification_map: &HashMap<String, QualifiedRef>,
) -> SqlResult<String> {
    if qualification_map.is_empty() {
        return Ok(sql.to_string());
    }

    let dialect = DuckDbDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
        let msg = e.to_string();
        SqlError::ParseError {
            message: msg,
            line: 0,
            column: 0,
        }
    })?;

    for stmt in &mut statements {
        let _ = visit_relations_mut(stmt, |name: &mut ObjectName| {
            // Only qualify single-part names (bare references)
            if name.0.len() == 1 {
                if let Some(ObjectNamePart::Identifier(ident)) = name.0.first() {
                    let bare = ident.value.to_lowercase();
                    if let Some(qualified) = qualification_map.get(&bare) {
                        let mut parts = Vec::with_capacity(3);
                        if let Some(ref db) = qualified.database {
                            parts.push(ObjectNamePart::Identifier(Ident::new(db)));
                        }
                        parts.push(ObjectNamePart::Identifier(Ident::new(&qualified.schema)));
                        parts.push(ObjectNamePart::Identifier(Ident::new(&qualified.table)));
                        name.0 = parts;
                    }
                }
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(";\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a map with 2-part (default db) entries.
    fn make_map_2part(entries: &[(&str, &str, &str)]) -> HashMap<String, QualifiedRef> {
        entries
            .iter()
            .map(|(bare, schema, table)| {
                (
                    bare.to_string(),
                    QualifiedRef {
                        database: None,
                        schema: schema.to_string(),
                        table: table.to_string(),
                    },
                )
            })
            .collect()
    }

    /// Helper to build a map with 3-part (cross-database) entries.
    fn make_map_3part(entries: &[(&str, &str, &str, &str)]) -> HashMap<String, QualifiedRef> {
        entries
            .iter()
            .map(|(bare, db, schema, table)| {
                (
                    bare.to_string(),
                    QualifiedRef {
                        database: Some(db.to_string()),
                        schema: schema.to_string(),
                        table: table.to_string(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn test_qualify_bare_name_2part() {
        let sql = "SELECT id, name FROM stg_customers";
        let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("analytics.stg_customers"),
            "Expected 2-part name, got: {}",
            result
        );
        assert!(
            !result.contains("main.analytics"),
            "Should not have 3-part name for default db, got: {}",
            result
        );
    }

    #[test]
    fn test_qualify_join_2part() {
        let sql =
            "SELECT c.id FROM stg_customers c INNER JOIN stg_orders o ON c.id = o.customer_id";
        let map = make_map_2part(&[
            ("stg_customers", "analytics", "stg_customers"),
            ("stg_orders", "analytics", "stg_orders"),
        ]);
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("analytics.stg_customers"),
            "Expected qualified stg_customers, got: {}",
            result
        );
        assert!(
            result.contains("analytics.stg_orders"),
            "Expected qualified stg_orders, got: {}",
            result
        );
    }

    #[test]
    fn test_already_qualified_unchanged() {
        let sql = "SELECT id FROM staging.stg_customers";
        let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
        let result = qualify_table_references(sql, &map).unwrap();
        // Should NOT become analytics.stg_customers because it's already 2-part
        assert!(
            !result.contains("analytics.stg_customers"),
            "Should not re-qualify already-qualified name, got: {}",
            result
        );
    }

    #[test]
    fn test_unknown_table_unchanged() {
        let sql = "SELECT id FROM unknown_table";
        let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("unknown_table"),
            "Unknown table should be unchanged, got: {}",
            result
        );
    }

    #[test]
    fn test_case_insensitive_matching() {
        let sql = "SELECT id FROM STG_CUSTOMERS";
        let map = make_map_2part(&[("stg_customers", "analytics", "stg_customers")]);
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("analytics.stg_customers"),
            "Case-insensitive match should work, got: {}",
            result
        );
    }

    #[test]
    fn test_empty_map_returns_original() {
        let sql = "SELECT id FROM stg_customers";
        let map = HashMap::new();
        let result = qualify_table_references(sql, &map).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn test_qualify_with_different_schemas() {
        let sql =
            "SELECT c.id FROM stg_customers c INNER JOIN raw_orders r ON c.id = r.customer_id";
        let map = make_map_2part(&[
            ("stg_customers", "staging", "stg_customers"),
            ("raw_orders", "analytics", "raw_orders"),
        ]);
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("staging.stg_customers"),
            "Expected staging schema, got: {}",
            result
        );
        assert!(
            result.contains("analytics.raw_orders"),
            "Expected analytics schema, got: {}",
            result
        );
    }

    #[test]
    fn test_qualify_with_cross_database() {
        let sql = "SELECT id FROM external_table";
        let map = make_map_3part(&[("external_table", "ext_db", "raw", "external_table")]);
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("ext_db.raw.external_table"),
            "Expected 3-part name for cross-database, got: {}",
            result
        );
    }

    #[test]
    fn test_mixed_default_and_cross_database() {
        let sql = "SELECT a.id FROM local_table a JOIN remote_table b ON a.id = b.id";
        let mut map = make_map_2part(&[("local_table", "analytics", "local_table")]);
        map.extend(make_map_3part(&[(
            "remote_table",
            "ext_db",
            "raw",
            "remote_table",
        )]));
        let result = qualify_table_references(sql, &map).unwrap();
        assert!(
            result.contains("analytics.local_table"),
            "Expected 2-part for default db, got: {}",
            result
        );
        assert!(
            result.contains("ext_db.raw.remote_table"),
            "Expected 3-part for cross-db, got: {}",
            result
        );
    }
}
