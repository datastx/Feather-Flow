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
        let (line, column) = crate::dialect::parse_location_from_error(&msg);
        SqlError::ParseError {
            message: msg,
            line,
            column,
        }
    })?;

    for stmt in &mut statements {
        let _ = visit_relations_mut(stmt, |name: &mut ObjectName| {
            qualify_single_name(name, qualification_map);
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(";\n"))
}

/// Qualify a single-part (bare) table name using the qualification map.
///
/// Only rewrites names with exactly one part. Already-qualified names are
/// left unchanged.
fn qualify_single_name(name: &mut ObjectName, map: &HashMap<String, QualifiedRef>) {
    if name.0.len() != 1 {
        return;
    }
    let Some(ObjectNamePart::Identifier(ident)) = name.0.first() else {
        return;
    };
    let bare = ident.value.to_lowercase();
    let Some(qualified) = map.get(&bare) else {
        return;
    };
    let mut parts = Vec::with_capacity(3);
    if let Some(ref db) = qualified.database {
        parts.push(ObjectNamePart::Identifier(Ident::new(db)));
    }
    parts.push(ObjectNamePart::Identifier(Ident::new(&qualified.schema)));
    parts.push(ObjectNamePart::Identifier(Ident::new(&qualified.table)));
    name.0 = parts;
}

#[cfg(test)]
#[path = "qualify_test.rs"]
mod tests;
