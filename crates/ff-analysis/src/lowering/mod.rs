//! AST-to-IR lowering — converts sqlparser AST into RelOp IR

pub(crate) mod expr;
pub(crate) mod join;
pub(crate) mod query;
pub(crate) mod select;

use crate::error::{AnalysisError, AnalysisResult};
use crate::ir::relop::RelOp;
use crate::ir::schema::RelSchema;
use sqlparser::ast::Statement;
use std::collections::HashMap;

/// Schema catalog: maps table/model names to known schemas
pub type SchemaCatalog = HashMap<String, RelSchema>;

/// Lower a sqlparser Statement into a RelOp IR tree
///
/// Only `Statement::Query` is supported. Other statement types return an error.
pub fn lower_statement(stmt: &Statement, catalog: &SchemaCatalog) -> AnalysisResult<RelOp> {
    match stmt {
        Statement::Query(query) => query::lower_query(query, catalog),
        other => Err(AnalysisError::LoweringFailed {
            model: String::new(),
            message: format!(
                "Only SELECT queries are supported, got: {}",
                statement_kind(other)
            ),
        }),
    }
}

/// Return a human-readable name for a statement variant
fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        _ => "unsupported statement",
    }
}

#[cfg(test)]
#[path = "lowering_test.rs"]
mod tests;
