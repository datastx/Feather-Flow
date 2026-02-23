//! ff-sql - SQL parsing layer for Featherflow
//!
//! This crate provides SQL parsing using sqlparser-rs with dialect support,
//! table dependency extraction, column-level lineage via AST visitor,
//! and ephemeral model inlining.

pub mod dialect;
pub mod error;
pub mod extractor;
pub mod inline;
pub mod lineage;
pub mod parser;
pub mod qualify;
pub mod suggestions;
pub mod validator;

pub use dialect::{
    CaseSensitivity, DuckDbDialect, ResolvedIdent, ResolvedPart, SnowflakeDialect, SqlDialect,
    UnquotedCaseBehavior,
};
pub use error::SqlError;
pub use extractor::{
    categorize_dependencies, categorize_dependencies_resolved, extract_dependencies,
    extract_dependencies_resolved,
};
pub use inline::{collect_ephemeral_dependencies, inline_ephemeral_ctes};
pub use lineage::{
    extract_column_lineage, ColumnLineage, ColumnRef, DescriptionStatus, ExprType, LineageEdge,
    LineageKind, ModelLineage, ProjectLineage,
};
pub use parser::SqlParser;
pub use qualify::{qualify_statements, qualify_table_references};
pub use sqlparser::ast::Statement;
pub use suggestions::{suggest_tests, ColumnSuggestions, ModelSuggestions, TestSuggestion};
pub use validator::validate_no_complex_queries;

/// Convert a sqlparser `ObjectName` to a dot-separated string.
///
/// Shared utility used by both `extractor` and `lineage` modules.
pub(crate) fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(".")
}
