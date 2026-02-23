//! SQL dialect abstraction
//!
//! Provides dialect-specific identifier resolution. Unquoted identifiers are
//! folded to the dialect's default case (e.g. Snowflake → UPPER, PostgreSQL →
//! lower, DuckDB → preserve as-is). Quoted identifiers are always
//! case-sensitive and kept verbatim.

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Statement};
use sqlparser::dialect::{
    Dialect, DuckDbDialect as SqlParserDuckDb, SnowflakeDialect as SqlParserSnowflake,
};
use sqlparser::parser::Parser;
use std::fmt;

use crate::error::{SqlError, SqlResult};

/// How unquoted identifiers are folded by a SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnquotedCaseBehavior {
    /// Fold unquoted identifiers to upper case (e.g. Snowflake, Oracle, DB2).
    Upper,
    /// Fold unquoted identifiers to lower case (e.g. PostgreSQL).
    Lower,
    /// Preserve the original case of unquoted identifiers (e.g. DuckDB, MySQL).
    Preserve,
}

/// Whether an identifier is case-sensitive.
///
/// Quoted identifiers (e.g. `"MyTable"`) are always case-sensitive.
/// Unquoted identifiers are case-insensitive and resolved to the dialect's
/// default case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CaseSensitivity {
    /// The identifier was quoted — exact case matters.
    CaseSensitive,
    /// The identifier was unquoted — resolved to dialect default case,
    /// comparisons should be case-insensitive.
    CaseInsensitive,
}

/// A single identifier part resolved according to dialect case rules.
///
/// The `value` field holds the resolved form: for unquoted identifiers this
/// is the dialect-default case (e.g. `USERS` on Snowflake, `users` on
/// PostgreSQL, preserved on DuckDB). For quoted identifiers it is the exact
/// string inside the quotes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedPart {
    /// The resolved identifier value.
    pub value: String,
    /// Whether this part was quoted (case-sensitive) or unquoted.
    pub sensitivity: CaseSensitivity,
}

impl fmt::Display for ResolvedPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

/// A fully-resolved table reference with case-sensitivity metadata.
///
/// Each part (database, schema, table) carries its own [`CaseSensitivity`].
/// The `name` field is the dot-joined string representation using resolved
/// values (no quotes), suitable for display and as a HashMap key when
/// combined with sensitivity-aware comparison.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedIdent {
    /// The individual resolved parts (1–3 parts typically).
    /// Private — use [`Self::parts()`] accessor.
    parts: Vec<ResolvedPart>,
    /// Dot-joined resolved name (e.g. `"ANALYTICS.STG_CUSTOMERS"` on Snowflake).
    pub name: String,
    /// `true` if **any** part was quoted (meaning the whole reference
    /// should be treated as case-sensitive for matching purposes).
    pub is_case_sensitive: bool,
}

impl fmt::Display for ResolvedIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

impl ResolvedIdent {
    /// Build a `ResolvedIdent` from already-resolved parts.
    ///
    /// # Panics
    ///
    /// Panics if `parts` is empty. Every call-site in production passes at
    /// least one part (from `resolve_object_name`).
    pub fn from_parts(parts: Vec<ResolvedPart>) -> Self {
        assert!(
            !parts.is_empty(),
            "ResolvedIdent requires at least one part"
        );
        let is_case_sensitive = parts
            .iter()
            .any(|p| p.sensitivity == CaseSensitivity::CaseSensitive);
        let name = parts
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join(".");
        Self {
            parts,
            name,
            is_case_sensitive,
        }
    }

    /// The individual resolved parts (1–3 for typical table references).
    pub fn parts(&self) -> &[ResolvedPart] {
        &self.parts
    }

    /// Return the last part (the table/object name).
    pub fn table_part(&self) -> &ResolvedPart {
        self.parts
            .last()
            .expect("ResolvedIdent must have at least one part")
    }
}

/// Trait for SQL dialect implementations
pub trait SqlDialect: Send + Sync {
    /// Get the underlying sqlparser dialect
    fn parser_dialect(&self) -> &dyn Dialect;

    /// Parse SQL into AST statements
    fn parse(&self, sql: &str) -> SqlResult<Vec<Statement>> {
        Parser::parse_sql(self.parser_dialect(), sql).map_err(|e| {
            let msg = e.to_string();
            let (line, column) = parse_location_from_error(&msg);
            SqlError::ParseError {
                message: msg,
                line,
                column,
            }
        })
    }

    /// Quote an identifier for this dialect
    fn quote_ident(&self, ident: &str) -> String;

    /// Get the dialect name
    fn name(&self) -> &'static str;

    /// How this dialect folds unquoted identifiers.
    fn unquoted_case_behavior(&self) -> UnquotedCaseBehavior;

    /// Resolve a single sqlparser [`Ident`] according to dialect case rules.
    ///
    /// - Quoted idents → kept verbatim, marked [`CaseSensitivity::CaseSensitive`].
    /// - Unquoted idents → folded per [`Self::unquoted_case_behavior`], marked
    ///   [`CaseSensitivity::CaseInsensitive`].
    fn resolve_ident(&self, ident: &Ident) -> ResolvedPart {
        if ident.quote_style.is_some() {
            ResolvedPart {
                value: ident.value.clone(),
                sensitivity: CaseSensitivity::CaseSensitive,
            }
        } else {
            let value = match self.unquoted_case_behavior() {
                UnquotedCaseBehavior::Upper => ident.value.to_uppercase(),
                UnquotedCaseBehavior::Lower => ident.value.to_lowercase(),
                UnquotedCaseBehavior::Preserve => ident.value.clone(),
            };
            ResolvedPart {
                value,
                sensitivity: CaseSensitivity::CaseInsensitive,
            }
        }
    }

    /// Resolve a full [`ObjectName`] (e.g. `schema.table`) into a
    /// [`ResolvedIdent`] with per-part case metadata.
    fn resolve_object_name(&self, name: &ObjectName) -> ResolvedIdent {
        let parts: Vec<ResolvedPart> = name
            .0
            .iter()
            .filter_map(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(self.resolve_ident(ident)),
                _ => None,
            })
            .collect();
        ResolvedIdent::from_parts(parts)
    }
}

/// Parse line and column from sqlparser error message.
///
/// sqlparser 0.60's `ParserError` is a simple string wrapper with no structured
/// location data, so we extract "Line: N, Column: M" from the error message text.
pub(crate) fn parse_location_from_error(msg: &str) -> (usize, usize) {
    let Some(line_idx) = msg.find("Line: ") else {
        return (0, 0);
    };
    let line_start = line_idx + 6;
    let Some(comma_idx) = msg[line_start..].find(',') else {
        return (0, 0);
    };
    let Ok(line) = msg[line_start..line_start + comma_idx]
        .trim()
        .parse::<usize>()
    else {
        return (0, 0);
    };
    let Some(col_idx) = msg.find("Column: ") else {
        return (0, 0);
    };
    let col_start = col_idx + 8;
    let col_end = msg[col_start..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| col_start + i)
        .unwrap_or(msg.len());
    let Ok(column) = msg[col_start..col_end].trim().parse::<usize>() else {
        return (0, 0);
    };
    (line, column)
}

/// DuckDB SQL dialect
pub struct DuckDbDialect {
    dialect: SqlParserDuckDb,
}

impl DuckDbDialect {
    /// Create a new DuckDB dialect
    pub fn new() -> Self {
        Self {
            dialect: SqlParserDuckDb {},
        }
    }
}

impl Default for DuckDbDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDialect for DuckDbDialect {
    fn parser_dialect(&self) -> &dyn Dialect {
        &self.dialect
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn unquoted_case_behavior(&self) -> UnquotedCaseBehavior {
        // DuckDB is case-insensitive but preserves the original case
        UnquotedCaseBehavior::Preserve
    }
}

/// Snowflake SQL dialect
pub struct SnowflakeDialect {
    dialect: SqlParserSnowflake,
}

impl SnowflakeDialect {
    /// Create a new Snowflake dialect
    pub fn new() -> Self {
        Self {
            dialect: SqlParserSnowflake {},
        }
    }
}

impl Default for SnowflakeDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDialect for SnowflakeDialect {
    fn parser_dialect(&self) -> &dyn Dialect {
        &self.dialect
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn name(&self) -> &'static str {
        "snowflake"
    }

    fn unquoted_case_behavior(&self) -> UnquotedCaseBehavior {
        // Snowflake folds unquoted identifiers to UPPER CASE
        UnquotedCaseBehavior::Upper
    }
}

#[cfg(test)]
#[path = "dialect_test.rs"]
mod tests;
