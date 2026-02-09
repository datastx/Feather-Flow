//! SQL identifier quoting utilities
//!
//! Provides safe quoting for SQL identifiers and qualified names to prevent
//! SQL injection when constructing dynamic SQL statements.

/// Quote a SQL identifier to prevent injection.
///
/// Wraps the identifier in double quotes and escapes any embedded double quotes
/// by doubling them, following the SQL standard.
///
/// # Examples
/// ```
/// use ff_core::sql_utils::quote_ident;
/// assert_eq!(quote_ident("users"), r#""users""#);
/// assert_eq!(quote_ident(r#"my"table"#), r#""my""table""#);
/// ```
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote a potentially schema-qualified name (e.g. `schema.table`).
///
/// Splits on `.` and individually quotes each component.
///
/// # Examples
/// ```
/// use ff_core::sql_utils::quote_qualified;
/// assert_eq!(quote_qualified("users"), r#""users""#);
/// assert_eq!(quote_qualified("staging.orders"), r#""staging"."orders""#);
/// ```
pub fn quote_qualified(name: &str) -> String {
    name.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// Split a potentially schema-qualified table name into (schema, table).
///
/// Uses the last `.` as the separator. If no `.` is present, returns
/// `("main", name)` as the default schema.
///
/// # Examples
/// ```
/// use ff_core::sql_utils::split_qualified_name;
/// assert_eq!(split_qualified_name("users"), ("main", "users"));
/// assert_eq!(split_qualified_name("staging.orders"), ("staging", "orders"));
/// assert_eq!(split_qualified_name("catalog.schema.table"), ("catalog.schema", "table"));
/// ```
pub fn split_qualified_name(name: &str) -> (&str, &str) {
    if let Some(pos) = name.rfind('.') {
        (&name[..pos], &name[pos + 1..])
    } else {
        ("main", name)
    }
}

/// Escape a SQL string literal value by doubling single quotes.
///
/// This is for use inside single-quoted SQL string literals, not identifiers.
pub fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), r#""users""#);
    }

    #[test]
    fn test_quote_ident_with_embedded_quotes() {
        assert_eq!(quote_ident(r#"my"table"#), r#""my""table""#);
    }

    #[test]
    fn test_quote_ident_empty() {
        assert_eq!(quote_ident(""), r#""""#);
    }

    #[test]
    fn test_quote_ident_with_dots() {
        // Dots are NOT special inside quote_ident — they're just characters
        assert_eq!(quote_ident("schema.table"), r#""schema.table""#);
    }

    #[test]
    fn test_quote_qualified_simple() {
        assert_eq!(quote_qualified("users"), r#""users""#);
    }

    #[test]
    fn test_quote_qualified_two_parts() {
        assert_eq!(quote_qualified("staging.orders"), r#""staging"."orders""#);
    }

    #[test]
    fn test_quote_qualified_three_parts() {
        assert_eq!(
            quote_qualified("catalog.schema.table"),
            r#""catalog"."schema"."table""#
        );
    }

    #[test]
    fn test_quote_qualified_with_embedded_quotes() {
        assert_eq!(
            quote_qualified(r#"my"schema.my"table"#),
            r#""my""schema"."my""table""#
        );
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("O'Brien's"), "O''Brien''s");
    }

    #[test]
    fn test_split_qualified_name_no_dot() {
        assert_eq!(split_qualified_name("users"), ("main", "users"));
    }

    #[test]
    fn test_split_qualified_name_single_dot() {
        assert_eq!(
            split_qualified_name("staging.orders"),
            ("staging", "orders")
        );
    }

    #[test]
    fn test_split_qualified_name_multiple_dots() {
        assert_eq!(
            split_qualified_name("catalog.schema.table"),
            ("catalog.schema", "table")
        );
    }
}
