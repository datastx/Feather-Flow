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
