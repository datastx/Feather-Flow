use super::*;
use sqlparser::ast::Ident;

#[test]
fn test_duckdb_parse() {
    let dialect = DuckDbDialect::new();
    let stmts = dialect.parse("SELECT * FROM users").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_snowflake_parse() {
    let dialect = SnowflakeDialect::new();
    let stmts = dialect.parse("SELECT * FROM users").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_quote_ident() {
    let dialect = DuckDbDialect::new();
    assert_eq!(dialect.quote_ident("user"), "\"user\"");
    assert_eq!(dialect.quote_ident("user\"name"), "\"user\"\"name\"");
}

#[test]
fn test_parse_error_location() {
    let dialect = DuckDbDialect::new();
    // Error is on line 2 (the FROM keyword with no columns)
    let result = dialect.parse("SELECT\nFROM users");
    assert!(result.is_err());
    if let Err(crate::error::SqlError::ParseError {
        line,
        column,
        message,
    }) = result
    {
        // The error should have line 2 (FROM is on line 2)
        assert_eq!(
            line, 2,
            "Expected line 2, got line {} (message: {})",
            line, message
        );
        // Column points to where the error is detected (depends on parser implementation)
        // Just verify it's non-zero
        assert!(
            column > 0,
            "Expected non-zero column, got {} (message: {})",
            column,
            message
        );
        // Verify the error message contains location info
        assert!(
            message.contains("Line: 2"),
            "Expected 'Line: 2' in message: {}",
            message
        );
    }
}

#[test]
fn test_parse_location_extraction() {
    // Test the helper function
    let (line, col) =
        super::parse_location_from_error("Expected: something at Line: 5, Column: 10");
    assert_eq!(line, 5);
    assert_eq!(col, 10);

    // Test with no location info
    let (line, col) = super::parse_location_from_error("Some error without location");
    assert_eq!(line, 0);
    assert_eq!(col, 0);
}

// -- UnquotedCaseBehavior --

#[test]
fn test_duckdb_unquoted_case_behavior() {
    let dialect = DuckDbDialect::new();
    assert_eq!(
        dialect.unquoted_case_behavior(),
        UnquotedCaseBehavior::Preserve
    );
}

#[test]
fn test_snowflake_unquoted_case_behavior() {
    let dialect = SnowflakeDialect::new();
    assert_eq!(
        dialect.unquoted_case_behavior(),
        UnquotedCaseBehavior::Upper
    );
}

// -- resolve_ident --

#[test]
fn test_duckdb_resolve_unquoted_ident() {
    let dialect = DuckDbDialect::new();
    let ident = Ident::new("MyTable");
    let resolved = dialect.resolve_ident(&ident);
    // DuckDB preserves case for unquoted
    assert_eq!(resolved.value, "MyTable");
    assert_eq!(resolved.sensitivity, CaseSensitivity::CaseInsensitive);
}

#[test]
fn test_duckdb_resolve_quoted_ident() {
    let dialect = DuckDbDialect::new();
    let ident = Ident::with_quote('"', "MyTable");
    let resolved = dialect.resolve_ident(&ident);
    assert_eq!(resolved.value, "MyTable");
    assert_eq!(resolved.sensitivity, CaseSensitivity::CaseSensitive);
}

#[test]
fn test_snowflake_resolve_unquoted_ident() {
    let dialect = SnowflakeDialect::new();
    let ident = Ident::new("myTable");
    let resolved = dialect.resolve_ident(&ident);
    // Snowflake folds to UPPER
    assert_eq!(resolved.value, "MYTABLE");
    assert_eq!(resolved.sensitivity, CaseSensitivity::CaseInsensitive);
}

#[test]
fn test_snowflake_resolve_quoted_ident() {
    let dialect = SnowflakeDialect::new();
    let ident = Ident::with_quote('"', "myTable");
    let resolved = dialect.resolve_ident(&ident);
    // Quoted: preserved exactly
    assert_eq!(resolved.value, "myTable");
    assert_eq!(resolved.sensitivity, CaseSensitivity::CaseSensitive);
}

// -- resolve_object_name --

#[test]
fn test_duckdb_resolve_object_name_bare() {
    let dialect = DuckDbDialect::new();
    let stmts = dialect.parse("SELECT * FROM users").unwrap();
    // Parse and extract the object name via visit_relations
    let mut names = Vec::new();
    for stmt in &stmts {
        let _ = sqlparser::ast::visit_relations(stmt, |rel| {
            names.push(dialect.resolve_object_name(rel));
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].name, "users");
    assert!(!names[0].is_case_sensitive);
}

#[test]
fn test_snowflake_resolve_object_name_bare() {
    let dialect = SnowflakeDialect::new();
    let stmts = dialect.parse("SELECT * FROM users").unwrap();
    let mut names = Vec::new();
    for stmt in &stmts {
        let _ = sqlparser::ast::visit_relations(stmt, |rel| {
            names.push(dialect.resolve_object_name(rel));
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].name, "USERS");
    assert!(!names[0].is_case_sensitive);
}

#[test]
fn test_duckdb_resolve_object_name_quoted() {
    let dialect = DuckDbDialect::new();
    let stmts = dialect.parse(r#"SELECT * FROM "CaseSensitive""#).unwrap();
    let mut names = Vec::new();
    for stmt in &stmts {
        let _ = sqlparser::ast::visit_relations(stmt, |rel| {
            names.push(dialect.resolve_object_name(rel));
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].name, "CaseSensitive");
    assert!(names[0].is_case_sensitive);
}

#[test]
fn test_snowflake_resolve_schema_qualified_mixed() {
    let dialect = SnowflakeDialect::new();
    // unquoted schema + quoted table
    let stmts = dialect.parse(r#"SELECT * FROM raw."myTable""#).unwrap();
    let mut names = Vec::new();
    for stmt in &stmts {
        let _ = sqlparser::ast::visit_relations(stmt, |rel| {
            names.push(dialect.resolve_object_name(rel));
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].name, "RAW.myTable");
    // One part is quoted → whole thing is case-sensitive
    assert!(names[0].is_case_sensitive);
    assert_eq!(names[0].parts()[0].value, "RAW");
    assert_eq!(
        names[0].parts()[0].sensitivity,
        CaseSensitivity::CaseInsensitive
    );
    assert_eq!(names[0].parts()[1].value, "myTable");
    assert_eq!(
        names[0].parts()[1].sensitivity,
        CaseSensitivity::CaseSensitive
    );
}
