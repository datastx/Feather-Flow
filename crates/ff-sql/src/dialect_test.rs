use super::*;

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
