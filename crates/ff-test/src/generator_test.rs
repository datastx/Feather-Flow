use super::*;

#[test]
fn test_generate_unique_test() {
    let sql = generate_unique_test("users", "id");
    assert!(sql.contains(r#"GROUP BY "id""#));
    assert!(sql.contains("HAVING COUNT(*) > 1"));
}

#[test]
fn test_generate_not_null_test() {
    let sql = generate_not_null_test("users", "email");
    assert!(sql.contains("IS NULL"));
    assert!(sql.contains(r#""email""#));
}

#[test]
fn test_generate_positive_test() {
    let sql = generate_positive_test("orders", "amount");
    assert!(sql.contains(r#""amount" <= 0"#));
    assert!(sql.contains(r#"FROM "orders""#));
}

#[test]
fn test_generate_non_negative_test() {
    let sql = generate_non_negative_test("orders", "quantity");
    assert!(sql.contains(r#""quantity" < 0"#));
    assert!(sql.contains(r#"FROM "orders""#));
}

#[test]
fn test_generate_accepted_values_test_quoted() {
    let values = vec!["pending".to_string(), "completed".to_string()];
    let sql = generate_accepted_values_test("orders", "status", &values, true);
    assert!(sql.contains("NOT IN ('pending', 'completed')"));
    assert!(sql.contains(r#""status" IS NULL"#));
}

#[test]
fn test_generate_accepted_values_test_unquoted() {
    let values = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    let sql = generate_accepted_values_test("orders", "priority", &values, false);
    assert!(sql.contains("NOT IN (1, 2, 3)"));
}

#[test]
fn test_generate_min_value_test() {
    let sql = generate_min_value_test("products", "price", 0.0).unwrap();
    assert!(sql.contains(r#""price" < 0"#));
    assert!(sql.contains(r#"FROM "products""#));
}

#[test]
fn test_generate_max_value_test() {
    let sql = generate_max_value_test("products", "discount", 100.0).unwrap();
    assert!(sql.contains(r#""discount" > 100"#));
    assert!(sql.contains(r#"FROM "products""#));
}

#[test]
fn test_generate_regex_test() {
    let sql = generate_regex_test(
        "users",
        "email",
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    );
    assert!(sql.contains(r#"regexp_matches("email","#));
    assert!(sql.contains("NOT"));
}

#[test]
fn test_generate_regex_test_escapes_quotes() {
    // Verify that single quotes in patterns are escaped to prevent SQL injection
    let sql = generate_regex_test("users", "name", "O'Brien");
    assert!(sql.contains("O''Brien"), "Single quotes should be escaped");
    assert!(
        !sql.contains("O'Brien' OR"),
        "SQL injection should be prevented"
    );
}

#[test]
fn test_generated_test_name() {
    let schema_test = SchemaTest {
        test_type: TestType::Unique,
        column: "order_id".to_string(),
        model: "stg_orders".to_string(),
        config: Default::default(),
    };

    let generated = GeneratedTest::from_schema_test(&schema_test);
    assert_eq!(generated.name, "unique_stg_orders__order_id");
}

#[test]
fn test_generated_test_accepted_values() {
    let schema_test = SchemaTest {
        test_type: TestType::AcceptedValues {
            values: vec!["a".to_string(), "b".to_string()],
            quote: true,
        },
        column: "status".to_string(),
        model: "orders".to_string(),
        config: Default::default(),
    };

    let generated = GeneratedTest::from_schema_test(&schema_test);
    assert_eq!(generated.name, "accepted_values_orders__status");
    assert!(generated.sql.contains("NOT IN ('a', 'b')"));
}

#[test]
fn test_generate_relationship_test() {
    let sql = generate_relationship_test("orders", "customer_id", "customers", "id");
    assert!(sql.contains(r#"FROM "orders" AS src"#));
    assert!(sql.contains(r#"FROM "customers" AS ref_tbl"#));
    assert!(sql.contains(r#"ref_tbl."id" = src."customer_id""#));
    assert!(sql.contains(r#"src."customer_id" IS NOT NULL"#));
    assert!(sql.contains("NOT EXISTS"));
}

#[test]
fn test_generate_relationship_test_same_column() {
    let sql = generate_relationship_test("orders", "customer_id", "customers", "customer_id");
    assert!(sql.contains(r#"ref_tbl."customer_id" = src."customer_id""#));
}

#[test]
fn test_generated_test_relationship() {
    let schema_test = SchemaTest {
        test_type: TestType::Relationship {
            to: "customers".to_string(),
            field: Some("id".to_string()),
        },
        column: "customer_id".to_string(),
        model: "orders".to_string(),
        config: Default::default(),
    };

    let generated = GeneratedTest::from_schema_test(&schema_test);
    assert_eq!(generated.name, "relationship_orders__customer_id");
    assert!(generated.sql.contains(r#"FROM "orders" AS src"#));
    assert!(generated.sql.contains(r#"FROM "customers" AS ref_tbl"#));
    assert!(generated
        .sql
        .contains(r#"ref_tbl."id" = src."customer_id""#));
}

#[test]
fn test_generated_test_relationship_default_field() {
    // When field is not specified, it should default to the same column name
    let schema_test = SchemaTest {
        test_type: TestType::Relationship {
            to: "users".to_string(),
            field: None,
        },
        column: "user_id".to_string(),
        model: "posts".to_string(),
        config: Default::default(),
    };

    let generated = GeneratedTest::from_schema_test(&schema_test);
    assert!(generated
        .sql
        .contains(r#"ref_tbl."user_id" = src."user_id""#));
}

#[test]
fn test_generated_test_relationship_qualified() {
    let schema_test = SchemaTest {
        test_type: TestType::Relationship {
            to: "dim_customers".to_string(),
            field: Some("customer_id".to_string()),
        },
        column: "customer_id".to_string(),
        model: "fct_orders".to_string(),
        config: Default::default(),
    };

    let generated = GeneratedTest::from_schema_test_qualified(&schema_test, "analytics.fct_orders");
    assert!(generated
        .sql
        .contains(r#"FROM "analytics"."fct_orders" AS src"#));
    assert!(generated.sql.contains(r#"FROM "dim_customers" AS ref_tbl"#));
}

#[test]
fn test_generated_test_relationship_qualified_with_refs() {
    let schema_test = SchemaTest {
        test_type: TestType::Relationship {
            to: "dim_customers".to_string(),
            field: Some("customer_id".to_string()),
        },
        column: "customer_id".to_string(),
        model: "fct_orders".to_string(),
        config: Default::default(),
    };

    // Resolver that qualifies referenced tables
    let resolver = |name: &str| format!("analytics.{}", name);

    let generated = GeneratedTest::from_schema_test_qualified_with_refs(
        &schema_test,
        "analytics.fct_orders",
        resolver,
    );
    assert!(generated
        .sql
        .contains(r#"FROM "analytics"."fct_orders" AS src"#));
    assert!(generated
        .sql
        .contains(r#"FROM "analytics"."dim_customers" AS ref_tbl"#));
}

#[test]
fn test_where_clause_keyword_blocklist_true_positive() {
    let config = ff_core::model::TestConfig {
        where_clause: Some("1=1 UNION SELECT * FROM secrets".to_string()),
        ..Default::default()
    };
    let base = "SELECT * FROM users WHERE id IS NULL";
    let result = apply_test_config(base, &config);
    assert!(result.contains("ERROR"), "UNION keyword should be blocked");
}

#[test]
fn test_where_clause_keyword_blocklist_false_positive_avoided() {
    // Column names that contain dangerous keywords as substrings
    // should NOT trigger the blocklist.
    for clause in &[
        "created_at > '2024-01-01'", // contains CREATE
        "updated_at IS NOT NULL",    // contains UPDATE
        "deleted_flag = false",      // contains DELETE
    ] {
        let config = ff_core::model::TestConfig {
            where_clause: Some(clause.to_string()),
            ..Default::default()
        };
        let base = "SELECT * FROM users WHERE id IS NULL";
        let result = apply_test_config(base, &config);
        assert!(
            !result.contains("ERROR"),
            "Column name '{}' should NOT trigger keyword blocklist",
            clause
        );
    }
}

#[test]
fn test_where_clause_with_standalone_keyword_blocked() {
    // Standalone keywords (not part of column names) should still be blocked
    for kw in &["DROP", "ALTER", "INSERT", "DELETE", "CREATE"] {
        let config = ff_core::model::TestConfig {
            where_clause: Some(format!("{} TABLE foo", kw)),
            ..Default::default()
        };
        let base = "SELECT * FROM users WHERE id IS NULL";
        let result = apply_test_config(base, &config);
        assert!(
            result.contains("ERROR"),
            "Standalone keyword '{}' should be blocked",
            kw
        );
    }
}
