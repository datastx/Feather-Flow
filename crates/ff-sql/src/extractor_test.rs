use super::*;
use crate::dialect::{CaseSensitivity, DuckDbDialect, ResolvedIdent, SnowflakeDialect};
use crate::parser::SqlParser;

fn parse_and_extract(sql: &str) -> HashSet<String> {
    let parser = SqlParser::duckdb();
    let stmts = parser.parse(sql).unwrap();
    extract_dependencies(&stmts)
}

#[test]
fn test_extract_from_simple_select() {
    let deps = parse_and_extract("SELECT * FROM users");
    assert!(deps.contains("users"));
    assert_eq!(deps.len(), 1);
}

#[test]
fn test_extract_from_join() {
    let deps = parse_and_extract("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id");
    assert!(deps.contains("orders"));
    assert!(deps.contains("customers"));
    assert_eq!(deps.len(), 2);
}

#[test]
fn test_extract_from_subquery() {
    let deps = parse_and_extract(
        "SELECT * FROM (SELECT * FROM raw_data) AS sub JOIN other_table ON sub.id = other_table.id",
    );
    assert!(deps.contains("raw_data"));
    assert!(deps.contains("other_table"));
}

#[test]
fn test_extract_schema_qualified() {
    let deps = parse_and_extract("SELECT * FROM raw.orders");
    assert!(deps.contains("raw.orders"));
}

#[test]
fn test_extract_from_cte() {
    let deps = parse_and_extract(
        r#"
        WITH staged AS (
            SELECT * FROM raw_orders
        )
        SELECT * FROM staged
        JOIN customers ON staged.customer_id = customers.id
        "#,
    );
    assert!(deps.contains("raw_orders"));
    assert!(deps.contains("customers"));
    // CTEs should NOT appear in dependencies
    assert!(
        !deps.contains("staged"),
        "CTE 'staged' should not be in dependencies"
    );
    assert_eq!(deps.len(), 2);
}

#[test]
fn test_extract_from_multiple_ctes() {
    let deps = parse_and_extract(
        r#"
        WITH
            orders_cte AS (SELECT * FROM raw_orders),
            customers_cte AS (SELECT * FROM raw_customers)
        SELECT * FROM orders_cte
        JOIN customers_cte ON orders_cte.customer_id = customers_cte.id
        JOIN products ON orders_cte.product_id = products.id
        "#,
    );
    assert!(deps.contains("raw_orders"));
    assert!(deps.contains("raw_customers"));
    assert!(deps.contains("products"));
    // CTEs should NOT appear in dependencies
    assert!(
        !deps.contains("orders_cte"),
        "CTE 'orders_cte' should not be in dependencies"
    );
    assert!(
        !deps.contains("customers_cte"),
        "CTE 'customers_cte' should not be in dependencies"
    );
    assert_eq!(deps.len(), 3);
}

#[test]
fn test_recursive_cte_not_in_deps() {
    let deps = parse_and_extract(
        r#"
        WITH RECURSIVE emp_tree AS (
            SELECT * FROM employees WHERE manager_id IS NULL
            UNION ALL
            SELECT e.* FROM employees e
            JOIN emp_tree t ON e.manager_id = t.id
        )
        SELECT * FROM emp_tree
        "#,
    );
    assert!(deps.contains("employees"));
    // Recursive CTE should NOT appear in dependencies
    assert!(
        !deps.contains("emp_tree"),
        "Recursive CTE 'emp_tree' should not be in dependencies"
    );
    assert_eq!(deps.len(), 1);
}

#[test]
fn test_extract_from_union() {
    let deps = parse_and_extract("SELECT * FROM table1 UNION ALL SELECT * FROM table2");
    assert!(deps.contains("table1"));
    assert!(deps.contains("table2"));
}

#[test]
fn test_categorize_dependencies() {
    let deps = HashSet::from([
        "stg_orders".to_string(),
        "raw.orders".to_string(),
        "unknown_table".to_string(),
    ]);

    let known_models = HashSet::from(["stg_orders"]);
    let external_tables = HashSet::from(["raw.orders".to_string()]);

    let (model_deps, external_deps) =
        categorize_dependencies(deps, &known_models, &external_tables);

    assert_eq!(model_deps, vec!["stg_orders"]);
    assert!(external_deps.contains(&"raw.orders".to_string()));
    assert!(external_deps.contains(&"unknown_table".to_string()));
}

#[test]
fn test_normalize_table_name() {
    assert_eq!(normalize_table_name("users"), "users");
    assert_eq!(normalize_table_name("schema.users"), "users");
    assert_eq!(normalize_table_name("db.schema.users"), "users");
}

#[test]
fn test_case_insensitive_model_matching() {
    // SQL references "STG_ORDERS" but model is "stg_orders"
    let deps = HashSet::from(["STG_ORDERS".to_string(), "RAW_DATA".to_string()]);
    let known_models = HashSet::from(["stg_orders"]);
    let external_tables = HashSet::from(["raw_data".to_string()]);

    let (model_deps, external_deps) =
        categorize_dependencies(deps, &known_models, &external_tables);

    // Should match case-insensitively and preserve original model name
    assert_eq!(model_deps, vec!["stg_orders"]);
    // External tables should also match the reference (uppercase in this case)
    assert!(external_deps.contains(&"RAW_DATA".to_string()));
}

#[test]
fn test_extract_left_join() {
    let deps = parse_and_extract(
        "SELECT * FROM orders LEFT JOIN customers ON orders.customer_id = customers.id",
    );
    assert!(deps.contains("orders"));
    assert!(deps.contains("customers"));
}

#[test]
fn test_extract_multiple_joins() {
    let deps = parse_and_extract(
        r#"
        SELECT o.*, c.name, p.product_name
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.id
        LEFT JOIN products p ON o.product_id = p.id
        "#,
    );
    assert!(deps.contains("orders"));
    assert!(deps.contains("customers"));
    assert!(deps.contains("products"));
    assert_eq!(deps.len(), 3);
}

// ---------------------------------------------------------------------------
// Dialect-aware (resolved) extraction tests
// ---------------------------------------------------------------------------

fn parse_and_extract_resolved(
    sql: &str,
    dialect: &dyn crate::dialect::SqlDialect,
) -> Vec<ResolvedIdent> {
    let stmts = dialect.parse(sql).unwrap();
    extract_dependencies_resolved(&stmts, dialect)
}

fn find_dep<'a>(deps: &'a [ResolvedIdent], name: &str) -> Option<&'a ResolvedIdent> {
    deps.iter().find(|d| d.name == name)
}

// -- DuckDB: unquoted preserves case, case-insensitive --

#[test]
fn test_resolved_duckdb_unquoted_preserves_case() {
    let dialect = DuckDbDialect::new();
    let deps = parse_and_extract_resolved("SELECT * FROM Users", &dialect);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].name, "Users");
    assert!(!deps[0].is_case_sensitive);
    assert_eq!(
        deps[0].table_part().sensitivity,
        CaseSensitivity::CaseInsensitive
    );
}

#[test]
fn test_resolved_duckdb_quoted_is_case_sensitive() {
    let dialect = DuckDbDialect::new();
    let deps = parse_and_extract_resolved(r#"SELECT * FROM "MyTable""#, &dialect);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].name, "MyTable");
    assert!(deps[0].is_case_sensitive);
    assert_eq!(
        deps[0].table_part().sensitivity,
        CaseSensitivity::CaseSensitive
    );
}

#[test]
fn test_resolved_duckdb_mixed_quoted_unquoted() {
    let dialect = DuckDbDialect::new();
    let deps = parse_and_extract_resolved(
        r#"SELECT * FROM users JOIN "SpecialTable" ON users.id = "SpecialTable".id"#,
        &dialect,
    );
    assert_eq!(deps.len(), 2);

    let users = find_dep(&deps, "users").expect("should find 'users'");
    assert!(!users.is_case_sensitive);

    let special = find_dep(&deps, "SpecialTable").expect("should find 'SpecialTable'");
    assert!(special.is_case_sensitive);
}

#[test]
fn test_resolved_duckdb_schema_qualified_quoted() {
    let dialect = DuckDbDialect::new();
    let deps = parse_and_extract_resolved(r#"SELECT * FROM "MySchema"."MyTable""#, &dialect);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].name, "MySchema.MyTable");
    assert!(deps[0].is_case_sensitive);
    // Both parts are quoted
    assert_eq!(deps[0].parts.len(), 2);
    assert_eq!(deps[0].parts[0].sensitivity, CaseSensitivity::CaseSensitive);
    assert_eq!(deps[0].parts[1].sensitivity, CaseSensitivity::CaseSensitive);
}

#[test]
fn test_resolved_duckdb_schema_qualified_mixed() {
    let dialect = DuckDbDialect::new();
    // Unquoted schema, quoted table
    let deps = parse_and_extract_resolved(r#"SELECT * FROM raw."MyTable""#, &dialect);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].name, "raw.MyTable");
    // The table part is quoted so the whole ref is case-sensitive
    assert!(deps[0].is_case_sensitive);
    assert_eq!(
        deps[0].parts[0].sensitivity,
        CaseSensitivity::CaseInsensitive
    );
    assert_eq!(deps[0].parts[1].sensitivity, CaseSensitivity::CaseSensitive);
}

// -- Snowflake: unquoted folds to UPPER CASE --

#[test]
fn test_resolved_snowflake_unquoted_folds_upper() {
    let dialect = SnowflakeDialect::new();
    let deps = parse_and_extract_resolved("SELECT * FROM users", &dialect);
    assert_eq!(deps.len(), 1);
    // Snowflake folds unquoted "users" to "USERS"
    assert_eq!(deps[0].name, "USERS");
    assert!(!deps[0].is_case_sensitive);
}

#[test]
fn test_resolved_snowflake_quoted_preserves_case() {
    let dialect = SnowflakeDialect::new();
    let deps = parse_and_extract_resolved(r#"SELECT * FROM "myTable""#, &dialect);
    assert_eq!(deps.len(), 1);
    // Quoted: exact case preserved
    assert_eq!(deps[0].name, "myTable");
    assert!(deps[0].is_case_sensitive);
}

#[test]
fn test_resolved_snowflake_mixed_case() {
    let dialect = SnowflakeDialect::new();
    let deps = parse_and_extract_resolved(
        r#"SELECT * FROM orders JOIN "SpecialOrders" ON orders.id = "SpecialOrders".order_id"#,
        &dialect,
    );
    assert_eq!(deps.len(), 2);

    let orders = find_dep(&deps, "ORDERS").expect("should find 'ORDERS' (folded)");
    assert!(!orders.is_case_sensitive);

    let special = find_dep(&deps, "SpecialOrders").expect("should find 'SpecialOrders'");
    assert!(special.is_case_sensitive);
}

#[test]
fn test_resolved_snowflake_schema_qualified_unquoted() {
    let dialect = SnowflakeDialect::new();
    let deps = parse_and_extract_resolved("SELECT * FROM raw.orders", &dialect);
    assert_eq!(deps.len(), 1);
    // Both parts folded to upper
    assert_eq!(deps[0].name, "RAW.ORDERS");
    assert!(!deps[0].is_case_sensitive);
}

#[test]
fn test_resolved_snowflake_three_part_name() {
    let dialect = SnowflakeDialect::new();
    let deps = parse_and_extract_resolved("SELECT * FROM mydb.myschema.mytable", &dialect);
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].name, "MYDB.MYSCHEMA.MYTABLE");
    assert_eq!(deps[0].parts.len(), 3);
    assert!(!deps[0].is_case_sensitive);
}

// -- CTE filtering still works with resolved extraction --

#[test]
fn test_resolved_cte_filtered_out() {
    let dialect = DuckDbDialect::new();
    let deps = parse_and_extract_resolved(
        r#"
        WITH staged AS (
            SELECT * FROM raw_orders
        )
        SELECT * FROM staged JOIN customers ON staged.id = customers.id
        "#,
        &dialect,
    );
    assert!(find_dep(&deps, "raw_orders").is_some());
    assert!(find_dep(&deps, "customers").is_some());
    assert!(
        find_dep(&deps, "staged").is_none(),
        "CTE should be filtered out"
    );
    assert_eq!(deps.len(), 2);
}

#[test]
fn test_resolved_cte_filtered_snowflake() {
    let dialect = SnowflakeDialect::new();
    let deps = parse_and_extract_resolved(
        r#"
        WITH staged AS (
            SELECT * FROM raw_orders
        )
        SELECT * FROM staged JOIN customers ON staged.id = customers.id
        "#,
        &dialect,
    );
    assert!(find_dep(&deps, "RAW_ORDERS").is_some());
    assert!(find_dep(&deps, "CUSTOMERS").is_some());
    assert!(
        find_dep(&deps, "STAGED").is_none(),
        "CTE should be filtered out"
    );
    assert_eq!(deps.len(), 2);
}

// -- categorize_dependencies_resolved --

#[test]
fn test_categorize_resolved_unquoted_case_insensitive() {
    // DuckDB: unquoted "STG_ORDERS" should match model "stg_orders"
    let dialect = DuckDbDialect::new();
    let stmts = dialect.parse("SELECT * FROM STG_ORDERS").unwrap();
    let deps = extract_dependencies_resolved(&stmts, &dialect);

    let known_models = HashSet::from(["stg_orders"]);
    let external_tables = HashSet::new();

    let (model_deps, _external, _unknown) =
        categorize_dependencies_resolved(deps, &known_models, &external_tables);

    assert_eq!(model_deps, vec!["stg_orders"]);
}

#[test]
fn test_categorize_resolved_quoted_requires_exact_match() {
    // DuckDB: quoted "stg_orders" should match model "stg_orders" exactly
    let dialect = DuckDbDialect::new();
    let stmts = dialect.parse(r#"SELECT * FROM "stg_orders""#).unwrap();
    let deps = extract_dependencies_resolved(&stmts, &dialect);

    let known_models = HashSet::from(["stg_orders"]);
    let external_tables = HashSet::new();

    let (model_deps, _external, _unknown) =
        categorize_dependencies_resolved(deps, &known_models, &external_tables);

    assert_eq!(model_deps, vec!["stg_orders"]);
}

#[test]
fn test_categorize_resolved_quoted_no_match_wrong_case() {
    // DuckDB: quoted "STG_ORDERS" should NOT match model "stg_orders"
    // because quoted identifiers are case-sensitive
    let dialect = DuckDbDialect::new();
    let stmts = dialect.parse(r#"SELECT * FROM "STG_ORDERS""#).unwrap();
    let deps = extract_dependencies_resolved(&stmts, &dialect);

    let known_models = HashSet::from(["stg_orders"]);
    let external_tables = HashSet::new();

    let (model_deps, _external, unknown) =
        categorize_dependencies_resolved(deps, &known_models, &external_tables);

    assert!(
        model_deps.is_empty(),
        "Quoted 'STG_ORDERS' should not match model 'stg_orders'"
    );
    assert_eq!(unknown, vec!["STG_ORDERS"]);
}

#[test]
fn test_categorize_resolved_snowflake_unquoted_matches_upper() {
    // Snowflake: unquoted "users" → folded to "USERS"
    // Model list has "USERS" → should match
    let dialect = SnowflakeDialect::new();
    let stmts = dialect.parse("SELECT * FROM users").unwrap();
    let deps = extract_dependencies_resolved(&stmts, &dialect);

    let known_models = HashSet::from(["USERS"]);
    let external_tables = HashSet::new();

    let (model_deps, _external, _unknown) =
        categorize_dependencies_resolved(deps, &known_models, &external_tables);

    assert_eq!(model_deps, vec!["USERS"]);
}

#[test]
fn test_categorize_resolved_snowflake_quoted_lowercase() {
    // Snowflake: quoted "users" stays lowercase and is case-sensitive
    // Model list only has "USERS" → should NOT match
    let dialect = SnowflakeDialect::new();
    let stmts = dialect.parse(r#"SELECT * FROM "users""#).unwrap();
    let deps = extract_dependencies_resolved(&stmts, &dialect);

    let known_models = HashSet::from(["USERS"]);
    let external_tables = HashSet::new();

    let (model_deps, _external, unknown) =
        categorize_dependencies_resolved(deps, &known_models, &external_tables);

    assert!(
        model_deps.is_empty(),
        "Quoted lowercase 'users' should not match 'USERS' on Snowflake"
    );
    assert_eq!(unknown, vec!["users"]);
}
