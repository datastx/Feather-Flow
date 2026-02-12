use super::*;

#[test]
fn test_inline_single_ephemeral() {
    let sql = "SELECT * FROM stg_orders";
    let mut ephemeral_deps = HashMap::new();
    ephemeral_deps.insert(
        "stg_orders".to_string(),
        "SELECT id, amount FROM raw_orders".to_string(),
    );
    let order = vec!["stg_orders".to_string()];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
    assert!(
        result.contains(r#""stg_orders" AS"#),
        "expected quoted CTE name, got: {result}"
    );
    assert!(
        result.contains("raw_orders"),
        "expected raw_orders in CTE body, got: {result}"
    );
    assert!(
        result.contains("stg_orders"),
        "expected stg_orders in final SELECT, got: {result}"
    );
}

#[test]
fn test_inline_multiple_ephemerals() {
    let sql = "SELECT o.*, c.name FROM stg_orders o JOIN stg_customers c ON o.customer_id = c.id";
    let mut ephemeral_deps = HashMap::new();
    ephemeral_deps.insert(
        "stg_orders".to_string(),
        "SELECT id, customer_id, amount FROM raw_orders".to_string(),
    );
    ephemeral_deps.insert(
        "stg_customers".to_string(),
        "SELECT id, name FROM raw_customers".to_string(),
    );
    let order = vec!["stg_orders".to_string(), "stg_customers".to_string()];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
    assert!(
        result.contains(r#""stg_orders" AS"#),
        "expected quoted stg_orders CTE, got: {result}"
    );
    assert!(
        result.contains(r#""stg_customers" AS"#),
        "expected quoted stg_customers CTE, got: {result}"
    );
    // Both CTEs should be present
    assert!(
        result.contains("raw_orders"),
        "expected raw_orders, got: {result}"
    );
    assert!(
        result.contains("raw_customers"),
        "expected raw_customers, got: {result}"
    );
}

#[test]
fn test_inline_with_existing_cte() {
    let sql = "WITH my_cte AS (SELECT 1) SELECT * FROM my_cte, stg_orders";
    let mut ephemeral_deps = HashMap::new();
    ephemeral_deps.insert(
        "stg_orders".to_string(),
        "SELECT id FROM raw_orders".to_string(),
    );
    let order = vec!["stg_orders".to_string()];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
    // Should merge CTEs — new ephemeral CTE is prepended before existing ones
    assert!(
        result.contains(r#""stg_orders" AS"#),
        "expected quoted stg_orders CTE, got: {result}"
    );
    assert!(
        result.contains("my_cte AS"),
        "expected existing my_cte CTE, got: {result}"
    );
}

#[test]
fn test_inline_no_ephemerals() {
    let sql = "SELECT * FROM orders";
    let ephemeral_deps = HashMap::new();
    let order: Vec<String> = vec![];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
    assert_eq!(result, "SELECT * FROM orders");
}

#[test]
fn test_inline_removes_trailing_semicolon() {
    let sql = "SELECT * FROM stg_orders";
    let mut ephemeral_deps = HashMap::new();
    ephemeral_deps.insert(
        "stg_orders".to_string(),
        "SELECT id FROM raw_orders;".to_string(),
    );
    let order = vec!["stg_orders".to_string()];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
    // The inner SQL should not have a trailing semicolon
    assert!(
        result.contains("raw_orders"),
        "expected raw_orders in CTE body, got: {result}"
    );
    assert!(
        !result.contains("raw_orders;"),
        "semicolon should have been stripped, got: {result}"
    );
}

#[test]
fn test_collect_ephemeral_dependencies() {
    let mut model_deps: HashMap<String, Vec<String>> = HashMap::new();
    model_deps.insert(
        "fct_orders".to_string(),
        vec!["stg_orders".to_string(), "dim_customers".to_string()],
    );
    model_deps.insert("stg_orders".to_string(), vec!["raw_orders".to_string()]);
    model_deps.insert("dim_customers".to_string(), vec![]);

    let is_ephemeral = |name: &str| name == "stg_orders";
    let get_sql = |name: &str| {
        if name == "stg_orders" {
            Some("SELECT id FROM raw_orders".to_string())
        } else {
            None
        }
    };

    let (ephemeral_sql, order) =
        collect_ephemeral_dependencies("fct_orders", &model_deps, is_ephemeral, get_sql);

    assert_eq!(ephemeral_sql.len(), 1);
    assert!(ephemeral_sql.contains_key("stg_orders"));
    assert_eq!(order, vec!["stg_orders".to_string()]);
}

#[test]
fn test_collect_nested_ephemeral_dependencies() {
    // stg_orders (ephemeral) -> stg_raw (ephemeral) -> raw_orders
    let mut model_deps: HashMap<String, Vec<String>> = HashMap::new();
    model_deps.insert("fct_orders".to_string(), vec!["stg_orders".to_string()]);
    model_deps.insert("stg_orders".to_string(), vec!["stg_raw".to_string()]);
    model_deps.insert("stg_raw".to_string(), vec!["raw_orders".to_string()]);

    let is_ephemeral = |name: &str| name == "stg_orders" || name == "stg_raw";
    let get_sql = |name: &str| match name {
        "stg_orders" => Some("SELECT id FROM stg_raw".to_string()),
        "stg_raw" => Some("SELECT id FROM raw_orders".to_string()),
        _ => None,
    };

    let (ephemeral_sql, order) =
        collect_ephemeral_dependencies("fct_orders", &model_deps, is_ephemeral, get_sql);

    assert_eq!(ephemeral_sql.len(), 2);
    assert!(ephemeral_sql.contains_key("stg_orders"));
    assert!(ephemeral_sql.contains_key("stg_raw"));
    // stg_raw should come before stg_orders (dependency order)
    assert_eq!(order, vec!["stg_raw".to_string(), "stg_orders".to_string()]);
}

#[test]
fn test_cte_order_preserved() {
    // When inlining, CTEs should appear in the correct dependency order
    let sql = "SELECT * FROM a";
    let mut ephemeral_deps = HashMap::new();
    ephemeral_deps.insert("a".to_string(), "SELECT * FROM b".to_string());
    ephemeral_deps.insert("b".to_string(), "SELECT * FROM c".to_string());
    ephemeral_deps.insert("c".to_string(), "SELECT 1 AS x".to_string());
    // Order matters: c must come before b, b before a
    let order = vec!["c".to_string(), "b".to_string(), "a".to_string()];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();

    // Find positions of each CTE in the result (names are double-quoted)
    let pos_c = result
        .find(r#""c" AS"#)
        .unwrap_or_else(|| panic!("expected \"c\" AS in: {result}"));
    let pos_b = result
        .find(r#""b" AS"#)
        .unwrap_or_else(|| panic!("expected \"b\" AS in: {result}"));
    let pos_a = result
        .find(r#""a" AS"#)
        .unwrap_or_else(|| panic!("expected \"a\" AS in: {result}"));

    assert!(pos_c < pos_b, "c should come before b in: {result}");
    assert!(pos_b < pos_a, "b should come before a in: {result}");
}

#[test]
fn test_inline_reserved_word_cte_name() {
    // CTE names that are SQL reserved words should be properly quoted
    let sql = "SELECT * FROM \"select\"";
    let mut ephemeral_deps = HashMap::new();
    ephemeral_deps.insert(
        "select".to_string(),
        "SELECT id, name FROM raw_data".to_string(),
    );
    let order = vec!["select".to_string()];

    let result = inline_ephemeral_ctes(sql, &ephemeral_deps, &order).unwrap();
    // The reserved word must be double-quoted in the CTE definition
    assert!(
        result.contains(r#""select" AS"#),
        "expected quoted reserved-word CTE name, got: {result}"
    );
    assert!(
        result.contains("raw_data"),
        "expected CTE body, got: {result}"
    );

    // Also test with another reserved word: "order"
    let sql2 = "SELECT * FROM \"order\"";
    let mut ephemeral_deps2 = HashMap::new();
    ephemeral_deps2.insert(
        "order".to_string(),
        "SELECT id, total FROM raw_orders".to_string(),
    );
    let order2 = vec!["order".to_string()];

    let result2 = inline_ephemeral_ctes(sql2, &ephemeral_deps2, &order2).unwrap();
    assert!(
        result2.contains(r#""order" AS"#),
        "expected quoted reserved-word CTE name, got: {result2}"
    );
    assert!(
        result2.contains("raw_orders"),
        "expected CTE body, got: {result2}"
    );
}
