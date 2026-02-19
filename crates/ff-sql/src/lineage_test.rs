use super::*;
use crate::parser::SqlParser;

fn parse_and_extract_lineage(sql: &str, model_name: &str) -> Option<ModelLineage> {
    let parser = SqlParser::duckdb();
    let stmts = parser.parse(sql).unwrap();
    stmts
        .first()
        .and_then(|stmt| extract_column_lineage(stmt, model_name))
}

#[test]
fn test_simple_select() {
    let lineage = parse_and_extract_lineage("SELECT id, name FROM users", "test_model").unwrap();

    assert_eq!(lineage.model_name, "test_model");
    assert_eq!(lineage.columns.len(), 2);

    let id_col = lineage.get_column("id").unwrap();
    assert!(id_col.is_direct);
    assert_eq!(id_col.expr_type, ExprType::Column);

    let name_col = lineage.get_column("name").unwrap();
    assert!(name_col.is_direct);
}

#[test]
fn test_aliased_columns() {
    let lineage = parse_and_extract_lineage(
        "SELECT id AS user_id, name AS user_name FROM users",
        "test_model",
    )
    .unwrap();

    assert_eq!(lineage.columns.len(), 2);

    let user_id = lineage.get_column("user_id").unwrap();
    assert!(user_id.is_direct);
    assert!(user_id.source_columns.contains(&ColumnRef::simple("id")));

    let user_name = lineage.get_column("user_name").unwrap();
    assert!(user_name
        .source_columns
        .contains(&ColumnRef::simple("name")));
}

#[test]
fn test_qualified_column_refs() {
    let lineage =
        parse_and_extract_lineage("SELECT u.id, u.name FROM users u", "test_model").unwrap();

    assert_eq!(lineage.columns.len(), 2);

    // Check that aliases are resolved
    let id_col = lineage.get_column("id").unwrap();
    assert!(id_col
        .source_columns
        .contains(&ColumnRef::qualified("users", "id")));
}

#[test]
fn test_function_lineage() {
    let lineage = parse_and_extract_lineage(
        "SELECT COUNT(id) AS cnt, UPPER(name) AS upper_name FROM users",
        "test_model",
    )
    .unwrap();

    assert_eq!(lineage.columns.len(), 2);

    let cnt = lineage.get_column("cnt").unwrap();
    assert!(!cnt.is_direct);
    assert_eq!(cnt.expr_type, ExprType::Function);
    assert!(cnt.source_columns.contains(&ColumnRef::simple("id")));

    let upper_name = lineage.get_column("upper_name").unwrap();
    assert!(!upper_name.is_direct);
    assert_eq!(upper_name.expr_type, ExprType::Function);
}

#[test]
fn test_expression_lineage() {
    let lineage =
        parse_and_extract_lineage("SELECT price * quantity AS total FROM orders", "test_model")
            .unwrap();

    let total = lineage.get_column("total").unwrap();
    assert!(!total.is_direct);
    assert_eq!(total.expr_type, ExprType::Expression);
    assert!(total.source_columns.contains(&ColumnRef::simple("price")));
    assert!(total
        .source_columns
        .contains(&ColumnRef::simple("quantity")));
}

#[test]
fn test_case_expression() {
    let lineage = parse_and_extract_lineage(
        "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users",
        "test_model",
    )
    .unwrap();

    let is_active = lineage.get_column("is_active").unwrap();
    assert!(!is_active.is_direct);
    assert_eq!(is_active.expr_type, ExprType::Case);
    assert!(is_active
        .source_columns
        .contains(&ColumnRef::simple("status")));
}

#[test]
fn test_join_lineage() {
    let lineage = parse_and_extract_lineage(
        "SELECT o.id AS order_id, c.name AS customer_name
         FROM orders o
         JOIN customers c ON o.customer_id = c.id",
        "test_model",
    )
    .unwrap();

    assert_eq!(lineage.source_tables.len(), 2);
    assert!(lineage.source_tables.contains("orders"));
    assert!(lineage.source_tables.contains("customers"));

    let order_id = lineage.get_column("order_id").unwrap();
    assert!(order_id
        .source_columns
        .contains(&ColumnRef::qualified("orders", "id")));

    let customer_name = lineage.get_column("customer_name").unwrap();
    assert!(customer_name
        .source_columns
        .contains(&ColumnRef::qualified("customers", "name")));
}

#[test]
fn test_wildcard_select() {
    let lineage = parse_and_extract_lineage("SELECT * FROM users", "test_model").unwrap();

    assert_eq!(lineage.columns.len(), 1);
    let wildcard = &lineage.columns[0];
    assert_eq!(wildcard.output_column, "*");
    assert_eq!(wildcard.expr_type, ExprType::Wildcard);
}

#[test]
fn test_literal_column() {
    let lineage = parse_and_extract_lineage(
        "SELECT id, 'constant' AS const_col, 42 AS num_col FROM users",
        "test_model",
    )
    .unwrap();

    let const_col = lineage.get_column("const_col").unwrap();
    assert!(!const_col.is_direct);
    assert_eq!(const_col.expr_type, ExprType::Literal);
    assert!(const_col.source_columns.is_empty());
}

#[test]
fn test_cast_expression() {
    let lineage = parse_and_extract_lineage(
        "SELECT CAST(amount AS DECIMAL(10,2)) AS amount_decimal FROM orders",
        "test_model",
    )
    .unwrap();

    let amount = lineage.get_column("amount_decimal").unwrap();
    assert_eq!(amount.expr_type, ExprType::Cast);
    assert!(amount.source_columns.contains(&ColumnRef::simple("amount")));
}

#[test]
fn test_cte_lineage() {
    let lineage = parse_and_extract_lineage(
        "WITH staged AS (SELECT id, name FROM raw_users)
         SELECT id AS user_id, name AS user_name FROM staged",
        "test_model",
    )
    .unwrap();

    // The main query references the CTE "staged" as its source
    // (raw_users is inside the CTE, so it's not directly visible in the main select's FROM)
    assert!(lineage.source_tables.contains("staged"));

    // Output columns should exist
    let user_id = lineage.get_column("user_id");
    assert!(user_id.is_some());
}

// --- Recursive traversal tests ---

/// Helper to build a ProjectLineage with given edges
fn build_project_lineage(edges: Vec<LineageEdge>) -> ProjectLineage {
    ProjectLineage {
        models: HashMap::new(),
        edges,
    }
}

fn make_edge(src_model: &str, src_col: &str, tgt_model: &str, tgt_col: &str) -> LineageEdge {
    LineageEdge {
        source_model: src_model.to_string(),
        source_column: src_col.to_string(),
        target_model: tgt_model.to_string(),
        target_column: tgt_col.to_string(),
        is_direct: true,
        expr_type: ExprType::Column,
        classification: None,
    }
}

#[test]
fn test_recursive_downstream_three_model_chain() {
    // A.x → B.x → C.x
    let lineage = build_project_lineage(vec![
        make_edge("A", "x", "B", "x"),
        make_edge("B", "x", "C", "x"),
    ]);

    let downstream = lineage.column_consumers_recursive("A", "x");
    assert_eq!(downstream.len(), 2);
    assert!(downstream.iter().any(|e| e.target_model == "B"));
    assert!(downstream.iter().any(|e| e.target_model == "C"));
}

#[test]
fn test_recursive_upstream_three_model_chain() {
    // A.x → B.x → C.x
    let lineage = build_project_lineage(vec![
        make_edge("A", "x", "B", "x"),
        make_edge("B", "x", "C", "x"),
    ]);

    let upstream = lineage.trace_column_recursive("C", "x");
    assert_eq!(upstream.len(), 2);
    assert!(upstream.iter().any(|e| e.source_model == "A"));
    assert!(upstream.iter().any(|e| e.source_model == "B"));
}

#[test]
fn test_recursive_downstream_diamond() {
    // A.x → B.x, A.x → C.x, B.x → D.x, C.x → D.x
    let lineage = build_project_lineage(vec![
        make_edge("A", "x", "B", "x"),
        make_edge("A", "x", "C", "x"),
        make_edge("B", "x", "D", "x"),
        make_edge("C", "x", "D", "x"),
    ]);

    let downstream = lineage.column_consumers_recursive("A", "x");
    // Should get all 4 edges, no duplicates
    assert_eq!(downstream.len(), 4);
    // D.x appears as target twice (from B and C) — both edges should be present
    let d_edges: Vec<_> = downstream.iter().filter(|e| e.target_model == "D").collect();
    assert_eq!(d_edges.len(), 2);
}

#[test]
fn test_recursive_upstream_diamond() {
    // A.x → C.x, B.x → C.x, C.x → D.x
    let lineage = build_project_lineage(vec![
        make_edge("A", "x", "C", "x"),
        make_edge("B", "x", "C", "x"),
        make_edge("C", "x", "D", "x"),
    ]);

    let upstream = lineage.trace_column_recursive("D", "x");
    assert_eq!(upstream.len(), 3);
    assert!(upstream.iter().any(|e| e.source_model == "A"));
    assert!(upstream.iter().any(|e| e.source_model == "B"));
    assert!(upstream.iter().any(|e| e.source_model == "C"));
}

#[test]
fn test_recursive_cycle_protection() {
    // A.x → B.x → A.x (cycle)
    let lineage = build_project_lineage(vec![
        make_edge("A", "x", "B", "x"),
        make_edge("B", "x", "A", "x"),
    ]);

    // Should not loop forever; visited set prevents re-expansion
    let downstream = lineage.column_consumers_recursive("A", "x");
    assert_eq!(downstream.len(), 2);

    let upstream = lineage.trace_column_recursive("A", "x");
    assert_eq!(upstream.len(), 2);
}

#[test]
fn test_recursive_single_hop() {
    // Only one hop: A.x → B.x
    let lineage = build_project_lineage(vec![make_edge("A", "x", "B", "x")]);

    let downstream = lineage.column_consumers_recursive("A", "x");
    assert_eq!(downstream.len(), 1);
    assert_eq!(downstream[0].target_model, "B");

    let upstream = lineage.trace_column_recursive("B", "x");
    assert_eq!(upstream.len(), 1);
    assert_eq!(upstream[0].source_model, "A");
}

#[test]
fn test_recursive_no_matches() {
    let lineage = build_project_lineage(vec![make_edge("A", "x", "B", "x")]);

    // No downstream from B.x
    let downstream = lineage.column_consumers_recursive("B", "x");
    assert!(downstream.is_empty());

    // No upstream to A.x
    let upstream = lineage.trace_column_recursive("A", "x");
    assert!(upstream.is_empty());
}
