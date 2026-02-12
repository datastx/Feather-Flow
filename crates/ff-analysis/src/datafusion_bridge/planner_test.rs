use super::*;
use crate::schema::{RelSchema, SchemaCatalog};
use crate::types::{IntBitWidth, Nullability, SqlType, TypedColumn};
use std::collections::HashMap;

fn make_catalog() -> SchemaCatalog {
    let mut catalog: SchemaCatalog = HashMap::new();
    catalog.insert(
        "orders".to_string(),
        RelSchema::new(vec![
            TypedColumn {
                name: "id".to_string(),
                source_table: None,
                sql_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::NotNull,
                provenance: vec![],
            },
            TypedColumn {
                name: "customer_id".to_string(),
                source_table: None,
                sql_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::Nullable,
                provenance: vec![],
            },
            TypedColumn {
                name: "amount".to_string(),
                source_table: None,
                sql_type: SqlType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
                nullability: Nullability::Nullable,
                provenance: vec![],
            },
            TypedColumn {
                name: "status".to_string(),
                source_table: None,
                sql_type: SqlType::String { max_length: None },
                nullability: Nullability::Nullable,
                provenance: vec![],
            },
        ]),
    );
    catalog.insert(
        "customers".to_string(),
        RelSchema::new(vec![
            TypedColumn {
                name: "id".to_string(),
                source_table: None,
                sql_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::NotNull,
                provenance: vec![],
            },
            TypedColumn {
                name: "name".to_string(),
                source_table: None,
                sql_type: SqlType::String { max_length: None },
                nullability: Nullability::Nullable,
                provenance: vec![],
            },
        ]),
    );
    catalog
}

fn plan_sql(sql: &str) -> AnalysisResult<LogicalPlan> {
    let catalog = make_catalog();
    let provider = FeatherFlowProvider::new(&catalog);
    sql_to_plan(sql, &provider)
}

#[test]
fn test_simple_select() {
    let plan = plan_sql("SELECT id, amount FROM orders").unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
}

#[test]
fn test_select_star() {
    let plan = plan_sql("SELECT * FROM orders").unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 4);
}

#[test]
fn test_join() {
    let plan =
        plan_sql("SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id")
            .unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
}

#[test]
fn test_group_by() {
    let plan = plan_sql("SELECT status, SUM(amount) AS total FROM orders GROUP BY status").unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
}

#[test]
fn test_unknown_table_error() {
    let result = plan_sql("SELECT * FROM nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_union() {
    let plan = plan_sql(
        "SELECT id, status FROM orders UNION ALL SELECT id, name AS status FROM customers",
    )
    .unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
}

// ── Additional planner tests ────────────────────────────────────────

#[test]
fn test_aggregate_with_having() {
    let plan =
        plan_sql("SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status HAVING COUNT(*) > 1")
            .unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
}

#[test]
fn test_subquery_in_where() {
    let plan =
        plan_sql("SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)").unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 1);
}

#[test]
fn test_left_join_plan() {
    let plan =
        plan_sql("SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.customer_id = c.id")
            .unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
    // Right side (name) should be nullable after LEFT JOIN
    assert!(schema.fields()[1].is_nullable());
}

#[test]
fn test_order_by_limit() {
    let plan = plan_sql("SELECT id, amount FROM orders ORDER BY amount DESC LIMIT 10").unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);
}

#[test]
fn test_case_expression_plan() {
    let plan =
        plan_sql("SELECT CASE WHEN status = 'shipped' THEN 1 ELSE 0 END AS is_shipped FROM orders")
            .unwrap();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.fields()[0].name(), "is_shipped");
}

#[test]
fn test_empty_sql_error() {
    let catalog = make_catalog();
    let provider = FeatherFlowProvider::new(&catalog);
    let result = sql_to_plan("", &provider);
    assert!(result.is_err());
}
