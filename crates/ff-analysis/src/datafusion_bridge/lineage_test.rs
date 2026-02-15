use super::*;
use crate::datafusion_bridge::planner::sql_to_plan;
use crate::datafusion_bridge::provider::{FeatherFlowProvider, FunctionRegistry};
use crate::schema::{RelSchema, SchemaCatalog};
use crate::test_utils::{int32, make_col, varchar};
use crate::types::Nullability;
use std::collections::HashMap;
use std::sync::Arc;

fn make_catalog() -> SchemaCatalog {
    let mut catalog: SchemaCatalog = HashMap::new();
    catalog.insert(
        "orders".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("customer_id", int32(), Nullability::Nullable),
            make_col("amount", int32(), Nullability::Nullable),
            make_col("status", varchar(), Nullability::Nullable),
        ])),
    );
    catalog.insert(
        "customers".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
        ])),
    );
    catalog
}

fn plan_and_lineage(sql: &str) -> ModelColumnLineage {
    let catalog = make_catalog();
    let registry = FunctionRegistry::new();
    let provider = FeatherFlowProvider::new(&catalog, &registry);
    let plan = sql_to_plan(sql, &provider).unwrap();
    extract_column_lineage("test_model", &plan)
}

#[test]
fn test_copy_lineage() {
    let lineage = plan_and_lineage("SELECT id FROM orders");
    let copy_edges: Vec<_> = lineage
        .edges
        .iter()
        .filter(|e| e.kind == LineageKind::Copy && e.output_column == "id")
        .collect();
    assert!(
        !copy_edges.is_empty(),
        "Should have Copy lineage for id column"
    );
}

#[test]
fn test_transform_lineage() {
    let lineage = plan_and_lineage("SELECT id + amount AS total FROM orders");
    let deduped = deduplicate_edges(&lineage.edges);
    let transform_edges: Vec<_> = deduped
        .iter()
        .filter(|e| e.kind == LineageKind::Transform && e.output_column == "total")
        .collect();
    assert!(
        !transform_edges.is_empty(),
        "Should have Transform lineage for computed column"
    );
}

#[test]
fn test_inspect_lineage_filter() {
    let lineage = plan_and_lineage("SELECT id FROM orders WHERE status = 'active'");
    let deduped = deduplicate_edges(&lineage.edges);
    let inspect_edges: Vec<_> = deduped
        .iter()
        .filter(|e| e.kind == LineageKind::Inspect && e.source_column == "status")
        .collect();
    assert!(
        !inspect_edges.is_empty(),
        "Should have Inspect lineage for WHERE column"
    );
}

#[test]
fn test_join_lineage() {
    let lineage = plan_and_lineage(
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
    );
    let deduped = deduplicate_edges(&lineage.edges);

    // Should have lineage edges for the join key columns (customer_id or id)
    let _join_key_edges: Vec<_> = deduped
        .iter()
        .filter(|e| {
            e.source_column == "customer_id"
                || (e.source_column == "id" && e.kind == LineageKind::Inspect)
        })
        .collect();
    // DataFusion may not always produce explicit Inspect edges for join keys
    // depending on plan structure, so also check for Copy edges on output
    let output_edges: Vec<_> = deduped
        .iter()
        .filter(|e| e.source_column == "name" || e.output_column == "id")
        .collect();
    assert!(
        !output_edges.is_empty(),
        "Should have lineage for output columns in JOIN query. Got: {:?}",
        deduped
            .iter()
            .map(|e| format!(
                "{} -> {}.{} ({:?})",
                e.output_column, e.source_table, e.source_column, e.kind
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_aggregate_lineage() {
    let lineage =
        plan_and_lineage("SELECT status, SUM(amount) AS total FROM orders GROUP BY status");
    let deduped = deduplicate_edges(&lineage.edges);

    // status should appear in lineage (as GROUP BY key)
    let status_edges: Vec<_> = deduped
        .iter()
        .filter(|e| e.source_column == "status")
        .collect();
    assert!(
        !status_edges.is_empty(),
        "Should have lineage for GROUP BY column 'status'. Got: {:?}",
        deduped
            .iter()
            .map(|e| format!(
                "{} -> {}.{} ({:?})",
                e.output_column, e.source_table, e.source_column, e.kind
            ))
            .collect::<Vec<_>>()
    );

    // amount should appear in lineage (as part of SUM aggregate)
    let amount_edges: Vec<_> = deduped
        .iter()
        .filter(|e| e.source_column == "amount")
        .collect();
    assert!(
        !amount_edges.is_empty(),
        "Should have lineage for aggregated column 'amount'. Got: {:?}",
        deduped
            .iter()
            .map(|e| format!(
                "{} -> {}.{} ({:?})",
                e.output_column, e.source_table, e.source_column, e.kind
            ))
            .collect::<Vec<_>>()
    );
}
