//! Multi-model DAG scenario integration tests

use ff_analysis::propagate_schemas;
use ff_analysis::test_utils::{
    decimal, float64, int32, int64, make_col, make_ctx, timestamp, varchar,
};
use ff_analysis::{
    DiagnosticCode, Nullability, PlanPassManager, RelSchema, SchemaMismatch, SqlType,
};
use std::collections::HashMap;
use std::sync::Arc;

type SchemaCatalog = HashMap<String, Arc<RelSchema>>;

// ── Clean E-Commerce DAG ────────────────────────────────────────────────

#[test]
fn test_clean_ecommerce_dag() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "raw_orders".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("customer_id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::Nullable),
            make_col("status", varchar(), Nullability::Nullable),
            make_col("created_at", timestamp(), Nullability::NotNull),
        ])),
    );
    initial.insert(
        "raw_customers".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::Nullable),
        ])),
    );

    let topo = vec![
        "stg_orders".to_string(),
        "stg_customers".to_string(),
        "fct_orders".to_string(),
    ];
    let mut sql = HashMap::new();
    sql.insert(
        "stg_orders".to_string(),
        "SELECT id, customer_id, amount, status FROM raw_orders".to_string(),
    );
    sql.insert(
        "stg_customers".to_string(),
        "SELECT id, name, email FROM raw_customers".to_string(),
    );
    sql.insert(
        "fct_orders".to_string(),
        "SELECT o.id, o.amount, o.status, c.name AS customer_name \
         FROM stg_orders o JOIN stg_customers c ON o.customer_id = c.id"
            .to_string(),
    );

    let result = propagate_schemas(&topo, &sql, &HashMap::new(), &initial, &[], &[]);
    assert!(
        result.failures.is_empty(),
        "Clean e-commerce DAG should have no failures: {:?}",
        result.failures
    );

    let fct = &result.model_plans["fct_orders"];
    assert_eq!(fct.inferred_schema.columns.len(), 4);
    let mut col_names: Vec<&str> = fct
        .inferred_schema
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    col_names.sort();
    assert_eq!(col_names, ["amount", "customer_name", "id", "status"]);
    assert!(fct.mismatches.is_empty());
}

// ── Simple Linear Chain ─────────────────────────────────────────────────

#[test]
fn test_simple_chain() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "raw".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val", varchar(), Nullability::Nullable),
        ])),
    );

    let topo = vec!["stg".to_string(), "mart".to_string()];
    let mut sql = HashMap::new();
    sql.insert("stg".to_string(), "SELECT id, val FROM raw".to_string());
    sql.insert("mart".to_string(), "SELECT id FROM stg".to_string());

    let result = propagate_schemas(&topo, &sql, &HashMap::new(), &initial, &[], &[]);
    assert!(result.failures.is_empty());
    assert_eq!(result.model_plans["mart"].inferred_schema.columns.len(), 1);
}

// ── Diamond DAG ─────────────────────────────────────────────────────────

#[test]
fn test_diamond_dag() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("a_val", varchar(), Nullability::Nullable),
            make_col("b_val", varchar(), Nullability::Nullable),
        ])),
    );

    let topo = vec![
        "left_model".to_string(),
        "right_model".to_string(),
        "merge_model".to_string(),
    ];
    let mut sql = HashMap::new();
    sql.insert(
        "left_model".to_string(),
        "SELECT id, a_val FROM source".to_string(),
    );
    sql.insert(
        "right_model".to_string(),
        "SELECT id, b_val FROM source".to_string(),
    );
    sql.insert(
        "merge_model".to_string(),
        "SELECT l.id, l.a_val, r.b_val FROM left_model l JOIN right_model r ON l.id = r.id"
            .to_string(),
    );

    let result = propagate_schemas(&topo, &sql, &HashMap::new(), &initial, &[], &[]);
    assert!(result.failures.is_empty());
    assert_eq!(
        result.model_plans["merge_model"]
            .inferred_schema
            .columns
            .len(),
        3
    );
}

// ── Wide Fan-Out ────────────────────────────────────────────────────────

#[test]
fn test_wide_fan_out() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("a", varchar(), Nullability::Nullable),
        ])),
    );

    let topo: Vec<String> = (1..=5).map(|i| format!("model_{i}")).collect();
    let mut sql = HashMap::new();
    for i in 1..=5 {
        sql.insert(format!("model_{i}"), "SELECT id, a FROM source".to_string());
    }

    let result = propagate_schemas(&topo, &sql, &HashMap::new(), &initial, &[], &[]);
    assert!(result.failures.is_empty());
    assert_eq!(result.model_plans.len(), 5);
}

// ── Deep Chain (10 models) ──────────────────────────────────────────────

#[test]
fn test_deep_chain() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "raw".to_string(),
        Arc::new(RelSchema::new(vec![make_col(
            "id",
            int32(),
            Nullability::NotNull,
        )])),
    );

    let topo: Vec<String> = (1..=10).map(|i| format!("m{i}")).collect();
    let mut sql = HashMap::new();
    sql.insert("m1".to_string(), "SELECT id FROM raw".to_string());
    for i in 2..=10 {
        sql.insert(format!("m{i}"), format!("SELECT id FROM m{}", i - 1));
    }

    let result = propagate_schemas(&topo, &sql, &HashMap::new(), &initial, &[], &[]);
    assert!(result.failures.is_empty());
    assert_eq!(result.model_plans.len(), 10);
}

// ── Schema Drift Detection ──────────────────────────────────────────────

#[test]
fn test_schema_drift_detection() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
        ])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert(
        "model".to_string(),
        "SELECT id, name FROM source".to_string(),
    );

    let mut yaml = HashMap::new();
    yaml.insert(
        "model".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::Nullable),
        ]),
    );

    let result = propagate_schemas(&topo, &sql, &yaml, &initial, &[], &[]);
    assert!(result.failures.is_empty());

    let model = &result.model_plans["model"];
    assert!(!model.mismatches.is_empty(), "Should detect schema drift");
}

// ── Type Mismatch Through Chain ─────────────────────────────────────────

#[test]
fn test_type_mismatch_in_chain() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::Nullable),
        ])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert(
        "model".to_string(),
        "SELECT id, amount FROM source".to_string(),
    );

    let mut yaml = HashMap::new();
    yaml.insert(
        "model".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", varchar(), Nullability::Nullable),
        ]),
    );

    let result = propagate_schemas(&topo, &sql, &yaml, &initial, &[], &[]);
    let model = &result.model_plans["model"];
    assert!(
        model.mismatches.iter().any(|m| {
            matches!(m, SchemaMismatch::TypeMismatch { column, .. } if column == "amount")
        }),
        "Should detect type mismatch on 'amount'"
    );
}

// ── Null Violations Through LEFT JOIN ───────────────────────────────────

#[test]
fn test_null_violation_through_left_join() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "orders".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("cust_id", int32(), Nullability::NotNull),
        ])),
    );
    initial.insert(
        "customers".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
        ])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert(
        "model".to_string(),
        "SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.cust_id = c.id".to_string(),
    );

    let mut yaml = HashMap::new();
    yaml.insert(
        "model".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
        ]),
    );

    let result = propagate_schemas(&topo, &sql, &yaml, &initial, &[], &[]);
    let model = &result.model_plans["model"];
    assert!(
        model.mismatches.iter().any(|m| {
            matches!(m, SchemaMismatch::NullabilityMismatch { column, .. } if column == "name")
        }),
        "Should detect nullability mismatch on 'name' after LEFT JOIN"
    );
}

// ── Cross-Model Consistency via PlanPassManager ─────────────────────────

#[test]
fn test_plan_pass_manager_clean_dag() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val", varchar(), Nullability::Nullable),
        ])),
    );

    let topo = vec!["m1".to_string()];
    let mut sql = HashMap::new();
    sql.insert("m1".to_string(), "SELECT id, val FROM source".to_string());

    let mut yaml = HashMap::new();
    yaml.insert(
        "m1".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val", varchar(), Nullability::Nullable),
        ]),
    );

    let result = propagate_schemas(&topo, &sql, &yaml, &initial, &[], &[]);
    let ctx = make_ctx();
    let pass_mgr = PlanPassManager::with_defaults();
    let diags = pass_mgr.run(&topo, &result.model_plans, &ctx, None);

    let schema_diags: Vec<_> = diags
        .iter()
        .filter(|d| d.code == DiagnosticCode::A040 || d.code == DiagnosticCode::A041)
        .collect();
    assert!(
        schema_diags.is_empty(),
        "Clean DAG should have no schema diagnostics, got: {:?}",
        schema_diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── Mixed Diagnostics ───────────────────────────────────────────────────

#[test]
fn test_mixed_diagnostics() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "orders".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("cust_id", int32(), Nullability::NotNull),
            make_col("amount", float64(), Nullability::Nullable),
        ])),
    );
    initial.insert(
        "customers".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
        ])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert(
        "model".to_string(),
        "SELECT o.id, o.amount, c.name \
         FROM orders o LEFT JOIN customers c ON o.cust_id = c.id"
            .to_string(),
    );

    let mut yaml = HashMap::new();
    yaml.insert(
        "model".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", float64(), Nullability::Nullable),
            make_col("name", varchar(), Nullability::NotNull),
            make_col("status", varchar(), Nullability::Nullable),
        ]),
    );

    let result = propagate_schemas(&topo, &sql, &yaml, &initial, &[], &[]);
    let model = &result.model_plans["model"];

    // Expect exactly 2 mismatches: MissingFromSql("status") and NullabilityMismatch("name")
    assert_eq!(
        model.mismatches.len(),
        2,
        "Expected exactly 2 mismatches, got: {:?}",
        model.mismatches
    );
    assert!(
        model.mismatches.iter().any(
            |m| matches!(m, SchemaMismatch::MissingFromSql { column, .. } if column == "status")
        ),
        "Expected MissingFromSql on 'status', got: {:?}",
        model.mismatches
    );
    assert!(
        model.mismatches.iter().any(
            |m| matches!(m, SchemaMismatch::NullabilityMismatch { column, .. } if column == "name")
        ),
        "Expected NullabilityMismatch on 'name', got: {:?}",
        model.mismatches
    );
}

// ── All DuckDB Types Propagation ────────────────────────────────────────

#[test]
fn test_all_duckdb_types_propagate() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "typed_source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("bool_col", SqlType::Boolean, Nullability::NotNull),
            make_col("int_col", int32(), Nullability::NotNull),
            make_col("bigint_col", int64(), Nullability::NotNull),
            make_col("float_col", float64(), Nullability::NotNull),
            make_col("decimal_col", decimal(10, 2), Nullability::Nullable),
            make_col("varchar_col", varchar(), Nullability::Nullable),
            make_col("date_col", SqlType::Date, Nullability::Nullable),
            make_col("ts_col", timestamp(), Nullability::NotNull),
        ])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert(
        "model".to_string(),
        "SELECT bool_col, int_col, bigint_col, float_col, decimal_col, varchar_col, date_col, ts_col FROM typed_source".to_string(),
    );

    let result = propagate_schemas(&topo, &sql, &HashMap::new(), &initial, &[], &[]);
    assert!(
        result.failures.is_empty(),
        "All types should propagate: {:?}",
        result.failures
    );
    let schema = &result.model_plans["model"].inferred_schema;
    assert_eq!(schema.columns.len(), 8);
    let mut col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    col_names.sort();
    assert_eq!(
        col_names,
        [
            "bigint_col",
            "bool_col",
            "date_col",
            "decimal_col",
            "float_col",
            "int_col",
            "ts_col",
            "varchar_col"
        ]
    );
}
