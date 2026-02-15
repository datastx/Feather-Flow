use super::*;
use crate::test_utils::*;
use std::sync::Arc;

#[test]
fn test_linear_chain_propagation() {
    // source_a → stg_a (selects from source_a)
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source_a".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["stg_a".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "stg_a".to_string(),
        "SELECT id, name FROM source_a".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    assert!(result.model_plans.contains_key("stg_a"));

    let stg_a = &result.model_plans["stg_a"];
    assert_eq!(stg_a.inferred_schema.columns.len(), 2);
    assert_eq!(stg_a.inferred_schema.columns[0].name, "id");
    assert_eq!(stg_a.inferred_schema.columns[1].name, "name");

    // The final catalog should contain both source_a and stg_a
    assert!(result.final_catalog.contains_key("source_a"));
    assert!(result.final_catalog.contains_key("stg_a"));
}

#[test]
fn test_multi_step_propagation() {
    // source → stg → mart
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "raw_orders".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", int32(), Nullability::Nullable),
            make_col("status", varchar(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["stg_orders".to_string(), "mart_orders".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "stg_orders".to_string(),
        "SELECT id, amount, status FROM raw_orders".to_string(),
    );
    sql_sources.insert(
        "mart_orders".to_string(),
        "SELECT id, status FROM stg_orders".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    assert!(result.model_plans.contains_key("stg_orders"));
    assert!(result.model_plans.contains_key("mart_orders"));

    // mart_orders should only have 2 columns (not 3)
    let mart = &result.model_plans["mart_orders"];
    assert_eq!(mart.inferred_schema.columns.len(), 2);
    assert_eq!(mart.inferred_schema.columns[0].name, "id");
    assert_eq!(mart.inferred_schema.columns[1].name, "status");
}

#[test]
fn test_diamond_dag_propagation() {
    // source → model_b, source → model_c, model_b + model_c → model_d
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val", varchar(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec![
        "model_b".to_string(),
        "model_c".to_string(),
        "model_d".to_string(),
    ];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "model_b".to_string(),
        "SELECT id, val AS b_val FROM source".to_string(),
    );
    sql_sources.insert(
        "model_c".to_string(),
        "SELECT id, val AS c_val FROM source".to_string(),
    );
    sql_sources.insert(
        "model_d".to_string(),
        "SELECT b.id, b.b_val, c.c_val FROM model_b b JOIN model_c c ON b.id = c.id".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    let model_d = &result.model_plans["model_d"];
    assert_eq!(model_d.inferred_schema.columns.len(), 3);
}

#[test]
fn test_schema_mismatch_detection() {
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["test_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "test_model".to_string(),
        "SELECT id, name FROM source".to_string(),
    );

    // YAML declares columns that don't match
    let mut yaml_schemas = HashMap::new();
    yaml_schemas.insert(
        "test_model".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("missing_col", varchar(), Nullability::Nullable),
        ])),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &yaml_schemas,
        &initial_catalog,
        &[],
        &[],
    );

    let model_result = &result.model_plans["test_model"];
    assert!(!model_result.mismatches.is_empty());

    let has_extra = model_result
        .mismatches
        .iter()
        .any(|m| matches!(m, SchemaMismatch::ExtraInSql { column } if column == "name"));
    let has_missing = model_result
        .mismatches
        .iter()
        .any(|m| matches!(m, SchemaMismatch::MissingFromSql { column } if column == "missing_col"));

    assert!(has_extra, "Should detect 'name' as extra in SQL");
    assert!(
        has_missing,
        "Should detect 'missing_col' as missing from SQL"
    );
}

#[test]
fn test_plan_failure_recorded() {
    let initial_catalog: SchemaCatalog = HashMap::new();

    let topo_order = vec!["bad_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "bad_model".to_string(),
        "SELECT * FROM nonexistent_table".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.contains_key("bad_model"));
    assert!(result.model_plans.is_empty());
}

// ── Additional propagation tests ────────────────────────────────────

#[test]
fn test_fan_out_propagation() {
    // source → model_a, source → model_b (one source, two consumers)
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
            make_col("amount", float64(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["model_a".to_string(), "model_b".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "model_a".to_string(),
        "SELECT id, name FROM source".to_string(),
    );
    sql_sources.insert(
        "model_b".to_string(),
        "SELECT id, amount FROM source".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    assert_eq!(
        result.model_plans["model_a"].inferred_schema.columns.len(),
        2
    );
    assert_eq!(
        result.model_plans["model_b"].inferred_schema.columns.len(),
        2
    );
}

#[test]
fn test_fan_in_propagation() {
    // source_a → final, source_b → final (two sources, one consumer)
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source_a".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val_a", varchar(), Nullability::Nullable),
        ])),
    );
    initial_catalog.insert(
        "source_b".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val_b", varchar(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["final_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "final_model".to_string(),
        "SELECT a.id, a.val_a, b.val_b FROM source_a a JOIN source_b b ON a.id = b.id".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    let schema = &result.model_plans["final_model"].inferred_schema;
    assert_eq!(schema.columns.len(), 3);
}

#[test]
fn test_column_narrowing() {
    // Source has 5 cols, downstream selects 2
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "wide_source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("a", int32(), Nullability::NotNull),
            make_col("b", int32(), Nullability::NotNull),
            make_col("c", int32(), Nullability::NotNull),
            make_col("d", int32(), Nullability::NotNull),
            make_col("e", int32(), Nullability::NotNull),
        ])),
    );

    let topo_order = vec!["narrow_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "narrow_model".to_string(),
        "SELECT a, c FROM wide_source".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    let schema = &result.model_plans["narrow_model"].inferred_schema;
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "a");
    assert_eq!(schema.columns[1].name, "c");
}

#[test]
fn test_column_rename_via_alias() {
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![make_col(
            "old_name",
            varchar(),
            Nullability::Nullable,
        )])),
    );

    let topo_order = vec!["renamed".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "renamed".to_string(),
        "SELECT old_name AS new_name FROM source".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    let schema = &result.model_plans["renamed"].inferred_schema;
    assert_eq!(schema.columns.len(), 1);
    assert_eq!(schema.columns[0].name, "new_name");
}

#[test]
fn test_deep_chain_propagation() {
    // a → b → c → d → e (5 models in chain)
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "raw".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val", varchar(), Nullability::Nullable),
        ])),
    );

    let topo_order: Vec<String> = (1..=5).map(|i| format!("model_{}", i)).collect();
    let mut sql_sources = HashMap::new();
    sql_sources.insert("model_1".to_string(), "SELECT id, val FROM raw".to_string());
    for i in 2..=5 {
        sql_sources.insert(
            format!("model_{}", i),
            format!("SELECT id, val FROM model_{}", i - 1),
        );
    }

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty());
    for i in 1..=5 {
        let name = format!("model_{}", i);
        assert!(
            result.model_plans.contains_key(&name),
            "Model {} should be planned",
            name
        );
        assert_eq!(result.model_plans[&name].inferred_schema.columns.len(), 2);
    }
}

#[test]
fn test_upstream_failure_isolation() {
    // bad_model fails, but good_model (independent) should still succeed
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![make_col(
            "id",
            int32(),
            Nullability::NotNull,
        )])),
    );

    let topo_order = vec!["bad_model".to_string(), "good_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "bad_model".to_string(),
        "SELECT * FROM nonexistent".to_string(),
    );
    sql_sources.insert(
        "good_model".to_string(),
        "SELECT id FROM source".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.contains_key("bad_model"));
    assert!(
        result.model_plans.contains_key("good_model"),
        "Independent good_model should succeed despite bad_model failure"
    );
}

#[test]
fn test_missing_sql_source_skipped() {
    // Model in topo_order but no SQL — should be skipped cleanly
    let initial_catalog: SchemaCatalog = HashMap::new();
    let topo_order = vec!["missing".to_string()];
    let sql_sources = HashMap::new(); // empty — no SQL for "missing"

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    // Should not panic, should produce empty results
    assert!(result.model_plans.is_empty());
}

#[test]
fn test_aggregate_type_preservation() {
    // SUM on DECIMAL and MAX on DATE should preserve the input types
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::Nullable),
            make_col("created_at", date(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["agg_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "agg_model".to_string(),
        "SELECT id, SUM(amount) AS total, MAX(created_at) AS latest FROM source GROUP BY id"
            .to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty(), "Planning should succeed");
    let schema = &result.model_plans["agg_model"].inferred_schema;
    assert_eq!(schema.columns.len(), 3);

    // SUM(amount) should be numeric (DECIMAL), not VARCHAR
    let total_col = schema
        .find_column("total")
        .expect("total column should exist");
    assert!(
        total_col.sql_type.is_numeric(),
        "SUM(DECIMAL) should infer a numeric type, got: {}",
        total_col.sql_type.display_name()
    );

    // MAX(created_at) should be DATE, not VARCHAR
    let latest_col = schema
        .find_column("latest")
        .expect("latest column should exist");
    assert_eq!(
        latest_col.sql_type,
        date(),
        "MAX(DATE) should infer DATE, got: {}",
        latest_col.sql_type.display_name()
    );
}

#[test]
fn test_coalesce_type_preservation() {
    // COALESCE on DECIMAL should preserve the DECIMAL type
    let mut initial_catalog: SchemaCatalog = HashMap::new();
    initial_catalog.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["coalesce_model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "coalesce_model".to_string(),
        "SELECT id, COALESCE(amount, 0) AS val FROM source".to_string(),
    );

    let result = propagate_schemas(
        &topo_order,
        &sql_sources,
        &HashMap::new(),
        &initial_catalog,
        &[],
        &[],
    );

    assert!(result.failures.is_empty(), "Planning should succeed");
    let schema = &result.model_plans["coalesce_model"].inferred_schema;
    assert_eq!(schema.columns.len(), 2);

    // COALESCE(amount, 0) should be numeric, not VARCHAR
    let val_col = schema.find_column("val").expect("val column should exist");
    assert!(
        val_col.sql_type.is_numeric(),
        "COALESCE(DECIMAL, 0) should infer a numeric type, got: {}",
        val_col.sql_type.display_name()
    );
}
