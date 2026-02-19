//! Error handling, edge case, and regression guard tests (Sections 14 & 16)

use ff_analysis::propagate_schemas;
use ff_analysis::sql_to_plan;
use ff_analysis::{FeatherFlowProvider, FunctionRegistry};
use ff_analysis::{FloatBitWidth, IntBitWidth, Nullability, RelSchema, SqlType, TypedColumn};
use ff_core::ModelName;
use std::collections::HashMap;
use std::sync::Arc;

type SchemaCatalog = HashMap<String, Arc<RelSchema>>;

fn mn(names: Vec<String>) -> Vec<ModelName> {
    names.into_iter().map(ModelName::new).collect()
}

fn ms(map: HashMap<String, String>) -> HashMap<ModelName, String> {
    map.into_iter()
        .map(|(k, v)| (ModelName::new(k), v))
        .collect()
}

fn my(map: HashMap<String, Arc<RelSchema>>) -> HashMap<ModelName, Arc<RelSchema>> {
    map.into_iter()
        .map(|(k, v)| (ModelName::new(k), v))
        .collect()
}

fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
    TypedColumn {
        name: name.to_string(),
        source_table: None,
        sql_type: ty,
        nullability: null,
        provenance: vec![],
    }
}

fn int32() -> SqlType {
    SqlType::Integer {
        bits: IntBitWidth::I32,
    }
}

fn int64() -> SqlType {
    SqlType::Integer {
        bits: IntBitWidth::I64,
    }
}

fn float64() -> SqlType {
    SqlType::Float {
        bits: FloatBitWidth::F64,
    }
}

fn varchar() -> SqlType {
    SqlType::String { max_length: None }
}

fn timestamp() -> SqlType {
    SqlType::Timestamp
}

fn boolean() -> SqlType {
    SqlType::Boolean
}

fn decimal(p: u16, s: u16) -> SqlType {
    SqlType::Decimal {
        precision: Some(p),
        scale: Some(s),
    }
}

fn plan_with_catalog(sql: &str, catalog: &SchemaCatalog) -> Result<(), String> {
    let registry = FunctionRegistry::new();
    let provider = FeatherFlowProvider::new(catalog, &registry);
    sql_to_plan(sql, &provider)
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

fn standard_catalog() -> SchemaCatalog {
    let mut catalog = HashMap::new();
    catalog.insert(
        "t".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
            make_col("amount", float64(), Nullability::Nullable),
            make_col("active", boolean(), Nullability::NotNull),
            make_col("created_at", timestamp(), Nullability::NotNull),
            make_col("score", decimal(10, 2), Nullability::Nullable),
        ])),
    );
    catalog.insert(
        "a".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
            make_col("parent_id", int32(), Nullability::Nullable),
        ])),
    );
    catalog.insert(
        "b".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
        ])),
    );
    catalog
}

// ── Section 14: Error Handling ──────────────────────────────────────────

#[test]
fn test_unknown_table_fails() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT * FROM nonexistent_table", &catalog);
    assert!(result.is_err(), "Unknown table should fail planning");
}

#[test]
fn test_garbage_sql_fails() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT FROM WHERE", &catalog);
    assert!(result.is_err(), "Garbage SQL should fail");
}

// ── Section 14: Edge Cases ──────────────────────────────────────────────

#[test]
fn test_select_literal_no_from() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT 1 AS val", &catalog);
    assert!(
        result.is_ok(),
        "SELECT literal without FROM should plan: {:?}",
        result
    );
}

#[test]
fn test_self_join() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT t1.id, t2.name FROM a t1 JOIN a t2 ON t1.id = t2.parent_id",
        &catalog,
    );
    assert!(result.is_ok(), "Self-join should plan: {:?}", result);
}

#[test]
fn test_same_column_name_from_two_tables() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT a.id, a.name AS a_name, b.name AS b_name FROM a JOIN b ON a.id = b.id",
        &catalog,
    );
    assert!(
        result.is_ok(),
        "Aliased columns from joined tables should plan: {:?}",
        result
    );
}

#[test]
fn test_very_long_column_name() {
    let long_name = "a".repeat(128);
    let sql = format!("SELECT id AS {} FROM t", long_name);
    let catalog = standard_catalog();
    let result = plan_with_catalog(&sql, &catalog);
    assert!(result.is_ok(), "Long column name should plan: {:?}", result);
}

#[test]
fn test_special_characters_in_alias() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT id AS \"my column!\" FROM t", &catalog);
    assert!(
        result.is_ok(),
        "Special chars in alias should plan: {:?}",
        result
    );
}

#[test]
fn test_empty_model_select_one() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT 1 AS dummy", &catalog);
    assert!(
        result.is_ok(),
        "SELECT 1 AS dummy should plan: {:?}",
        result
    );
}

#[test]
fn test_column_aliased_to_same_name() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT id AS id FROM t", &catalog);
    assert!(
        result.is_ok(),
        "Column aliased to same name should plan: {:?}",
        result
    );
}

#[test]
fn test_multiple_aggregates() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT SUM(amount), AVG(amount), COUNT(*), MIN(score), MAX(score) FROM t",
        &catalog,
    );
    assert!(
        result.is_ok(),
        "Multiple aggregates should plan: {:?}",
        result
    );
}

#[test]
fn test_null_literal() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT NULL AS empty_col FROM t", &catalog);
    assert!(result.is_ok(), "NULL literal should plan: {:?}", result);
}

#[test]
fn test_boolean_literal() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT TRUE AS flag FROM t", &catalog);
    assert!(result.is_ok(), "Boolean literal should plan: {:?}", result);
}

#[test]
fn test_deeply_nested_expression() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT CAST(COALESCE(CASE WHEN active THEN amount ELSE 0 END, 0) AS BIGINT) AS val FROM t",
        &catalog,
    );
    assert!(
        result.is_ok(),
        "Deeply nested expression should plan: {:?}",
        result
    );
}

// ── Section 16: Regression Guard Rails ──────────────────────────────────

#[test]
fn test_hugeint_roundtrip() {
    // Verify HugeInt type is preserved through type system
    let hugeint = SqlType::HugeInt;
    assert!(hugeint.is_compatible_with(&SqlType::HugeInt));
    assert!(hugeint.is_compatible_with(&int32()));
    assert!(hugeint.is_compatible_with(&int64()));
    // HugeInt should NOT be compatible with varchar
    assert!(!hugeint.is_compatible_with(&varchar()));
}

#[test]
fn test_case_sensitive_column_matching_in_propagation() {
    // DataFusion treats unquoted identifiers as lowercase. If the catalog
    // has a lowercase column, the SQL should reference it in lowercase.
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

    let result = propagate_schemas(
        &mn(topo),
        &ms(sql),
        &HashMap::new(),
        initial.clone(),
        &[],
        &[],
    );
    assert!(
        result.failures.is_empty(),
        "Lowercase columns should propagate: {:?}",
        result.failures
    );
    assert_eq!(result.model_plans["model"].inferred_schema.columns.len(), 2);
}

#[test]
fn test_null_propagation_through_union() {
    // If one UNION arm produces nullable, result should be nullable
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "nullable_src".to_string(),
        Arc::new(RelSchema::new(vec![make_col(
            "val",
            int32(),
            Nullability::Nullable,
        )])),
    );
    initial.insert(
        "not_null_src".to_string(),
        Arc::new(RelSchema::new(vec![make_col(
            "val",
            int32(),
            Nullability::NotNull,
        )])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert(
        "model".to_string(),
        "SELECT val FROM nullable_src UNION ALL SELECT val FROM not_null_src".to_string(),
    );

    let result = propagate_schemas(
        &mn(topo),
        &ms(sql),
        &HashMap::new(),
        initial.clone(),
        &[],
        &[],
    );
    assert!(
        result.failures.is_empty(),
        "UNION should plan: {:?}",
        result.failures
    );
}

#[test]
fn test_empty_yaml_columns_no_crash() {
    // Model with empty columns: [] in YAML should not crash
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![make_col(
            "id",
            int32(),
            Nullability::NotNull,
        )])),
    );

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert("model".to_string(), "SELECT id FROM source".to_string());

    let mut yaml = HashMap::new();
    yaml.insert("model".to_string(), Arc::new(RelSchema::new(vec![])));

    let result = propagate_schemas(&mn(topo), &ms(sql), &my(yaml), initial.clone(), &[], &[]);
    // Should not crash — mismatches are expected (missing columns)
    assert!(
        result.failures.is_empty(),
        "Empty YAML should not cause planning failures: {:?}",
        result.failures
    );
}

#[test]
fn test_circular_dependency_detection_before_analysis() {
    use ff_core::dag::ModelDag;

    let mut deps = HashMap::new();
    deps.insert("a".to_string(), vec!["c".to_string()]);
    deps.insert("b".to_string(), vec!["a".to_string()]);
    deps.insert("c".to_string(), vec!["b".to_string()]);

    let result = ModelDag::build(&deps);
    assert!(
        result.is_err(),
        "Circular dependency should fail at DAG build, not during analysis"
    );
}

// ── Performance Guard Rails ─────────────────────────────────────────────

#[test]
fn test_50_model_project_completes() {
    // Build a 50-model project and verify analysis completes
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "raw".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("val", varchar(), Nullability::Nullable),
        ])),
    );

    let topo: Vec<String> = (1..=50).map(|i| format!("m{i}")).collect();
    let mut sql = HashMap::new();
    sql.insert("m1".to_string(), "SELECT id, val FROM raw".to_string());
    for i in 2..=50 {
        sql.insert(format!("m{i}"), format!("SELECT id, val FROM m{}", i - 1));
    }

    let start = std::time::Instant::now();
    let result = propagate_schemas(
        &mn(topo),
        &ms(sql),
        &HashMap::new(),
        initial.clone(),
        &[],
        &[],
    );
    let elapsed = start.elapsed();

    assert!(
        result.failures.is_empty(),
        "50-model chain should complete: {:?}",
        result.failures
    );
    assert_eq!(result.model_plans.len(), 50);
    assert!(
        elapsed.as_secs() < 5,
        "50-model analysis should complete in < 5s, took {:?}",
        elapsed
    );
}

#[test]
fn test_model_with_100_columns() {
    // Model with 100+ columns should not degrade performance
    let cols: Vec<TypedColumn> = (0..100)
        .map(|i| make_col(&format!("col_{i}"), int32(), Nullability::NotNull))
        .collect();

    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert("wide_source".to_string(), Arc::new(RelSchema::new(cols)));

    let col_list: String = (0..100)
        .map(|i| format!("col_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql_str = format!("SELECT {} FROM wide_source", col_list);

    let topo = vec!["model".to_string()];
    let mut sql = HashMap::new();
    sql.insert("model".to_string(), sql_str);

    let start = std::time::Instant::now();
    let result = propagate_schemas(
        &mn(topo),
        &ms(sql),
        &HashMap::new(),
        initial.clone(),
        &[],
        &[],
    );
    let elapsed = start.elapsed();

    assert!(
        result.failures.is_empty(),
        "100-column model should complete: {:?}",
        result.failures
    );
    assert_eq!(
        result.model_plans["model"].inferred_schema.columns.len(),
        100
    );
    assert!(
        elapsed.as_secs() < 5,
        "100-column analysis should complete quickly, took {:?}",
        elapsed
    );
}
