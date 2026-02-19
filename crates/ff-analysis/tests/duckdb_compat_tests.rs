//! DuckDB compatibility, type coverage, and function stub integration tests

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
        "orders".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("customer_id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::Nullable),
            make_col("status", varchar(), Nullability::Nullable),
            make_col("created_at", timestamp(), Nullability::NotNull),
            make_col("is_active", boolean(), Nullability::NotNull),
        ])),
    );
    catalog.insert(
        "customers".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::Nullable),
        ])),
    );
    catalog
}

// ── DuckDB SQL Syntax Tests ─────────────────────────────────────────────

#[test]
fn test_duckdb_type_cast_shorthand() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT id::VARCHAR FROM orders", &catalog);
    assert!(result.is_ok(), "DuckDB :: cast should plan: {:?}", result);
}

#[test]
fn test_duckdb_exclude() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT * EXCLUDE(status) FROM orders", &catalog);
    assert!(result.is_ok(), "EXCLUDE should plan: {:?}", result);
}

#[test]
fn test_duckdb_replace() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT * REPLACE(amount * 2 AS amount) FROM orders",
        &catalog,
    );
    assert!(result.is_ok(), "REPLACE should plan: {:?}", result);
}

// ── Function Stub Tests — Scalar ────────────────────────────────────────

#[test]
fn test_stub_date_trunc() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT date_trunc('month', created_at) FROM orders",
        &catalog,
    );
    assert!(result.is_ok(), "date_trunc stub: {:?}", result);
}

#[test]
fn test_stub_date_part() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT date_part('year', created_at) FROM orders", &catalog);
    assert!(result.is_ok(), "date_part stub: {:?}", result);
}

#[test]
fn test_stub_epoch() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT epoch(created_at) FROM orders", &catalog);
    assert!(result.is_ok(), "epoch stub: {:?}", result);
}

#[test]
fn test_stub_regexp_matches() {
    let catalog = standard_catalog();
    let result = plan_with_catalog(
        "SELECT regexp_matches(status, '^[A-Z]') FROM orders",
        &catalog,
    );
    assert!(result.is_ok(), "regexp_matches stub: {:?}", result);
}

#[test]
fn test_stub_coalesce() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT coalesce(status, 'unknown') FROM orders", &catalog);
    assert!(result.is_ok(), "coalesce stub: {:?}", result);
}

#[test]
fn test_stub_ifnull() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT ifnull(status, 'default') FROM orders", &catalog);
    assert!(result.is_ok(), "ifnull stub: {:?}", result);
}

#[test]
fn test_stub_nullif() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT nullif(status, '') FROM orders", &catalog);
    assert!(result.is_ok(), "nullif stub: {:?}", result);
}

#[test]
fn test_stub_hash() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT hash(name) FROM customers", &catalog);
    assert!(result.is_ok(), "hash stub: {:?}", result);
}

#[test]
fn test_stub_md5() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT md5(name) FROM customers", &catalog);
    assert!(result.is_ok(), "md5 stub: {:?}", result);
}

// ── Function Stub Tests — Aggregate ─────────────────────────────────────

#[test]
fn test_stub_agg_sum() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT sum(amount) FROM orders", &catalog);
    assert!(result.is_ok(), "sum agg: {:?}", result);
}

#[test]
fn test_stub_agg_avg() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT avg(amount) FROM orders", &catalog);
    assert!(result.is_ok(), "avg agg: {:?}", result);
}

#[test]
fn test_stub_agg_count() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT count(*) FROM orders", &catalog);
    assert!(result.is_ok(), "count agg: {:?}", result);
}

#[test]
fn test_stub_agg_string_agg() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT string_agg(status, ',') FROM orders", &catalog);
    assert!(result.is_ok(), "string_agg: {:?}", result);
}

#[test]
fn test_stub_agg_bool_and() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT bool_and(is_active) FROM orders", &catalog);
    assert!(result.is_ok(), "bool_and: {:?}", result);
}

#[test]
fn test_stub_agg_bool_or() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT bool_or(is_active) FROM orders", &catalog);
    assert!(result.is_ok(), "bool_or: {:?}", result);
}

#[test]
fn test_stub_agg_median() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT median(amount) FROM orders", &catalog);
    assert!(result.is_ok(), "median: {:?}", result);
}

#[test]
fn test_stub_agg_mode() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT mode(status) FROM orders", &catalog);
    assert!(result.is_ok(), "mode: {:?}", result);
}

// ── Unstubbed Function — Graceful Failure ───────────────────────────────

#[test]
fn test_unstubbed_function_fails_gracefully() {
    let catalog = standard_catalog();
    let result = plan_with_catalog("SELECT totally_fake_function(status) FROM orders", &catalog);
    assert!(result.is_err(), "Unknown function should fail");
}

// ── Type Compatibility Matrix Tests ─────────────────────────────────────

#[test]
fn test_compat_int_int() {
    assert!(int32().is_compatible_with(&SqlType::Integer {
        bits: IntBitWidth::I64
    }));
}

#[test]
fn test_compat_int_float() {
    assert!(int32().is_compatible_with(&float64()));
    assert!(float64().is_compatible_with(&int32()));
}

#[test]
fn test_compat_int_decimal() {
    let dec = decimal(10, 2);
    assert!(int32().is_compatible_with(&dec));
    assert!(dec.is_compatible_with(&int32()));
}

#[test]
fn test_compat_float_decimal() {
    let dec = decimal(10, 2);
    assert!(float64().is_compatible_with(&dec));
}

#[test]
fn test_compat_int_hugeint() {
    assert!(int32().is_compatible_with(&SqlType::HugeInt));
    assert!(SqlType::HugeInt.is_compatible_with(&int32()));
}

#[test]
fn test_compat_varchar_varchar() {
    let v1 = SqlType::String {
        max_length: Some(100),
    };
    let v2 = SqlType::String { max_length: None };
    assert!(v1.is_compatible_with(&v2));
}

#[test]
fn test_compat_date_timestamp() {
    assert!(SqlType::Date.is_compatible_with(&SqlType::Timestamp));
    assert!(SqlType::Timestamp.is_compatible_with(&SqlType::Date));
}

#[test]
fn test_compat_boolean_boolean() {
    assert!(SqlType::Boolean.is_compatible_with(&SqlType::Boolean));
}

#[test]
fn test_incompat_int_varchar() {
    assert!(!int32().is_compatible_with(&varchar()));
    assert!(!varchar().is_compatible_with(&int32()));
}

#[test]
fn test_incompat_int_boolean() {
    assert!(!int32().is_compatible_with(&boolean()));
}

#[test]
fn test_incompat_varchar_boolean() {
    assert!(!varchar().is_compatible_with(&boolean()));
}

#[test]
fn test_incompat_varchar_date() {
    assert!(!varchar().is_compatible_with(&SqlType::Date));
}

#[test]
fn test_incompat_int_date() {
    assert!(!int32().is_compatible_with(&SqlType::Date));
}

#[test]
fn test_compat_unknown_with_anything() {
    let unknown = SqlType::Unknown("test".to_string());
    assert!(unknown.is_compatible_with(&int32()));
    assert!(unknown.is_compatible_with(&varchar()));
    assert!(int32().is_compatible_with(&unknown));
}

#[test]
fn test_compat_uuid_uuid() {
    assert!(SqlType::Uuid.is_compatible_with(&SqlType::Uuid));
}

#[test]
fn test_compat_uuid_varchar() {
    // UUID is stored as Utf8 in Arrow, so UUID and String are compatible
    assert!(SqlType::Uuid.is_compatible_with(&varchar()));
}

#[test]
fn test_compat_json_json() {
    assert!(SqlType::Json.is_compatible_with(&SqlType::Json));
}

#[test]
fn test_compat_binary_binary() {
    assert!(SqlType::Binary.is_compatible_with(&SqlType::Binary));
}

#[test]
fn test_compat_interval_interval() {
    assert!(SqlType::Interval.is_compatible_with(&SqlType::Interval));
}

#[test]
fn test_compat_array_same_inner() {
    let a1 = SqlType::Array(Box::new(int32()));
    let a2 = SqlType::Array(Box::new(SqlType::Integer {
        bits: IntBitWidth::I64,
    }));
    assert!(a1.is_compatible_with(&a2));
}

#[test]
fn test_incompat_array_different_inner() {
    let a1 = SqlType::Array(Box::new(int32()));
    let a2 = SqlType::Array(Box::new(varchar()));
    assert!(!a1.is_compatible_with(&a2));
}

#[test]
fn test_compat_map_same_types() {
    let m1 = SqlType::Map {
        key: Box::new(varchar()),
        value: Box::new(int32()),
    };
    let m2 = SqlType::Map {
        key: Box::new(varchar()),
        value: Box::new(SqlType::Integer {
            bits: IntBitWidth::I64,
        }),
    };
    assert!(m1.is_compatible_with(&m2));
}

#[test]
fn test_incompat_map_different_key_types() {
    let m1 = SqlType::Map {
        key: Box::new(varchar()),
        value: Box::new(int32()),
    };
    let m2 = SqlType::Map {
        key: Box::new(int32()),
        value: Box::new(int32()),
    };
    assert!(!m1.is_compatible_with(&m2));
}

// ── End-to-End Propagation with DuckDB Features ─────────────────────────

#[test]
fn test_propagation_with_cast_shorthand() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "source".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", float64(), Nullability::Nullable),
        ])),
    );

    let topo_order = vec!["model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "model".to_string(),
        "SELECT id, amount::INTEGER AS int_amount FROM source".to_string(),
    );

    let result = propagate_schemas(
        &mn(topo_order),
        &ms(sql_sources),
        &HashMap::new(),
        initial.clone(),
        &[],
        &[],
    );
    assert!(
        result.failures.is_empty(),
        "Cast shorthand should plan: {:?}",
        result.failures
    );
    assert_eq!(result.model_plans["model"].inferred_schema.columns.len(), 2);
}

#[test]
fn test_propagation_with_duckdb_function() {
    let mut initial: SchemaCatalog = HashMap::new();
    initial.insert(
        "events".to_string(),
        Arc::new(RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("ts", timestamp(), Nullability::NotNull),
        ])),
    );

    let topo_order = vec!["model".to_string()];
    let mut sql_sources = HashMap::new();
    sql_sources.insert(
        "model".to_string(),
        "SELECT id, date_trunc('month', ts) AS month FROM events".to_string(),
    );

    let result = propagate_schemas(
        &mn(topo_order),
        &ms(sql_sources),
        &HashMap::new(),
        initial.clone(),
        &[],
        &[],
    );
    assert!(
        result.failures.is_empty(),
        "date_trunc should plan: {:?}",
        result.failures
    );
}
