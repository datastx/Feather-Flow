//! Shared test utilities for ff-analysis

#![allow(dead_code)]

use crate::context::AnalysisContext;
use crate::pass::{Diagnostic, DiagnosticCode};
use crate::schema::RelSchema;
use crate::types::{FloatBitWidth, IntBitWidth, Nullability, SqlType, TypedColumn};
use ff_core::dag::ModelDag;
use ff_core::ModelName;
use ff_core::Project;
use ff_sql::ProjectLineage;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Create a typed column for testing
pub fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
    TypedColumn {
        name: name.to_string(),
        source_table: None,
        sql_type: ty,
        nullability: null,
        provenance: vec![],
    }
}

/// Create a typed column with a source table tag
pub fn make_col_with_table(name: &str, table: &str, ty: SqlType, null: Nullability) -> TypedColumn {
    TypedColumn {
        name: name.to_string(),
        source_table: Some(table.to_string()),
        sql_type: ty,
        nullability: null,
        provenance: vec![],
    }
}

/// Shorthand for `SqlType::Integer { bits: IntBitWidth::I8 }`
pub fn int8() -> SqlType {
    SqlType::Integer {
        bits: IntBitWidth::I8,
    }
}

/// Shorthand for `SqlType::Integer { bits: IntBitWidth::I16 }`
pub fn int16() -> SqlType {
    SqlType::Integer {
        bits: IntBitWidth::I16,
    }
}

/// Shorthand for `SqlType::Integer { bits: IntBitWidth::I32 }`
pub fn int32() -> SqlType {
    SqlType::Integer {
        bits: IntBitWidth::I32,
    }
}

/// Shorthand for `SqlType::Integer { bits: IntBitWidth::I64 }`
pub fn int64() -> SqlType {
    SqlType::Integer {
        bits: IntBitWidth::I64,
    }
}

/// Shorthand for `SqlType::Float { bits: FloatBitWidth::F32 }`
pub fn float32() -> SqlType {
    SqlType::Float {
        bits: FloatBitWidth::F32,
    }
}

/// Shorthand for `SqlType::Float { bits: FloatBitWidth::F64 }`
pub fn float64() -> SqlType {
    SqlType::Float {
        bits: FloatBitWidth::F64,
    }
}

/// Shorthand for `SqlType::String { max_length: None }`
pub fn varchar() -> SqlType {
    SqlType::String { max_length: None }
}

/// Shorthand for `SqlType::Boolean`
pub fn boolean() -> SqlType {
    SqlType::Boolean
}

/// Shorthand for `SqlType::Date`
pub fn date() -> SqlType {
    SqlType::Date
}

/// Shorthand for `SqlType::Timestamp`
pub fn timestamp() -> SqlType {
    SqlType::Timestamp
}

/// Shorthand for `SqlType::Uuid`
pub fn uuid() -> SqlType {
    SqlType::Uuid
}

/// Shorthand for `SqlType::Decimal { precision, scale }`
pub fn decimal(precision: u16, scale: u16) -> SqlType {
    SqlType::Decimal {
        precision: Some(precision),
        scale: Some(scale),
    }
}

/// Shorthand for `SqlType::Unknown(reason)`
pub fn unknown(reason: &str) -> SqlType {
    SqlType::Unknown(reason.to_string())
}

/// Standard e-commerce test catalog with raw_orders, customers, products, payments
pub fn ecommerce_catalog() -> HashMap<String, RelSchema> {
    let mut catalog = HashMap::new();
    catalog.insert(
        "raw_orders".to_string(),
        RelSchema::new(vec![
            make_col("order_id", int32(), Nullability::NotNull),
            make_col("customer_id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::Nullable),
            make_col("status", varchar(), Nullability::Nullable),
            make_col("created_at", timestamp(), Nullability::NotNull),
        ]),
    );
    catalog.insert(
        "customers".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::Nullable),
            make_col("phone", varchar(), Nullability::Nullable),
        ]),
    );
    catalog.insert(
        "products".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::NotNull),
            make_col("price", float64(), Nullability::NotNull),
            make_col("category", varchar(), Nullability::Nullable),
        ]),
    );
    catalog.insert(
        "payments".to_string(),
        RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("order_id", int32(), Nullability::NotNull),
            make_col("amount", decimal(10, 2), Nullability::NotNull),
            make_col("method", varchar(), Nullability::Nullable),
        ]),
    );
    catalog
}

/// Create a minimal in-memory `AnalysisContext` for testing.
///
/// Uses a synthetic `Project` to avoid filesystem dependencies.
pub fn make_ctx() -> AnalysisContext {
    make_ctx_with_schemas(HashMap::new())
}

/// Create an `AnalysisContext` with YAML schemas
pub fn make_ctx_with_schemas(yaml_schemas: HashMap<ModelName, Arc<RelSchema>>) -> AnalysisContext {
    let config: ff_core::config::Config = serde_yaml::from_str("name: test_project").unwrap();
    let project = Project::new(ff_core::project::ProjectParts {
        root: PathBuf::from("/tmp/test"),
        config,
        models: HashMap::new(),
        seeds: vec![],
        tests: vec![],
        singular_tests: vec![],
        sources: vec![],
        functions: vec![],
    });
    let dag = ModelDag::build(&HashMap::new()).unwrap();
    AnalysisContext::new(project, dag, yaml_schemas, ProjectLineage::new())
}

/// Create an `AnalysisContext` with a dependency map
pub fn make_ctx_with_dag(dep_map: &HashMap<String, Vec<String>>) -> AnalysisContext {
    let config: ff_core::config::Config = serde_yaml::from_str("name: test_project").unwrap();
    let project = Project::new(ff_core::project::ProjectParts {
        root: PathBuf::from("/tmp/test"),
        config,
        models: HashMap::new(),
        seeds: vec![],
        tests: vec![],
        singular_tests: vec![],
        sources: vec![],
        functions: vec![],
    });
    let dag = ModelDag::build(dep_map).unwrap();
    AnalysisContext::new(project, dag, HashMap::new(), ProjectLineage::new())
}

/// Assert that at least one diagnostic with the given code exists
pub fn assert_has_diagnostic(diags: &[Diagnostic], code: DiagnosticCode) {
    assert!(
        diags.iter().any(|d| d.code == code),
        "Expected diagnostic {:?} but found: {:?}",
        code,
        diags.iter().map(|d| d.code).collect::<Vec<_>>()
    );
}

/// Assert that no diagnostic with the given code exists
pub fn assert_no_diagnostic(diags: &[Diagnostic], code: DiagnosticCode) {
    assert!(
        !diags.iter().any(|d| d.code == code),
        "Did NOT expect diagnostic {:?} but found it in: {:?}",
        code,
        diags
            .iter()
            .filter(|d| d.code == code)
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
}

/// Assert that a diagnostic with the given code references a specific column
pub fn assert_diagnostic_on_column(diags: &[Diagnostic], code: DiagnosticCode, column: &str) {
    assert!(
        diags
            .iter()
            .any(|d| d.code == code && d.column.as_deref() == Some(column)),
        "Expected {:?} on column '{}', found: {:?}",
        code,
        column,
        diags
            .iter()
            .filter(|d| d.code == code)
            .map(|d| (&d.column, &d.message))
            .collect::<Vec<_>>()
    );
}

/// Count diagnostics with a given code
pub fn count_diagnostics(diags: &[Diagnostic], code: DiagnosticCode) -> usize {
    diags.iter().filter(|d| d.code == code).count()
}
