//! Shared test utilities for ff-analysis

#![allow(dead_code)]

use crate::context::AnalysisContext;
use crate::ir::expr::{BinOp, LiteralValue, TypedExpr};
use crate::ir::relop::{JoinType, RelOp, SetOpKind};
use crate::ir::schema::RelSchema;
use crate::ir::types::{FloatBitWidth, IntBitWidth, Nullability, SqlType, TypedColumn};
use crate::pass::{Diagnostic, DiagnosticCode};
use ff_core::dag::ModelDag;
use ff_core::Project;
use ff_sql::ProjectLineage;
use std::collections::HashMap;
use std::path::PathBuf;

// ─── Type constructors ──────────────────────────────────────────────────────

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

// ─── Expression builders ────────────────────────────────────────────────────

/// Create a column reference expression
pub fn col_ref(table: Option<&str>, col: &str, ty: SqlType, null: Nullability) -> TypedExpr {
    TypedExpr::ColumnRef {
        table: table.map(|s| s.to_string()),
        column: col.to_string(),
        resolved_type: ty,
        nullability: null,
    }
}

/// Create an integer literal expression
pub fn literal_int(v: i64) -> TypedExpr {
    TypedExpr::Literal {
        value: LiteralValue::Integer(v),
        resolved_type: int64(),
    }
}

/// Create a string literal expression
pub fn literal_str(v: &str) -> TypedExpr {
    TypedExpr::Literal {
        value: LiteralValue::String(v.to_string()),
        resolved_type: varchar(),
    }
}

/// Create a CAST expression
pub fn cast_expr(expr: TypedExpr, target: SqlType) -> TypedExpr {
    let nullability = expr.nullability();
    TypedExpr::Cast {
        expr: Box::new(expr),
        target_type: target,
        nullability,
    }
}

/// Create a binary operation expression
pub fn bin_op(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
    let nullability = left.nullability().combine(right.nullability());
    let resolved_type = if op.is_comparison() || op.is_logical() {
        boolean()
    } else {
        left.resolved_type().clone()
    };
    TypedExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        resolved_type,
        nullability,
    }
}

/// Create a function call expression
pub fn fn_call(
    name: &str,
    args: Vec<TypedExpr>,
    ret_type: SqlType,
    null: Nullability,
) -> TypedExpr {
    TypedExpr::FunctionCall {
        name: name.to_string(),
        args,
        resolved_type: ret_type,
        nullability: null,
    }
}

/// Create an IS NULL expression
pub fn is_null(expr: TypedExpr) -> TypedExpr {
    TypedExpr::IsNull {
        expr: Box::new(expr),
        negated: false,
    }
}

/// Create an IS NOT NULL expression
pub fn is_not_null(expr: TypedExpr) -> TypedExpr {
    TypedExpr::IsNull {
        expr: Box::new(expr),
        negated: true,
    }
}

// ─── RelOp builders ─────────────────────────────────────────────────────────

/// Create a Scan node
pub fn make_scan(table: &str, cols: Vec<TypedColumn>) -> RelOp {
    RelOp::Scan {
        table_name: table.to_string(),
        alias: None,
        schema: RelSchema::new(cols),
    }
}

/// Create a Scan node with an alias
pub fn make_scan_alias(table: &str, alias: &str, cols: Vec<TypedColumn>) -> RelOp {
    RelOp::Scan {
        table_name: table.to_string(),
        alias: Some(alias.to_string()),
        schema: RelSchema::new(cols),
    }
}

/// Create a Project node
pub fn make_project(input: RelOp, columns: Vec<(String, TypedExpr)>) -> RelOp {
    let schema = RelSchema::new(
        columns
            .iter()
            .map(|(name, expr)| TypedColumn {
                name: name.clone(),
                source_table: None,
                sql_type: expr.resolved_type().clone(),
                nullability: expr.nullability(),
                provenance: vec![],
            })
            .collect(),
    );
    RelOp::Project {
        input: Box::new(input),
        columns,
        schema,
    }
}

/// Create a Filter node
pub fn make_filter(input: RelOp, predicate: TypedExpr) -> RelOp {
    let schema = input.schema().clone();
    RelOp::Filter {
        input: Box::new(input),
        predicate,
        schema,
    }
}

/// Create a Join node
pub fn make_join(
    left: RelOp,
    right: RelOp,
    join_type: JoinType,
    condition: Option<TypedExpr>,
) -> RelOp {
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();

    // Apply nullability based on join type
    let (left_out, right_out) = match join_type {
        JoinType::LeftOuter => (
            left_schema,
            right_schema.with_nullability(Nullability::Nullable),
        ),
        JoinType::RightOuter => (
            left_schema.with_nullability(Nullability::Nullable),
            right_schema,
        ),
        JoinType::FullOuter => (
            left_schema.with_nullability(Nullability::Nullable),
            right_schema.with_nullability(Nullability::Nullable),
        ),
        _ => (left_schema, right_schema),
    };

    let schema = RelSchema::merge(&left_out, &right_out);
    RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition,
        schema,
    }
}

/// Create an Aggregate node
pub fn make_aggregate(
    input: RelOp,
    group_by: Vec<TypedExpr>,
    aggregates: Vec<(String, TypedExpr)>,
) -> RelOp {
    let mut cols: Vec<TypedColumn> = group_by
        .iter()
        .map(|e| TypedColumn {
            name: match e {
                TypedExpr::ColumnRef { column, .. } => column.clone(),
                _ => "expr".to_string(),
            },
            source_table: None,
            sql_type: e.resolved_type().clone(),
            nullability: e.nullability(),
            provenance: vec![],
        })
        .collect();
    cols.extend(aggregates.iter().map(|(name, expr)| TypedColumn {
        name: name.clone(),
        source_table: None,
        sql_type: expr.resolved_type().clone(),
        nullability: expr.nullability(),
        provenance: vec![],
    }));
    let schema = RelSchema::new(cols);
    RelOp::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates,
        schema,
    }
}

/// Create a SetOp node
pub fn make_set_op(left: RelOp, right: RelOp, op: SetOpKind) -> RelOp {
    let schema = left.schema().clone();
    RelOp::SetOp {
        left: Box::new(left),
        right: Box::new(right),
        op,
        schema,
    }
}

// ─── Catalog builders ───────────────────────────────────────────────────────

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

// ─── Context builders ───────────────────────────────────────────────────────

/// Create a minimal in-memory `AnalysisContext` for testing.
///
/// Uses a synthetic `Project` to avoid filesystem dependencies.
pub fn make_ctx() -> AnalysisContext {
    make_ctx_with_schemas(HashMap::new())
}

/// Create an `AnalysisContext` with YAML schemas
pub fn make_ctx_with_schemas(yaml_schemas: HashMap<String, RelSchema>) -> AnalysisContext {
    let config: ff_core::config::Config = serde_yaml::from_str("name: test_project").unwrap();
    let project = Project::new(ff_core::project::ProjectParts {
        root: PathBuf::from("/tmp/test"),
        config,
        models: HashMap::new(),
        tests: vec![],
        singular_tests: vec![],
        sources: vec![],
        exposures: vec![],
        metrics: vec![],
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
        tests: vec![],
        singular_tests: vec![],
        sources: vec![],
        exposures: vec![],
        metrics: vec![],
        functions: vec![],
    });
    let dag = ModelDag::build(dep_map).unwrap();
    AnalysisContext::new(project, dag, HashMap::new(), ProjectLineage::new())
}

// ─── Diagnostic assertions ──────────────────────────────────────────────────

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
