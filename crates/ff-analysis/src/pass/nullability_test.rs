use super::*;
use crate::ir::expr::{BinOp, LiteralValue, TypedExpr};
use crate::ir::relop::{JoinType, RelOp};
use crate::ir::schema::RelSchema;
use crate::ir::types::{IntBitWidth, Nullability, SqlType};
use crate::test_utils::*;
use ff_core::ModelName;
use std::collections::HashMap;

fn make_ctx_with_yaml(yaml_schemas: HashMap<ModelName, RelSchema>) -> AnalysisContext {
    make_ctx_with_schemas(yaml_schemas)
}

#[test]
fn test_a010_nullable_from_left_join_without_guard() {
    let left_scan = RelOp::Scan {
        table_name: "orders".to_string(),
        alias: Some("o".to_string()),
        schema: RelSchema::new(vec![
            make_col(
                "order_id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "customer_id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
        ]),
    };
    let right_scan = RelOp::Scan {
        table_name: "customers".to_string(),
        alias: Some("c".to_string()),
        schema: RelSchema::new(vec![
            make_col(
                "customer_id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "name",
                SqlType::String { max_length: None },
                Nullability::NotNull,
            ),
        ]),
    };
    let merged = RelSchema::merge(left_scan.schema(), right_scan.schema());
    let join = RelOp::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        join_type: JoinType::LeftOuter,
        condition: Some(TypedExpr::BinaryOp {
            left: Box::new(TypedExpr::ColumnRef {
                table: Some("o".to_string()),
                column: "customer_id".to_string(),
                resolved_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::NotNull,
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr::ColumnRef {
                table: Some("c".to_string()),
                column: "customer_id".to_string(),
                resolved_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::NotNull,
            }),
            resolved_type: SqlType::Boolean,
            nullability: Nullability::NotNull,
        }),
        schema: merged.clone(),
    };
    // Project without COALESCE on `name`
    let ir = RelOp::Project {
        input: Box::new(join),
        columns: vec![
            (
                "order_id".to_string(),
                TypedExpr::ColumnRef {
                    table: None,
                    column: "order_id".to_string(),
                    resolved_type: SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    nullability: Nullability::NotNull,
                },
            ),
            (
                "name".to_string(),
                TypedExpr::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                    resolved_type: SqlType::String { max_length: None },
                    nullability: Nullability::NotNull,
                },
            ),
        ],
        schema: RelSchema::new(vec![
            make_col(
                "order_id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "name",
                SqlType::String { max_length: None },
                Nullability::NotNull,
            ),
        ]),
    };

    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("test_model", &ir, &ctx);

    // `name` comes from right side of LEFT JOIN → nullable without guard → A010
    let a010s: Vec<_> = diags
        .iter()
        .filter(|d| d.code == DiagnosticCode::A010)
        .collect();
    assert!(
        a010s.iter().any(|d| d.column.as_deref() == Some("name")),
        "Expected A010 for 'name' column, got: {:?}",
        a010s
    );
}

#[test]
fn test_a010_not_emitted_when_coalesce_used() {
    let left_scan = RelOp::Scan {
        table_name: "orders".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let right_scan = RelOp::Scan {
        table_name: "customers".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "name",
            SqlType::String { max_length: None },
            Nullability::NotNull,
        )]),
    };
    let merged = RelSchema::merge(left_scan.schema(), right_scan.schema());
    let join = RelOp::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };
    // Wrap `name` in COALESCE
    let ir = RelOp::Project {
        input: Box::new(join),
        columns: vec![(
            "name".to_string(),
            TypedExpr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![
                    TypedExpr::ColumnRef {
                        table: None,
                        column: "name".to_string(),
                        resolved_type: SqlType::String { max_length: None },
                        nullability: Nullability::Nullable,
                    },
                    TypedExpr::Literal {
                        value: crate::ir::expr::LiteralValue::String("unknown".to_string()),
                        resolved_type: SqlType::String { max_length: None },
                    },
                ],
                resolved_type: SqlType::String { max_length: None },
                nullability: Nullability::NotNull,
            },
        )],
        schema: RelSchema::new(vec![make_col(
            "name",
            SqlType::String { max_length: None },
            Nullability::NotNull,
        )]),
    };

    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("test_model", &ir, &ctx);

    // name is guarded by COALESCE, so no A010 for it
    assert!(
        !diags
            .iter()
            .any(|d| d.code == DiagnosticCode::A010 && d.column.as_deref() == Some("name")),
        "Should NOT emit A010 for 'name' when wrapped in COALESCE"
    );
}

#[test]
fn test_a011_yaml_not_null_contradicts_join() {
    let left_scan = RelOp::Scan {
        table_name: "orders".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let right_scan = RelOp::Scan {
        table_name: "customers".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "cust_name",
            SqlType::String { max_length: None },
            Nullability::NotNull,
        )]),
    };
    let merged = RelSchema::merge(left_scan.schema(), right_scan.schema());
    let ir = RelOp::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };

    // YAML declares cust_name as NOT NULL
    let mut yaml_schemas = HashMap::new();
    yaml_schemas.insert(
        ModelName::new("test_model"),
        RelSchema::new(vec![make_col(
            "cust_name",
            SqlType::String { max_length: None },
            Nullability::NotNull,
        )]),
    );

    let ctx = make_ctx_with_yaml(yaml_schemas);
    let diags = NullabilityPropagation.run_model("test_model", &ir, &ctx);

    assert!(
        diags
            .iter()
            .any(|d| d.code == DiagnosticCode::A011 && d.column.as_deref() == Some("cust_name")),
        "Expected A011 for cust_name declared NOT NULL in YAML but nullable after LEFT JOIN"
    );
}

#[test]
fn test_a012_is_null_on_not_null_column() {
    let scan = RelOp::Scan {
        table_name: "t".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let ir = RelOp::Filter {
        input: Box::new(scan),
        predicate: TypedExpr::IsNull {
            expr: Box::new(TypedExpr::ColumnRef {
                table: None,
                column: "id".to_string(),
                resolved_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::NotNull,
            }),
            negated: false,
        },
        schema: RelSchema::new(vec![make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };

    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("test_model", &ir, &ctx);

    assert!(
        diags
            .iter()
            .any(|d| d.code == DiagnosticCode::A012 && d.column.as_deref() == Some("id")),
        "Expected A012 for IS NULL on NOT NULL column 'id'"
    );
}

#[test]
fn test_inner_join_no_nullable_columns() {
    let left = RelOp::Scan {
        table_name: "a".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let right = RelOp::Scan {
        table_name: "b".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "val",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        condition: None,
        schema: merged,
    };

    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("test_model", &ir, &ctx);

    // INNER JOIN doesn't make columns nullable → no A010
    assert!(
        !diags.iter().any(|d| d.code == DiagnosticCode::A010),
        "INNER JOIN should not produce A010 diagnostics"
    );
}

// ── A010: Additional nullable-from-JOIN tests ────────────────────────

#[test]
fn test_a010_right_join_left_side_nullable() {
    let left = make_scan(
        "orders",
        vec![make_col("id", int32(), Nullability::NotNull)],
    );
    let right = make_scan(
        "customers",
        vec![make_col("name", varchar(), Nullability::NotNull)],
    );
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::RightOuter,
        condition: None,
        schema: merged,
    };
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "id");
}

#[test]
fn test_a010_full_outer_join_both_sides() {
    let left = make_scan("a", vec![make_col("x", int32(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("y", int32(), Nullability::NotNull)]);
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::FullOuter,
        condition: None,
        schema: merged,
    };
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "x");
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "y");
}

#[test]
fn test_a010_left_join_chain() {
    // a LEFT JOIN b LEFT JOIN c — c columns should be nullable
    let a = make_scan("a", vec![make_col("id", int32(), Nullability::NotNull)]);
    let b = make_scan("b", vec![make_col("b_val", int32(), Nullability::NotNull)]);
    let c = make_scan("c", vec![make_col("c_val", int32(), Nullability::NotNull)]);
    let join1 = RelOp::Join {
        left: Box::new(a),
        right: Box::new(b),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: RelSchema::merge(
            &RelSchema::new(vec![make_col("id", int32(), Nullability::NotNull)]),
            &RelSchema::new(vec![make_col("b_val", int32(), Nullability::NotNull)]),
        ),
    };
    let merged = RelSchema::merge(
        join1.schema(),
        &RelSchema::new(vec![make_col("c_val", int32(), Nullability::NotNull)]),
    );
    let ir = RelOp::Join {
        left: Box::new(join1),
        right: Box::new(c),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "c_val");
}

#[test]
fn test_a010_multiple_right_columns() {
    let left = make_scan("o", vec![make_col("id", int32(), Nullability::NotNull)]);
    let right = make_scan(
        "c",
        vec![
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::NotNull),
            make_col("phone", varchar(), Nullability::NotNull),
        ],
    );
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "name");
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "email");
    assert_diagnostic_on_column(&diags, DiagnosticCode::A010, "phone");
}

#[test]
fn test_a010_is_not_null_guard_in_where() {
    let left = make_scan("o", vec![make_col("id", int32(), Nullability::NotNull)]);
    let right = make_scan("c", vec![make_col("name", varchar(), Nullability::NotNull)]);
    let merged = RelSchema::merge(left.schema(), right.schema());
    let join = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };
    // Filter with IS NOT NULL guard
    let ir = make_filter(
        join,
        is_not_null(col_ref(None, "name", varchar(), Nullability::Nullable)),
    );
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    // name should not fire A010 because it's guarded
    assert!(
        !diags
            .iter()
            .any(|d| d.code == DiagnosticCode::A010 && d.column.as_deref() == Some("name")),
        "IS NOT NULL guard should suppress A010 for 'name'"
    );
}

// ── A011: Additional YAML NOT NULL tests ─────────────────────────────

#[test]
fn test_a011_multiple_yaml_not_null_contradicted() {
    let left = make_scan("o", vec![make_col("id", int32(), Nullability::NotNull)]);
    let right = make_scan(
        "c",
        vec![
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::NotNull),
        ],
    );
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };
    let mut yaml_schemas = HashMap::new();
    yaml_schemas.insert(
        ModelName::new("m"),
        RelSchema::new(vec![
            make_col("name", varchar(), Nullability::NotNull),
            make_col("email", varchar(), Nullability::NotNull),
        ]),
    );
    let ctx = make_ctx_with_yaml(yaml_schemas);
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A011, "name");
    assert_diagnostic_on_column(&diags, DiagnosticCode::A011, "email");
}

#[test]
fn test_a011_full_outer_contradicts_yaml() {
    let left = make_scan("a", vec![make_col("id", int32(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("val", int32(), Nullability::NotNull)]);
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::FullOuter,
        condition: None,
        schema: merged,
    };
    let mut yaml_schemas = HashMap::new();
    yaml_schemas.insert(
        ModelName::new("m"),
        RelSchema::new(vec![make_col("id", int32(), Nullability::NotNull)]),
    );
    let ctx = make_ctx_with_yaml(yaml_schemas);
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A011, "id");
}

#[test]
fn test_a011_yaml_nullable_no_contradiction() {
    let left = make_scan("o", vec![make_col("id", int32(), Nullability::NotNull)]);
    let right = make_scan("c", vec![make_col("name", varchar(), Nullability::NotNull)]);
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::LeftOuter,
        condition: None,
        schema: merged,
    };
    // YAML says name is Nullable — no contradiction
    let mut yaml_schemas = HashMap::new();
    yaml_schemas.insert(
        ModelName::new("m"),
        RelSchema::new(vec![make_col("name", varchar(), Nullability::Nullable)]),
    );
    let ctx = make_ctx_with_yaml(yaml_schemas);
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert!(
        !diags
            .iter()
            .any(|d| d.code == DiagnosticCode::A011 && d.column.as_deref() == Some("name")),
        "YAML Nullable should not contradict LEFT JOIN nullable"
    );
}

#[test]
fn test_a011_inner_join_preserves_not_null() {
    let left = make_scan("o", vec![make_col("id", int32(), Nullability::NotNull)]);
    let right = make_scan("c", vec![make_col("name", varchar(), Nullability::NotNull)]);
    let merged = RelSchema::merge(left.schema(), right.schema());
    let ir = RelOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        condition: None,
        schema: merged,
    };
    let mut yaml_schemas = HashMap::new();
    yaml_schemas.insert(
        ModelName::new("m"),
        RelSchema::new(vec![make_col("name", varchar(), Nullability::NotNull)]),
    );
    let ctx = make_ctx_with_yaml(yaml_schemas);
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_no_diagnostic(&diags, DiagnosticCode::A011);
}

// ── A012: Additional redundant null check tests ──────────────────────

#[test]
fn test_a012_is_not_null_on_not_null() {
    let scan = make_scan("t", vec![make_col("id", int32(), Nullability::NotNull)]);
    let ir = make_filter(
        scan,
        is_not_null(col_ref(None, "id", int32(), Nullability::NotNull)),
    );
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A012, "id");
}

#[test]
fn test_a012_compound_where_with_redundant() {
    let scan = make_scan(
        "t",
        vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("active", boolean(), Nullability::NotNull),
        ],
    );
    let ir = make_filter(
        scan,
        bin_op(
            bin_op(
                col_ref(None, "active", boolean(), Nullability::NotNull),
                BinOp::Eq,
                TypedExpr::Literal {
                    value: LiteralValue::Boolean(true),
                    resolved_type: boolean(),
                },
            ),
            BinOp::And,
            is_not_null(col_ref(None, "id", int32(), Nullability::NotNull)),
        ),
    );
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A012, "id");
}

#[test]
fn test_a012_is_null_on_nullable_no_diagnostic() {
    let scan = make_scan(
        "t",
        vec![make_col("name", varchar(), Nullability::Nullable)],
    );
    let ir = make_filter(
        scan,
        is_null(col_ref(None, "name", varchar(), Nullability::Nullable)),
    );
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_no_diagnostic(&diags, DiagnosticCode::A012);
}

#[test]
fn test_a012_no_null_checks_no_diagnostic() {
    let scan = make_scan("t", vec![make_col("id", int32(), Nullability::NotNull)]);
    let ir = make_filter(
        scan,
        bin_op(
            col_ref(None, "id", int32(), Nullability::NotNull),
            BinOp::Gt,
            literal_int(0),
        ),
    );
    let ctx = make_ctx_with_yaml(HashMap::new());
    let diags = NullabilityPropagation.run_model("m", &ir, &ctx);
    assert_no_diagnostic(&diags, DiagnosticCode::A012);
}
