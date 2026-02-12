use super::*;
use crate::ir::expr::{BinOp, TypedExpr};
use crate::ir::relop::{RelOp, SetOpKind};
use crate::ir::schema::RelSchema;
use crate::ir::types::{FloatBitWidth, IntBitWidth, Nullability, SqlType};
use crate::test_utils::*;

#[test]
fn test_a001_unknown_type_for_column() {
    let ir = RelOp::Scan {
        table_name: "test_table".to_string(),
        alias: None,
        schema: RelSchema::new(vec![
            make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "data",
                SqlType::Unknown("no type declared".to_string()),
                Nullability::Unknown,
            ),
        ]),
    };
    let ctx = make_ctx();
    let pass = TypeInference;
    let diags = pass.run_model("test_model", &ir, &ctx);

    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].code, DiagnosticCode::A001);
    assert_eq!(diags[0].severity, Severity::Info);
    assert!(diags[0].message.contains("data"));
}

#[test]
fn test_a002_union_type_mismatch() {
    let left = RelOp::Scan {
        table_name: "a".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "val",
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
            SqlType::String { max_length: None },
            Nullability::NotNull,
        )]),
    };
    let ir = RelOp::SetOp {
        left: Box::new(left),
        right: Box::new(right),
        op: SetOpKind::Union,
        schema: RelSchema::new(vec![make_col(
            "val",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let ctx = make_ctx();
    let diags = TypeInference.run_model("test_model", &ir, &ctx);

    assert!(diags.iter().any(|d| d.code == DiagnosticCode::A002));
    let a002 = diags
        .iter()
        .find(|d| d.code == DiagnosticCode::A002)
        .unwrap();
    assert_eq!(a002.severity, Severity::Warning);
    assert!(a002.message.contains("val"));
}

#[test]
fn test_a003_union_column_count_mismatch() {
    let left = RelOp::Scan {
        table_name: "a".to_string(),
        alias: None,
        schema: RelSchema::new(vec![
            make_col(
                "x",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "y",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
        ]),
    };
    let right = RelOp::Scan {
        table_name: "b".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "x",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };
    let ir = RelOp::SetOp {
        left: Box::new(left),
        right: Box::new(right),
        op: SetOpKind::UnionAll,
        schema: RelSchema::empty(),
    };
    let ctx = make_ctx();
    let diags = TypeInference.run_model("test_model", &ir, &ctx);

    assert!(diags.iter().any(|d| d.code == DiagnosticCode::A003));
    let a003 = diags
        .iter()
        .find(|d| d.code == DiagnosticCode::A003)
        .unwrap();
    assert_eq!(a003.severity, Severity::Error);
    assert!(a003.message.contains("left=2"));
    assert!(a003.message.contains("right=1"));
}

#[test]
fn test_a004_sum_on_string_column() {
    let input = RelOp::Scan {
        table_name: "t".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "name",
            SqlType::String { max_length: None },
            Nullability::Nullable,
        )]),
    };
    let ir = RelOp::Aggregate {
        input: Box::new(input),
        group_by: vec![],
        aggregates: vec![(
            "total".to_string(),
            TypedExpr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![TypedExpr::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                    resolved_type: SqlType::String { max_length: None },
                    nullability: Nullability::Nullable,
                }],
                resolved_type: SqlType::String { max_length: None },
                nullability: Nullability::Nullable,
            },
        )],
        schema: RelSchema::new(vec![make_col(
            "total",
            SqlType::String { max_length: None },
            Nullability::Nullable,
        )]),
    };
    let ctx = make_ctx();
    let diags = TypeInference.run_model("test_model", &ir, &ctx);

    assert!(diags.iter().any(|d| d.code == DiagnosticCode::A004));
    let a004 = diags
        .iter()
        .find(|d| d.code == DiagnosticCode::A004)
        .unwrap();
    assert_eq!(a004.severity, Severity::Warning);
    assert!(a004.message.contains("SUM"));
}

#[test]
fn test_a005_lossy_cast() {
    let input = RelOp::Scan {
        table_name: "t".to_string(),
        alias: None,
        schema: RelSchema::empty(),
    };
    let ir = RelOp::Project {
        input: Box::new(input),
        columns: vec![(
            "truncated".to_string(),
            TypedExpr::Cast {
                expr: Box::new(TypedExpr::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                    resolved_type: SqlType::Float {
                        bits: FloatBitWidth::F64,
                    },
                    nullability: Nullability::Nullable,
                }),
                target_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::Nullable,
            },
        )],
        schema: RelSchema::new(vec![make_col(
            "truncated",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::Nullable,
        )]),
    };
    let ctx = make_ctx();
    let diags = TypeInference.run_model("test_model", &ir, &ctx);

    assert!(diags.iter().any(|d| d.code == DiagnosticCode::A005));
    let a005 = diags
        .iter()
        .find(|d| d.code == DiagnosticCode::A005)
        .unwrap();
    assert_eq!(a005.severity, Severity::Info);
    assert!(a005.message.contains("lossy"));
}

#[test]
fn test_no_diagnostics_for_clean_scan() {
    let ir = RelOp::Scan {
        table_name: "clean_table".to_string(),
        alias: None,
        schema: RelSchema::new(vec![
            make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "name",
                SqlType::String { max_length: None },
                Nullability::Nullable,
            ),
        ]),
    };
    let ctx = make_ctx();
    let diags = TypeInference.run_model("test_model", &ir, &ctx);
    assert!(diags.is_empty());
}

#[test]
fn test_compatible_union_no_diagnostics() {
    let left = RelOp::Scan {
        table_name: "a".to_string(),
        alias: None,
        schema: RelSchema::new(vec![make_col(
            "val",
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
                bits: IntBitWidth::I64,
            },
            Nullability::NotNull,
        )]),
    };
    let ir = RelOp::SetOp {
        left: Box::new(left),
        right: Box::new(right),
        op: SetOpKind::Union,
        schema: RelSchema::new(vec![make_col(
            "val",
            SqlType::Integer {
                bits: IntBitWidth::I64,
            },
            Nullability::NotNull,
        )]),
    };
    let ctx = make_ctx();
    let diags = TypeInference.run_model("test_model", &ir, &ctx);
    // INTEGER and BIGINT are compatible — no A002
    assert!(diags.iter().all(|d| d.code != DiagnosticCode::A002));
}

// ── A001: Additional unknown-type tests ──────────────────────────────

#[test]
fn test_a001_multiple_unknown_columns() {
    let ir = make_scan(
        "src",
        vec![
            make_col("a", unknown("no type"), Nullability::Unknown),
            make_col("b", unknown("no type"), Nullability::Unknown),
            make_col("c", unknown("no type"), Nullability::Unknown),
        ],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_eq!(count_diagnostics(&diags, DiagnosticCode::A001), 3);
}

#[test]
fn test_a001_mix_known_and_unknown() {
    let ir = make_scan(
        "src",
        vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("data", unknown("no type"), Nullability::Unknown),
        ],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_eq!(count_diagnostics(&diags, DiagnosticCode::A001), 1);
    assert_diagnostic_on_column(&diags, DiagnosticCode::A001, "data");
}

#[test]
fn test_a001_all_typed_no_diagnostic() {
    let ir = make_scan(
        "src",
        vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
        ],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A001);
}

#[test]
fn test_a001_computed_column_with_known_inputs_no_diagnostic() {
    let scan = make_scan("src", vec![make_col("id", int32(), Nullability::NotNull)]);
    let ir = make_project(
        scan,
        vec![(
            "inc".to_string(),
            bin_op(
                col_ref(None, "id", int32(), Nullability::NotNull),
                BinOp::Plus,
                literal_int(1),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A001);
}

// ── A002: Additional UNION type mismatch tests ───────────────────────

#[test]
fn test_a002_boolean_vs_integer() {
    let left = make_scan("a", vec![make_col("val", boolean(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("val", int32(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_date_vs_varchar() {
    let left = make_scan("a", vec![make_col("val", date(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("val", varchar(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_intersect_type_mismatch() {
    let left = make_scan("a", vec![make_col("val", int32(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("val", varchar(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::Intersect);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_except_type_mismatch() {
    let left = make_scan("a", vec![make_col("val", int32(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("val", varchar(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::Except);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_three_way_union_middle_mismatch() {
    let a = make_scan("a", vec![make_col("val", int32(), Nullability::NotNull)]);
    let b = make_scan("b", vec![make_col("val", varchar(), Nullability::NotNull)]);
    let c = make_scan("c", vec![make_col("val", int32(), Nullability::NotNull)]);
    // (a UNION ALL b) UNION ALL c
    let inner = make_set_op(a, b, SetOpKind::UnionAll);
    let ir = make_set_op(inner, c, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_float_union_decimal_compatible() {
    let left = make_scan("a", vec![make_col("val", float64(), Nullability::NotNull)]);
    let right = make_scan(
        "b",
        vec![make_col("val", decimal(10, 2), Nullability::NotNull)],
    );
    let ir = make_set_op(left, right, SetOpKind::Union);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_varchar_different_lengths_compatible() {
    let left = make_scan(
        "a",
        vec![make_col(
            "val",
            SqlType::String {
                max_length: Some(50),
            },
            Nullability::NotNull,
        )],
    );
    let right = make_scan(
        "b",
        vec![make_col(
            "val",
            SqlType::String {
                max_length: Some(100),
            },
            Nullability::NotNull,
        )],
    );
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_date_timestamp_compatible() {
    let left = make_scan("a", vec![make_col("val", date(), Nullability::NotNull)]);
    let right = make_scan(
        "b",
        vec![make_col("val", timestamp(), Nullability::NotNull)],
    );
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A002);
}

#[test]
fn test_a002_identical_types_no_diagnostic() {
    let left = make_scan("a", vec![make_col("val", int32(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("val", int32(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A002);
}

// ── A003: Additional column count mismatch tests ─────────────────────

#[test]
fn test_a003_left_1_right_3() {
    let left = make_scan("a", vec![make_col("x", int32(), Nullability::NotNull)]);
    let right = make_scan(
        "b",
        vec![
            make_col("x", int32(), Nullability::NotNull),
            make_col("y", int32(), Nullability::NotNull),
            make_col("z", int32(), Nullability::NotNull),
        ],
    );
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A003);
}

#[test]
fn test_a003_intersect_count_mismatch() {
    let left = make_scan(
        "a",
        vec![
            make_col("x", int32(), Nullability::NotNull),
            make_col("y", int32(), Nullability::NotNull),
        ],
    );
    let right = make_scan("b", vec![make_col("x", int32(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::Intersect);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A003);
}

#[test]
fn test_a003_except_count_mismatch() {
    let left = make_scan(
        "a",
        vec![
            make_col("x", int32(), Nullability::NotNull),
            make_col("y", int32(), Nullability::NotNull),
        ],
    );
    let right = make_scan("b", vec![make_col("x", int32(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::Except);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A003);
}

#[test]
fn test_a003_matching_counts_no_diagnostic() {
    let left = make_scan(
        "a",
        vec![
            make_col("x", int32(), Nullability::NotNull),
            make_col("y", int32(), Nullability::NotNull),
            make_col("z", int32(), Nullability::NotNull),
        ],
    );
    let right = make_scan(
        "b",
        vec![
            make_col("a", int32(), Nullability::NotNull),
            make_col("b", int32(), Nullability::NotNull),
            make_col("c", int32(), Nullability::NotNull),
        ],
    );
    let ir = make_set_op(left, right, SetOpKind::UnionAll);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A003);
}

#[test]
fn test_a003_single_column_union_no_diagnostic() {
    let left = make_scan("a", vec![make_col("x", int32(), Nullability::NotNull)]);
    let right = make_scan("b", vec![make_col("x", int32(), Nullability::NotNull)]);
    let ir = make_set_op(left, right, SetOpKind::Union);
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A003);
}

// ── A004: Additional SUM/AVG on string tests ─────────────────────────

#[test]
fn test_a004_avg_on_varchar() {
    let input = make_scan(
        "t",
        vec![make_col("status", varchar(), Nullability::Nullable)],
    );
    let ir = make_aggregate(
        input,
        vec![],
        vec![(
            "avg_status".to_string(),
            fn_call(
                "AVG",
                vec![col_ref(None, "status", varchar(), Nullability::Nullable)],
                varchar(),
                Nullability::Nullable,
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A004);
}

#[test]
fn test_a004_sum_on_integer_no_diagnostic() {
    let input = make_scan(
        "t",
        vec![make_col("amount", int32(), Nullability::Nullable)],
    );
    let ir = make_aggregate(
        input,
        vec![],
        vec![(
            "total".to_string(),
            fn_call(
                "SUM",
                vec![col_ref(None, "amount", int32(), Nullability::Nullable)],
                int32(),
                Nullability::Nullable,
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A004);
}

#[test]
fn test_a004_avg_on_decimal_no_diagnostic() {
    let input = make_scan(
        "t",
        vec![make_col("price", decimal(10, 2), Nullability::Nullable)],
    );
    let ir = make_aggregate(
        input,
        vec![],
        vec![(
            "avg_price".to_string(),
            fn_call(
                "AVG",
                vec![col_ref(
                    None,
                    "price",
                    decimal(10, 2),
                    Nullability::Nullable,
                )],
                decimal(10, 2),
                Nullability::Nullable,
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A004);
}

#[test]
fn test_a004_sum_on_float_no_diagnostic() {
    let input = make_scan(
        "t",
        vec![make_col("weight", float64(), Nullability::Nullable)],
    );
    let ir = make_aggregate(
        input,
        vec![],
        vec![(
            "total_weight".to_string(),
            fn_call(
                "SUM",
                vec![col_ref(None, "weight", float64(), Nullability::Nullable)],
                float64(),
                Nullability::Nullable,
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A004);
}

#[test]
fn test_a004_count_on_varchar_no_diagnostic() {
    let input = make_scan(
        "t",
        vec![make_col("name", varchar(), Nullability::Nullable)],
    );
    let ir = make_aggregate(
        input,
        vec![],
        vec![(
            "cnt".to_string(),
            fn_call(
                "COUNT",
                vec![col_ref(None, "name", varchar(), Nullability::Nullable)],
                int64(),
                Nullability::NotNull,
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A004);
}

#[test]
fn test_a004_min_on_varchar_no_diagnostic() {
    let input = make_scan(
        "t",
        vec![make_col("name", varchar(), Nullability::Nullable)],
    );
    let ir = make_aggregate(
        input,
        vec![],
        vec![(
            "min_name".to_string(),
            fn_call(
                "MIN",
                vec![col_ref(None, "name", varchar(), Nullability::Nullable)],
                varchar(),
                Nullability::Nullable,
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A004);
}

// ── A005: Additional lossy cast tests ────────────────────────────────

#[test]
fn test_a005_decimal_to_integer() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                col_ref(None, "amount", decimal(10, 2), Nullability::Nullable),
                int32(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_varchar_to_integer() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                col_ref(None, "code", varchar(), Nullability::Nullable),
                int32(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_varchar_to_float() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                col_ref(None, "rating", varchar(), Nullability::Nullable),
                float64(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_timestamp_to_date() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                col_ref(None, "created_at", timestamp(), Nullability::Nullable),
                date(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_has_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_nested_lossy_cast() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                cast_expr(
                    col_ref(None, "name", varchar(), Nullability::Nullable),
                    float64(),
                ),
                int32(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    // Both casts are lossy: varchar→float64 and float64→int32
    assert_eq!(count_diagnostics(&diags, DiagnosticCode::A005), 2);
}

#[test]
fn test_a005_integer_to_bigint_safe() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(col_ref(None, "id", int32(), Nullability::NotNull), int64()),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_integer_to_float_safe() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                col_ref(None, "id", int32(), Nullability::NotNull),
                float64(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_date_to_timestamp_safe() {
    let scan = make_scan("t", vec![]);
    let ir = make_project(
        scan,
        vec![(
            "val".to_string(),
            cast_expr(
                col_ref(None, "d", date(), Nullability::NotNull),
                timestamp(),
            ),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A005);
}

#[test]
fn test_a005_no_cast_no_diagnostic() {
    let scan = make_scan("t", vec![make_col("id", int32(), Nullability::NotNull)]);
    let ir = make_project(
        scan,
        vec![(
            "id".to_string(),
            col_ref(None, "id", int32(), Nullability::NotNull),
        )],
    );
    let diags = TypeInference.run_model("m", &ir, &make_ctx());
    assert_no_diagnostic(&diags, DiagnosticCode::A005);
}
