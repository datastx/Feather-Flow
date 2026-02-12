use super::*;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::RelOp;
use crate::ir::schema::RelSchema;
use crate::ir::types::{IntBitWidth, Nullability, SqlType};
use crate::test_utils::*;

#[test]
fn test_a021_select_star_warning() {
    // Model that uses SELECT *
    let ir = RelOp::Project {
        input: Box::new(RelOp::Scan {
            table_name: "source".to_string(),
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
        }),
        columns: vec![("*".to_string(), TypedExpr::Wildcard { table: None })],
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

    // stg has a dependent so it's not terminal
    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), ir);
    // fct model just reads from stg
    models.insert(
        "fct".to_string(),
        RelOp::Scan {
            table_name: "stg".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        },
    );

    let ctx = make_ctx_with_dag(&dep_map);
    let pass = UnusedColumnDetection;
    let diags = pass.run_project(&models, &ctx);

    assert!(
        diags
            .iter()
            .any(|d| d.code == DiagnosticCode::A021 && d.model == "stg"),
        "Expected A021 for model using SELECT *"
    );
}

#[test]
fn test_terminal_model_skipped() {
    // Terminal model (no dependents) should not produce diagnostics
    let ir = RelOp::Project {
        input: Box::new(RelOp::Scan {
            table_name: "source".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        }),
        columns: vec![(
            "id".to_string(),
            TypedExpr::ColumnRef {
                table: None,
                column: "id".to_string(),
                resolved_type: SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                nullability: Nullability::NotNull,
            },
        )],
        schema: RelSchema::new(vec![make_col(
            "id",
            SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            Nullability::NotNull,
        )]),
    };

    let mut dep_map = HashMap::new();
    dep_map.insert("terminal".to_string(), vec![]);

    let mut models = HashMap::new();
    models.insert("terminal".to_string(), ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    assert!(
        diags.is_empty(),
        "Terminal model should not produce unused column diagnostics"
    );
}

#[test]
fn test_a020_unused_column_detected() {
    // stg produces id, name, internal_code
    // fct only references id and name
    let stg_ir = RelOp::Project {
        input: Box::new(RelOp::Scan {
            table_name: "raw".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        }),
        columns: vec![
            (
                "id".to_string(),
                TypedExpr::ColumnRef {
                    table: None,
                    column: "id".to_string(),
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
                    nullability: Nullability::Nullable,
                },
            ),
            (
                "internal_code".to_string(),
                TypedExpr::ColumnRef {
                    table: None,
                    column: "internal_code".to_string(),
                    resolved_type: SqlType::String { max_length: None },
                    nullability: Nullability::Nullable,
                },
            ),
        ],
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
            make_col(
                "internal_code",
                SqlType::String { max_length: None },
                Nullability::Nullable,
            ),
        ]),
    };

    let fct_ir = RelOp::Project {
        input: Box::new(RelOp::Scan {
            table_name: "stg".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        }),
        columns: vec![
            (
                "id".to_string(),
                TypedExpr::ColumnRef {
                    table: None,
                    column: "id".to_string(),
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
                    nullability: Nullability::Nullable,
                },
            ),
        ],
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

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), stg_ir);
    models.insert("fct".to_string(), fct_ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    let a020s: Vec<_> = diags
        .iter()
        .filter(|d| d.code == DiagnosticCode::A020 && d.model == "stg")
        .collect();
    assert!(
        a020s
            .iter()
            .any(|d| d.column.as_deref() == Some("internal_code")),
        "Expected A020 for 'internal_code' which is produced by stg but not consumed by fct"
    );
}

// ── A020: Additional unused column tests ────────────────────────────

#[test]
fn test_a020_multiple_unused_columns() {
    // stg produces id, name, code, internal
    // fct only references id
    let stg_ir = make_project(
        make_scan("raw", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "name".to_string(),
                col_ref(None, "name", varchar(), Nullability::Nullable),
            ),
            (
                "code".to_string(),
                col_ref(None, "code", varchar(), Nullability::Nullable),
            ),
            (
                "internal".to_string(),
                col_ref(None, "internal", varchar(), Nullability::Nullable),
            ),
        ],
    );

    let fct_ir = make_project(
        make_scan("stg", vec![]),
        vec![(
            "id".to_string(),
            col_ref(None, "id", int32(), Nullability::NotNull),
        )],
    );

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), stg_ir);
    models.insert("fct".to_string(), fct_ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    let a020s: Vec<_> = diags
        .iter()
        .filter(|d| d.code == DiagnosticCode::A020 && d.model == "stg")
        .collect();
    assert!(
        a020s.len() >= 3,
        "Expected at least 3 unused columns, got {}",
        a020s.len()
    );
}

#[test]
fn test_a020_all_columns_consumed_no_diagnostic() {
    // stg produces id, name
    // fct references both id and name
    let stg_ir = make_project(
        make_scan("raw", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "name".to_string(),
                col_ref(None, "name", varchar(), Nullability::Nullable),
            ),
        ],
    );

    let fct_ir = make_project(
        make_scan("stg", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "name".to_string(),
                col_ref(None, "name", varchar(), Nullability::Nullable),
            ),
        ],
    );

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), stg_ir);
    models.insert("fct".to_string(), fct_ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    assert_no_diagnostic(&diags, DiagnosticCode::A020);
}

#[test]
fn test_a020_column_used_in_where_not_unused() {
    // stg produces id, status
    // fct references id in SELECT and status in WHERE
    let stg_ir = make_project(
        make_scan("raw", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "status".to_string(),
                col_ref(None, "status", varchar(), Nullability::Nullable),
            ),
        ],
    );

    let fct_ir = make_filter(
        make_project(
            make_scan("stg", vec![]),
            vec![(
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            )],
        ),
        col_ref(None, "status", varchar(), Nullability::Nullable),
    );

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), stg_ir);
    models.insert("fct".to_string(), fct_ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    // status is consumed in the WHERE clause, so it should not be flagged
    let status_diags: Vec<_> = diags
        .iter()
        .filter(|d| d.code == DiagnosticCode::A020 && d.column.as_deref() == Some("status"))
        .collect();
    assert!(
        status_diags.is_empty(),
        "Column 'status' used in WHERE should not be flagged as unused"
    );
}

#[test]
fn test_a020_diamond_dag_all_consumed() {
    // stg produces id, name, amount
    // dim references id, name
    // fct references id, amount
    // All columns are consumed across the two downstreams
    let stg_ir = make_project(
        make_scan("raw", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "name".to_string(),
                col_ref(None, "name", varchar(), Nullability::Nullable),
            ),
            (
                "amount".to_string(),
                col_ref(None, "amount", float64(), Nullability::Nullable),
            ),
        ],
    );

    let dim_ir = make_project(
        make_scan("stg", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "name".to_string(),
                col_ref(None, "name", varchar(), Nullability::Nullable),
            ),
        ],
    );

    let fct_ir = make_project(
        make_scan("stg", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "amount".to_string(),
                col_ref(None, "amount", float64(), Nullability::Nullable),
            ),
        ],
    );

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("dim".to_string(), vec!["stg".to_string()]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), stg_ir);
    models.insert("dim".to_string(), dim_ir);
    models.insert("fct".to_string(), fct_ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    // All stg columns are consumed by at least one downstream
    let stg_a020s: Vec<_> = diags
        .iter()
        .filter(|d| d.code == DiagnosticCode::A020 && d.model == "stg")
        .collect();
    assert!(
        stg_a020s.is_empty(),
        "All stg columns are consumed in diamond DAG, got: {:?}",
        stg_a020s.iter().map(|d| &d.column).collect::<Vec<_>>()
    );
}

// ── A021: Additional SELECT * tests ─────────────────────────────────

#[test]
fn test_a021_select_t_star_in_non_terminal() {
    // Model uses SELECT t.* (qualified wildcard) — should also trigger A021
    let ir = RelOp::Project {
        input: Box::new(make_scan(
            "source",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ],
        )),
        columns: vec![(
            "t.*".to_string(),
            TypedExpr::Wildcard {
                table: Some("t".to_string()),
            },
        )],
        schema: RelSchema::new(vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("name", varchar(), Nullability::Nullable),
        ]),
    };

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), ir);
    models.insert("fct".to_string(), make_scan("stg", vec![]));

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    assert_has_diagnostic(&diags, DiagnosticCode::A021);
}

#[test]
fn test_a021_explicit_columns_no_diagnostic() {
    // Model uses explicit column list — A021 should NOT fire
    let ir = make_project(
        make_scan("source", vec![]),
        vec![
            (
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            ),
            (
                "name".to_string(),
                col_ref(None, "name", varchar(), Nullability::Nullable),
            ),
        ],
    );

    let mut dep_map = HashMap::new();
    dep_map.insert("stg".to_string(), vec![]);
    dep_map.insert("fct".to_string(), vec!["stg".to_string()]);

    let mut models = HashMap::new();
    models.insert("stg".to_string(), ir);
    models.insert(
        "fct".to_string(),
        make_project(
            make_scan("stg", vec![]),
            vec![(
                "id".to_string(),
                col_ref(None, "id", int32(), Nullability::NotNull),
            )],
        ),
    );

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    assert_no_diagnostic(&diags, DiagnosticCode::A021);
}

#[test]
fn test_a021_select_star_in_terminal_no_diagnostic() {
    // Terminal model (no dependents) with SELECT * — should NOT trigger A021
    let ir = RelOp::Project {
        input: Box::new(make_scan(
            "source",
            vec![make_col("id", int32(), Nullability::NotNull)],
        )),
        columns: vec![("*".to_string(), TypedExpr::Wildcard { table: None })],
        schema: RelSchema::new(vec![make_col("id", int32(), Nullability::NotNull)]),
    };

    let mut dep_map = HashMap::new();
    dep_map.insert("terminal".to_string(), vec![]);

    let mut models = HashMap::new();
    models.insert("terminal".to_string(), ir);

    let ctx = make_ctx_with_dag(&dep_map);
    let diags = UnusedColumnDetection.run_project(&models, &ctx);

    assert!(
        diags.is_empty(),
        "Terminal model with SELECT * should produce no diagnostics"
    );
}
