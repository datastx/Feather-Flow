//! NullabilityPropagation pass — tracks how joins affect nullability (A010-A019)

use crate::context::AnalysisContext;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::{JoinType, RelOp};
use crate::ir::types::Nullability;
use crate::pass::{AnalysisPass, Diagnostic, DiagnosticCode, Severity};
use std::collections::HashSet;

/// Nullability propagation analysis pass
pub struct NullabilityPropagation;

impl AnalysisPass for NullabilityPropagation {
    fn name(&self) -> &'static str {
        "nullability"
    }

    fn description(&self) -> &'static str {
        "Detects nullable columns from JOINs used without null guards"
    }

    fn run_model(&self, model_name: &str, ir: &RelOp, ctx: &AnalysisContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Collect columns that become nullable due to JOINs
        let mut nullable_from_join: HashSet<String> = HashSet::new();
        collect_join_nullable_columns(ir, &mut nullable_from_join);

        // Collect columns that have null guards (COALESCE, IS NULL checks)
        let mut guarded_columns: HashSet<String> = HashSet::new();
        collect_null_guarded_columns(ir, &mut guarded_columns);

        // A010: Nullable column from JOIN used without null guard
        for col_name in &nullable_from_join {
            if !guarded_columns.contains(col_name) {
                diagnostics.push(Diagnostic {
                    code: DiagnosticCode::A010,
                    severity: Severity::Warning,
                    message: format!(
                        "Column '{}' is nullable after JOIN but used without a null guard (e.g., COALESCE)",
                        col_name
                    ),
                    model: model_name.to_string(),
                    column: Some(col_name.clone()),
                    hint: Some("Wrap with COALESCE() or add an IS NOT NULL filter".to_string()),
                    pass_name: "nullability".to_string(),
                });
            }
        }

        // A011: Column declared NOT NULL in YAML but nullable after JOIN
        if let Some(yaml_schema) = ctx.model_schema(model_name) {
            for col in &yaml_schema.columns {
                if col.nullability == Nullability::NotNull && nullable_from_join.contains(&col.name)
                {
                    diagnostics.push(Diagnostic {
                        code: DiagnosticCode::A011,
                        severity: Severity::Warning,
                        message: format!(
                            "Column '{}' is declared NOT NULL in YAML but becomes nullable after JOIN",
                            col.name
                        ),
                        model: model_name.to_string(),
                        column: Some(col.name.clone()),
                        hint: Some("Add a COALESCE or filter to ensure NOT NULL".to_string()),
                        pass_name: "nullability".to_string(),
                    });
                }
            }
        }

        // A012: IS NULL check on always-NotNull column
        check_redundant_null_checks(model_name, ir, &mut diagnostics);

        diagnostics
    }
}

/// Collect column names that become nullable due to outer joins
fn collect_join_nullable_columns(op: &RelOp, nullable: &mut HashSet<String>) {
    match op {
        RelOp::Join {
            left,
            right,
            join_type,
            ..
        } => {
            collect_join_nullable_columns(left, nullable);
            collect_join_nullable_columns(right, nullable);

            match join_type {
                JoinType::LeftOuter => {
                    // Right side becomes nullable
                    for col in &right.schema().columns {
                        nullable.insert(col.name.clone());
                    }
                }
                JoinType::RightOuter => {
                    // Left side becomes nullable
                    for col in &left.schema().columns {
                        nullable.insert(col.name.clone());
                    }
                }
                JoinType::FullOuter => {
                    // Both sides become nullable
                    for col in &left.schema().columns {
                        nullable.insert(col.name.clone());
                    }
                    for col in &right.schema().columns {
                        nullable.insert(col.name.clone());
                    }
                }
                _ => {}
            }
        }
        RelOp::Project { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Filter { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Aggregate { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Sort { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::Limit { input, .. } => collect_join_nullable_columns(input, nullable),
        RelOp::SetOp { left, right, .. } => {
            collect_join_nullable_columns(left, nullable);
            collect_join_nullable_columns(right, nullable);
        }
        RelOp::Scan { .. } => {}
    }
}

/// Collect columns that have null guards applied (COALESCE, IS NOT NULL filter)
fn collect_null_guarded_columns(op: &RelOp, guarded: &mut HashSet<String>) {
    match op {
        RelOp::Project { input, columns, .. } => {
            collect_null_guarded_columns(input, guarded);
            for (_name, expr) in columns {
                collect_coalesce_columns(expr, guarded);
            }
        }
        RelOp::Filter {
            input, predicate, ..
        } => {
            collect_null_guarded_columns(input, guarded);
            // IS NOT NULL in a WHERE clause guards the column
            collect_is_not_null_columns(predicate, guarded);
        }
        RelOp::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_null_guarded_columns(left, guarded);
            collect_null_guarded_columns(right, guarded);
            if let Some(cond) = condition {
                collect_is_not_null_columns(cond, guarded);
            }
        }
        RelOp::Aggregate { input, .. } => collect_null_guarded_columns(input, guarded),
        RelOp::Sort { input, .. } => collect_null_guarded_columns(input, guarded),
        RelOp::Limit { input, .. } => collect_null_guarded_columns(input, guarded),
        RelOp::SetOp { left, right, .. } => {
            collect_null_guarded_columns(left, guarded);
            collect_null_guarded_columns(right, guarded);
        }
        RelOp::Scan { .. } => {}
    }
}

/// Collect column names wrapped in COALESCE
fn collect_coalesce_columns(expr: &TypedExpr, guarded: &mut HashSet<String>) {
    match expr {
        TypedExpr::FunctionCall { name, args, .. } if name == "COALESCE" => {
            for arg in args {
                if let TypedExpr::ColumnRef { column, .. } = arg {
                    guarded.insert(column.clone());
                }
            }
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            collect_coalesce_columns(left, guarded);
            collect_coalesce_columns(right, guarded);
        }
        TypedExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_coalesce_columns(arg, guarded);
            }
        }
        TypedExpr::Case {
            results,
            else_result,
            ..
        } => {
            for r in results {
                collect_coalesce_columns(r, guarded);
            }
            if let Some(e) = else_result {
                collect_coalesce_columns(e, guarded);
            }
        }
        _ => {}
    }
}

/// Collect columns from IS NOT NULL expressions
fn collect_is_not_null_columns(expr: &TypedExpr, guarded: &mut HashSet<String>) {
    match expr {
        TypedExpr::IsNull {
            expr: inner,
            negated: true,
        } => {
            if let TypedExpr::ColumnRef { column, .. } = inner.as_ref() {
                guarded.insert(column.clone());
            }
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            collect_is_not_null_columns(left, guarded);
            collect_is_not_null_columns(right, guarded);
        }
        _ => {}
    }
}

/// Check for redundant IS NULL/IS NOT NULL on always-NotNull columns
fn check_redundant_null_checks(model: &str, op: &RelOp, diags: &mut Vec<Diagnostic>) {
    match op {
        RelOp::Filter {
            input, predicate, ..
        } => {
            check_redundant_null_checks(model, input, diags);
            check_is_null_on_not_null(model, predicate, input.schema(), diags);
        }
        RelOp::Project { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::Join { left, right, .. } => {
            check_redundant_null_checks(model, left, diags);
            check_redundant_null_checks(model, right, diags);
        }
        RelOp::Aggregate { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::Sort { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::Limit { input, .. } => check_redundant_null_checks(model, input, diags),
        RelOp::SetOp { left, right, .. } => {
            check_redundant_null_checks(model, left, diags);
            check_redundant_null_checks(model, right, diags);
        }
        RelOp::Scan { .. } => {}
    }
}

/// Check if IS NULL is used on an always-NotNull column
fn check_is_null_on_not_null(
    model: &str,
    expr: &TypedExpr,
    schema: &crate::ir::schema::RelSchema,
    diags: &mut Vec<Diagnostic>,
) {
    match expr {
        TypedExpr::IsNull {
            expr: inner,
            negated,
        } => {
            if let TypedExpr::ColumnRef { column, .. } = inner.as_ref() {
                if let Some(col) = schema.find_column(column) {
                    if col.nullability == Nullability::NotNull {
                        let check_type = if *negated { "IS NOT NULL" } else { "IS NULL" };
                        diags.push(Diagnostic {
                            code: DiagnosticCode::A012,
                            severity: Severity::Info,
                            message: format!(
                                "{} check on column '{}' which is always NOT NULL",
                                check_type, column
                            ),
                            model: model.to_string(),
                            column: Some(column.clone()),
                            hint: Some("This check is redundant and can be removed".to_string()),
                            pass_name: "nullability".to_string(),
                        });
                    }
                }
            }
        }
        TypedExpr::BinaryOp { left, right, .. } => {
            check_is_null_on_not_null(model, left, schema, diags);
            check_is_null_on_not_null(model, right, schema, diags);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expr::{BinOp, TypedExpr};
    use crate::ir::relop::{JoinType, RelOp};
    use crate::ir::schema::RelSchema;
    use crate::ir::types::{Nullability, SqlType, TypedColumn};
    use ff_core::dag::ModelDag;
    use ff_core::Project;
    use ff_sql::ProjectLineage;
    use std::collections::HashMap;
    use std::path::Path;

    fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            source_table: None,
            sql_type: ty,
            nullability: null,
            provenance: vec![],
        }
    }

    fn make_ctx_with_yaml(yaml_schemas: HashMap<String, RelSchema>) -> AnalysisContext {
        let project = Project::load(Path::new("../../tests/fixtures/sample_project")).unwrap();
        let dag = ModelDag::build(&HashMap::new()).unwrap();
        AnalysisContext::new(project, dag, yaml_schemas, ProjectLineage::new())
    }

    #[test]
    fn test_a010_nullable_from_left_join_without_guard() {
        let left_scan = RelOp::Scan {
            table_name: "orders".to_string(),
            alias: Some("o".to_string()),
            schema: RelSchema::new(vec![
                make_col(
                    "order_id",
                    SqlType::Integer { bits: 32 },
                    Nullability::NotNull,
                ),
                make_col(
                    "customer_id",
                    SqlType::Integer { bits: 32 },
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
                    SqlType::Integer { bits: 32 },
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
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr::ColumnRef {
                    table: Some("c".to_string()),
                    column: "customer_id".to_string(),
                    resolved_type: SqlType::Integer { bits: 32 },
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
                        resolved_type: SqlType::Integer { bits: 32 },
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
                    SqlType::Integer { bits: 32 },
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
                SqlType::Integer { bits: 32 },
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
                SqlType::Integer { bits: 32 },
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
            "test_model".to_string(),
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
                .any(|d| d.code == DiagnosticCode::A011
                    && d.column.as_deref() == Some("cust_name")),
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
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let ir = RelOp::Filter {
            input: Box::new(scan),
            predicate: TypedExpr::IsNull {
                expr: Box::new(TypedExpr::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                    resolved_type: SqlType::Integer { bits: 32 },
                    nullability: Nullability::NotNull,
                }),
                negated: false,
            },
            schema: RelSchema::new(vec![make_col(
                "id",
                SqlType::Integer { bits: 32 },
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
                SqlType::Integer { bits: 32 },
                Nullability::NotNull,
            )]),
        };
        let right = RelOp::Scan {
            table_name: "b".to_string(),
            alias: None,
            schema: RelSchema::new(vec![make_col(
                "val",
                SqlType::Integer { bits: 32 },
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
}
