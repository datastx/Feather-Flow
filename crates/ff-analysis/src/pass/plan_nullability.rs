//! NullabilityPropagation pass on DataFusion LogicalPlans (A010-A012)
//!
//! Detects columns that become nullable after outer JOINs and are used
//! without null guards (COALESCE, IS NOT NULL).

use std::collections::HashSet;

use datafusion_expr::{Expr, JoinType, LogicalPlan};

use crate::context::AnalysisContext;
use crate::types::Nullability;

use super::expr_utils::collect_column_refs;
use super::plan_pass::PlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// Nullability propagation analysis pass (DataFusion LogicalPlan)
pub(crate) struct PlanNullability;

impl PlanPass for PlanNullability {
    fn name(&self) -> &'static str {
        "plan_nullability"
    }

    fn description(&self) -> &'static str {
        "Detects nullable columns from JOINs used without null guards"
    }

    fn run_model(
        &self,
        model_name: &str,
        plan: &LogicalPlan,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        let mut nullable_from_join: HashSet<String> = HashSet::new();
        collect_join_nullable_columns(plan, &mut nullable_from_join);

        let output_columns: HashSet<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        nullable_from_join.retain(|col| output_columns.contains(col));

        let mut guarded_columns: HashSet<String> = HashSet::new();
        collect_null_guarded_columns(plan, &mut guarded_columns);
        collect_aggregate_guarded_columns(plan, &mut guarded_columns);

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
                    hint: Some(
                        "Wrap with COALESCE() or add an IS NOT NULL filter".to_string(),
                    ),
                    pass_name: "plan_nullability".into(),
                });
            }
        }

        check_yaml_nullability_conflicts(model_name, ctx, &nullable_from_join, &mut diagnostics);

        check_redundant_null_checks(model_name, plan, &mut diagnostics);

        diagnostics
    }
}

/// Check YAML NOT NULL declarations against join-nullable columns (A011)
fn check_yaml_nullability_conflicts(
    model_name: &str,
    ctx: &AnalysisContext,
    nullable_from_join: &HashSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let Some(yaml_schema) = ctx.model_schema(model_name) else {
        return;
    };
    for col in &yaml_schema.columns {
        if col.nullability != Nullability::NotNull || !nullable_from_join.contains(&col.name) {
            continue;
        }
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
            pass_name: "plan_nullability".into(),
        });
    }
}

/// Insert all field names from a plan's schema into the nullable set
fn insert_schema_fields(plan: &LogicalPlan, nullable: &mut HashSet<String>) {
    for field in plan.schema().fields() {
        nullable.insert(field.name().clone());
    }
}

/// Collect column names that become nullable due to outer joins
fn collect_join_nullable_columns(plan: &LogicalPlan, nullable: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Join(join) => {
            collect_join_nullable_columns(&join.left, nullable);
            collect_join_nullable_columns(&join.right, nullable);

            match join.join_type {
                JoinType::Left => insert_schema_fields(&join.right, nullable),
                JoinType::Right => insert_schema_fields(&join.left, nullable),
                JoinType::Full => {
                    insert_schema_fields(&join.left, nullable);
                    insert_schema_fields(&join.right, nullable);
                }
                _ => {}
            }
        }
        _ => {
            for input in plan.inputs() {
                collect_join_nullable_columns(input, nullable);
            }
        }
    }
}

/// Collect columns that have null guards applied (COALESCE, IS NOT NULL filter)
fn collect_null_guarded_columns(plan: &LogicalPlan, guarded: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Projection(proj) => {
            for expr in &proj.expr {
                collect_coalesce_columns(expr, guarded);
            }
            collect_null_guarded_columns(&proj.input, guarded);
        }
        LogicalPlan::Filter(filter) => {
            collect_is_not_null_columns(&filter.predicate, guarded);
            collect_null_guarded_columns(&filter.input, guarded);
        }
        LogicalPlan::Join(join) => {
            collect_null_guarded_columns(&join.left, guarded);
            collect_null_guarded_columns(&join.right, guarded);
            if let Some(ref filter_expr) = join.filter {
                collect_is_not_null_columns(filter_expr, guarded);
            }
        }
        _ => {
            for input in plan.inputs() {
                collect_null_guarded_columns(input, guarded);
            }
        }
    }
}

/// Collect column names wrapped in COALESCE (recursing through aggregates)
fn collect_coalesce_columns(expr: &Expr, guarded: &mut HashSet<String>) {
    match expr {
        Expr::ScalarFunction(func) if func.func.name().eq_ignore_ascii_case("coalesce") => {
            for arg in &func.args {
                collect_column_refs(arg, guarded);
            }
        }
        Expr::Alias(alias) => collect_coalesce_columns(&alias.expr, guarded),
        Expr::BinaryExpr(bin) => {
            collect_coalesce_columns(&bin.left, guarded);
            collect_coalesce_columns(&bin.right, guarded);
        }
        Expr::ScalarFunction(func) => {
            for arg in &func.args {
                collect_coalesce_columns(arg, guarded);
            }
        }
        Expr::Case(case) => {
            for (_, then) in &case.when_then_expr {
                collect_coalesce_columns(then, guarded);
            }
            if let Some(ref e) = case.else_expr {
                collect_coalesce_columns(e, guarded);
            }
        }
        _ => {}
    }
}

/// Collect columns used inside aggregate function expressions.
///
/// Aggregate functions (COUNT, SUM, MAX, MIN, AVG) handle NULL values
/// internally, so columns used only inside aggregates should not trigger A010.
fn collect_aggregate_guarded_columns(plan: &LogicalPlan, guarded: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            for expr in &agg.aggr_expr {
                collect_agg_column_refs(expr, guarded);
            }
            collect_aggregate_guarded_columns(&agg.input, guarded);
        }
        _ => {
            for input in plan.inputs() {
                collect_aggregate_guarded_columns(input, guarded);
            }
        }
    }
}

/// Extract column refs from aggregate function expressions
fn collect_agg_column_refs(expr: &Expr, guarded: &mut HashSet<String>) {
    match expr {
        Expr::AggregateFunction(func) => {
            for arg in &func.params.args {
                collect_column_refs(arg, guarded);
            }
        }
        Expr::Alias(alias) => collect_agg_column_refs(&alias.expr, guarded),
        _ => {}
    }
}

/// Collect columns from IS NOT NULL expressions
fn collect_is_not_null_columns(expr: &Expr, guarded: &mut HashSet<String>) {
    match expr {
        Expr::IsNotNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                guarded.insert(col.name.clone());
            }
        }
        Expr::BinaryExpr(bin) => {
            collect_is_not_null_columns(&bin.left, guarded);
            collect_is_not_null_columns(&bin.right, guarded);
        }
        _ => {}
    }
}

/// Check for redundant IS NULL/IS NOT NULL on always-NotNull columns (A012)
fn check_redundant_null_checks(model: &str, plan: &LogicalPlan, diags: &mut Vec<Diagnostic>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            check_is_null_on_not_null(model, &filter.predicate, &filter.input, diags);
            check_redundant_null_checks(model, &filter.input, diags);
        }
        _ => {
            for input in plan.inputs() {
                check_redundant_null_checks(model, input, diags);
            }
        }
    }
}

/// Check if IS NULL / IS NOT NULL is used on a column that is always NOT NULL
fn check_is_null_on_not_null(
    model: &str,
    expr: &Expr,
    input_plan: &LogicalPlan,
    diags: &mut Vec<Diagnostic>,
) {
    match expr {
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            emit_redundant_null_check(model, expr, inner, input_plan, diags);
        }
        Expr::BinaryExpr(bin) => {
            check_is_null_on_not_null(model, &bin.left, input_plan, diags);
            check_is_null_on_not_null(model, &bin.right, input_plan, diags);
        }
        _ => {}
    }
}

fn emit_redundant_null_check(
    model: &str,
    outer_expr: &Expr,
    inner: &Expr,
    input_plan: &LogicalPlan,
    diags: &mut Vec<Diagnostic>,
) {
    let Expr::Column(col) = inner else { return };
    let schema = input_plan.schema();
    let Ok(field) = schema.field_with_unqualified_name(&col.name) else {
        return;
    };
    if field.is_nullable() {
        return;
    }
    let check_type = if matches!(outer_expr, Expr::IsNotNull(_)) {
        "IS NOT NULL"
    } else {
        "IS NULL"
    };
    diags.push(Diagnostic {
        code: DiagnosticCode::A012,
        severity: Severity::Info,
        message: format!(
            "{} check on column '{}' which is always NOT NULL",
            check_type, col.name
        ),
        model: model.to_string(),
        column: Some(col.name.clone()),
        hint: Some("This check is redundant and can be removed".to_string()),
        pass_name: "plan_nullability".into(),
    });
}
