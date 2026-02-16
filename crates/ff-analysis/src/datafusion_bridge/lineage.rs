//! Column-level lineage extraction from DataFusion LogicalPlans
//!
//! Walks a LogicalPlan to determine how each output column relates to
//! its source columns — whether it's a direct copy, a transformation,
//! or merely inspected (e.g. in a WHERE clause).

use std::collections::HashSet;

use datafusion_expr::{Expr, LogicalPlan};

/// How a column is used in producing the output
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageKind {
    /// Direct column reference (e.g. `SELECT a FROM t`)
    Copy,
    /// Column used in a computation (e.g. `SELECT a + b AS c`)
    Transform,
    /// Column read but not in output (e.g. in WHERE, JOIN ON, GROUP BY)
    Inspect,
}

/// A single edge in the column lineage graph
#[derive(Debug, Clone)]
pub struct ColumnLineageEdge {
    /// The output column name
    pub output_column: String,
    /// The source table name (model or external)
    pub source_table: String,
    /// The source column name
    pub source_column: String,
    /// How the source column contributes
    pub kind: LineageKind,
}

/// All lineage edges for one model
#[derive(Debug, Clone)]
pub struct ModelColumnLineage {
    /// Model name
    pub model_name: String,
    /// All column lineage edges
    pub edges: Vec<ColumnLineageEdge>,
}

/// Extract column-level lineage from a LogicalPlan
pub fn extract_column_lineage(model_name: &str, plan: &LogicalPlan) -> ModelColumnLineage {
    let mut edges = Vec::new();
    walk_plan(plan, &mut edges);
    ModelColumnLineage {
        model_name: model_name.to_string(),
        edges,
    }
}

/// Collect lineage edges for a list of expressions.
///
/// When `kind_override` is `Some`, all edges use that kind; otherwise the kind
/// is inferred from each expression via [`classify_expr`].
fn collect_expr_edges(
    exprs: &[Expr],
    kind_override: Option<LineageKind>,
    edges: &mut Vec<ColumnLineageEdge>,
) {
    for expr in exprs {
        let output_name = expr_output_name(expr);
        let mut sources = Vec::new();
        collect_column_refs(expr, &mut sources);
        if sources.is_empty() {
            continue;
        }
        let kind = kind_override.unwrap_or_else(|| classify_expr(expr));
        for (table, column) in sources {
            edges.push(ColumnLineageEdge {
                output_column: output_name.clone(),
                source_table: table,
                source_column: column,
                kind,
            });
        }
    }
}

/// Collect Inspect-kind edges from join key pairs
fn collect_join_inspect_edges(on: &[(Expr, Expr)], edges: &mut Vec<ColumnLineageEdge>) {
    for (left_key, right_key) in on {
        let mut sources = Vec::new();
        collect_column_refs(left_key, &mut sources);
        collect_column_refs(right_key, &mut sources);
        for (table, column) in sources {
            edges.push(ColumnLineageEdge {
                output_column: String::new(),
                source_table: table,
                source_column: column,
                kind: LineageKind::Inspect,
            });
        }
    }
}

/// Walk the LogicalPlan tree to collect lineage edges
fn walk_plan(plan: &LogicalPlan, edges: &mut Vec<ColumnLineageEdge>) {
    match plan {
        LogicalPlan::Projection(proj) => {
            collect_expr_edges(&proj.expr, None, edges);
            walk_plan(proj.input.as_ref(), edges);
        }
        LogicalPlan::Filter(filter) => {
            // Columns in filter predicates are Inspect
            let mut sources = Vec::new();
            collect_column_refs(&filter.predicate, &mut sources);
            for (table, column) in sources {
                edges.push(ColumnLineageEdge {
                    output_column: String::new(), // not in output
                    source_table: table,
                    source_column: column,
                    kind: LineageKind::Inspect,
                });
            }
            walk_plan(filter.input.as_ref(), edges);
        }
        LogicalPlan::Join(join) => {
            collect_join_inspect_edges(&join.on, edges);
            walk_plan(join.left.as_ref(), edges);
            walk_plan(join.right.as_ref(), edges);
        }
        LogicalPlan::Aggregate(agg) => {
            collect_expr_edges(&agg.group_expr, None, edges);
            collect_expr_edges(&agg.aggr_expr, Some(LineageKind::Transform), edges);
            walk_plan(agg.input.as_ref(), edges);
        }
        LogicalPlan::SubqueryAlias(alias) => {
            walk_plan(alias.input.as_ref(), edges);
        }
        LogicalPlan::Sort(sort) => {
            walk_plan(sort.input.as_ref(), edges);
        }
        LogicalPlan::Limit(limit) => {
            walk_plan(limit.input.as_ref(), edges);
        }
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                walk_plan(input.as_ref(), edges);
            }
        }
        LogicalPlan::TableScan(_) => {
            // Leaf node — no further traversal needed
        }
        _ => {
            // For unhandled plan nodes, try to traverse inputs
            for input in plan.inputs() {
                walk_plan(input, edges);
            }
        }
    }
}

/// Get the output name for an expression
fn expr_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => alias.name.clone(),
        Expr::Column(col) => col.name.clone(),
        _ => expr.schema_name().to_string(),
    }
}

/// Classify whether an expression is a Copy or Transform
fn classify_expr(expr: &Expr) -> LineageKind {
    match expr {
        Expr::Column(_) => LineageKind::Copy,
        Expr::Alias(alias) => classify_expr(&alias.expr),
        _ => LineageKind::Transform,
    }
}

/// Collect all column references from an expression as `(table, name)` pairs.
fn collect_column_refs(expr: &Expr, refs: &mut Vec<(String, String)>) {
    crate::pass::expr_utils::walk_expr_columns(expr, &mut |col| {
        let table = col
            .relation
            .as_ref()
            .map(|r| r.to_string())
            .unwrap_or_default();
        refs.push((table, col.name.clone()));
    });
}

/// Deduplicate lineage edges, keeping the first occurrence per (output, source) pair
pub fn deduplicate_edges(edges: &[ColumnLineageEdge]) -> Vec<ColumnLineageEdge> {
    let mut seen: HashSet<(&str, &str, &str)> = HashSet::with_capacity(edges.len());
    let mut result = Vec::with_capacity(edges.len());

    for edge in edges {
        let key = (
            edge.output_column.as_str(),
            edge.source_table.as_str(),
            edge.source_column.as_str(),
        );
        if seen.insert(key) {
            result.push(edge.clone());
        }
    }

    result
}

#[cfg(test)]
#[path = "lineage_test.rs"]
mod tests;
