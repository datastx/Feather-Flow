//! Column-level lineage extraction from SQL AST
//!
//! This module extracts column-level lineage information from SQL queries,
//! tracking which source columns flow into which output columns.

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Query, Select, SelectItem, SelectItemQualifiedWildcardKind,
    SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::collections::{HashMap, HashSet};

/// Direction for recursive lineage tracing.
#[derive(Debug, Clone, Copy)]
enum TraceDirection {
    /// Follow edges from target to source (upstream).
    Upstream,
    /// Follow edges from source to target (downstream).
    Downstream,
}

/// Expression type for a lineage column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExprType {
    /// Unknown or unrecognized expression
    #[default]
    Unknown,
    /// Direct column reference (pass-through)
    Column,
    /// Function call
    Function,
    /// Literal value
    Literal,
    /// Computed expression (binary op, unary op, etc.)
    Expression,
    /// SELECT * or table.*
    Wildcard,
    /// CAST expression
    Cast,
    /// CASE expression
    Case,
    /// Scalar subquery
    Subquery,
}

impl std::fmt::Display for ExprType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExprType::Unknown => write!(f, "unknown"),
            ExprType::Column => write!(f, "column"),
            ExprType::Function => write!(f, "function"),
            ExprType::Literal => write!(f, "literal"),
            ExprType::Expression => write!(f, "expression"),
            ExprType::Wildcard => write!(f, "wildcard"),
            ExprType::Cast => write!(f, "cast"),
            ExprType::Case => write!(f, "case"),
            ExprType::Subquery => write!(f, "subquery"),
        }
    }
}

/// Represents a column reference with its source table
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Table name (or alias) the column belongs to
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

impl ColumnRef {
    /// Create from a simple column name
    pub(crate) fn simple(column: &str) -> Self {
        Self {
            table: None,
            column: column.to_string(),
        }
    }

    /// Create from table.column
    pub(crate) fn qualified(table: &str, column: &str) -> Self {
        Self {
            table: Some(table.to_string()),
            column: column.to_string(),
        }
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.table {
            Some(t) => write!(f, "{}.{}", t, self.column),
            None => write!(f, "{}", self.column),
        }
    }
}

/// Represents column lineage for a single output column
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnLineage {
    /// The output column name
    pub output_column: String,
    /// Source columns that contribute to this output
    pub source_columns: HashSet<ColumnRef>,
    /// Whether this is a direct pass-through (no transformation)
    pub is_direct: bool,
    /// Expression type
    pub expr_type: ExprType,
}

impl ColumnLineage {
    /// Create a new column lineage entry
    pub(crate) fn new(output_column: &str) -> Self {
        Self {
            output_column: output_column.to_string(),
            source_columns: HashSet::new(),
            is_direct: false,
            expr_type: ExprType::Unknown,
        }
    }

    /// Create a direct column reference (pass-through)
    pub(crate) fn direct(output_column: &str, source: ColumnRef) -> Self {
        let mut lineage = Self::new(output_column);
        lineage.source_columns.insert(source);
        lineage.is_direct = true;
        lineage.expr_type = ExprType::Column;
        lineage
    }

    /// Create from a function call
    pub(crate) fn from_function(output_column: &str, sources: HashSet<ColumnRef>) -> Self {
        Self {
            output_column: output_column.to_string(),
            source_columns: sources,
            is_direct: false,
            expr_type: ExprType::Function,
        }
    }

    /// Create from a literal value
    pub(crate) fn literal(output_column: &str) -> Self {
        Self {
            output_column: output_column.to_string(),
            source_columns: HashSet::new(),
            is_direct: false,
            expr_type: ExprType::Literal,
        }
    }
}

/// Model-level lineage containing all column lineages
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelLineage {
    /// Model name
    pub model_name: String,
    /// Column lineages for this model
    pub columns: Vec<ColumnLineage>,
    /// Table aliases used in the query
    pub table_aliases: HashMap<String, String>,
    /// Source tables referenced
    pub source_tables: HashSet<String>,
}

impl ModelLineage {
    /// Create a new model lineage
    pub(crate) fn new(model_name: &str) -> Self {
        Self {
            model_name: model_name.to_string(),
            columns: Vec::new(),
            table_aliases: HashMap::new(),
            source_tables: HashSet::new(),
        }
    }

    /// Add a column lineage
    pub(crate) fn add_column(&mut self, lineage: ColumnLineage) {
        self.columns.push(lineage);
    }

    #[cfg(test)]
    /// Get column lineage by name
    pub(crate) fn get_column(&self, name: &str) -> Option<&ColumnLineage> {
        self.columns.iter().find(|c| c.output_column == name)
    }
}

/// A cross-model lineage edge connecting a source column to a target column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Source model name
    pub source_model: String,
    /// Source column name
    pub source_column: String,
    /// Target model name
    pub target_model: String,
    /// Target column name
    pub target_column: String,
    /// Whether this is a direct pass-through (no transformation)
    pub is_direct: bool,
    /// Expression type
    pub expr_type: ExprType,
    /// Data classification from source column (propagated from schema)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
}

/// Project-wide column lineage across all models
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectLineage {
    /// Per-model lineage
    pub models: HashMap<String, ModelLineage>,
    /// Cross-model lineage edges
    pub edges: Vec<LineageEdge>,
}

impl ProjectLineage {
    /// Create a new empty project lineage
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a model's lineage
    pub fn add_model_lineage(&mut self, lineage: ModelLineage) {
        self.models.insert(lineage.model_name.clone(), lineage);
    }

    /// Resolve cross-model edges by matching source tables to known models
    pub fn resolve_edges(&mut self, known_models: &HashSet<&str>) {
        let new_edges: Vec<_> = self
            .models
            .iter()
            .flat_map(|(target, lineage)| {
                lineage.columns.iter().flat_map(move |cl| {
                    cl.source_columns.iter().filter_map(move |sr| {
                        resolve_single_edge(target, lineage, cl, sr, known_models)
                    })
                })
            })
            .collect();

        self.edges.extend(new_edges);
    }

    /// Trace a column upstream — find all source columns that contribute to it
    pub fn trace_column(&self, model: &str, column: &str) -> Vec<&LineageEdge> {
        self.edges
            .iter()
            .filter(|e| e.target_model == model && e.target_column == column)
            .collect()
    }

    /// Find all downstream consumers of a column
    pub fn column_consumers(&self, model: &str, column: &str) -> Vec<&LineageEdge> {
        self.edges
            .iter()
            .filter(|e| e.source_model == model && e.source_column == column)
            .collect()
    }

    /// Trace a column upstream recursively — find all transitive source columns
    pub fn trace_column_recursive(&self, model: &str, column: &str) -> Vec<&LineageEdge> {
        self.trace_recursive(model, column, TraceDirection::Upstream)
    }

    /// Find all downstream consumers of a column recursively
    pub fn column_consumers_recursive(&self, model: &str, column: &str) -> Vec<&LineageEdge> {
        self.trace_recursive(model, column, TraceDirection::Downstream)
    }

    /// BFS traversal of lineage edges in the given direction.
    fn trace_recursive(
        &self,
        model: &str,
        column: &str,
        direction: TraceDirection,
    ) -> Vec<&LineageEdge> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((model.to_string(), column.to_string()));
        visited.insert((model.to_string(), column.to_string()));

        while let Some((m, c)) = queue.pop_front() {
            for edge in &self.edges {
                let (match_model, match_col, next_model, next_col) = match direction {
                    TraceDirection::Upstream => (
                        &edge.target_model,
                        &edge.target_column,
                        &edge.source_model,
                        &edge.source_column,
                    ),
                    TraceDirection::Downstream => (
                        &edge.source_model,
                        &edge.source_column,
                        &edge.target_model,
                        &edge.target_column,
                    ),
                };
                if match_model == &m && match_col == &c {
                    result.push(edge);
                    let key = (next_model.clone(), next_col.clone());
                    if visited.insert(key) {
                        queue.push_back((next_model.clone(), next_col.clone()));
                    }
                }
            }
        }

        result
    }

    /// Propagate data classifications from schema definitions onto lineage edges
    ///
    /// For each edge, looks up the source column's classification in the provided
    /// lookup map and sets it on the edge.
    pub fn propagate_classifications(
        &mut self,
        column_classifications: &HashMap<String, HashMap<String, String>>,
    ) {
        for edge in &mut self.edges {
            let cls = column_classifications
                .get(&edge.source_model)
                .and_then(|model_cols| model_cols.get(&edge.source_column));
            if let Some(cls) = cls {
                edge.classification = Some(cls.clone());
            }
        }
    }

    /// Generate DOT graph output for visualization
    pub fn to_dot(&self) -> String {
        let mut dot = String::from("digraph lineage {\n  rankdir=LR;\n  node [shape=record];\n\n");

        for (name, lineage) in &self.models {
            let cols: Vec<&str> = lineage
                .columns
                .iter()
                .map(|c| c.output_column.as_str())
                .collect();
            let label = format!("{}|{}", name, cols.join("\\l"));
            dot.push_str(&format!("  \"{}\" [label=\"{{{}}}\"];\n", name, label));
        }

        dot.push('\n');

        for edge in &self.edges {
            let style = if edge.is_direct {
                ""
            } else {
                " [style=dashed]"
            };
            dot.push_str(&format!(
                "  \"{}\":\"{}\" -> \"{}\":\"{}\"{};\n",
                edge.source_model, edge.source_column, edge.target_model, edge.target_column, style
            ));
        }

        dot.push_str("}\n");
        dot
    }
}

fn resolve_single_edge(
    target_model: &str,
    lineage: &ModelLineage,
    col_lineage: &ColumnLineage,
    source_ref: &ColumnRef,
    known_models: &HashSet<&str>,
) -> Option<LineageEdge> {
    let source_table = source_ref.table.as_deref().unwrap_or("");
    let resolved_table = lineage
        .table_aliases
        .get(source_table)
        .map(|s| s.as_str())
        .unwrap_or(source_table);
    let source_model = known_models
        .iter()
        .find(|m| m.eq_ignore_ascii_case(resolved_table))?;
    Some(LineageEdge {
        source_model: source_model.to_string(),
        source_column: source_ref.column.clone(),
        target_model: target_model.to_string(),
        target_column: col_lineage.output_column.clone(),
        is_direct: col_lineage.is_direct,
        expr_type: col_lineage.expr_type,
        classification: None,
    })
}

/// Extract column lineage from a SQL statement
pub fn extract_column_lineage(stmt: &Statement, model_name: &str) -> Option<ModelLineage> {
    match stmt {
        Statement::Query(query) => Some(extract_lineage_from_query(query, model_name)),
        _ => None,
    }
}

/// Extract lineage from a query
fn extract_lineage_from_query(query: &Query, model_name: &str) -> ModelLineage {
    let mut lineage = ModelLineage::new(model_name);

    match query.body.as_ref() {
        SetExpr::Select(select) => {
            extract_lineage_from_select(select, &mut lineage);
        }
        SetExpr::SetOperation { left, .. } => {
            // For UNION/INTERSECT/EXCEPT, column names come from the left operand
            // by SQL convention. Recursively extract from the left side.
            extract_lineage_from_set_expr(left, &mut lineage);
        }
        SetExpr::Query(inner_query) => {
            let inner = extract_lineage_from_query(inner_query, model_name);
            lineage.columns = inner.columns;
            lineage.table_aliases = inner.table_aliases;
            lineage.source_tables = inner.source_tables;
        }
        _ => {}
    }

    lineage
}

/// Recursively extract lineage from a SetExpr (handles nested UNION/INTERSECT/EXCEPT)
fn extract_lineage_from_set_expr(set_expr: &SetExpr, lineage: &mut ModelLineage) {
    match set_expr {
        SetExpr::Select(select) => {
            extract_lineage_from_select(select, lineage);
        }
        SetExpr::SetOperation { left, .. } => {
            // Column names come from the leftmost SELECT
            extract_lineage_from_set_expr(left, lineage);
        }
        SetExpr::Query(query) => {
            let inner = extract_lineage_from_query(query, &lineage.model_name);
            lineage.columns.extend(inner.columns);
            lineage.table_aliases.extend(inner.table_aliases);
            lineage.source_tables.extend(inner.source_tables);
        }
        _ => {}
    }
}

/// Extract lineage from a SELECT clause
fn extract_lineage_from_select(select: &Select, lineage: &mut ModelLineage) {
    for table in &select.from {
        extract_table_aliases(table, lineage);
    }

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let col_lineage = extract_lineage_from_expr(expr, lineage);
                lineage.add_column(col_lineage);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut col_lineage = extract_lineage_from_expr(expr, lineage);
                col_lineage.output_column = alias.value.clone();
                lineage.add_column(col_lineage);
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let table_name = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        crate::object_name_to_string(name)
                    }
                    SelectItemQualifiedWildcardKind::Expr(expr) => format!("{expr}"),
                };
                let mut col_lineage = ColumnLineage::new(&format!("{}.*", table_name));
                col_lineage.expr_type = ExprType::Wildcard;
                col_lineage
                    .source_columns
                    .insert(ColumnRef::qualified(&table_name, "*"));
                lineage.add_column(col_lineage);
            }
            SelectItem::Wildcard(_) => {
                let mut col_lineage = ColumnLineage::new("*");
                col_lineage.expr_type = ExprType::Wildcard;
                col_lineage.source_columns.extend(
                    lineage
                        .source_tables
                        .iter()
                        .map(|table| ColumnRef::qualified(table, "*")),
                );
                lineage.add_column(col_lineage);
            }
        }
    }
}

/// Extract table aliases from a FROM clause table reference
fn extract_table_aliases(table_with_joins: &TableWithJoins, lineage: &mut ModelLineage) {
    extract_table_factor_alias(&table_with_joins.relation, lineage);
    for join in &table_with_joins.joins {
        extract_table_factor_alias(&join.relation, lineage);
    }
}

/// Extract alias from a table factor
fn extract_table_factor_alias(factor: &TableFactor, lineage: &mut ModelLineage) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = crate::object_name_to_string(name);
            lineage.source_tables.insert(table_name.clone());
            if let Some(alias) = alias {
                lineage
                    .table_aliases
                    .insert(alias.name.value.clone(), table_name);
            }
        }
        TableFactor::Derived {
            alias, subquery, ..
        } => {
            if let Some(alias) = alias {
                lineage.table_aliases.insert(
                    alias.name.value.clone(),
                    format!("(subquery:{})", alias.name.value),
                );
            }
            if let SetExpr::Select(select) = subquery.body.as_ref() {
                extract_lineage_from_select(select, lineage);
            }
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            extract_table_aliases(table_with_joins, lineage);
            if let Some(alias) = alias {
                lineage
                    .table_aliases
                    .insert(alias.name.value.clone(), "(nested_join)".to_string());
            }
        }
        _ => {}
    }
}

/// Extract lineage information from an expression
fn extract_lineage_from_expr(expr: &Expr, lineage: &ModelLineage) -> ColumnLineage {
    match expr {
        Expr::Identifier(ident) => {
            let col_ref = ColumnRef::simple(&ident.value);
            ColumnLineage::direct(&ident.value, col_ref)
        }
        Expr::CompoundIdentifier(idents) => {
            if idents.len() >= 2 {
                let col_name = idents.last().map(|i| i.value.clone()).unwrap_or_default();
                let table_name = idents[..idents.len() - 1]
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");

                let resolved_table = lineage
                    .table_aliases
                    .get(&table_name)
                    .map(|s| s.as_str())
                    .unwrap_or(&table_name);

                let col_ref = ColumnRef::qualified(resolved_table, &col_name);
                ColumnLineage::direct(&col_name, col_ref)
            } else {
                let col_name = idents
                    .last()
                    .map(|i| i.value.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                ColumnLineage::new(&col_name)
            }
        }
        Expr::Function(func) => {
            let func_name = crate::object_name_to_string(&func.name);

            let mut sources = HashSet::new();
            extract_columns_from_function_args(&func.args, lineage, &mut sources);

            let mut col_lineage = ColumnLineage::from_function(&func_name, sources);
            col_lineage.output_column = func_name;
            col_lineage
        }
        Expr::BinaryOp { left, right, .. } => {
            let left_lineage = extract_lineage_from_expr(left, lineage);
            let right_lineage = extract_lineage_from_expr(right, lineage);

            let mut combined = ColumnLineage::new("expression");
            combined.expr_type = ExprType::Expression;
            combined.source_columns.extend(left_lineage.source_columns);
            combined.source_columns.extend(right_lineage.source_columns);
            combined
        }
        Expr::UnaryOp { expr, .. } => {
            let inner = extract_lineage_from_expr(expr, lineage);
            let mut col_lineage = ColumnLineage::new(&inner.output_column);
            col_lineage.expr_type = ExprType::Expression;
            col_lineage.source_columns = inner.source_columns;
            col_lineage
        }
        Expr::Cast { expr, .. } => {
            let inner = extract_lineage_from_expr(expr, lineage);
            let mut col_lineage = ColumnLineage::new(&inner.output_column);
            col_lineage.expr_type = ExprType::Cast;
            col_lineage.source_columns = inner.source_columns;
            col_lineage
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut sources = HashSet::new();

            if let Some(op) = operand {
                let op_lineage = extract_lineage_from_expr(op, lineage);
                sources.extend(op_lineage.source_columns);
            }

            for case_when in conditions {
                let cond_lineage = extract_lineage_from_expr(&case_when.condition, lineage);
                sources.extend(cond_lineage.source_columns);
                let result_lineage = extract_lineage_from_expr(&case_when.result, lineage);
                sources.extend(result_lineage.source_columns);
            }
            if let Some(else_expr) = else_result {
                let else_lineage = extract_lineage_from_expr(else_expr, lineage);
                sources.extend(else_lineage.source_columns);
            }

            let mut col_lineage = ColumnLineage::new("case_expr");
            col_lineage.expr_type = ExprType::Case;
            col_lineage.source_columns = sources;
            col_lineage
        }
        Expr::Subquery(query) => {
            let sub_lineage = extract_lineage_from_query(query, "subquery");
            let mut col_lineage = ColumnLineage::new("subquery");
            col_lineage.expr_type = ExprType::Subquery;
            for col in sub_lineage.columns {
                col_lineage.source_columns.extend(col.source_columns);
            }
            col_lineage
        }
        Expr::Nested(inner) => extract_lineage_from_expr(inner, lineage),
        Expr::Value(..) | Expr::TypedString { .. } => ColumnLineage::literal("literal"),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let inner_lineage = extract_lineage_from_expr(inner, lineage);
            let mut col_lineage = ColumnLineage::new(&inner_lineage.output_column);
            col_lineage.expr_type = ExprType::Expression;
            col_lineage.source_columns = inner_lineage.source_columns;
            col_lineage
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let mut sources = HashSet::new();
            let expr_lineage = extract_lineage_from_expr(expr, lineage);
            let low_lineage = extract_lineage_from_expr(low, lineage);
            let high_lineage = extract_lineage_from_expr(high, lineage);
            sources.extend(expr_lineage.source_columns);
            sources.extend(low_lineage.source_columns);
            sources.extend(high_lineage.source_columns);

            let mut col_lineage = ColumnLineage::new("between_expr");
            col_lineage.expr_type = ExprType::Expression;
            col_lineage.source_columns = sources;
            col_lineage
        }
        Expr::InList { expr, list, .. } => {
            let mut sources = HashSet::new();
            let expr_lineage = extract_lineage_from_expr(expr, lineage);
            sources.extend(expr_lineage.source_columns);
            for item in list {
                let item_lineage = extract_lineage_from_expr(item, lineage);
                sources.extend(item_lineage.source_columns);
            }

            let mut col_lineage = ColumnLineage::new("in_expr");
            col_lineage.expr_type = ExprType::Expression;
            col_lineage.source_columns = sources;
            col_lineage
        }
        _ => ColumnLineage::new("unknown"),
    }
}

/// Extract column references from a single function argument expression.
fn extract_from_arg_expr(
    arg_expr: &FunctionArgExpr,
    lineage: &ModelLineage,
    sources: &mut HashSet<ColumnRef>,
) {
    match arg_expr {
        FunctionArgExpr::Expr(expr) => {
            let expr_lineage = extract_lineage_from_expr(expr, lineage);
            sources.extend(expr_lineage.source_columns);
        }
        FunctionArgExpr::QualifiedWildcard(name) => {
            let table_name = crate::object_name_to_string(name);
            sources.insert(ColumnRef::qualified(&table_name, "*"));
        }
        FunctionArgExpr::Wildcard => {
            sources.insert(ColumnRef::simple("*"));
        }
    }
}

/// Extract column references from function arguments
fn extract_columns_from_function_args(
    args: &sqlparser::ast::FunctionArguments,
    lineage: &ModelLineage,
    sources: &mut HashSet<ColumnRef>,
) {
    match args {
        sqlparser::ast::FunctionArguments::List(arg_list) => {
            for arg in &arg_list.args {
                let arg_expr = match arg {
                    FunctionArg::Unnamed(e)
                    | FunctionArg::Named { arg: e, .. }
                    | FunctionArg::ExprNamed { arg: e, .. } => e,
                };
                extract_from_arg_expr(arg_expr, lineage, sources);
            }
        }
        sqlparser::ast::FunctionArguments::None => {}
        sqlparser::ast::FunctionArguments::Subquery(query) => {
            let sub_lineage = extract_lineage_from_query(query, "subquery");
            for col in sub_lineage.columns {
                sources.extend(col.source_columns);
            }
        }
    }
}

#[cfg(test)]
#[path = "lineage_test.rs"]
mod tests;
