//! Column-level lineage extraction from SQL AST
//!
//! This module extracts column-level lineage information from SQL queries,
//! tracking which source columns flow into which output columns.

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, ObjectName, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::collections::{HashMap, HashSet};

/// Expression type for a lineage column
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    /// Create a new column reference
    pub fn new(table: Option<String>, column: String) -> Self {
        Self { table, column }
    }

    /// Create from a simple column name
    pub fn simple(column: &str) -> Self {
        Self {
            table: None,
            column: column.to_string(),
        }
    }

    /// Create from table.column
    pub fn qualified(table: &str, column: &str) -> Self {
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
    pub fn new(output_column: &str) -> Self {
        Self {
            output_column: output_column.to_string(),
            source_columns: HashSet::new(),
            is_direct: false,
            expr_type: ExprType::Unknown,
        }
    }

    /// Create a direct column reference (pass-through)
    pub fn direct(output_column: &str, source: ColumnRef) -> Self {
        let mut lineage = Self::new(output_column);
        lineage.source_columns.insert(source);
        lineage.is_direct = true;
        lineage.expr_type = ExprType::Column;
        lineage
    }

    /// Create from a function call
    pub fn from_function(output_column: &str, sources: HashSet<ColumnRef>) -> Self {
        Self {
            output_column: output_column.to_string(),
            source_columns: sources,
            is_direct: false,
            expr_type: ExprType::Function,
        }
    }

    /// Create from a literal value
    pub fn literal(output_column: &str) -> Self {
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
    pub fn new(model_name: &str) -> Self {
        Self {
            model_name: model_name.to_string(),
            columns: Vec::new(),
            table_aliases: HashMap::new(),
            source_tables: HashSet::new(),
        }
    }

    /// Add a column lineage
    pub fn add_column(&mut self, lineage: ColumnLineage) {
        self.columns.push(lineage);
    }

    /// Get column lineage by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnLineage> {
        self.columns.iter().find(|c| c.output_column == name)
    }

    /// Get all source columns for a given output column
    pub fn get_sources(&self, output_column: &str) -> HashSet<ColumnRef> {
        self.get_column(output_column)
            .map(|c| c.source_columns.clone())
            .unwrap_or_default()
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
    pub fn resolve_edges(&mut self, known_models: &HashSet<String>) {
        // Collect edges into a local Vec to avoid borrowing self.models while mutating self.edges
        let mut new_edges = Vec::new();

        for (target_model, lineage) in &self.models {
            for col_lineage in &lineage.columns {
                for source_ref in &col_lineage.source_columns {
                    // Resolve the table reference to a model name
                    let source_table = source_ref.table.as_deref().unwrap_or("");

                    // Check table aliases to get actual table name
                    let resolved_table = lineage
                        .table_aliases
                        .get(source_table)
                        .map(|s| s.as_str())
                        .unwrap_or(source_table);

                    // Case-insensitive model matching: find the original-case
                    // model name for consistent edge creation
                    let matched_model = known_models
                        .iter()
                        .find(|m| m.eq_ignore_ascii_case(resolved_table));
                    if let Some(source_model) = matched_model {
                        new_edges.push(LineageEdge {
                            source_model: source_model.clone(),
                            source_column: source_ref.column.clone(),
                            target_model: target_model.clone(),
                            target_column: col_lineage.output_column.clone(),
                            is_direct: col_lineage.is_direct,
                            expr_type: col_lineage.expr_type.clone(),
                            classification: None,
                        });
                    }
                }
            }
        }

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

    /// Propagate data classifications from schema definitions onto lineage edges
    ///
    /// For each edge, looks up the source column's classification in the provided
    /// lookup map and sets it on the edge.
    pub fn propagate_classifications(
        &mut self,
        column_classifications: &HashMap<String, HashMap<String, String>>,
    ) {
        for edge in &mut self.edges {
            if let Some(model_cols) = column_classifications.get(&edge.source_model) {
                if let Some(cls) = model_cols.get(&edge.source_column) {
                    edge.classification = Some(cls.clone());
                }
            }
        }
    }

    /// Get all edges flowing into a model that have a specific classification
    pub fn classified_inputs<'a>(
        &'a self,
        model: &str,
        classification: &str,
    ) -> Vec<&'a LineageEdge> {
        self.edges
            .iter()
            .filter(|e| {
                e.target_model == model && e.classification.as_deref() == Some(classification)
            })
            .collect()
    }

    /// Generate DOT graph output for visualization
    pub fn to_dot(&self) -> String {
        let mut dot = String::from("digraph lineage {\n  rankdir=LR;\n  node [shape=record];\n\n");

        // Create nodes for each model with columns
        for (name, lineage) in &self.models {
            let cols: Vec<String> = lineage
                .columns
                .iter()
                .map(|c| c.output_column.clone())
                .collect();
            let label = format!("{}|{}", name, cols.join("\\l"));
            dot.push_str(&format!("  \"{}\" [label=\"{{{}}}\"];\n", name, label));
        }

        dot.push('\n');

        // Create edges
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

    // Handle the main body of the query
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
            let model_name = lineage.model_name.clone();
            let inner = extract_lineage_from_query(query, &model_name);
            lineage.columns.extend(inner.columns);
            lineage.table_aliases.extend(inner.table_aliases);
            lineage.source_tables.extend(inner.source_tables);
        }
        _ => {}
    }
}

/// Extract lineage from a SELECT clause
fn extract_lineage_from_select(select: &Select, lineage: &mut ModelLineage) {
    // First, extract table aliases from FROM clause
    for table in &select.from {
        extract_table_aliases(table, lineage);
    }

    // Then extract column lineage from projection
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
                // SELECT table.*
                let table_name = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        object_name_to_string(name)
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
                // SELECT *
                let mut col_lineage = ColumnLineage::new("*");
                col_lineage.expr_type = ExprType::Wildcard;
                // Add all source tables as potential sources
                for table in &lineage.source_tables.clone() {
                    col_lineage
                        .source_columns
                        .insert(ColumnRef::qualified(table, "*"));
                }
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
            let table_name = object_name_to_string(name);
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
            // Recursively extract from subquery
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

/// Convert ObjectName to string
fn object_name_to_string(name: &ObjectName) -> String {
    crate::object_name_to_string(name)
}

/// Extract lineage information from an expression
fn extract_lineage_from_expr(expr: &Expr, lineage: &ModelLineage) -> ColumnLineage {
    match expr {
        Expr::Identifier(ident) => {
            // Simple column reference
            let col_ref = ColumnRef::simple(&ident.value);
            ColumnLineage::direct(&ident.value, col_ref)
        }
        Expr::CompoundIdentifier(idents) => {
            // table.column or schema.table.column
            if idents.len() >= 2 {
                let col_name = idents.last().map(|i| i.value.clone()).unwrap_or_default();
                let table_name = idents[..idents.len() - 1]
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");

                // Resolve alias if present
                let resolved_table = lineage
                    .table_aliases
                    .get(&table_name)
                    .cloned()
                    .unwrap_or(table_name.clone());

                let col_ref = ColumnRef::qualified(&resolved_table, &col_name);
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
            // Function call - extract all column references from arguments
            let func_name = object_name_to_string(&func.name);

            let mut sources = HashSet::new();
            extract_columns_from_function_args(&func.args, lineage, &mut sources);

            let mut col_lineage = ColumnLineage::from_function(&func_name, sources);
            col_lineage.output_column = func_name;
            col_lineage
        }
        Expr::BinaryOp { left, right, .. } => {
            // Binary operation - combine sources from both sides
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

            // Extract from operand if present
            if let Some(op) = operand {
                let op_lineage = extract_lineage_from_expr(op, lineage);
                sources.extend(op_lineage.source_columns);
            }

            // Extract from conditions and results (now CaseWhen structs)
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
            // For subqueries, extract lineage from the subquery
            let sub_lineage = extract_lineage_from_query(query, "subquery");
            let mut col_lineage = ColumnLineage::new("subquery");
            col_lineage.expr_type = ExprType::Subquery;
            for col in sub_lineage.columns {
                col_lineage.source_columns.extend(col.source_columns);
            }
            col_lineage
        }
        Expr::Nested(inner) => extract_lineage_from_expr(inner, lineage),
        Expr::Value(..) | Expr::TypedString { .. } => {
            // Literal value - no source columns
            ColumnLineage::literal("literal")
        }
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
        _ => {
            // For other expressions, return empty lineage
            ColumnLineage::new("unknown")
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
                match arg {
                    FunctionArg::Unnamed(arg_expr)
                    | FunctionArg::Named { arg: arg_expr, .. }
                    | FunctionArg::ExprNamed { arg: arg_expr, .. } => match arg_expr {
                        FunctionArgExpr::Expr(expr) => {
                            let expr_lineage = extract_lineage_from_expr(expr, lineage);
                            sources.extend(expr_lineage.source_columns);
                        }
                        FunctionArgExpr::QualifiedWildcard(name) => {
                            let table_name = object_name_to_string(name);
                            sources.insert(ColumnRef::qualified(&table_name, "*"));
                        }
                        FunctionArgExpr::Wildcard => {
                            sources.insert(ColumnRef::simple("*"));
                        }
                    },
                }
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
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn parse_and_extract_lineage(sql: &str, model_name: &str) -> Option<ModelLineage> {
        let parser = SqlParser::duckdb();
        let stmts = parser.parse(sql).unwrap();
        stmts
            .first()
            .and_then(|stmt| extract_column_lineage(stmt, model_name))
    }

    #[test]
    fn test_simple_select() {
        let lineage =
            parse_and_extract_lineage("SELECT id, name FROM users", "test_model").unwrap();

        assert_eq!(lineage.model_name, "test_model");
        assert_eq!(lineage.columns.len(), 2);

        let id_col = lineage.get_column("id").unwrap();
        assert!(id_col.is_direct);
        assert_eq!(id_col.expr_type, ExprType::Column);

        let name_col = lineage.get_column("name").unwrap();
        assert!(name_col.is_direct);
    }

    #[test]
    fn test_aliased_columns() {
        let lineage = parse_and_extract_lineage(
            "SELECT id AS user_id, name AS user_name FROM users",
            "test_model",
        )
        .unwrap();

        assert_eq!(lineage.columns.len(), 2);

        let user_id = lineage.get_column("user_id").unwrap();
        assert!(user_id.is_direct);
        assert!(user_id.source_columns.contains(&ColumnRef::simple("id")));

        let user_name = lineage.get_column("user_name").unwrap();
        assert!(user_name
            .source_columns
            .contains(&ColumnRef::simple("name")));
    }

    #[test]
    fn test_qualified_column_refs() {
        let lineage =
            parse_and_extract_lineage("SELECT u.id, u.name FROM users u", "test_model").unwrap();

        assert_eq!(lineage.columns.len(), 2);

        // Check that aliases are resolved
        let id_col = lineage.get_column("id").unwrap();
        assert!(id_col
            .source_columns
            .contains(&ColumnRef::qualified("users", "id")));
    }

    #[test]
    fn test_function_lineage() {
        let lineage = parse_and_extract_lineage(
            "SELECT COUNT(id) AS cnt, UPPER(name) AS upper_name FROM users",
            "test_model",
        )
        .unwrap();

        assert_eq!(lineage.columns.len(), 2);

        let cnt = lineage.get_column("cnt").unwrap();
        assert!(!cnt.is_direct);
        assert_eq!(cnt.expr_type, ExprType::Function);
        assert!(cnt.source_columns.contains(&ColumnRef::simple("id")));

        let upper_name = lineage.get_column("upper_name").unwrap();
        assert!(!upper_name.is_direct);
        assert_eq!(upper_name.expr_type, ExprType::Function);
    }

    #[test]
    fn test_expression_lineage() {
        let lineage =
            parse_and_extract_lineage("SELECT price * quantity AS total FROM orders", "test_model")
                .unwrap();

        let total = lineage.get_column("total").unwrap();
        assert!(!total.is_direct);
        assert_eq!(total.expr_type, ExprType::Expression);
        assert!(total.source_columns.contains(&ColumnRef::simple("price")));
        assert!(total
            .source_columns
            .contains(&ColumnRef::simple("quantity")));
    }

    #[test]
    fn test_case_expression() {
        let lineage = parse_and_extract_lineage(
            "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users",
            "test_model",
        )
        .unwrap();

        let is_active = lineage.get_column("is_active").unwrap();
        assert!(!is_active.is_direct);
        assert_eq!(is_active.expr_type, ExprType::Case);
        assert!(is_active
            .source_columns
            .contains(&ColumnRef::simple("status")));
    }

    #[test]
    fn test_join_lineage() {
        let lineage = parse_and_extract_lineage(
            "SELECT o.id AS order_id, c.name AS customer_name
             FROM orders o
             JOIN customers c ON o.customer_id = c.id",
            "test_model",
        )
        .unwrap();

        assert_eq!(lineage.source_tables.len(), 2);
        assert!(lineage.source_tables.contains("orders"));
        assert!(lineage.source_tables.contains("customers"));

        let order_id = lineage.get_column("order_id").unwrap();
        assert!(order_id
            .source_columns
            .contains(&ColumnRef::qualified("orders", "id")));

        let customer_name = lineage.get_column("customer_name").unwrap();
        assert!(customer_name
            .source_columns
            .contains(&ColumnRef::qualified("customers", "name")));
    }

    #[test]
    fn test_wildcard_select() {
        let lineage = parse_and_extract_lineage("SELECT * FROM users", "test_model").unwrap();

        assert_eq!(lineage.columns.len(), 1);
        let wildcard = &lineage.columns[0];
        assert_eq!(wildcard.output_column, "*");
        assert_eq!(wildcard.expr_type, ExprType::Wildcard);
    }

    #[test]
    fn test_literal_column() {
        let lineage = parse_and_extract_lineage(
            "SELECT id, 'constant' AS const_col, 42 AS num_col FROM users",
            "test_model",
        )
        .unwrap();

        let const_col = lineage.get_column("const_col").unwrap();
        assert!(!const_col.is_direct);
        assert_eq!(const_col.expr_type, ExprType::Literal);
        assert!(const_col.source_columns.is_empty());
    }

    #[test]
    fn test_cast_expression() {
        let lineage = parse_and_extract_lineage(
            "SELECT CAST(amount AS DECIMAL(10,2)) AS amount_decimal FROM orders",
            "test_model",
        )
        .unwrap();

        let amount = lineage.get_column("amount_decimal").unwrap();
        assert_eq!(amount.expr_type, ExprType::Cast);
        assert!(amount.source_columns.contains(&ColumnRef::simple("amount")));
    }

    #[test]
    fn test_cte_lineage() {
        let lineage = parse_and_extract_lineage(
            "WITH staged AS (SELECT id, name FROM raw_users)
             SELECT id AS user_id, name AS user_name FROM staged",
            "test_model",
        )
        .unwrap();

        // The main query references the CTE "staged" as its source
        // (raw_users is inside the CTE, so it's not directly visible in the main select's FROM)
        assert!(lineage.source_tables.contains("staged"));

        // Output columns should exist
        let user_id = lineage.get_column("user_id");
        assert!(user_id.is_some());
    }
}
