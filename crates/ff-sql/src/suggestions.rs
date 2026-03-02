//! Test suggestion generation from SQL AST
//!
//! This module analyzes SQL queries to suggest appropriate tests for columns
//! based on their usage patterns in the query.

use sqlparser::ast::{
    Expr, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use std::collections::{HashMap, HashSet};

/// Suggested test for a column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TestSuggestion {
    /// Column appears to be a primary key or unique identifier
    Unique,
    /// Column is used in a JOIN condition
    NotNull,
    /// Column appears to reference another table's column
    Relationship { model: String, column: String },
    /// Column appears to be a foreign key
    ForeignKey,
    /// Column appears to be a timestamp/date
    DateColumn,
    /// Column appears to be an amount/currency
    NonNegative,
}

impl TestSuggestion {
    /// Get the test name for this suggestion
    pub fn test_name(&self) -> &'static str {
        match self {
            TestSuggestion::Unique => "unique",
            TestSuggestion::NotNull => "not_null",
            TestSuggestion::Relationship { .. } => "relationship",
            TestSuggestion::ForeignKey => "not_null",
            TestSuggestion::DateColumn => "not_null",
            TestSuggestion::NonNegative => "non_negative",
        }
    }

    /// Get a description of why this test is suggested
    pub fn reason(&self) -> String {
        match self {
            TestSuggestion::Unique => "Column appears to be a unique identifier".to_string(),
            TestSuggestion::NotNull => "Column is used in JOIN conditions".to_string(),
            TestSuggestion::Relationship { model, column } => {
                format!("Column references {}.{}", model, column)
            }
            TestSuggestion::ForeignKey => "Column appears to be a foreign key".to_string(),
            TestSuggestion::DateColumn => "Column appears to be a date/timestamp".to_string(),
            TestSuggestion::NonNegative => {
                "Column appears to be an amount that should not be negative".to_string()
            }
        }
    }
}

/// Column suggestion with all suggested tests
#[derive(Debug, Clone, Default)]
pub struct ColumnSuggestions {
    /// Column name
    pub column: String,
    /// Suggested tests with reasons
    pub suggestions: Vec<TestSuggestion>,
}

/// Model-level suggestions
#[derive(Debug, Clone, Default)]
pub struct ModelSuggestions {
    /// Model name
    pub model_name: String,
    /// Suggestions by column
    pub columns: HashMap<String, ColumnSuggestions>,
}

impl ModelSuggestions {
    /// Create new model suggestions
    pub fn new(model_name: &str) -> Self {
        Self {
            model_name: model_name.to_string(),
            columns: HashMap::new(),
        }
    }

    /// Add a suggestion for a column
    pub fn add_suggestion(&mut self, column: &str, suggestion: TestSuggestion) {
        self.columns
            .entry(column.to_string())
            .or_insert_with(|| ColumnSuggestions {
                column: column.to_string(),
                suggestions: Vec::new(),
            })
            .suggestions
            .push(suggestion);
    }
}

#[cfg(test)]
impl ModelSuggestions {
    /// Get suggestions for a column
    pub(crate) fn get_suggestions(&self, column: &str) -> Option<&ColumnSuggestions> {
        self.columns.get(column)
    }
}

/// Generate test suggestions from SQL
pub fn suggest_tests(stmt: &Statement, model_name: &str) -> ModelSuggestions {
    let mut suggestions = ModelSuggestions::new(model_name);

    if let Statement::Query(query) = stmt {
        analyze_query(query, &mut suggestions);
    }

    suggestions
}

/// Analyze a query for test suggestions
fn analyze_query(query: &Query, suggestions: &mut ModelSuggestions) {
    if let SetExpr::Select(select) = query.body.as_ref() {
        analyze_select(select, suggestions);
    }
}

/// Analyze a SELECT statement for suggestions
fn analyze_select(select: &Select, suggestions: &mut ModelSuggestions) {
    let mut output_columns: HashSet<&str> = HashSet::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(col) = get_column_name(expr) {
                    output_columns.insert(col);
                }
            }
            SelectItem::ExprWithAlias { alias, expr } => {
                output_columns.insert(&alias.value);
                analyze_expression_for_suggestions(expr, &alias.value, suggestions);
            }
            _ => {}
        }
    }

    for table in &select.from {
        analyze_table_joins(table, suggestions);
    }

    for col in &output_columns {
        analyze_column_name(col, suggestions);
    }
}

/// Analyze table joins for not_null suggestions
fn analyze_table_joins(table: &TableWithJoins, suggestions: &mut ModelSuggestions) {
    for col in table
        .joins
        .iter()
        .filter_map(|join| extract_join_on_expr(&join.join_operator))
        .flat_map(|expr| extract_columns_from_expr(expr))
    {
        suggestions.add_suggestion(col, TestSuggestion::NotNull);
    }

    for join in &table.joins {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = &join.relation
        {
            analyze_table_joins(table_with_joins, suggestions);
        }
    }
}

/// Extract the ON expression from a join operator, if present.
fn extract_join_on_expr(op: &JoinOperator) -> Option<&Expr> {
    let constraint = match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => Some(c),
        _ => None,
    };
    match constraint {
        Some(JoinConstraint::On(expr)) => Some(expr),
        _ => None,
    }
}

/// Analyze an expression for test suggestions
fn analyze_expression_for_suggestions(
    expr: &Expr,
    output_col: &str,
    suggestions: &mut ModelSuggestions,
) {
    let Expr::BinaryOp { left, op, right } = expr else {
        return;
    };
    if !matches!(
        op,
        sqlparser::ast::BinaryOperator::Divide | sqlparser::ast::BinaryOperator::Minus
    ) {
        return;
    }
    let has_amount = extract_columns_from_expr(left)
        .into_iter()
        .chain(extract_columns_from_expr(right))
        .any(is_amount_column_name);
    if has_amount {
        suggestions.add_suggestion(output_col, TestSuggestion::NonNegative);
    }
}

/// Analyze a column name for pattern-based suggestions
fn analyze_column_name(col: &str, suggestions: &mut ModelSuggestions) {
    let lower = col.to_lowercase();

    if lower.ends_with("_id") && !lower.contains("fk_") {
        let potential_ref = lower.trim_end_matches("_id");
        if !potential_ref.is_empty() {
            suggestions.add_suggestion(
                col,
                TestSuggestion::Relationship {
                    model: potential_ref.to_string(),
                    column: "id".to_string(),
                },
            );
        }
    }

    if lower == "id"
        || lower == "pk"
        || lower.ends_with("_pk")
        || lower.starts_with("pk_")
        || lower == "primary_key"
    {
        suggestions.add_suggestion(col, TestSuggestion::Unique);
        suggestions.add_suggestion(col, TestSuggestion::NotNull);
    }

    if lower.ends_with("_at")
        || lower.ends_with("_date")
        || lower.ends_with("_time")
        || lower.ends_with("_timestamp")
        || lower.contains("created")
        || lower.contains("updated")
        || lower.contains("deleted")
    {
        suggestions.add_suggestion(col, TestSuggestion::DateColumn);
    }

    if is_amount_column_name(&lower) {
        suggestions.add_suggestion(col, TestSuggestion::NonNegative);
    }
}

/// Check if a column name looks like an amount/currency.
fn is_amount_column_name(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.contains("amount")
        || lower.contains("price")
        || lower.contains("cost")
        || lower.contains("total")
        || lower.contains("revenue")
        || lower.contains("balance")
        || lower.ends_with("_usd")
        || lower.ends_with("_cents")
}

/// Extract column name from an expression
fn get_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// Extract all column references from an expression
fn extract_columns_from_expr(expr: &Expr) -> Vec<&str> {
    let mut columns = Vec::new();

    match expr {
        Expr::Identifier(ident) => {
            columns.push(ident.value.as_str());
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                columns.push(last.value.as_str());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            columns.extend(extract_columns_from_expr(left));
            columns.extend(extract_columns_from_expr(right));
        }
        Expr::UnaryOp { expr, .. } => {
            columns.extend(extract_columns_from_expr(expr));
        }
        Expr::Nested(inner) => {
            columns.extend(extract_columns_from_expr(inner));
        }
        Expr::Function(func) => {
            extract_columns_from_function_args(&func.args, &mut columns);
        }
        _ => {}
    }

    columns
}

/// Extract column references from function arguments
fn extract_columns_from_function_args<'a>(
    args: &'a sqlparser::ast::FunctionArguments,
    columns: &mut Vec<&'a str>,
) {
    let sqlparser::ast::FunctionArguments::List(arg_list) = args else {
        return;
    };
    for arg in &arg_list.args {
        if let Some(e) = extract_expr_from_function_arg(arg) {
            columns.extend(extract_columns_from_expr(e));
        }
    }
}

/// Extract the inner [`Expr`] from a function argument, if present.
fn extract_expr_from_function_arg(arg: &sqlparser::ast::FunctionArg) -> Option<&Expr> {
    match arg {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e))
        | sqlparser::ast::FunctionArg::Named {
            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
            ..
        }
        | sqlparser::ast::FunctionArg::ExprNamed {
            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
            ..
        } => Some(e),
        _ => None,
    }
}

#[cfg(test)]
#[path = "suggestions_test.rs"]
mod tests;
