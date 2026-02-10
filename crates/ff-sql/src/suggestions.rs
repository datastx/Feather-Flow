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

    /// Get suggestions for a column
    pub fn get_suggestions(&self, column: &str) -> Option<&ColumnSuggestions> {
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
    // Collect output columns
    let mut output_columns: HashSet<String> = HashSet::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(col) = get_column_name(expr) {
                    output_columns.insert(col);
                }
            }
            SelectItem::ExprWithAlias { alias, expr } => {
                output_columns.insert(alias.value.clone());
                // Also analyze the expression for patterns
                analyze_expression_for_suggestions(expr, &alias.value, suggestions);
            }
            _ => {}
        }
    }

    // Analyze JOIN conditions for not_null suggestions
    for table in &select.from {
        analyze_table_joins(table, suggestions);
    }

    // Analyze column names for pattern-based suggestions
    for col in &output_columns {
        analyze_column_name(col, suggestions);
    }
}

/// Analyze table joins for not_null suggestions
fn analyze_table_joins(table: &TableWithJoins, suggestions: &mut ModelSuggestions) {
    // Get columns used in join conditions
    for join in &table.joins {
        if let JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint) = &join.join_operator
        {
            if let JoinConstraint::On(expr) = constraint {
                let columns = extract_columns_from_expr(expr);
                for col in columns {
                    suggestions.add_suggestion(&col, TestSuggestion::NotNull);
                }
            }
        }

        // Recursively analyze nested joins
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = &join.relation
        {
            analyze_table_joins(table_with_joins, suggestions);
        }
    }
}

/// Analyze an expression for test suggestions
fn analyze_expression_for_suggestions(
    expr: &Expr,
    output_col: &str,
    suggestions: &mut ModelSuggestions,
) {
    if let Expr::BinaryOp { left, op, right } = expr {
        // Division or subtraction might indicate amounts
        if matches!(
            op,
            sqlparser::ast::BinaryOperator::Divide | sqlparser::ast::BinaryOperator::Minus
        ) {
            // Check if this involves amount-like columns
            let left_cols = extract_columns_from_expr(left);
            let right_cols = extract_columns_from_expr(right);
            for col in left_cols.iter().chain(right_cols.iter()) {
                if is_amount_column_name(col) {
                    suggestions.add_suggestion(output_col, TestSuggestion::NonNegative);
                    break;
                }
            }
        }
    }
}

/// Analyze a column name for pattern-based suggestions
fn analyze_column_name(col: &str, suggestions: &mut ModelSuggestions) {
    let lower = col.to_lowercase();

    // ID columns should be unique
    if lower.ends_with("_id") && !lower.contains("fk_") {
        // Foreign keys typically reference other tables
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

    // Primary key patterns
    if lower == "id"
        || lower == "pk"
        || lower.ends_with("_pk")
        || lower.starts_with("pk_")
        || lower == "primary_key"
    {
        suggestions.add_suggestion(col, TestSuggestion::Unique);
        suggestions.add_suggestion(col, TestSuggestion::NotNull);
    }

    // Date/timestamp columns
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

    // Amount/currency columns (should be non-negative)
    if is_amount_column_name(&lower) {
        suggestions.add_suggestion(col, TestSuggestion::NonNegative);
    }
}

/// Check if a column name looks like an amount/currency.
///
/// Callers must pass an already-lowered name (i.e. `name.to_lowercase()`).
fn is_amount_column_name(name: &str) -> bool {
    name.contains("amount")
        || name.contains("price")
        || name.contains("cost")
        || name.contains("total")
        || name.contains("revenue")
        || name.contains("balance")
        || name.ends_with("_usd")
        || name.ends_with("_cents")
}

/// Extract column name from an expression
fn get_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.value.clone()),
        _ => None,
    }
}

/// Extract all column references from an expression
fn extract_columns_from_expr(expr: &Expr) -> Vec<String> {
    let mut columns = Vec::new();

    match expr {
        Expr::Identifier(ident) => {
            columns.push(ident.value.clone());
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                columns.push(last.value.clone());
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
            if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                for arg in &args.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        )
                        | sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            columns.extend(extract_columns_from_expr(e));
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }

    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn parse_and_suggest(sql: &str, model_name: &str) -> ModelSuggestions {
        let parser = SqlParser::duckdb();
        let stmts = parser.parse(sql).unwrap();
        stmts
            .first()
            .map(|stmt| suggest_tests(stmt, model_name))
            .unwrap_or_else(|| ModelSuggestions::new(model_name))
    }

    #[test]
    fn test_id_column_suggestions() {
        let suggestions = parse_and_suggest("SELECT id, name, customer_id FROM users", "stg_users");

        let id_sugg = suggestions.get_suggestions("id").unwrap();
        assert!(id_sugg.suggestions.contains(&TestSuggestion::Unique));
        assert!(id_sugg.suggestions.contains(&TestSuggestion::NotNull));

        let customer_id_sugg = suggestions.get_suggestions("customer_id").unwrap();
        assert!(customer_id_sugg
            .suggestions
            .iter()
            .any(|s| matches!(s, TestSuggestion::Relationship { .. })));
    }

    #[test]
    fn test_date_column_suggestions() {
        let suggestions = parse_and_suggest(
            "SELECT created_at, updated_at, order_date FROM orders",
            "stg_orders",
        );

        assert!(suggestions
            .get_suggestions("created_at")
            .unwrap()
            .suggestions
            .contains(&TestSuggestion::DateColumn));
        assert!(suggestions
            .get_suggestions("updated_at")
            .unwrap()
            .suggestions
            .contains(&TestSuggestion::DateColumn));
        assert!(suggestions
            .get_suggestions("order_date")
            .unwrap()
            .suggestions
            .contains(&TestSuggestion::DateColumn));
    }

    #[test]
    fn test_amount_column_suggestions() {
        let suggestions = parse_and_suggest(
            "SELECT total_amount, price, revenue_usd FROM sales",
            "stg_sales",
        );

        assert!(suggestions
            .get_suggestions("total_amount")
            .unwrap()
            .suggestions
            .contains(&TestSuggestion::NonNegative));
        assert!(suggestions
            .get_suggestions("price")
            .unwrap()
            .suggestions
            .contains(&TestSuggestion::NonNegative));
        assert!(suggestions
            .get_suggestions("revenue_usd")
            .unwrap()
            .suggestions
            .contains(&TestSuggestion::NonNegative));
    }

    #[test]
    fn test_join_not_null_suggestions() {
        let suggestions = parse_and_suggest(
            "SELECT o.id, c.name
             FROM orders o
             JOIN customers c ON o.customer_id = c.id",
            "fct_orders",
        );

        // Both columns used in JOIN should get not_null suggestion
        assert!(suggestions
            .get_suggestions("customer_id")
            .map(|s| s.suggestions.contains(&TestSuggestion::NotNull))
            .unwrap_or(false));
    }

    #[test]
    fn test_no_suggestions_for_plain_columns() {
        let suggestions = parse_and_suggest("SELECT foo, bar, baz FROM table1", "test_model");

        // These generic columns shouldn't have any specific suggestions
        assert!(suggestions.get_suggestions("foo").is_none());
        assert!(suggestions.get_suggestions("bar").is_none());
        assert!(suggestions.get_suggestions("baz").is_none());
    }

    #[test]
    fn test_multiple_suggestions_for_column() {
        let suggestions = parse_and_suggest("SELECT id FROM users", "stg_users");

        let id_sugg = suggestions.get_suggestions("id").unwrap();
        // ID should get both unique and not_null
        assert!(id_sugg.suggestions.len() >= 2);
        assert!(id_sugg.suggestions.contains(&TestSuggestion::Unique));
        assert!(id_sugg.suggestions.contains(&TestSuggestion::NotNull));
    }
}
