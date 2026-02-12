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
