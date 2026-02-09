//! AST-to-IR lowering — converts sqlparser AST into RelOp IR

pub mod expr;
pub mod join;
pub mod query;
pub mod select;

use crate::error::{AnalysisError, AnalysisResult};
use crate::ir::relop::RelOp;
use crate::ir::schema::RelSchema;
use sqlparser::ast::Statement;
use std::collections::HashMap;

/// Schema catalog: maps table/model names to known schemas
pub type SchemaCatalog = HashMap<String, RelSchema>;

/// Lower a sqlparser Statement into a RelOp IR tree
///
/// Only `Statement::Query` is supported. Other statement types return an error.
pub fn lower_statement(stmt: &Statement, catalog: &SchemaCatalog) -> AnalysisResult<RelOp> {
    match stmt {
        Statement::Query(query) => query::lower_query(query, catalog),
        other => Err(AnalysisError::LoweringFailed {
            model: String::new(),
            message: format!(
                "Only SELECT queries are supported, got: {}",
                statement_kind(other)
            ),
        }),
    }
}

/// Return a human-readable name for a statement variant
fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        _ => "unsupported statement",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::relop::{JoinType, RelOp};
    use crate::ir::schema::RelSchema;
    use crate::ir::types::{Nullability, SqlType, TypedColumn};
    use sqlparser::dialect::DuckDbDialect;
    use sqlparser::parser::Parser;

    fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            source_table: None,
            sql_type: ty,
            nullability: null,
            provenance: vec![],
        }
    }

    fn parse_and_lower(sql: &str, catalog: &SchemaCatalog) -> RelOp {
        let dialect = DuckDbDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).expect("SQL parse failed");
        let stmt = stmts.first().expect("No statement");
        lower_statement(stmt, catalog).expect("Lowering failed")
    }

    fn empty_catalog() -> SchemaCatalog {
        HashMap::new()
    }

    fn catalog_with(entries: Vec<(&str, Vec<TypedColumn>)>) -> SchemaCatalog {
        entries
            .into_iter()
            .map(|(name, cols)| (name.to_string(), RelSchema::new(cols)))
            .collect()
    }

    #[test]
    fn test_lower_simple_select() {
        let catalog = catalog_with(vec![(
            "users",
            vec![
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                make_col(
                    "name",
                    SqlType::String { max_length: None },
                    Nullability::Nullable,
                ),
            ],
        )]);
        let ir = parse_and_lower("SELECT id, name FROM users", &catalog);

        // Should be Project(Scan)
        if let RelOp::Project {
            input,
            columns,
            schema,
        } = &ir
        {
            assert!(matches!(input.as_ref(), RelOp::Scan { .. }));
            assert_eq!(columns.len(), 2);
            assert_eq!(schema.columns.len(), 2);
            assert_eq!(schema.columns[0].name, "id");
            assert_eq!(schema.columns[1].name, "name");
        } else {
            panic!("Expected Project, got: {:?}", ir);
        }
    }

    #[test]
    fn test_lower_select_with_where() {
        let catalog = catalog_with(vec![(
            "orders",
            vec![
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                make_col(
                    "amount",
                    SqlType::Decimal {
                        precision: Some(10),
                        scale: Some(2),
                    },
                    Nullability::Nullable,
                ),
            ],
        )]);
        let ir = parse_and_lower("SELECT id FROM orders WHERE amount > 100", &catalog);

        // Should be Project(Filter(Scan))
        if let RelOp::Project { input, .. } = &ir {
            assert!(
                matches!(input.as_ref(), RelOp::Filter { .. }),
                "Expected Filter inside Project"
            );
            if let RelOp::Filter { input: inner, .. } = input.as_ref() {
                assert!(matches!(inner.as_ref(), RelOp::Scan { .. }));
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_lower_left_join() {
        let catalog = catalog_with(vec![
            (
                "orders",
                vec![
                    make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                    make_col(
                        "cust_id",
                        SqlType::Integer { bits: 32 },
                        Nullability::NotNull,
                    ),
                ],
            ),
            (
                "customers",
                vec![
                    make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                    make_col(
                        "name",
                        SqlType::String { max_length: None },
                        Nullability::NotNull,
                    ),
                ],
            ),
        ]);
        let sql = "SELECT o.id, c.name \
                    FROM orders o LEFT JOIN customers c ON o.cust_id = c.id";
        let ir = parse_and_lower(sql, &catalog);

        // Walk to find the Join node
        fn find_join(op: &RelOp) -> Option<&RelOp> {
            match op {
                RelOp::Join { .. } => Some(op),
                RelOp::Project { input, .. } => find_join(input),
                RelOp::Filter { input, .. } => find_join(input),
                _ => None,
            }
        }

        let join = find_join(&ir).expect("No Join found in IR");
        if let RelOp::Join {
            join_type, schema, ..
        } = join
        {
            assert_eq!(*join_type, JoinType::LeftOuter);
            // Right side columns should be nullable after LEFT JOIN
            let name_col = schema.find_column("name").expect("name column missing");
            assert_eq!(
                name_col.nullability,
                Nullability::Nullable,
                "Right-side column 'name' should be Nullable after LEFT JOIN"
            );
        }
    }

    #[test]
    fn test_lower_union() {
        let catalog = catalog_with(vec![
            (
                "a",
                vec![make_col(
                    "val",
                    SqlType::Integer { bits: 32 },
                    Nullability::NotNull,
                )],
            ),
            (
                "b",
                vec![make_col(
                    "val",
                    SqlType::Integer { bits: 32 },
                    Nullability::NotNull,
                )],
            ),
        ]);
        let ir = parse_and_lower("SELECT val FROM a UNION ALL SELECT val FROM b", &catalog);

        // Top level should be SetOp
        fn find_set_op(op: &RelOp) -> bool {
            matches!(op, RelOp::SetOp { .. })
        }
        assert!(
            find_set_op(&ir),
            "Expected SetOp at top level for UNION ALL"
        );
    }

    #[test]
    fn test_lower_group_by_with_aggregate() {
        let catalog = catalog_with(vec![(
            "orders",
            vec![
                make_col(
                    "status",
                    SqlType::String { max_length: None },
                    Nullability::NotNull,
                ),
                make_col(
                    "amount",
                    SqlType::Decimal {
                        precision: None,
                        scale: None,
                    },
                    Nullability::Nullable,
                ),
            ],
        )]);
        let sql = "SELECT status, SUM(amount) FROM orders GROUP BY status";
        let ir = parse_and_lower(sql, &catalog);

        fn find_aggregate(op: &RelOp) -> Option<&RelOp> {
            match op {
                RelOp::Aggregate { .. } => Some(op),
                RelOp::Project { input, .. } => find_aggregate(input),
                RelOp::Filter { input, .. } => find_aggregate(input),
                _ => None,
            }
        }

        let agg = find_aggregate(&ir).expect("No Aggregate node found");
        if let RelOp::Aggregate {
            group_by,
            aggregates,
            ..
        } = agg
        {
            assert!(!group_by.is_empty(), "Expected GROUP BY expressions");
            assert!(!aggregates.is_empty(), "Expected aggregate expressions");
        }
    }

    #[test]
    fn test_lower_order_by_limit() {
        let catalog = catalog_with(vec![(
            "items",
            vec![
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                make_col("price", SqlType::Float { bits: 64 }, Nullability::Nullable),
            ],
        )]);
        let sql = "SELECT id, price FROM items ORDER BY price DESC LIMIT 10 OFFSET 5";
        let ir = parse_and_lower(sql, &catalog);

        // Should have Limit wrapping Sort
        if let RelOp::Limit {
            input,
            limit,
            offset,
            ..
        } = &ir
        {
            assert_eq!(*limit, Some(10));
            assert_eq!(*offset, Some(5));
            assert!(
                matches!(input.as_ref(), RelOp::Sort { .. }),
                "Expected Sort inside Limit"
            );
        } else {
            panic!("Expected Limit at top level, got: {:?}", ir);
        }
    }

    #[test]
    fn test_lower_non_select_fails() {
        let dialect = DuckDbDialect {};
        let stmts = Parser::parse_sql(&dialect, "INSERT INTO t VALUES (1)").expect("parse failed");
        let result = lower_statement(&stmts[0], &empty_catalog());
        assert!(result.is_err(), "Non-SELECT should return error");
    }

    #[test]
    fn test_lower_select_star() {
        let catalog = catalog_with(vec![(
            "t",
            vec![
                make_col("a", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                make_col("b", SqlType::Integer { bits: 32 }, Nullability::NotNull),
            ],
        )]);
        let ir = parse_and_lower("SELECT * FROM t", &catalog);

        if let RelOp::Project {
            columns, schema, ..
        } = &ir
        {
            assert_eq!(columns.len(), 2, "SELECT * should expand to 2 columns");
            assert_eq!(schema.columns.len(), 2);
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_lower_cast_expression() {
        let catalog = catalog_with(vec![(
            "t",
            vec![make_col(
                "val",
                SqlType::Float { bits: 64 },
                Nullability::Nullable,
            )],
        )]);
        let ir = parse_and_lower("SELECT CAST(val AS INTEGER) AS int_val FROM t", &catalog);

        if let RelOp::Project { columns, .. } = &ir {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "int_val");
            assert!(
                matches!(columns[0].1, crate::ir::expr::TypedExpr::Cast { .. }),
                "Expected Cast expression"
            );
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_schema_propagation_through_filter() {
        let catalog = catalog_with(vec![(
            "t",
            vec![
                make_col("id", SqlType::Integer { bits: 32 }, Nullability::NotNull),
                make_col("active", SqlType::Boolean, Nullability::NotNull),
            ],
        )]);
        let ir = parse_and_lower("SELECT id FROM t WHERE active = true", &catalog);

        if let RelOp::Project { input, .. } = &ir {
            if let RelOp::Filter { schema, .. } = input.as_ref() {
                // Filter should preserve the full input schema (both columns)
                assert!(schema.find_column("id").is_some());
                assert!(schema.find_column("active").is_some());
            }
        }
    }
}
