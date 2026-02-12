use super::*;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::{JoinType, RelOp};
use crate::ir::schema::RelSchema;
use crate::ir::types::{FloatBitWidth, IntBitWidth, Nullability, SqlType, TypedColumn};
use crate::test_utils::*;
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

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
            make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
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
                make_col(
                    "id",
                    SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    Nullability::NotNull,
                ),
                make_col(
                    "cust_id",
                    SqlType::Integer {
                        bits: IntBitWidth::I32,
                    },
                    Nullability::NotNull,
                ),
            ],
        ),
        (
            "customers",
            vec![
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
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            )],
        ),
        (
            "b",
            vec![make_col(
                "val",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
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
            make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "price",
                SqlType::Float {
                    bits: FloatBitWidth::F64,
                },
                Nullability::Nullable,
            ),
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
            make_col(
                "a",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
            make_col(
                "b",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
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
            SqlType::Float {
                bits: FloatBitWidth::F64,
            },
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
            make_col(
                "id",
                SqlType::Integer {
                    bits: IntBitWidth::I32,
                },
                Nullability::NotNull,
            ),
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

// ── Additional JOIN type tests ──────────────────────────────────────

fn orders_customers_catalog() -> SchemaCatalog {
    catalog_with(vec![
        (
            "orders",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("cust_id", int32(), Nullability::NotNull),
                make_col("amount", float64(), Nullability::Nullable),
            ],
        ),
        (
            "customers",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::NotNull),
            ],
        ),
    ])
}

fn find_join(op: &RelOp) -> Option<&RelOp> {
    match op {
        RelOp::Join { .. } => Some(op),
        RelOp::Project { input, .. } | RelOp::Filter { input, .. } => find_join(input),
        _ => None,
    }
}

#[test]
fn test_lower_inner_join() {
    let catalog = orders_customers_catalog();
    let sql = "SELECT o.id, c.name FROM orders o INNER JOIN customers c ON o.cust_id = c.id";
    let ir = parse_and_lower(sql, &catalog);

    let join = find_join(&ir).expect("No Join found");
    if let RelOp::Join {
        join_type, schema, ..
    } = join
    {
        assert_eq!(*join_type, JoinType::Inner);
        // Both sides stay at their original nullability for INNER JOIN
        let name_col = schema.find_column("name").expect("name missing");
        assert_eq!(name_col.nullability, Nullability::NotNull);
    }
}

#[test]
fn test_lower_right_join() {
    let catalog = orders_customers_catalog();
    let sql = "SELECT o.id, c.name FROM orders o RIGHT JOIN customers c ON o.cust_id = c.id";
    let ir = parse_and_lower(sql, &catalog);

    let join = find_join(&ir).expect("No Join found");
    if let RelOp::Join {
        join_type, schema, ..
    } = join
    {
        assert_eq!(*join_type, JoinType::RightOuter);
        // Left-side columns should be nullable after RIGHT JOIN
        let id_col = schema.find_column("id").expect("id missing");
        assert_eq!(
            id_col.nullability,
            Nullability::Nullable,
            "Left-side 'id' should be Nullable after RIGHT JOIN"
        );
    }
}

#[test]
fn test_lower_full_outer_join() {
    let catalog = orders_customers_catalog();
    let sql = "SELECT o.id, c.name FROM orders o FULL OUTER JOIN customers c ON o.cust_id = c.id";
    let ir = parse_and_lower(sql, &catalog);

    let join = find_join(&ir).expect("No Join found");
    if let RelOp::Join {
        join_type, schema, ..
    } = join
    {
        assert_eq!(*join_type, JoinType::FullOuter);
        let id_col = schema.find_column("id").expect("id missing");
        let name_col = schema.find_column("name").expect("name missing");
        assert_eq!(id_col.nullability, Nullability::Nullable);
        assert_eq!(name_col.nullability, Nullability::Nullable);
    }
}

#[test]
fn test_lower_cross_join() {
    let catalog = orders_customers_catalog();
    let sql = "SELECT o.id, c.name FROM orders o CROSS JOIN customers c";
    let ir = parse_and_lower(sql, &catalog);

    let join = find_join(&ir).expect("No Join found");
    if let RelOp::Join { join_type, .. } = join {
        assert_eq!(*join_type, JoinType::Cross);
    }
}

// ── Expression lowering tests ───────────────────────────────────────

#[test]
fn test_lower_case_expression() {
    let catalog = catalog_with(vec![(
        "t",
        vec![
            make_col("status", varchar(), Nullability::NotNull),
            make_col("amount", float64(), Nullability::Nullable),
        ],
    )]);
    let sql = "SELECT CASE WHEN status = 'active' THEN amount ELSE 0 END AS val FROM t";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project { columns, .. } = &ir {
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].0, "val");
        assert!(
            matches!(columns[0].1, TypedExpr::Case { .. }),
            "Expected Case expression, got: {:?}",
            columns[0].1
        );
    } else {
        panic!("Expected Project");
    }
}

#[test]
fn test_lower_is_null_expression() {
    let catalog = catalog_with(vec![(
        "t",
        vec![make_col("val", varchar(), Nullability::Nullable)],
    )]);
    let sql = "SELECT val FROM t WHERE val IS NULL";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project { input, .. } = &ir {
        if let RelOp::Filter { predicate, .. } = input.as_ref() {
            assert!(
                matches!(predicate, TypedExpr::IsNull { negated: false, .. }),
                "Expected IsNull, got: {:?}",
                predicate
            );
        } else {
            panic!("Expected Filter inside Project");
        }
    }
}

#[test]
fn test_lower_is_not_null_expression() {
    let catalog = catalog_with(vec![(
        "t",
        vec![make_col("val", varchar(), Nullability::Nullable)],
    )]);
    let sql = "SELECT val FROM t WHERE val IS NOT NULL";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project { input, .. } = &ir {
        if let RelOp::Filter { predicate, .. } = input.as_ref() {
            assert!(
                matches!(predicate, TypedExpr::IsNull { negated: true, .. }),
                "Expected IsNull(negated=true), got: {:?}",
                predicate
            );
        } else {
            panic!("Expected Filter");
        }
    }
}

#[test]
fn test_lower_coalesce_expression() {
    let catalog = catalog_with(vec![(
        "t",
        vec![make_col("val", varchar(), Nullability::Nullable)],
    )]);
    let sql = "SELECT COALESCE(val, 'default') AS result FROM t";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project { columns, .. } = &ir {
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].0, "result");
        // COALESCE is typically lowered as a function call
        match &columns[0].1 {
            TypedExpr::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_lowercase(), "coalesce");
                assert_eq!(args.len(), 2);
            }
            TypedExpr::Case { .. } => {
                // Some implementations lower COALESCE to CASE WHEN
            }
            other => {
                panic!(
                    "Expected FunctionCall or Case for COALESCE, got: {:?}",
                    other
                );
            }
        }
    }
}

#[test]
fn test_lower_aliased_table() {
    let catalog = catalog_with(vec![(
        "orders",
        vec![
            make_col("id", int32(), Nullability::NotNull),
            make_col("amount", float64(), Nullability::Nullable),
        ],
    )]);
    let sql = "SELECT o.id, o.amount FROM orders o";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project { columns, .. } = &ir {
        assert_eq!(columns.len(), 2);
    } else {
        panic!("Expected Project");
    }
}

#[test]
fn test_lower_column_alias() {
    let catalog = catalog_with(vec![(
        "t",
        vec![make_col("val", int32(), Nullability::NotNull)],
    )]);
    let sql = "SELECT val AS value FROM t";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project {
        columns, schema, ..
    } = &ir
    {
        assert_eq!(columns[0].0, "value");
        assert_eq!(schema.columns[0].name, "value");
    }
}

#[test]
fn test_lower_multi_table_join() {
    let catalog = catalog_with(vec![
        (
            "orders",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("cust_id", int32(), Nullability::NotNull),
                make_col("product_id", int32(), Nullability::NotNull),
            ],
        ),
        (
            "customers",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::NotNull),
            ],
        ),
        (
            "products",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::NotNull),
            ],
        ),
    ]);
    let sql = "SELECT o.id, c.name, p.name \
                FROM orders o \
                JOIN customers c ON o.cust_id = c.id \
                JOIN products p ON o.product_id = p.id";
    let ir = parse_and_lower(sql, &catalog);

    if let RelOp::Project { columns, .. } = &ir {
        assert_eq!(columns.len(), 3);
    } else {
        panic!("Expected Project");
    }
}

// ── Should-fail cases ───────────────────────────────────────────────

#[test]
fn test_lower_update_fails() {
    let dialect = DuckDbDialect {};
    let stmts = Parser::parse_sql(&dialect, "UPDATE t SET x = 1").expect("parse failed");
    let result = lower_statement(&stmts[0], &empty_catalog());
    assert!(result.is_err());
}

#[test]
fn test_lower_delete_fails() {
    let dialect = DuckDbDialect {};
    let stmts = Parser::parse_sql(&dialect, "DELETE FROM t WHERE id = 1").expect("parse failed");
    let result = lower_statement(&stmts[0], &empty_catalog());
    assert!(result.is_err());
}

#[test]
fn test_lower_create_table_fails() {
    let dialect = DuckDbDialect {};
    let stmts = Parser::parse_sql(&dialect, "CREATE TABLE t (id INTEGER)").expect("parse failed");
    let result = lower_statement(&stmts[0], &empty_catalog());
    assert!(result.is_err());
}

// ── HAVING test ─────────────────────────────────────────────────────

#[test]
fn test_lower_having() {
    let catalog = catalog_with(vec![(
        "orders",
        vec![
            make_col("status", varchar(), Nullability::NotNull),
            make_col("amount", float64(), Nullability::Nullable),
        ],
    )]);
    let sql = "SELECT status, SUM(amount) AS total \
                FROM orders GROUP BY status HAVING SUM(amount) > 1000";
    let ir = parse_and_lower(sql, &catalog);

    // Should have a Filter (HAVING) wrapping an Aggregate
    fn find_having_filter(op: &RelOp) -> bool {
        match op {
            RelOp::Filter { input, .. } => matches!(input.as_ref(), RelOp::Aggregate { .. }),
            RelOp::Project { input, .. } => find_having_filter(input),
            _ => false,
        }
    }
    assert!(
        find_having_filter(&ir),
        "Expected HAVING as Filter wrapping Aggregate"
    );
}

// ── INTERSECT / EXCEPT tests ────────────────────────────────────────

#[test]
fn test_lower_intersect() {
    let catalog = catalog_with(vec![
        ("a", vec![make_col("val", int32(), Nullability::NotNull)]),
        ("b", vec![make_col("val", int32(), Nullability::NotNull)]),
    ]);
    let ir = parse_and_lower("SELECT val FROM a INTERSECT SELECT val FROM b", &catalog);
    assert!(
        matches!(ir, RelOp::SetOp { .. }),
        "Expected SetOp for INTERSECT"
    );
}

#[test]
fn test_lower_except() {
    let catalog = catalog_with(vec![
        ("a", vec![make_col("val", int32(), Nullability::NotNull)]),
        ("b", vec![make_col("val", int32(), Nullability::NotNull)]),
    ]);
    let ir = parse_and_lower("SELECT val FROM a EXCEPT SELECT val FROM b", &catalog);
    assert!(
        matches!(ir, RelOp::SetOp { .. }),
        "Expected SetOp for EXCEPT"
    );
}

// ── Chained LEFT JOINs nullability ──────────────────────────────────

#[test]
fn test_lower_chained_left_joins_nullability() {
    let catalog = catalog_with(vec![
        (
            "a",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("b_id", int32(), Nullability::NotNull),
            ],
        ),
        (
            "b",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("c_id", int32(), Nullability::NotNull),
            ],
        ),
        (
            "c",
            vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("val", varchar(), Nullability::NotNull),
            ],
        ),
    ]);
    let sql = "SELECT a.id, b.id AS b_id, c.val \
                FROM a LEFT JOIN b ON a.b_id = b.id \
                LEFT JOIN c ON b.c_id = c.id";
    let ir = parse_and_lower(sql, &catalog);

    // c.val should be nullable (from second LEFT JOIN)
    if let RelOp::Project { schema, .. } = &ir {
        let val_col = schema.find_column("val");
        if let Some(col) = val_col {
            assert_eq!(
                col.nullability,
                Nullability::Nullable,
                "c.val should be Nullable after chained LEFT JOINs"
            );
        }
    }
}
