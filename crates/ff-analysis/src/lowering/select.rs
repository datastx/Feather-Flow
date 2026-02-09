//! SELECT clause lowering: FROM → WHERE → GROUP BY → HAVING → projection

use crate::error::AnalysisResult;
use crate::ir::expr::TypedExpr;
use crate::ir::relop::RelOp;
use crate::ir::schema::RelSchema;
use crate::ir::types::TypedColumn;
use crate::lowering::expr::lower_expr;
use crate::lowering::join::lower_join;
use crate::lowering::SchemaCatalog;
use sqlparser::ast::{GroupByExpr, Select, SelectItem, TableAlias, TableFactor, TableWithJoins};

/// Lower a SELECT statement into a RelOp plan
pub(crate) fn lower_select(select: &Select, catalog: &SchemaCatalog) -> AnalysisResult<RelOp> {
    // 1. FROM clause → Scan/Join tree
    let from_plan = lower_from(&select.from, catalog)?;
    let current_schema = from_plan
        .as_ref()
        .map(|p| p.schema().clone())
        .unwrap_or_default();

    // 2. WHERE clause → Filter
    let after_where = if let Some(ref where_clause) = select.selection {
        let predicate = lower_expr(where_clause, &current_schema);
        let input = from_plan.unwrap_or_else(|| RelOp::Scan {
            table_name: "<dual>".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        });
        let schema = input.schema().clone();
        Some(RelOp::Filter {
            input: Box::new(input),
            predicate,
            schema,
        })
    } else {
        from_plan
    };

    let current_schema = after_where
        .as_ref()
        .map(|p| p.schema().clone())
        .unwrap_or_default();

    // 3. GROUP BY → Aggregate
    let after_group = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
            let group_exprs: Vec<TypedExpr> = exprs
                .iter()
                .map(|e| lower_expr(e, &current_schema))
                .collect();

            // Collect aggregate functions from the projection
            let mut agg_items = Vec::new();
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    collect_aggregates(expr, &current_schema, &mut agg_items);
                }
            }

            let input = after_where.unwrap_or_else(|| RelOp::Scan {
                table_name: "<dual>".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            });

            // Build output schema from group-by keys + aggregates
            let mut out_cols = Vec::new();
            for ge in &group_exprs {
                if let TypedExpr::ColumnRef {
                    column,
                    resolved_type,
                    nullability,
                    ..
                } = ge
                {
                    out_cols.push(TypedColumn {
                        name: column.clone(),
                        source_table: None,
                        sql_type: resolved_type.clone(),
                        nullability: *nullability,
                        provenance: vec![],
                    });
                }
            }
            for (name, agg_expr) in &agg_items {
                out_cols.push(TypedColumn {
                    name: name.clone(),
                    source_table: None,
                    sql_type: agg_expr.resolved_type().clone(),
                    nullability: agg_expr.nullability(),
                    provenance: vec![],
                });
            }

            let schema = RelSchema::new(out_cols);
            Some(RelOp::Aggregate {
                input: Box::new(input),
                group_by: group_exprs,
                aggregates: agg_items,
                schema,
            })
        }
        _ => after_where,
    };

    let current_schema = after_group
        .as_ref()
        .map(|p| p.schema().clone())
        .unwrap_or_default();

    // 4. HAVING → Filter
    let after_having = if let Some(ref having) = select.having {
        let predicate = lower_expr(having, &current_schema);
        let input = after_group.unwrap_or_else(|| RelOp::Scan {
            table_name: "<dual>".to_string(),
            alias: None,
            schema: RelSchema::empty(),
        });
        let schema = input.schema().clone();
        Some(RelOp::Filter {
            input: Box::new(input),
            predicate,
            schema,
        })
    } else {
        after_group
    };

    let current_schema = after_having
        .as_ref()
        .map(|p| p.schema().clone())
        .unwrap_or_default();

    // 5. SELECT projection → Project
    let (proj_cols, proj_schema) = lower_projection(&select.projection, &current_schema);

    let input = after_having.unwrap_or_else(|| RelOp::Scan {
        table_name: "<dual>".to_string(),
        alias: None,
        schema: RelSchema::empty(),
    });

    Ok(RelOp::Project {
        input: Box::new(input),
        columns: proj_cols,
        schema: proj_schema,
    })
}

/// Lower FROM clause into a Scan/Join tree
fn lower_from(from: &[TableWithJoins], catalog: &SchemaCatalog) -> AnalysisResult<Option<RelOp>> {
    if from.is_empty() {
        return Ok(None);
    }

    // Start with the first table
    let mut plan = lower_table_factor(&from[0].relation, catalog)?;

    // Apply joins from the first FROM item
    for join in &from[0].joins {
        plan = lower_join(plan, join, catalog)?;
    }

    // Handle implicit cross joins from additional FROM items
    for twj in &from[1..] {
        let right = lower_table_factor(&twj.relation, catalog)?;
        let schema = RelSchema::merge(plan.schema(), right.schema());
        plan = RelOp::Join {
            left: Box::new(plan),
            right: Box::new(right),
            join_type: crate::ir::relop::JoinType::Cross,
            condition: None,
            schema,
        };
        // Apply joins on this table
        for join in &twj.joins {
            plan = lower_join(plan, join, catalog)?;
        }
    }

    Ok(Some(plan))
}

/// Lower a single table factor (table name, subquery, etc.)
pub(crate) fn lower_table_factor(
    factor: &TableFactor,
    catalog: &SchemaCatalog,
) -> AnalysisResult<RelOp> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            // Resolve schema: try full name, then last part only
            let schema = catalog
                .get(&table_name)
                .or_else(|| {
                    let short_string = name.0.last().map(|i| i.to_string());
                    let short = short_string.as_deref().unwrap_or("");
                    catalog.get(short)
                })
                .cloned()
                .unwrap_or_else(RelSchema::empty);

            let alias_name = alias.as_ref().map(|a| a.name.value.clone());

            // Tag all columns with their source table (alias takes precedence)
            let source_label = alias_name.as_deref().unwrap_or(&table_name);
            let schema = schema.with_source_table(source_label);

            Ok(RelOp::Scan {
                table_name,
                alias: alias_name,
                schema,
            })
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let plan = crate::lowering::query::lower_query(subquery, catalog)?;
            let schema = plan.schema().clone();
            let alias_name = alias.as_ref().map(|a: &TableAlias| a.name.value.clone());
            // Wrap derived table as a Scan with alias
            Ok(RelOp::Scan {
                table_name: alias_name
                    .clone()
                    .unwrap_or_else(|| "<derived>".to_string()),
                alias: alias_name,
                schema,
            })
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let mut plan = lower_table_factor(&table_with_joins.relation, catalog)?;
            for join in &table_with_joins.joins {
                plan = lower_join(plan, join, catalog)?;
            }
            if let Some(a) = alias {
                // Re-alias the result
                let schema = plan.schema().clone();
                plan = RelOp::Scan {
                    table_name: a.name.value.clone(),
                    alias: Some(a.name.value.clone()),
                    schema,
                };
            }
            Ok(plan)
        }
        _ => {
            // Unsupported table factor
            Ok(RelOp::Scan {
                table_name: "<unsupported>".to_string(),
                alias: None,
                schema: RelSchema::empty(),
            })
        }
    }
}

/// Lower the SELECT projection into a list of (name, expr) pairs and output schema
fn lower_projection(
    items: &[SelectItem],
    input_schema: &RelSchema,
) -> (Vec<(String, TypedExpr)>, RelSchema) {
    let mut columns = Vec::new();
    let mut out_cols = Vec::new();

    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let typed = lower_expr(expr, input_schema);
                let name = infer_column_name(expr);
                out_cols.push(TypedColumn {
                    name: name.clone(),
                    source_table: None,
                    sql_type: typed.resolved_type().clone(),
                    nullability: typed.nullability(),
                    provenance: vec![],
                });
                columns.push((name, typed));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let typed = lower_expr(expr, input_schema);
                let name = alias.value.clone();
                out_cols.push(TypedColumn {
                    name: name.clone(),
                    source_table: None,
                    sql_type: typed.resolved_type().clone(),
                    nullability: typed.nullability(),
                    provenance: vec![],
                });
                columns.push((name, typed));
            }
            SelectItem::Wildcard(_) => {
                // Expand * from input schema
                for col in &input_schema.columns {
                    let typed = TypedExpr::ColumnRef {
                        table: None,
                        column: col.name.clone(),
                        resolved_type: col.sql_type.clone(),
                        nullability: col.nullability,
                    };
                    out_cols.push(col.clone());
                    columns.push((col.name.clone(), typed));
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let table_name = kind.to_string();
                let lower_table = table_name.to_lowercase();
                // Expand table.* — filter by source_table when available
                for col in &input_schema.columns {
                    let belongs = col
                        .source_table
                        .as_ref()
                        .is_some_and(|t| t.to_lowercase() == lower_table);
                    // If no columns have source_table set, fall back to including all
                    let any_tagged = input_schema
                        .columns
                        .iter()
                        .any(|c| c.source_table.is_some());
                    if !any_tagged || belongs {
                        let typed = TypedExpr::ColumnRef {
                            table: Some(table_name.clone()),
                            column: col.name.clone(),
                            resolved_type: col.sql_type.clone(),
                            nullability: col.nullability,
                        };
                        out_cols.push(col.clone());
                        columns.push((col.name.clone(), typed));
                    }
                }
            }
        }
    }

    (columns, RelSchema::new(out_cols))
}

/// Infer a column name from an expression (used when no alias is given)
fn infer_column_name(expr: &sqlparser::ast::Expr) -> String {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|i| i.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        sqlparser::ast::Expr::Function(f) => f.name.to_string().to_lowercase(),
        _ => "?column?".to_string(),
    }
}

/// Collect aggregate function calls from an expression (for GROUP BY processing)
fn collect_aggregates(
    expr: &sqlparser::ast::Expr,
    schema: &RelSchema,
    out: &mut Vec<(String, TypedExpr)>,
) {
    match expr {
        sqlparser::ast::Expr::Function(f) => {
            let name_upper = f.name.to_string().to_uppercase();
            if is_aggregate_function(&name_upper) {
                let typed = lower_expr(expr, schema);
                let col_name = name_upper.to_lowercase();
                out.push((col_name, typed));
            }
        }
        // Recurse into binary ops, etc.
        sqlparser::ast::Expr::BinaryOp { left, right, .. } => {
            collect_aggregates(left, schema, out);
            collect_aggregates(right, schema, out);
        }
        sqlparser::ast::Expr::UnaryOp { expr: inner, .. } => {
            collect_aggregates(inner, schema, out);
        }
        sqlparser::ast::Expr::Nested(inner) => {
            collect_aggregates(inner, schema, out);
        }
        _ => {}
    }
}

/// Check if a function name is a known aggregate
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name,
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "BOOL_AND"
            | "BOOL_OR"
            | "EVERY"
            | "STRING_AGG"
            | "ARRAY_AGG"
            | "LISTAGG"
            | "GROUP_CONCAT"
    )
}
