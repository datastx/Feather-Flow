//! Incremental strategies and Write-Audit-Publish (WAP) execution.

use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use ff_core::model::ModelSchema;
use ff_core::sql_utils::quote_qualified;
use ff_db::Database;
use std::sync::Arc;

use super::compile::CompiledModel;

/// Parameters for Write-Audit-Publish execution
pub(super) struct WapParams<'a> {
    /// Database connection
    pub(super) db: &'a Arc<dyn Database>,
    /// Model name
    pub(super) name: &'a str,
    /// Fully qualified table name in the production schema
    pub(super) qualified_name: &'a str,
    /// Schema used for WAP staging tables
    pub(super) wap_schema: &'a str,
    /// Compiled model metadata
    pub(super) compiled: &'a CompiledModel,
    /// Whether to drop and recreate regardless of existing state
    pub(super) full_refresh: bool,
    /// Rendered SQL with query comment appended
    pub(super) exec_sql: &'a str,
}

/// Execute an incremental model with schema change handling
pub(super) async fn execute_incremental(
    db: &Arc<dyn Database>,
    table_name: &str,
    compiled: &CompiledModel,
    full_refresh: bool,
    exec_sql: &str,
) -> ff_db::error::DbResult<()> {
    // Check if table exists — propagate DB errors to avoid silent full refresh
    let exists = db.relation_exists(table_name).await?;

    if !exists || full_refresh {
        if !exists {
            // First run: create an empty stub table so self-referencing queries
            // (e.g. `WHERE id NOT IN (SELECT id FROM this_table)`) can resolve.
            // The stub is immediately replaced by the CREATE OR REPLACE below.
            create_stub_table(db, table_name, compiled).await?;
        }
        // Full refresh or first run: (re)create table from full query.
        // Uses CREATE OR REPLACE so the existing/stub table is readable
        // during query evaluation before being replaced with the result.
        return db.create_table_as(table_name, exec_sql, true).await;
    }

    // Check for schema changes
    let on_schema_change = compiled.on_schema_change.unwrap_or(OnSchemaChange::Ignore);

    if on_schema_change != OnSchemaChange::Ignore {
        handle_schema_changes(db, table_name, compiled, on_schema_change).await?;
    }

    // Execute the incremental strategy
    execute_strategy(db, table_name, compiled, exec_sql).await
}

/// Create an empty stub table so self-referencing queries can resolve on first run.
///
/// Uses the model's YAML column schema to generate the correct DDL.  If no
/// schema is defined, the stub is skipped and the CTAS will either succeed
/// (no self-reference) or produce the usual "table not found" error.
async fn create_stub_table(
    db: &Arc<dyn Database>,
    table_name: &str,
    compiled: &CompiledModel,
) -> ff_db::error::DbResult<()> {
    let columns = match compiled.model_schema {
        Some(ref schema) if !schema.columns.is_empty() => &schema.columns,
        _ => return Ok(()),
    };

    let col_defs: Vec<String> = columns
        .iter()
        .map(|col| format!("\"{}\" {}", col.name, col.data_type))
        .collect();
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_qualified(table_name),
        col_defs.join(", ")
    );
    db.execute(&sql).await.map(|_| ())
}

/// Check for and handle schema changes on an incremental model
async fn handle_schema_changes(
    db: &Arc<dyn Database>,
    table_name: &str,
    compiled: &CompiledModel,
    on_schema_change: OnSchemaChange,
) -> ff_db::error::DbResult<()> {
    let existing_schema = db.get_table_schema(table_name).await?;
    let existing_columns: std::collections::HashSet<String> = existing_schema
        .iter()
        .map(|(name, _)| name.to_lowercase())
        .collect();

    let new_schema = db.describe_query(&compiled.sql).await?;
    let new_columns: std::collections::HashMap<String, String> = new_schema
        .iter()
        .map(|(name, typ)| (name.to_lowercase(), typ.clone()))
        .collect();

    let added_columns: Vec<(String, String)> = new_schema
        .iter()
        .filter(|(name, _)| !existing_columns.contains(&name.to_lowercase()))
        .map(|(name, typ)| (name.clone(), typ.clone()))
        .collect();

    let removed_columns: Vec<String> = existing_schema
        .iter()
        .filter(|(name, _)| !new_columns.contains_key(&name.to_lowercase()))
        .map(|(name, _)| name.clone())
        .collect();

    if added_columns.is_empty() && removed_columns.is_empty() {
        return Ok(());
    }

    match on_schema_change {
        OnSchemaChange::Fail => {
            let msg = format_schema_change_message(&added_columns, &removed_columns);
            return Err(ff_db::DbError::ExecutionError(msg));
        }
        OnSchemaChange::AppendNewColumns => {
            if !added_columns.is_empty() {
                db.add_columns(table_name, &added_columns).await?;
            }
        }
        OnSchemaChange::Ignore => {}
    }

    Ok(())
}

/// Build an error message describing which columns were added or removed.
fn format_schema_change_message(
    added_columns: &[(String, String)],
    removed_columns: &[String],
) -> String {
    let mut msg = String::from("Schema change detected: ");
    if !added_columns.is_empty() {
        msg.push_str(&format!(
            "new columns: {}; ",
            added_columns
                .iter()
                .map(|(n, t)| format!("{} ({})", n, t))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !removed_columns.is_empty() {
        msg.push_str(&format!("removed columns: {}", removed_columns.join(", ")));
    }
    msg
}

/// Execute the incremental strategy (append, merge, or delete+insert)
async fn execute_strategy(
    db: &Arc<dyn Database>,
    table_name: &str,
    compiled: &CompiledModel,
    exec_sql: &str,
) -> ff_db::error::DbResult<()> {
    let strategy = compiled
        .incremental_strategy
        .unwrap_or(IncrementalStrategy::Append);

    match strategy {
        IncrementalStrategy::Append => {
            let insert_sql = format!("INSERT INTO {} {}", quote_qualified(table_name), exec_sql);
            db.execute(&insert_sql).await.map(|_| ())
        }
        IncrementalStrategy::Merge => {
            let unique_keys = compiled.unique_key.as_deref().unwrap_or_default();
            if unique_keys.is_empty() {
                Err(ff_db::DbError::ExecutionError(
                    "Merge strategy requires unique_key to be specified".to_string(),
                ))
            } else {
                db.merge_into(table_name, exec_sql, unique_keys).await
            }
        }
        IncrementalStrategy::DeleteInsert => {
            let unique_keys = compiled.unique_key.as_deref().unwrap_or_default();
            if unique_keys.is_empty() {
                Err(ff_db::DbError::ExecutionError(
                    "Delete+insert strategy requires unique_key to be specified".to_string(),
                ))
            } else {
                db.delete_insert(table_name, exec_sql, unique_keys).await
            }
        }
    }
}

/// Named references for WAP incremental execution.
struct WapIncrementalCtx<'a> {
    qualified_name: &'a str,
    wap_qualified: &'a str,
    quoted_wap: &'a str,
    quoted_name: &'a str,
}

/// Copy the production table to the WAP schema and apply incremental logic.
///
/// This is the `Materialization::Incremental` arm of `execute_wap`, extracted
/// to keep the match body shallow.
async fn wap_incremental(
    db: &Arc<dyn Database>,
    ctx: &WapIncrementalCtx<'_>,
    compiled: &CompiledModel,
    full_refresh: bool,
    exec_sql: &str,
) -> Result<(), ff_db::error::DbError> {
    if !full_refresh {
        let exists = db.relation_exists(ctx.qualified_name).await?;
        if exists {
            let copy_sql = format!(
                "CREATE OR REPLACE TABLE {} AS FROM {}",
                ctx.quoted_wap, ctx.quoted_name
            );
            db.execute(&copy_sql).await?;
        }
    }
    execute_incremental(db, ctx.wap_qualified, compiled, full_refresh, exec_sql).await
}

/// Execute Write-Audit-Publish flow for a model.
///
/// 1. Create WAP schema if needed
/// 2. For tables: CTAS into wap_schema
///    For incremental: copy prod to wap_schema, then apply incremental
/// 3. Run schema tests against wap_schema copy
/// 4. If tests pass: DROP prod + CTAS from wap to prod
/// 5. If tests fail: keep wap table, return error
pub(super) async fn execute_wap(params: &WapParams<'_>) -> Result<(), ff_db::error::DbError> {
    let WapParams {
        db,
        name,
        qualified_name,
        wap_schema,
        compiled,
        full_refresh,
        exec_sql,
    } = params;

    let wap_qualified = format!("{}.{}", wap_schema, name);
    let quoted_wap = quote_qualified(&wap_qualified);
    let quoted_name = quote_qualified(qualified_name);

    // 1. Create WAP schema
    db.create_schema_if_not_exists(wap_schema).await?;

    // 2. Materialize into WAP schema
    match compiled.materialization {
        Materialization::Table => {
            db.create_table_as(&wap_qualified, exec_sql, true).await?;
        }
        Materialization::Incremental => {
            let inc_ctx = WapIncrementalCtx {
                qualified_name,
                wap_qualified: &wap_qualified,
                quoted_wap: &quoted_wap,
                quoted_name: &quoted_name,
            };
            wap_incremental(db, &inc_ctx, compiled, *full_refresh, exec_sql).await?;
        }
        other => {
            return Err(ff_db::error::DbError::ExecutionError(format!(
                "WAP only applies to table/incremental, got {:?}",
                other
            )));
        }
    }

    // 3. Run schema tests against WAP copy
    let test_failures =
        run_wap_tests(db, name, &wap_qualified, compiled.model_schema.as_ref()).await;

    if test_failures > 0 {
        return Err(ff_db::error::DbError::ExecutionError(format!(
            "WAP audit failed: {} test(s) failed for '{}'. \
             Staging table preserved at '{}' for debugging. \
             Production table is untouched.",
            test_failures, name, wap_qualified
        )));
    }

    // 4. Tests passed -- publish: DROP prod + CTAS from WAP
    db.drop_if_exists(qualified_name).await?;

    let publish_sql = format!("CREATE TABLE {} AS FROM {}", quoted_name, quoted_wap);
    db.execute(&publish_sql).await?;

    // Clean up WAP table after successful publish
    let _ = db.drop_if_exists(&wap_qualified).await;

    Ok(())
}

/// Run schema tests for a single model against a specific qualified name.
///
/// Returns the number of test failures.
async fn run_wap_tests(
    db: &Arc<dyn Database>,
    model_name: &str,
    wap_qualified_name: &str,
    model_schema: Option<&ModelSchema>,
) -> usize {
    let schema = match model_schema {
        Some(s) => s,
        None => return 0, // No schema = no tests = pass
    };

    let tests = schema.extract_tests(model_name);
    if tests.is_empty() {
        return 0;
    }

    let mut failures = 0;

    for test in &tests {
        let generated =
            ff_test::generator::GeneratedTest::from_schema_test_qualified(test, wap_qualified_name);

        match db.query_count(&generated.sql).await {
            Ok(count) if count > 0 => {
                println!(
                    "    WAP audit FAIL: {} on {}.{} ({} failures)",
                    test.test_type, model_name, test.column, count
                );
                failures += 1;
            }
            Err(e) => {
                println!(
                    "    WAP audit ERROR: {} on {}.{}: {}",
                    test.test_type, model_name, test.column, e
                );
                failures += 1;
            }
            _ => {}
        }
    }

    failures
}
