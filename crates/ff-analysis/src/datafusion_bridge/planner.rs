//! Convert SQL statements to DataFusion LogicalPlans
//!
//! Since DataFusion 52.x uses sqlparser 0.59 while Feather-Flow uses 0.60,
//! we re-parse SQL strings through DataFusion's own parser to avoid
//! cross-version type mismatches. The SQL string is the canonical form.

use datafusion_expr::LogicalPlan;
use datafusion_sql::parser::DFParserBuilder;
use datafusion_sql::planner::SqlToRel;

use crate::datafusion_bridge::provider::FeatherFlowProvider;
use crate::error::{AnalysisError, AnalysisResult};

/// Convert a SQL string to a DataFusion LogicalPlan
///
/// Re-parses the SQL through DataFusion's own parser (sqlparser 0.59)
/// and then plans it using the provided schema catalog.
pub fn sql_to_plan(sql: &str, provider: &FeatherFlowProvider) -> AnalysisResult<LogicalPlan> {
    // Use DataFusion's re-exported DuckDbDialect (sqlparser 0.59)
    // to avoid cross-version type mismatches with our sqlparser 0.60.
    let dialect = datafusion_expr::sqlparser::dialect::DuckDbDialect {};
    let mut parser = DFParserBuilder::new(sql)
        .with_dialect(&dialect)
        .build()
        .map_err(|e| AnalysisError::PlanningError(format!("Parse error: {e}")))?;

    let df_stmts = parser.parse_statements().map_err(|e| {
        AnalysisError::PlanningError(format!("Failed to parse SQL for planning: {e}"))
    })?;

    let planner = SqlToRel::new(provider);
    let first_stmt = df_stmts
        .into_iter()
        .next()
        .ok_or_else(|| AnalysisError::PlanningError("No statements found in SQL".to_string()))?;
    let plan = planner
        .statement_to_plan(first_stmt)
        .map_err(|e| AnalysisError::PlanningError(e.to_string()))?;

    Ok(plan)
}

#[cfg(test)]
#[path = "planner_test.rs"]
mod tests;
