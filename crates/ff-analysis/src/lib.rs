//! ff-analysis: LLVM-style static analysis for SQL models
//!
//! This crate provides a relational algebra IR, AST-to-IR lowering,
//! and composable analysis passes for detecting SQL issues.

pub(crate) mod context;
pub mod datafusion_bridge;
pub(crate) mod error;
pub(crate) mod ir;
pub(crate) mod lowering;
pub(crate) mod pass;

#[cfg(any(test, feature = "test-support"))]
pub mod test_utils;

pub use context::AnalysisContext;
pub use error::{AnalysisError, AnalysisResult};
pub use ir::expr::TypedExpr;
pub use ir::relop::RelOp;
pub use ir::schema::RelSchema;
pub use ir::types::{
    parse_sql_type, FloatBitWidth, IntBitWidth, Nullability, SqlType, TypedColumn,
};
pub use lowering::{lower_statement, SchemaCatalog};
pub use pass::plan_pass::{DagPlanPass, PlanPass, PlanPassManager};
pub use pass::{AnalysisPass, DagPass, Diagnostic, DiagnosticCode, PassManager, Severity};

// DataFusion bridge re-exports
pub use datafusion_bridge::lineage::{
    deduplicate_edges, extract_column_lineage as extract_plan_column_lineage, ColumnLineageEdge,
    LineageKind, ModelColumnLineage,
};
pub use datafusion_bridge::planner::sql_to_plan;
pub use datafusion_bridge::propagation::{
    propagate_schemas, ModelPlanResult, PropagationResult, SchemaMismatch,
};
pub use datafusion_bridge::provider::UserFunctionStub;
