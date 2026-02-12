//! ff-analysis: Static analysis for SQL models using DataFusion LogicalPlans
//!
//! This crate provides composable analysis passes that operate on DataFusion
//! LogicalPlans, type/schema definitions shared with the DataFusion bridge,
//! and schema propagation infrastructure.

pub(crate) mod context;
pub mod datafusion_bridge;
pub(crate) mod error;
pub(crate) mod pass;
pub mod schema;
pub mod types;

#[cfg(any(test, feature = "test-support"))]
pub mod test_utils;

pub use context::AnalysisContext;
pub use error::{AnalysisError, AnalysisResult};
pub use pass::plan_pass::{DagPlanPass, PlanPass, PlanPassManager};
pub use pass::{Diagnostic, DiagnosticCode, Severity};
pub use schema::{RelSchema, SchemaCatalog};
pub use types::{parse_sql_type, FloatBitWidth, IntBitWidth, Nullability, SqlType, TypedColumn};

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
