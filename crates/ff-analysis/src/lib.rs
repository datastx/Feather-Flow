//! ff-analysis: LLVM-style static analysis for SQL models
//!
//! This crate provides a relational algebra IR, AST-to-IR lowering,
//! and composable analysis passes for detecting SQL issues.

pub mod context;
pub mod error;
pub mod ir;
pub mod lowering;
pub mod pass;

pub use context::AnalysisContext;
pub use error::{AnalysisError, AnalysisResult};
pub use ir::expr::TypedExpr;
pub use ir::relop::RelOp;
pub use ir::schema::RelSchema;
pub use ir::types::{Nullability, SqlType, TypedColumn};
pub use lowering::{lower_statement, SchemaCatalog};
pub use pass::{AnalysisPass, DagPass, Diagnostic, DiagnosticCode, PassManager, Severity};
