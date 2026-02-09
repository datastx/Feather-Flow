//! Shared test utilities for ff-analysis

use crate::context::AnalysisContext;
use crate::ir::types::{Nullability, SqlType, TypedColumn};
use ff_core::dag::ModelDag;
use ff_core::Project;
use ff_sql::ProjectLineage;
use std::collections::HashMap;
use std::path::PathBuf;

/// Create a typed column for testing
pub(crate) fn make_col(name: &str, ty: SqlType, null: Nullability) -> TypedColumn {
    TypedColumn {
        name: name.to_string(),
        source_table: None,
        sql_type: ty,
        nullability: null,
        provenance: vec![],
    }
}

/// Shorthand for `SqlType::Integer { bits: IntBitWidth::I32 }`
pub(crate) fn int32() -> SqlType {
    SqlType::Integer {
        bits: crate::ir::types::IntBitWidth::I32,
    }
}

/// Shorthand for `SqlType::String { max_length: None }`
pub(crate) fn varchar() -> SqlType {
    SqlType::String { max_length: None }
}

/// Create a minimal in-memory `AnalysisContext` for testing.
///
/// Uses a synthetic `Project` to avoid filesystem dependencies.
pub(crate) fn make_ctx() -> AnalysisContext {
    let config: ff_core::config::Config = serde_yaml::from_str("name: test_project").unwrap();
    let project = Project {
        root: PathBuf::from("/tmp/test"),
        config,
        models: HashMap::new(),
        tests: vec![],
        singular_tests: vec![],
        sources: vec![],
        exposures: vec![],
        metrics: vec![],
    };
    let dag = ModelDag::build(&HashMap::new()).unwrap();
    AnalysisContext::new(project, dag, HashMap::new(), ProjectLineage::new())
}
