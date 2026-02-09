//! Shared test utilities for ff-analysis

use crate::context::AnalysisContext;
use crate::ir::schema::RelSchema;
use crate::ir::types::{Nullability, SqlType, TypedColumn};
use ff_core::dag::ModelDag;
use ff_core::ModelName;
use ff_core::Project;
use ff_sql::ProjectLineage;
use std::collections::HashMap;
use std::path::Path;

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

/// Create a typed column with a source table for testing qualified lookups
pub(crate) fn make_col_sourced(
    name: &str,
    source: &str,
    ty: SqlType,
    null: Nullability,
) -> TypedColumn {
    TypedColumn {
        name: name.to_string(),
        source_table: Some(source.to_string()),
        sql_type: ty,
        nullability: null,
        provenance: vec![],
    }
}

/// Create a minimal `AnalysisContext` for testing (empty YAML schemas and lineage)
pub(crate) fn make_ctx() -> AnalysisContext {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/sample_project");
    let project = Project::load(&fixtures).unwrap();
    let dag = ModelDag::build(&HashMap::new()).unwrap();
    AnalysisContext::new(project, dag, HashMap::new(), ProjectLineage::new())
}

/// Create an `AnalysisContext` with YAML-declared schemas for nullability testing
pub(crate) fn make_ctx_with_yaml(yaml_schemas: HashMap<ModelName, RelSchema>) -> AnalysisContext {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/sample_project");
    let project = Project::load(&fixtures).unwrap();
    let dag = ModelDag::build(&HashMap::new()).unwrap();
    AnalysisContext::new(project, dag, yaml_schemas, ProjectLineage::new())
}
