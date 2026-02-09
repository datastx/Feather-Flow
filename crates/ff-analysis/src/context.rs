//! Analysis context — project-wide data available to all passes

use crate::ir::schema::RelSchema;
use ff_core::dag::ModelDag;
use ff_core::Project;
use ff_sql::ProjectLineage;
use std::collections::{HashMap, HashSet};

/// Context object passed to analysis passes, providing access to project metadata
pub struct AnalysisContext {
    /// The loaded project
    pub project: Project,
    /// Model dependency DAG
    pub dag: ModelDag,
    /// Schemas derived from YAML column definitions
    pub yaml_schemas: HashMap<String, RelSchema>,
    /// Column-level lineage from ff-sql
    pub lineage: ProjectLineage,
    /// Set of known model names
    pub known_models: HashSet<String>,
}

impl AnalysisContext {
    /// Create a new analysis context
    pub fn new(
        project: Project,
        dag: ModelDag,
        yaml_schemas: HashMap<String, RelSchema>,
        lineage: ProjectLineage,
    ) -> Self {
        let known_models = project.models.keys().map(|k| k.to_string()).collect();
        Self {
            project,
            dag,
            yaml_schemas,
            lineage,
            known_models,
        }
    }

    /// Get the YAML-declared schema for a model, if available
    pub fn model_schema(&self, model_name: &str) -> Option<&RelSchema> {
        self.yaml_schemas.get(model_name)
    }
}
