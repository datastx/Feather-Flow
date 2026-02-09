//! Cross-model consistency checks using DataFusion LogicalPlans
//!
//! Detects type and nullability mismatches between YAML declarations
//! and inferred schemas from LogicalPlan output.

use std::collections::HashMap;

use crate::context::AnalysisContext;
use crate::datafusion_bridge::propagation::{ModelPlanResult, SchemaMismatch};

use super::plan_pass::DagPlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// Cross-model consistency pass
///
/// Checks that inferred schemas from DataFusion LogicalPlans match the
/// YAML column declarations. Emits A040 for type mismatches and A041
/// for nullability mismatches between YAML and inferred output.
pub struct CrossModelConsistency;

impl DagPlanPass for CrossModelConsistency {
    fn name(&self) -> &'static str {
        "cross_model_consistency"
    }

    fn description(&self) -> &'static str {
        "Checks YAML declarations against inferred schemas from LogicalPlan output"
    }

    fn run_project(
        &self,
        models: &HashMap<String, ModelPlanResult>,
        _ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        for (model_name, result) in models {
            for mismatch in &result.mismatches {
                match mismatch {
                    SchemaMismatch::ExtraInSql { column } => {
                        diagnostics.push(Diagnostic {
                            code: DiagnosticCode::A040,
                            severity: Severity::Warning,
                            message: format!(
                                "Column '{column}' is in SQL output but not declared in YAML"
                            ),
                            model: model_name.clone(),
                            column: Some(column.clone()),
                            hint: Some(format!(
                                "Add '{column}' to the YAML schema or remove it from SELECT"
                            )),
                            pass_name: self.name().to_string(),
                        });
                    }
                    SchemaMismatch::MissingFromSql { column } => {
                        diagnostics.push(Diagnostic {
                            code: DiagnosticCode::A040,
                            severity: Severity::Error,
                            message: format!(
                                "Column '{column}' declared in YAML but missing from SQL output"
                            ),
                            model: model_name.clone(),
                            column: Some(column.clone()),
                            hint: Some(format!("Add '{column}' to SELECT or remove it from YAML")),
                            pass_name: self.name().to_string(),
                        });
                    }
                    SchemaMismatch::TypeMismatch {
                        column,
                        yaml_type,
                        inferred_type,
                    } => {
                        diagnostics.push(Diagnostic {
                            code: DiagnosticCode::A040,
                            severity: Severity::Warning,
                            message: format!(
                                "Column '{column}' type mismatch: YAML declares {yaml_type}, SQL infers {inferred_type}"
                            ),
                            model: model_name.clone(),
                            column: Some(column.clone()),
                            hint: Some(format!(
                                "Update YAML type to '{inferred_type}' or add explicit CAST"
                            )),
                            pass_name: self.name().to_string(),
                        });
                    }
                    SchemaMismatch::NullabilityMismatch {
                        column,
                        yaml_nullable: _,
                        inferred_nullable: _,
                    } => {
                        diagnostics.push(Diagnostic {
                            code: DiagnosticCode::A041,
                            severity: Severity::Warning,
                            message: format!(
                                "Column '{column}' declared NOT NULL in YAML but SQL may produce NULL"
                            ),
                            model: model_name.clone(),
                            column: Some(column.clone()),
                            hint: Some(
                                "Add COALESCE() or WHERE IS NOT NULL guard, or relax YAML constraint"
                                    .to_string(),
                            ),
                            pass_name: self.name().to_string(),
                        });
                    }
                }
            }
        }

        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion_bridge::propagation::propagate_schemas;
    use crate::ir::schema::RelSchema;
    use crate::ir::types::Nullability;
    use crate::lowering::SchemaCatalog;
    use crate::test_utils::{int32, make_col, varchar};
    use ff_core::dag::ModelDag;
    use ff_core::Project;
    use ff_sql::ProjectLineage;
    use std::path::PathBuf;

    fn make_test_context() -> AnalysisContext {
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

    #[test]
    fn test_a040_extra_column_in_sql() {
        let mut initial_catalog: SchemaCatalog = HashMap::new();
        initial_catalog.insert(
            "source".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ]),
        );

        let topo_order = vec!["test_model".to_string()];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "test_model".to_string(),
            "SELECT id, name FROM source".to_string(),
        );

        // YAML only declares 'id', not 'name'
        let mut yaml_schemas = HashMap::new();
        yaml_schemas.insert(
            "test_model".to_string(),
            RelSchema::new(vec![make_col("id", int32(), Nullability::NotNull)]),
        );

        let result = propagate_schemas(&topo_order, &sql_sources, &yaml_schemas, &initial_catalog);
        let ctx = make_test_context();
        let pass = CrossModelConsistency;
        let diagnostics = pass.run_project(&result.model_plans, &ctx);

        assert!(
            diagnostics
                .iter()
                .any(|d| d.code == DiagnosticCode::A040 && d.column.as_deref() == Some("name")),
            "Should emit A040 for extra 'name' column"
        );
    }

    #[test]
    fn test_a040_missing_column_from_sql() {
        let mut initial_catalog: SchemaCatalog = HashMap::new();
        initial_catalog.insert(
            "source".to_string(),
            RelSchema::new(vec![make_col("id", int32(), Nullability::NotNull)]),
        );

        let topo_order = vec!["test_model".to_string()];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "test_model".to_string(),
            "SELECT id FROM source".to_string(),
        );

        // YAML declares 'id' and 'name', but SQL only outputs 'id'
        let mut yaml_schemas = HashMap::new();
        yaml_schemas.insert(
            "test_model".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ]),
        );

        let result = propagate_schemas(&topo_order, &sql_sources, &yaml_schemas, &initial_catalog);
        let ctx = make_test_context();
        let pass = CrossModelConsistency;
        let diagnostics = pass.run_project(&result.model_plans, &ctx);

        let missing = diagnostics
            .iter()
            .find(|d| d.code == DiagnosticCode::A040 && d.column.as_deref() == Some("name"));
        assert!(missing.is_some(), "Should emit A040 for missing 'name'");
        assert_eq!(missing.unwrap().severity, Severity::Error);
    }

    #[test]
    fn test_no_diagnostics_for_matching_schema() {
        let mut initial_catalog: SchemaCatalog = HashMap::new();
        initial_catalog.insert(
            "source".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ]),
        );

        let topo_order = vec!["test_model".to_string()];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "test_model".to_string(),
            "SELECT id, name FROM source".to_string(),
        );

        // YAML matches SQL output exactly
        let mut yaml_schemas = HashMap::new();
        yaml_schemas.insert(
            "test_model".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ]),
        );

        let result = propagate_schemas(&topo_order, &sql_sources, &yaml_schemas, &initial_catalog);
        let ctx = make_test_context();
        let pass = CrossModelConsistency;
        let diagnostics = pass.run_project(&result.model_plans, &ctx);

        assert!(
            diagnostics.is_empty(),
            "Should have no diagnostics for matching schemas, got: {:?}",
            diagnostics.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }
}
