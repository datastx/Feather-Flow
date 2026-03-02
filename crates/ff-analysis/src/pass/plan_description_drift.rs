//! Description drift detection pass (A050-A052)
//!
//! Checks column-level lineage edges for documentation drift:
//! - A050: Copy/Rename column with missing description — suggest inheriting from upstream
//! - A051: Copy/Rename column with modified description — potential drift
//! - A052: Transform column with missing description — needs new documentation

use std::collections::HashMap;

use ff_core::ModelName;

use crate::context::AnalysisContext;
use crate::datafusion_bridge::propagation::ModelPlanResult;

use super::plan_pass::DagPlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// DAG-level pass that checks description propagation across lineage edges.
pub struct PlanDescriptionDrift;

impl DagPlanPass for PlanDescriptionDrift {
    fn name(&self) -> &'static str {
        "description_drift"
    }

    fn description(&self) -> &'static str {
        "Detect missing or drifted column descriptions across lineage edges"
    }

    fn run_project(
        &self,
        _models: &HashMap<ModelName, ModelPlanResult>,
        ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let lineage = ctx.lineage();
        let project = ctx.project();

        let desc_lookup = build_project_descriptions(project);

        for edge in &lineage.edges {
            let targets_a_model = project.models.contains_key(edge.target_model.as_str());
            if !targets_a_model {
                continue;
            }

            let src_desc = desc_lookup
                .get(&edge.source_model)
                .and_then(|cols| cols.get(&edge.source_column.to_lowercase()));
            let tgt_desc = desc_lookup
                .get(&edge.target_model)
                .and_then(|cols| cols.get(&edge.target_column.to_lowercase()));

            match (edge.is_direct, src_desc, tgt_desc) {
                (true, Some(_src), None) => {
                    diagnostics.push(Diagnostic {
                        code: DiagnosticCode::A050,
                        severity: Severity::Warning,
                        message: format!(
                            "Column '{}' is a direct pass-through from '{}.{}' but has no description — consider inheriting from upstream",
                            edge.target_column, edge.source_model, edge.source_column
                        ),
                        model: ModelName::new(&edge.target_model),
                        column: Some(edge.target_column.clone()),
                        hint: Some(format!(
                            "Add a description to '{}' in the YAML schema, or copy it from '{}.{}'",
                            edge.target_column, edge.source_model, edge.source_column
                        )),
                        pass_name: "description_drift".into(),
                    });
                }
                (true, Some(src), Some(tgt)) if src != tgt => {
                    diagnostics.push(Diagnostic {
                        code: DiagnosticCode::A051,
                        severity: Severity::Info,
                        message: format!(
                            "Column '{}' is a direct pass-through from '{}.{}' but has a different description — verify this is intentional",
                            edge.target_column, edge.source_model, edge.source_column
                        ),
                        model: ModelName::new(&edge.target_model),
                        column: Some(edge.target_column.clone()),
                        hint: None,
                        pass_name: "description_drift".into(),
                    });
                }
                (false, _, None) => {
                    diagnostics.push(Diagnostic {
                        code: DiagnosticCode::A052,
                        severity: Severity::Warning,
                        message: format!(
                            "Column '{}' is a transformation but has no description — consider documenting it",
                            edge.target_column,
                        ),
                        model: ModelName::new(&edge.target_model),
                        column: Some(edge.target_column.clone()),
                        hint: Some(format!(
                            "Add a description to '{}' in the YAML schema",
                            edge.target_column
                        )),
                        pass_name: "description_drift".into(),
                    });
                }
                _ => {}
            }
        }

        diagnostics
    }
}

/// Build a `column_name_lowercase -> description` map from an iterator of
/// `(name, optional_description)` pairs.
fn cols_to_desc_map<'a>(
    cols: impl Iterator<Item = (&'a str, Option<&'a String>)>,
) -> HashMap<String, String> {
    cols.filter_map(|(name, desc)| desc.map(|d| (name.to_lowercase(), d.clone())))
        .collect()
}

/// Build a lookup of model_name -> { column_name_lowercase -> description }
/// from model YAML schemas and source definitions.
fn build_project_descriptions(
    project: &ff_core::Project,
) -> HashMap<String, HashMap<String, String>> {
    let models_iter = project.models.iter().filter_map(|(name, model)| {
        let schema = model.schema.as_ref()?;
        let col_descs = cols_to_desc_map(
            schema
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.description.as_ref())),
        );
        if col_descs.is_empty() {
            None
        } else {
            Some((name.to_string(), col_descs))
        }
    });

    let sources_iter = project.sources.iter().flat_map(|sf| {
        sf.tables.iter().filter_map(|table| {
            let col_descs = cols_to_desc_map(
                table
                    .columns
                    .iter()
                    .map(|c| (c.name.as_str(), c.description.as_ref())),
            );
            if col_descs.is_empty() {
                None
            } else {
                Some((table.name.clone(), col_descs))
            }
        })
    });

    models_iter.chain(sources_iter).collect()
}
