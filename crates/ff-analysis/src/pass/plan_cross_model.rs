//! Cross-model consistency checks using DataFusion LogicalPlans
//!
//! Detects type and nullability mismatches between YAML declarations
//! and inferred schemas from LogicalPlan output.

use std::collections::HashMap;

use ff_core::ModelName;

use crate::context::AnalysisContext;
use crate::datafusion_bridge::propagation::{ModelPlanResult, SchemaMismatch};

use super::plan_pass::DagPlanPass;
use super::{Diagnostic, DiagnosticCode, Severity};

/// Convert a single schema mismatch into a diagnostic.
fn mismatch_to_diagnostic(
    model_name: &str,
    mismatch: &SchemaMismatch,
    pass_name: &'static str,
) -> Diagnostic {
    match mismatch {
        SchemaMismatch::ExtraInSql { column } => Diagnostic {
            code: DiagnosticCode::A040,
            severity: Severity::Warning,
            message: format!("Column '{column}' is in SQL output but not declared in YAML"),
            model: ModelName::new(model_name),
            column: Some(column.clone()),
            hint: Some(format!(
                "Add '{column}' to the YAML schema or remove it from SELECT"
            )),
            pass_name: pass_name.into(),
        },
        SchemaMismatch::MissingFromSql { column } => Diagnostic {
            code: DiagnosticCode::A040,
            severity: Severity::Error,
            message: format!("Column '{column}' declared in YAML but missing from SQL output"),
            model: ModelName::new(model_name),
            column: Some(column.clone()),
            hint: Some(format!("Add '{column}' to SELECT or remove it from YAML")),
            pass_name: pass_name.into(),
        },
        SchemaMismatch::TypeMismatch {
            column,
            yaml_type,
            inferred_type,
        } => Diagnostic {
            code: DiagnosticCode::A040,
            severity: Severity::Warning,
            message: format!(
                "Column '{column}' type mismatch: YAML declares {yaml_type}, SQL infers {inferred_type}"
            ),
            model: ModelName::new(model_name),
            column: Some(column.clone()),
            hint: Some(format!(
                "Update YAML type to '{inferred_type}' or add explicit CAST"
            )),
            pass_name: pass_name.into(),
        },
        SchemaMismatch::NullabilityMismatch {
            column,
            yaml_nullable,
            inferred_nullable,
        } => {
            let hint = if !yaml_nullable && *inferred_nullable {
                "Add COALESCE() or WHERE IS NOT NULL guard, or mark the column as nullable in YAML"
            } else {
                "Consider marking the column as NOT NULL in YAML to match the SQL"
            };
            Diagnostic {
                code: DiagnosticCode::A041,
                severity: Severity::Warning,
                message: format!(
                    "Column '{column}' nullability mismatch: YAML declares nullable={yaml_nullable}, SQL infers nullable={inferred_nullable}"
                ),
                model: ModelName::new(model_name),
                column: Some(column.clone()),
                hint: Some(hint.to_string()),
                pass_name: pass_name.into(),
            }
        }
    }
}

/// Cross-model consistency pass
///
/// Checks that inferred schemas from DataFusion LogicalPlans match the
/// YAML column declarations. Emits A040 for type mismatches and A041
/// for nullability mismatches between YAML and inferred output.
pub(crate) struct CrossModelConsistency;

impl DagPlanPass for CrossModelConsistency {
    fn name(&self) -> &'static str {
        "cross_model_consistency"
    }

    fn description(&self) -> &'static str {
        "Checks YAML declarations against inferred schemas from LogicalPlan output"
    }

    fn run_project(
        &self,
        models: &HashMap<ModelName, ModelPlanResult>,
        _ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut model_names: Vec<_> = models.keys().collect();
        model_names.sort();

        model_names
            .iter()
            .filter_map(|name| models.get(*name).map(|result| (*name, result)))
            .flat_map(|(name, result)| {
                result
                    .mismatches
                    .iter()
                    .map(move |m| mismatch_to_diagnostic(name, m, self.name()))
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "plan_cross_model_test.rs"]
mod tests;
