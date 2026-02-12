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
        models: &HashMap<String, ModelPlanResult>,
        _ctx: &AnalysisContext,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        let mut model_names: Vec<_> = models.keys().collect();
        model_names.sort();

        for model_name in model_names {
            let result = &models[model_name];
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
                            pass_name: self.name().into(),
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
                            pass_name: self.name().into(),
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
                            pass_name: self.name().into(),
                        });
                    }
                    SchemaMismatch::NullabilityMismatch {
                        column,
                        yaml_nullable,
                        inferred_nullable,
                    } => {
                        let hint = if !yaml_nullable && *inferred_nullable {
                            // YAML says NOT NULL but SQL may produce NULL
                            "Add COALESCE() or WHERE IS NOT NULL guard, or mark the column as nullable in YAML"
                        } else {
                            // YAML says nullable but SQL always produces NOT NULL
                            "Consider marking the column as NOT NULL in YAML to match the SQL"
                        };
                        diagnostics.push(Diagnostic {
                            code: DiagnosticCode::A041,
                            severity: Severity::Warning,
                            message: format!(
                                "Column '{column}' nullability mismatch: YAML declares nullable={yaml_nullable}, SQL infers nullable={inferred_nullable}"
                            ),
                            model: model_name.clone(),
                            column: Some(column.clone()),
                            hint: Some(hint.to_string()),
                            pass_name: self.name().into(),
                        });
                    }
                }
            }
        }

        diagnostics
    }
}

#[cfg(test)]
#[path = "plan_cross_model_test.rs"]
mod tests;
