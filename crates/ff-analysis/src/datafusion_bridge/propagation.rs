//! DAG-wide schema propagation using DataFusion LogicalPlans
//!
//! Walks models in topological order, plans each one via DataFusion,
//! extracts inferred output schemas, cross-checks against YAML declarations,
//! and feeds schemas forward for downstream models.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_expr::LogicalPlan;

use crate::datafusion_bridge::planner::sql_to_plan;
use crate::datafusion_bridge::provider::{
    FeatherFlowProvider, FunctionRegistry, UserFunctionStub, UserTableFunctionStub,
};
use crate::datafusion_bridge::types::arrow_to_sql_type;
use crate::schema::{RelSchema, SchemaCatalog};
use crate::types::{Nullability, TypedColumn};

/// Result of planning a single model via DataFusion.
///
/// Contains the logical plan, the inferred output schema, and any
/// mismatches detected when cross-checking against the YAML declaration.
pub struct ModelPlanResult {
    /// The DataFusion LogicalPlan for this model
    pub plan: LogicalPlan,
    /// The inferred output schema (from the plan), shared with the catalog
    pub inferred_schema: Arc<RelSchema>,
    /// Schema mismatches between YAML declaration and inferred output
    pub mismatches: Vec<SchemaMismatch>,
}

/// A mismatch between the YAML-declared schema and the inferred schema
#[derive(Debug, Clone)]
pub enum SchemaMismatch {
    /// Column exists in SQL output but not in YAML
    ExtraInSql { column: String },
    /// Column declared in YAML but not in SQL output
    MissingFromSql { column: String },
    /// YAML type vs inferred type differ
    TypeMismatch {
        column: String,
        yaml_type: String,
        inferred_type: String,
    },
    /// YAML nullability vs inferred nullability differ
    NullabilityMismatch {
        column: String,
        yaml_nullable: bool,
        inferred_nullable: bool,
    },
}

impl std::fmt::Display for SchemaMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaMismatch::ExtraInSql { column } => {
                write!(
                    f,
                    "Column '{column}' in SQL output but not declared in YAML"
                )
            }
            SchemaMismatch::MissingFromSql { column } => {
                write!(
                    f,
                    "Column '{column}' declared in YAML but missing from SQL output"
                )
            }
            SchemaMismatch::TypeMismatch {
                column,
                yaml_type,
                inferred_type,
            } => {
                write!(
                    f,
                    "Column '{column}' type mismatch: YAML={yaml_type}, inferred={inferred_type}"
                )
            }
            SchemaMismatch::NullabilityMismatch {
                column,
                yaml_nullable,
                inferred_nullable,
            } => {
                write!(
                    f,
                    "Column '{column}' nullability mismatch: YAML nullable={yaml_nullable}, inferred nullable={inferred_nullable}"
                )
            }
        }
    }
}

/// Propagation results for the entire DAG.
///
/// Aggregates per-model planning outcomes, the final schema catalog with all
/// inferred schemas registered, and any models that failed to plan.
pub struct PropagationResult {
    /// Per-model planning results (model name → result)
    pub model_plans: HashMap<String, ModelPlanResult>,
    /// Final schema catalog after propagation
    pub final_catalog: SchemaCatalog,
    /// Models that failed to plan (model name → error message)
    pub failures: HashMap<String, String>,
}

/// Propagate schemas through the DAG in topological order
///
/// For each model, renders its SQL, plans it via DataFusion, extracts the
/// output schema, compares against YAML declarations, and registers the
/// inferred schema for downstream models.
pub fn propagate_schemas(
    topo_order: &[String],
    sql_sources: &HashMap<String, String>,
    yaml_schemas: &HashMap<String, Arc<RelSchema>>,
    initial_catalog: &SchemaCatalog,
    user_functions: &[UserFunctionStub],
    user_table_functions: &[UserTableFunctionStub],
) -> PropagationResult {
    let mut catalog = initial_catalog.clone();
    let mut model_plans: HashMap<String, ModelPlanResult> =
        HashMap::with_capacity(topo_order.len());
    let mut failures: HashMap<String, String> = HashMap::new();

    let registry = FunctionRegistry::with_user_functions(user_functions, user_table_functions);

    for model_name in topo_order {
        let sql = match sql_sources.get(model_name) {
            Some(s) => s,
            None => {
                failures.insert(model_name.clone(), "No rendered SQL available".to_string());
                continue;
            }
        };

        let provider = FeatherFlowProvider::new(&catalog, &registry);
        match sql_to_plan(sql, &provider) {
            Ok(plan) => {
                let inferred_schema = Arc::new(extract_schema_from_plan(&plan));

                // Cross-check with YAML if available
                let mismatches = if let Some(yaml_schema) = yaml_schemas.get(model_name) {
                    compare_schemas(yaml_schema, &inferred_schema)
                } else {
                    vec![]
                };

                // Register the inferred schema for downstream models
                catalog.insert(model_name.clone(), Arc::clone(&inferred_schema));

                model_plans.insert(
                    model_name.clone(),
                    ModelPlanResult {
                        plan,
                        inferred_schema,
                        mismatches,
                    },
                );
            }
            Err(e) => {
                failures.insert(model_name.clone(), e.to_string());
            }
        }
    }

    PropagationResult {
        model_plans,
        final_catalog: catalog,
        failures,
    }
}

/// Extract a RelSchema from a DataFusion LogicalPlan's output schema
fn extract_schema_from_plan(plan: &LogicalPlan) -> RelSchema {
    let df_schema = plan.schema();
    let columns: Vec<TypedColumn> = df_schema
        .fields()
        .iter()
        .map(|field| {
            let sql_type = arrow_to_sql_type(field.data_type());
            let nullability = if field.is_nullable() {
                Nullability::Nullable
            } else {
                Nullability::NotNull
            };
            TypedColumn {
                name: field.name().clone(),
                source_table: None,
                sql_type,
                nullability,
                provenance: vec![],
            }
        })
        .collect();
    RelSchema::new(columns)
}

/// Compare a YAML-declared schema against an inferred schema
fn compare_schemas(yaml: &RelSchema, inferred: &RelSchema) -> Vec<SchemaMismatch> {
    let mut mismatches = Vec::new();

    // Check for columns in SQL output but not in YAML
    for inferred_col in &inferred.columns {
        if yaml.find_column(&inferred_col.name).is_none() {
            mismatches.push(SchemaMismatch::ExtraInSql {
                column: inferred_col.name.clone(),
            });
        }
    }

    // Check for columns in YAML but not in SQL output
    for yaml_col in &yaml.columns {
        match inferred.find_column(&yaml_col.name) {
            None => {
                mismatches.push(SchemaMismatch::MissingFromSql {
                    column: yaml_col.name.clone(),
                });
            }
            Some(inferred_col) => {
                // Check type compatibility
                if !yaml_col.sql_type.is_compatible_with(&inferred_col.sql_type) {
                    mismatches.push(SchemaMismatch::TypeMismatch {
                        column: yaml_col.name.clone(),
                        yaml_type: yaml_col.sql_type.display_name().to_string(),
                        inferred_type: inferred_col.sql_type.display_name().to_string(),
                    });
                }

                // Check nullability
                let yaml_nullable = !matches!(yaml_col.nullability, Nullability::NotNull);
                let inferred_nullable = !matches!(inferred_col.nullability, Nullability::NotNull);
                if !yaml_nullable && inferred_nullable {
                    // YAML says NOT NULL but SQL infers nullable — potential issue
                    mismatches.push(SchemaMismatch::NullabilityMismatch {
                        column: yaml_col.name.clone(),
                        yaml_nullable,
                        inferred_nullable,
                    });
                }
            }
        }
    }

    mismatches
}

#[cfg(test)]
#[path = "propagation_test.rs"]
mod tests;
