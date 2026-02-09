//! DAG-wide schema propagation using DataFusion LogicalPlans
//!
//! Walks models in topological order, plans each one via DataFusion,
//! extracts inferred output schemas, cross-checks against YAML declarations,
//! and feeds schemas forward for downstream models.

use std::collections::HashMap;

use datafusion_expr::LogicalPlan;

use crate::datafusion_bridge::planner::sql_to_plan;
use crate::datafusion_bridge::provider::FeatherFlowProvider;
use crate::datafusion_bridge::types::arrow_to_sql_type;
use crate::ir::schema::RelSchema;
use crate::ir::types::{Nullability, TypedColumn};
use crate::lowering::SchemaCatalog;

/// Result of planning a single model
pub struct ModelPlanResult {
    /// The DataFusion LogicalPlan for this model
    pub plan: LogicalPlan,
    /// The inferred output schema (from the plan)
    pub inferred_schema: RelSchema,
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

/// Propagation results for the entire DAG
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
    yaml_schemas: &HashMap<String, RelSchema>,
    initial_catalog: &SchemaCatalog,
) -> PropagationResult {
    let mut catalog = initial_catalog.clone();
    let mut model_plans: HashMap<String, ModelPlanResult> = HashMap::new();
    let mut failures: HashMap<String, String> = HashMap::new();

    for model_name in topo_order {
        let sql = match sql_sources.get(model_name) {
            Some(s) => s,
            None => {
                failures.insert(model_name.clone(), "No rendered SQL available".to_string());
                continue;
            }
        };

        let provider = FeatherFlowProvider::new(&catalog);
        match sql_to_plan(sql, &provider) {
            Ok(plan) => {
                let inferred_schema = extract_schema_from_plan(&plan);

                // Cross-check with YAML if available
                let mismatches = if let Some(yaml_schema) = yaml_schemas.get(model_name) {
                    compare_schemas(yaml_schema, &inferred_schema)
                } else {
                    vec![]
                };

                // Register the inferred schema for downstream models
                catalog.insert(model_name.clone(), inferred_schema.clone());

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
mod tests {
    use super::*;
    use crate::test_utils::{int32, make_col, varchar};

    #[test]
    fn test_linear_chain_propagation() {
        // source_a → stg_a (selects from source_a)
        let mut initial_catalog: SchemaCatalog = HashMap::new();
        initial_catalog.insert(
            "source_a".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("name", varchar(), Nullability::Nullable),
            ]),
        );

        let topo_order = vec!["stg_a".to_string()];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "stg_a".to_string(),
            "SELECT id, name FROM source_a".to_string(),
        );

        let result =
            propagate_schemas(&topo_order, &sql_sources, &HashMap::new(), &initial_catalog);

        assert!(result.failures.is_empty());
        assert!(result.model_plans.contains_key("stg_a"));

        let stg_a = &result.model_plans["stg_a"];
        assert_eq!(stg_a.inferred_schema.columns.len(), 2);
        assert_eq!(stg_a.inferred_schema.columns[0].name, "id");
        assert_eq!(stg_a.inferred_schema.columns[1].name, "name");

        // The final catalog should contain both source_a and stg_a
        assert!(result.final_catalog.contains_key("source_a"));
        assert!(result.final_catalog.contains_key("stg_a"));
    }

    #[test]
    fn test_multi_step_propagation() {
        // source → stg → mart
        let mut initial_catalog: SchemaCatalog = HashMap::new();
        initial_catalog.insert(
            "raw_orders".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("amount", int32(), Nullability::Nullable),
                make_col("status", varchar(), Nullability::Nullable),
            ]),
        );

        let topo_order = vec!["stg_orders".to_string(), "mart_orders".to_string()];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "stg_orders".to_string(),
            "SELECT id, amount, status FROM raw_orders".to_string(),
        );
        sql_sources.insert(
            "mart_orders".to_string(),
            "SELECT id, status FROM stg_orders".to_string(),
        );

        let result =
            propagate_schemas(&topo_order, &sql_sources, &HashMap::new(), &initial_catalog);

        assert!(result.failures.is_empty());
        assert!(result.model_plans.contains_key("stg_orders"));
        assert!(result.model_plans.contains_key("mart_orders"));

        // mart_orders should only have 2 columns (not 3)
        let mart = &result.model_plans["mart_orders"];
        assert_eq!(mart.inferred_schema.columns.len(), 2);
        assert_eq!(mart.inferred_schema.columns[0].name, "id");
        assert_eq!(mart.inferred_schema.columns[1].name, "status");
    }

    #[test]
    fn test_diamond_dag_propagation() {
        // source → model_b, source → model_c, model_b + model_c → model_d
        let mut initial_catalog: SchemaCatalog = HashMap::new();
        initial_catalog.insert(
            "source".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("val", varchar(), Nullability::Nullable),
            ]),
        );

        let topo_order = vec![
            "model_b".to_string(),
            "model_c".to_string(),
            "model_d".to_string(),
        ];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "model_b".to_string(),
            "SELECT id, val AS b_val FROM source".to_string(),
        );
        sql_sources.insert(
            "model_c".to_string(),
            "SELECT id, val AS c_val FROM source".to_string(),
        );
        sql_sources.insert(
            "model_d".to_string(),
            "SELECT b.id, b.b_val, c.c_val FROM model_b b JOIN model_c c ON b.id = c.id"
                .to_string(),
        );

        let result =
            propagate_schemas(&topo_order, &sql_sources, &HashMap::new(), &initial_catalog);

        assert!(result.failures.is_empty());
        let model_d = &result.model_plans["model_d"];
        assert_eq!(model_d.inferred_schema.columns.len(), 3);
    }

    #[test]
    fn test_schema_mismatch_detection() {
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

        // YAML declares columns that don't match
        let mut yaml_schemas = HashMap::new();
        yaml_schemas.insert(
            "test_model".to_string(),
            RelSchema::new(vec![
                make_col("id", int32(), Nullability::NotNull),
                make_col("missing_col", varchar(), Nullability::Nullable),
            ]),
        );

        let result = propagate_schemas(&topo_order, &sql_sources, &yaml_schemas, &initial_catalog);

        let model_result = &result.model_plans["test_model"];
        assert!(!model_result.mismatches.is_empty());

        let has_extra = model_result
            .mismatches
            .iter()
            .any(|m| matches!(m, SchemaMismatch::ExtraInSql { column } if column == "name"));
        let has_missing = model_result.mismatches.iter().any(
            |m| matches!(m, SchemaMismatch::MissingFromSql { column } if column == "missing_col"),
        );

        assert!(has_extra, "Should detect 'name' as extra in SQL");
        assert!(
            has_missing,
            "Should detect 'missing_col' as missing from SQL"
        );
    }

    #[test]
    fn test_plan_failure_recorded() {
        let initial_catalog: SchemaCatalog = HashMap::new();

        let topo_order = vec!["bad_model".to_string()];
        let mut sql_sources = HashMap::new();
        sql_sources.insert(
            "bad_model".to_string(),
            "SELECT * FROM nonexistent_table".to_string(),
        );

        let result =
            propagate_schemas(&topo_order, &sql_sources, &HashMap::new(), &initial_catalog);

        assert!(result.failures.contains_key("bad_model"));
        assert!(result.model_plans.is_empty());
    }
}
