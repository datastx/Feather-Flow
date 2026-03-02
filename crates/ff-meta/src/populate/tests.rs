//! Populate the `tests` and `singular_tests` tables.

use crate::error::{MetaResult, MetaResultExt};
use duckdb::Connection;
use ff_core::model::testing::{SchemaTest, SingularTest, TestType};
use ff_core::ModelName;
use std::collections::HashMap;

/// Insert schema tests. The `model_id_map` maps model name → model_id.
pub fn populate_schema_tests(
    conn: &Connection,
    project_id: i64,
    tests: &[SchemaTest],
    model_id_map: &HashMap<ModelName, i64>,
) -> MetaResult<()> {
    for test in tests {
        let test_type = test_type_name(&test.test_type);
        let model_id = model_id_map.get(test.model.as_ref()).copied();
        let severity = test.config.severity.to_string();
        let where_clause = test.config.where_clause.as_deref();
        let config_json = test_config_json(&test.test_type);

        conn.execute(
            "INSERT INTO ff_meta.tests (project_id, test_type, model_id, column_name, severity, where_clause, config_json)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                project_id,
                test_type,
                model_id,
                test.column,
                severity,
                where_clause,
                config_json,
            ],
        )
        .populate_context("insert tests")?;
    }
    Ok(())
}

/// Insert singular tests (standalone SQL test files).
pub fn populate_singular_tests(
    conn: &Connection,
    project_id: i64,
    tests: &[SingularTest],
) -> MetaResult<()> {
    for test in tests {
        conn.execute(
            "INSERT INTO ff_meta.singular_tests (project_id, name, path, sql_text) VALUES (?, ?, ?, ?)",
            duckdb::params![
                project_id,
                test.name.as_str(),
                test.path.display().to_string(),
                test.sql,
            ],
        )
        .populate_context("insert singular_tests")?;
    }
    Ok(())
}

fn test_type_name(tt: &TestType) -> &'static str {
    match tt {
        TestType::Unique => "unique",
        TestType::NotNull => "not_null",
        TestType::Positive => "positive",
        TestType::NonNegative => "non_negative",
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::MinValue { .. } => "min_value",
        TestType::MaxValue { .. } => "max_value",
        TestType::Regex { .. } => "regex",
        TestType::Relationship { .. } => "relationship",
        TestType::Custom { .. } => "custom",
    }
}

fn test_config_json(tt: &TestType) -> Option<String> {
    match tt {
        TestType::AcceptedValues { values, quote } => {
            let obj = serde_json::json!({ "values": values, "quote": quote });
            Some(obj.to_string())
        }
        TestType::MinValue { value } => {
            let obj = serde_json::json!({ "value": value });
            Some(obj.to_string())
        }
        TestType::MaxValue { value } => {
            let obj = serde_json::json!({ "value": value });
            Some(obj.to_string())
        }
        TestType::Regex { pattern } => {
            let obj = serde_json::json!({ "pattern": pattern });
            Some(obj.to_string())
        }
        TestType::Relationship { to, field } => {
            let obj = serde_json::json!({ "to": to, "field": field });
            Some(obj.to_string())
        }
        TestType::Custom { name, kwargs } => {
            let mut obj = serde_json::Map::new();
            obj.insert("name".to_string(), serde_json::json!(name));
            for (k, v) in kwargs {
                obj.insert(k.clone(), v.clone());
            }
            Some(serde_json::Value::Object(obj).to_string())
        }
        TestType::Unique | TestType::NotNull | TestType::Positive | TestType::NonNegative => None,
    }
}
