//! Shared documentation data types and builder functions

use ff_core::exposure::Exposure;
use ff_core::model::Model;
use ff_core::source::SourceFile;
use ff_sql::{extract_column_lineage, suggest_tests, SqlParser};
use serde::Serialize;

/// Model documentation data for JSON output
#[derive(Debug, Serialize)]
pub(crate) struct ModelDoc {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub columns: Vec<ColumnDoc>,
    pub depends_on: Vec<String>,
    pub external_deps: Vec<String>,
    /// Column-level lineage extracted from SQL AST
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub column_lineage: Vec<ColumnLineageDoc>,
    /// Suggested tests from AST analysis
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub test_suggestions: Vec<TestSuggestionDoc>,
    /// Raw SQL for display
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_sql: Option<String>,
}

/// Test suggestion documentation data
#[derive(Debug, Serialize)]
pub(crate) struct TestSuggestionDoc {
    /// Column name
    pub column: String,
    /// Suggested test type
    pub test_type: String,
    /// Reason for suggestion
    pub reason: String,
}

/// Column documentation data
#[derive(Debug, Serialize)]
pub(crate) struct ColumnDoc {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub primary_key: bool,
    pub tests: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<ColumnRefDoc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
}

/// Column reference documentation data
#[derive(Debug, Serialize)]
pub(crate) struct ColumnRefDoc {
    pub model: String,
    pub column: String,
}

/// Column lineage documentation data (from AST analysis)
#[derive(Debug, Serialize)]
pub(crate) struct ColumnLineageDoc {
    /// Output column name
    pub output_column: String,
    /// Source columns that contribute to this output
    pub source_columns: Vec<String>,
    /// Whether this is a direct pass-through
    pub is_direct: bool,
    /// Expression type (column, function, expression, etc.)
    pub expr_type: String, // Serialized as string for JSON docs output
}

/// Model summary for index
#[derive(Debug, Serialize)]
pub(crate) struct ModelSummary {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub has_schema: bool,
}

/// Source documentation data for JSON output
#[derive(Debug, Serialize)]
pub(crate) struct SourceDoc {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub tables: Vec<SourceTableDoc>,
}

/// Source table documentation data
#[derive(Debug, Serialize)]
pub(crate) struct SourceTableDoc {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub columns: Vec<SourceColumnDoc>,
}

/// Source column documentation data
#[derive(Debug, Serialize)]
pub(crate) struct SourceColumnDoc {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub tests: Vec<String>,
}

/// Source summary for index
#[derive(Debug, Serialize)]
pub(crate) struct SourceSummary {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub table_count: usize,
}

/// Exposure documentation data for JSON output
#[derive(Debug, Serialize)]
pub(crate) struct ExposureDoc {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub exposure_type: String,
    pub owner: ExposureOwnerDoc,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub maturity: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Exposure owner documentation data
#[derive(Debug, Serialize)]
pub(crate) struct ExposureOwnerDoc {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Exposure summary for index
#[derive(Debug, Serialize)]
pub(crate) struct ExposureSummary {
    pub name: String,
    pub exposure_type: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Build documentation data for a model
pub(crate) fn build_model_doc(model: &Model) -> ModelDoc {
    let mut columns = Vec::new();
    let mut description = None;
    let mut tags = Vec::new();

    // Extract from schema if available
    if let Some(schema) = &model.schema {
        description = schema.description.clone();
        tags = schema.tags.clone();

        for col in &schema.columns {
            let test_names: Vec<String> = col
                .tests
                .iter()
                .map(|t| match t {
                    ff_core::model::TestDefinition::Simple(name) => name.clone(),
                    ff_core::model::TestDefinition::Parameterized(map) => {
                        map.keys().next().cloned().unwrap_or_default()
                    }
                })
                .collect();
            let references = col.references.as_ref().map(|r| ColumnRefDoc {
                model: r.model.to_string(),
                column: r.column.clone(),
            });
            columns.push(ColumnDoc {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                description: col.description.clone(),
                primary_key: col.primary_key,
                tests: test_names,
                references,
                classification: col
                    .classification
                    .map(|c| format!("{:?}", c).to_lowercase()),
            });
        }
    }

    // Get materialization and schema from SQL config()
    let materialized = model.config.materialized.map(|m| m.to_string());
    let schema = model.config.schema.clone();

    // Get dependencies
    let depends_on: Vec<String> = model.depends_on.iter().map(|m| m.to_string()).collect();
    let external_deps: Vec<String> = model.external_deps.iter().map(|t| t.to_string()).collect();

    // Extract column lineage from SQL
    let column_lineage = extract_column_lineage_from_model(model);

    // Generate test suggestions from SQL
    let test_suggestions = generate_test_suggestions(model);

    // Get owner - uses get_owner() which checks direct owner field and meta.owner
    let owner = model.get_owner();

    // Get team and contact from meta
    let team = model.get_meta_string("team");
    let contact = model.get_meta_string("contact");

    ModelDoc {
        name: model.name.to_string(),
        description,
        owner,
        team,
        contact,
        tags,
        materialized,
        schema,
        columns,
        depends_on,
        external_deps,
        column_lineage,
        test_suggestions,
        raw_sql: Some(model.raw_sql.clone()),
    }
}

/// Extract column-level lineage from a model's SQL
pub(crate) fn extract_column_lineage_from_model(model: &Model) -> Vec<ColumnLineageDoc> {
    let parser = SqlParser::duckdb();

    // Use compiled SQL if available, otherwise raw SQL
    let sql = model.compiled_sql.as_ref().unwrap_or(&model.raw_sql);

    // Try to parse the SQL
    let stmts = match parser.parse(sql) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    // Extract lineage from the first statement
    let lineage = match stmts
        .first()
        .and_then(|stmt| extract_column_lineage(stmt, &model.name))
    {
        Some(l) => l,
        None => return Vec::new(),
    };

    // Convert to documentation format
    lineage
        .columns
        .into_iter()
        .map(|col| ColumnLineageDoc {
            output_column: col.output_column,
            source_columns: col
                .source_columns
                .into_iter()
                .map(|c| c.to_string())
                .collect(),
            is_direct: col.is_direct,
            expr_type: col.expr_type.to_string(),
        })
        .collect()
}

/// Generate test suggestions from a model's SQL
pub(crate) fn generate_test_suggestions(model: &Model) -> Vec<TestSuggestionDoc> {
    let parser = SqlParser::duckdb();

    // Use compiled SQL if available, otherwise raw SQL
    let sql = model.compiled_sql.as_ref().unwrap_or(&model.raw_sql);

    // Try to parse the SQL
    let stmts = match parser.parse(sql) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    // Get suggestions from the first statement
    let suggestions = match stmts.first() {
        Some(stmt) => suggest_tests(stmt, &model.name),
        None => return Vec::new(),
    };

    // Convert to documentation format
    let mut docs: Vec<TestSuggestionDoc> = Vec::new();
    for (column, col_suggestions) in suggestions.columns {
        for suggestion in col_suggestions.suggestions {
            docs.push(TestSuggestionDoc {
                column: column.clone(),
                test_type: suggestion.test_name().to_string(),
                reason: suggestion.reason(),
            });
        }
    }

    // Sort by column name
    docs.sort_by(|a, b| a.column.cmp(&b.column));
    docs
}

/// Build documentation data for a source
pub(crate) fn build_source_doc(source: &SourceFile) -> SourceDoc {
    let tables: Vec<SourceTableDoc> = source
        .tables
        .iter()
        .map(|table| SourceTableDoc {
            name: table.name.clone(),
            description: table.description.clone(),
            columns: table
                .columns
                .iter()
                .map(|col| {
                    // Convert TestDefinition to test name strings
                    let tests: Vec<String> = col
                        .tests
                        .iter()
                        .map(|t| match t {
                            ff_core::model::TestDefinition::Simple(name) => name.clone(),
                            ff_core::model::TestDefinition::Parameterized(map) => {
                                map.keys().next().cloned().unwrap_or_default()
                            }
                        })
                        .collect();
                    SourceColumnDoc {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        description: col.description.clone(),
                        tests,
                    }
                })
                .collect(),
        })
        .collect();

    SourceDoc {
        name: source.name.clone(),
        description: source.description.clone(),
        schema: source.schema.clone(),
        owner: source.owner.clone(),
        tags: source.tags.clone(),
        tables,
    }
}

/// Build exposure documentation from an Exposure
pub(crate) fn build_exposure_doc(exposure: &Exposure) -> ExposureDoc {
    ExposureDoc {
        name: exposure.name.clone(),
        description: exposure.description.clone(),
        exposure_type: exposure.exposure_type.to_string(),
        owner: ExposureOwnerDoc {
            name: exposure.owner.name.clone(),
            email: exposure.owner.email.clone(),
        },
        depends_on: exposure.depends_on.clone(),
        url: exposure.url.clone(),
        maturity: exposure.maturity.to_string(),
        tags: exposure.tags.clone(),
    }
}
