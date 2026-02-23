//! Shared documentation data types and builder functions

use ff_core::model::Model;
use ff_core::source::SourceFile;
use ff_sql::{extract_column_lineage, suggest_tests, SqlParser};
use serde::Serialize;

/// Model documentation data for JSON output.
#[derive(Debug, Serialize)]
pub(crate) struct ModelDoc {
    /// Model name
    pub(crate) name: String,
    /// Human-readable description from YAML schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Model owner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    /// Owning team
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) team: Option<String>,
    /// Contact info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) contact: Option<String>,
    /// Tags from YAML schema
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<String>,
    /// Materialization strategy (view, table, incremental, ephemeral)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) materialized: Option<String>,
    /// Target schema for the materialized relation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schema: Option<String>,
    /// Column definitions from YAML schema
    pub(crate) columns: Vec<ColumnDoc>,
    /// Internal model dependencies
    pub(crate) depends_on: Vec<String>,
    /// External table dependencies
    pub(crate) external_deps: Vec<String>,
    /// Column-level lineage extracted from SQL AST
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) column_lineage: Vec<ColumnLineageDoc>,
    /// Suggested tests from AST analysis
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) test_suggestions: Vec<TestSuggestionDoc>,
    /// Raw SQL for display
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) raw_sql: Option<String>,
}

/// Test suggestion documentation data
#[derive(Debug, Serialize)]
pub(crate) struct TestSuggestionDoc {
    /// Column name
    pub(crate) column: String,
    /// Suggested test type
    pub(crate) test_type: String,
    /// Reason for suggestion
    pub(crate) reason: String,
}

/// Column documentation data.
#[derive(Debug, Serialize)]
pub(crate) struct ColumnDoc {
    /// Column name
    pub(crate) name: String,
    /// SQL data type
    pub(crate) data_type: String,
    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Whether this column is a primary key
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) primary_key: bool,
    /// Test names applied to this column
    pub(crate) tests: Vec<String>,
    /// Foreign key reference to another model's column
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) references: Option<ColumnRefDoc>,
    /// Data governance classification (e.g., pii, sensitive, public)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) classification: Option<String>,
}

/// Column reference documentation data.
#[derive(Debug, Serialize)]
pub(crate) struct ColumnRefDoc {
    /// Referenced model name
    pub(crate) model: String,
    /// Referenced column name
    pub(crate) column: String,
}

/// Column lineage documentation data (from AST analysis)
#[derive(Debug, Serialize)]
pub(crate) struct ColumnLineageDoc {
    /// Output column name
    pub(crate) output_column: String,
    /// Source columns that contribute to this output
    pub(crate) source_columns: Vec<String>,
    /// Whether this is a direct pass-through
    pub(crate) is_direct: bool,
    /// Expression type (column, function, expression, etc.)
    pub(crate) expr_type: String, // Serialized as string for JSON docs output
}

/// Model summary for the docs index page.
#[derive(Debug, Serialize)]
pub(crate) struct ModelSummary {
    /// Model name
    pub(crate) name: String,
    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Model owner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    /// Whether a YAML schema file exists
    pub(crate) has_schema: bool,
}

/// Source documentation data for JSON output.
#[derive(Debug, Serialize)]
pub(crate) struct SourceDoc {
    /// Source name
    pub(crate) name: String,
    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Database schema containing the source tables
    pub(crate) schema: String,
    /// Source owner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    /// Tags from source YAML
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<String>,
    /// Tables within this source
    pub(crate) tables: Vec<SourceTableDoc>,
}

/// Source table documentation data.
#[derive(Debug, Serialize)]
pub(crate) struct SourceTableDoc {
    /// Table name
    pub(crate) name: String,
    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Columns defined for this table
    pub(crate) columns: Vec<SourceColumnDoc>,
}

/// Source column documentation data.
#[derive(Debug, Serialize)]
pub(crate) struct SourceColumnDoc {
    /// Column name
    pub(crate) name: String,
    /// SQL data type
    pub(crate) data_type: String,
    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Test names applied to this column
    pub(crate) tests: Vec<String>,
}

/// Source summary for the docs index page.
#[derive(Debug, Serialize)]
pub(crate) struct SourceSummary {
    /// Source name
    pub(crate) name: String,
    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Number of tables in this source
    pub(crate) table_count: usize,
}

/// Build documentation data for a model
pub(crate) fn build_model_doc(model: &Model) -> ModelDoc {
    let mut columns = Vec::new();
    let mut description = None;
    let mut tags = Vec::new();

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

    let materialized = model.config.materialized.map(|m| m.to_string());
    let schema = model.config.schema.clone();

    let depends_on: Vec<String> = model.depends_on.iter().map(|m| m.to_string()).collect();
    let external_deps: Vec<String> = model.external_deps.iter().map(|t| t.to_string()).collect();

    let column_lineage = extract_column_lineage_from_model(model);

    let test_suggestions = generate_test_suggestions(model);

    let owner = model.get_owner().map(String::from);
    let team = model.get_meta_string("team").map(String::from);
    let contact = model.get_meta_string("contact").map(String::from);

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

    let sql = model.compiled_sql.as_ref().unwrap_or(&model.raw_sql);

    let stmts = match parser.parse(sql) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    let lineage = match stmts
        .first()
        .and_then(|stmt| extract_column_lineage(stmt, &model.name))
    {
        Some(l) => l,
        None => return Vec::new(),
    };

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

    let sql = model.compiled_sql.as_ref().unwrap_or(&model.raw_sql);

    let stmts = match parser.parse(sql) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    let suggestions = match stmts.first() {
        Some(stmt) => suggest_tests(stmt, &model.name),
        None => return Vec::new(),
    };

    let mut docs: Vec<TestSuggestionDoc> = suggestions
        .columns
        .into_iter()
        .flat_map(|(column, col_suggestions)| {
            col_suggestions
                .suggestions
                .into_iter()
                .map(move |suggestion| TestSuggestionDoc {
                    column: column.clone(),
                    test_type: suggestion.test_name().to_string(),
                    reason: suggestion.reason(),
                })
        })
        .collect();

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
        name: source.name.to_string(),
        description: source.description.clone(),
        schema: source.schema.clone(),
        owner: source.owner.clone(),
        tags: source.tags.clone(),
        tables,
    }
}
