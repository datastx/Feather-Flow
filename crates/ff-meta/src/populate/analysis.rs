//! Populate analysis-phase data: inferred schemas, column lineage, diagnostics, mismatches.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;

/// Inferred column schema from static analysis.
pub struct InferredColumn {
    pub model_id: i64,
    pub column_name: String,
    pub inferred_type: Option<String>,
    pub nullability_inferred: Option<String>,
}

/// A column-level lineage edge.
pub struct LineageEdge {
    pub target_model_id: i64,
    pub target_column: String,
    pub source_model_id: Option<i64>,
    pub source_table: Option<String>,
    pub source_column: String,
    pub lineage_kind: String,
    pub is_direct: bool,
}

/// A diagnostic emitted by an analysis pass.
pub struct Diagnostic {
    pub code: String,
    pub severity: String,
    pub message: String,
    pub model_id: Option<i64>,
    pub column_name: Option<String>,
    pub hint: Option<String>,
    pub pass_name: String,
}

/// A schema mismatch between declared YAML and inferred SQL.
pub struct SchemaMismatch {
    pub model_id: i64,
    pub column_name: String,
    pub mismatch_type: String,
    pub declared_value: Option<String>,
    pub inferred_value: Option<String>,
}

/// Update model_columns with inferred types and nullability from static analysis.
pub fn populate_inferred_schemas(conn: &Connection, columns: &[InferredColumn]) -> MetaResult<()> {
    for col in columns {
        conn.execute(
            "UPDATE ff_meta.model_columns SET inferred_type = ?, nullability_inferred = ? WHERE model_id = ? AND name = ?",
            duckdb::params![col.inferred_type, col.nullability_inferred, col.model_id, col.column_name],
        )
        .map_err(|e| MetaError::PopulationError(format!("update inferred schema: {e}")))?;
    }
    Ok(())
}

/// Insert column lineage edges discovered during analysis.
pub fn populate_column_lineage(conn: &Connection, edges: &[LineageEdge]) -> MetaResult<()> {
    for edge in edges {
        conn.execute(
            "INSERT INTO ff_meta.column_lineage (target_model_id, target_column, source_model_id, source_table, source_column, lineage_kind, is_direct)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                edge.target_model_id,
                edge.target_column,
                edge.source_model_id,
                edge.source_table,
                edge.source_column,
                edge.lineage_kind,
                edge.is_direct,
            ],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert column_lineage: {e}")))?;
    }
    Ok(())
}

/// Insert diagnostics from analysis passes.
pub fn populate_diagnostics(
    conn: &Connection,
    run_id: i64,
    diagnostics: &[Diagnostic],
) -> MetaResult<()> {
    for diag in diagnostics {
        conn.execute(
            "INSERT INTO ff_meta.diagnostics (run_id, code, severity, message, model_id, column_name, hint, pass_name)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                run_id,
                diag.code,
                diag.severity,
                diag.message,
                diag.model_id,
                diag.column_name,
                diag.hint,
                diag.pass_name,
            ],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert diagnostics: {e}")))?;
    }
    Ok(())
}

/// An effective classification to write to a model column.
pub struct EffectiveClassification {
    pub model_id: i64,
    pub column_name: String,
    pub effective_classification: String,
}

/// Update model_columns with effective (propagated) classifications.
///
/// For each entry, sets `effective_classification` on the matching row.
/// This is the classification computed by propagating declared classifications
/// through column-level lineage (copy/transform edges only).
pub fn populate_effective_classifications(
    conn: &Connection,
    entries: &[EffectiveClassification],
) -> MetaResult<()> {
    for entry in entries {
        conn.execute(
            "UPDATE ff_meta.model_columns SET effective_classification = ? WHERE model_id = ? AND name = ?",
            duckdb::params![
                entry.effective_classification,
                entry.model_id,
                entry.column_name,
            ],
        )
        .map_err(|e| {
            MetaError::PopulationError(format!("update effective_classification: {e}"))
        })?;
    }
    Ok(())
}

/// Insert schema mismatches (SA01/SA02) from validation.
pub fn populate_schema_mismatches(
    conn: &Connection,
    run_id: i64,
    mismatches: &[SchemaMismatch],
) -> MetaResult<()> {
    for m in mismatches {
        conn.execute(
            "INSERT INTO ff_meta.schema_mismatches (run_id, model_id, column_name, mismatch_type, declared_value, inferred_value)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![
                run_id,
                m.model_id,
                m.column_name,
                m.mismatch_type,
                m.declared_value,
                m.inferred_value,
            ],
        )
        .map_err(|e| MetaError::PopulationError(format!("insert schema_mismatches: {e}")))?;
    }
    Ok(())
}
