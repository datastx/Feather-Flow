//! Populate execution-phase data: model run state, input checksums, config snapshots.

use crate::error::{MetaResult, MetaResultExt};
use duckdb::Connection;
use ff_core::config::{IncrementalStrategy, Materialization, OnSchemaChange};

/// Model execution status.
pub enum ModelRunStatus {
    Success,
    Error,
    Skipped,
}

impl ModelRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ModelRunStatus::Success => "success",
            ModelRunStatus::Error => "error",
            ModelRunStatus::Skipped => "skipped",
        }
    }
}

/// A model execution record.
pub struct ModelRunRecord {
    pub model_id: i64,
    pub run_id: i64,
    pub status: ModelRunStatus,
    pub row_count: Option<i64>,
    pub sql_checksum: Option<String>,
    pub schema_checksum: Option<String>,
    pub duration_ms: Option<i64>,
}

/// Upstream model checksum for incremental change detection.
pub struct InputChecksum {
    pub upstream_model_id: i64,
    pub checksum: String,
}

/// Config snapshot for drift detection.
pub struct ConfigSnapshot {
    pub materialization: Materialization,
    pub schema_name: Option<String>,
    pub unique_key: Option<String>,
    pub incremental_strategy: Option<IncrementalStrategy>,
    pub on_schema_change: Option<OnSchemaChange>,
}

/// Record a model execution run.
pub fn record_model_run(conn: &Connection, record: &ModelRunRecord) -> MetaResult<()> {
    conn.execute(
        "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, row_count, sql_checksum, schema_checksum, duration_ms, started_at, completed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())",
        duckdb::params![
            record.model_id,
            record.run_id,
            record.status.as_str(),
            record.row_count,
            record.sql_checksum,
            record.schema_checksum,
            record.duration_ms,
        ],
    )
    .populate_context("insert model_run_state")?;
    Ok(())
}

/// Record input checksums for upstream dependencies.
pub fn record_input_checksums(
    conn: &Connection,
    model_id: i64,
    run_id: i64,
    checksums: &[InputChecksum],
) -> MetaResult<()> {
    for cs in checksums {
        conn.execute(
            "INSERT INTO ff_meta.model_run_input_checksums (model_id, run_id, upstream_model_id, checksum) VALUES (?, ?, ?, ?)",
            duckdb::params![model_id, run_id, cs.upstream_model_id, cs.checksum],
        )
        .populate_context("insert model_run_input_checksums")?;
    }
    Ok(())
}

/// Map a `Materialization` to its DuckDB-safe string.
///
/// The `model_run_config` CHECK constraint allows only 'view', 'table',
/// and 'incremental'. Ephemeral models are logically equivalent to views
/// (inlined, never materialized) so we store them as "view".
fn materialization_for_meta(mat: Materialization) -> &'static str {
    if mat.is_ephemeral() {
        "view"
    } else {
        mat.as_str()
    }
}

/// Record config snapshot for drift detection.
pub fn record_config_snapshot(
    conn: &Connection,
    model_id: i64,
    run_id: i64,
    config: &ConfigSnapshot,
) -> MetaResult<()> {
    let strategy_str = config.incremental_strategy.map(|s| s.as_str());
    let on_change_str = config.on_schema_change.map(|s| s.as_str());
    conn.execute(
        "INSERT INTO ff_meta.model_run_config (model_id, run_id, materialization, schema_name, unique_key, incremental_strategy, on_schema_change)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            model_id,
            run_id,
            materialization_for_meta(config.materialization),
            config.schema_name,
            config.unique_key,
            strategy_str,
            on_change_str,
        ],
    )
    .populate_context("insert model_run_config")?;
    Ok(())
}
