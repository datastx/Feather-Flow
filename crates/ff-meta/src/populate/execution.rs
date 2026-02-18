//! Populate execution-phase data: model run state, input checksums, config snapshots.

use crate::error::{MetaError, MetaResult};
use duckdb::Connection;

/// A model execution record.
pub struct ModelRunRecord {
    pub model_id: i64,
    pub run_id: i64,
    pub status: String,
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
    pub materialization: String,
    pub schema_name: Option<String>,
    pub unique_key: Option<String>,
    pub incremental_strategy: Option<String>,
    pub on_schema_change: Option<String>,
}

/// Record a model execution run.
pub fn record_model_run(conn: &Connection, record: &ModelRunRecord) -> MetaResult<()> {
    conn.execute(
        "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, row_count, sql_checksum, schema_checksum, duration_ms, started_at, completed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())",
        duckdb::params![
            record.model_id,
            record.run_id,
            record.status,
            record.row_count,
            record.sql_checksum,
            record.schema_checksum,
            record.duration_ms,
        ],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert model_run_state: {e}")))?;
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
        .map_err(|e| MetaError::PopulationError(format!("insert model_run_input_checksums: {e}")))?;
    }
    Ok(())
}

/// Record config snapshot for drift detection.
pub fn record_config_snapshot(
    conn: &Connection,
    model_id: i64,
    run_id: i64,
    config: &ConfigSnapshot,
) -> MetaResult<()> {
    conn.execute(
        "INSERT INTO ff_meta.model_run_config (model_id, run_id, materialization, schema_name, unique_key, incremental_strategy, on_schema_change)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            model_id,
            run_id,
            config.materialization,
            config.schema_name,
            config.unique_key,
            config.incremental_strategy,
            config.on_schema_change,
        ],
    )
    .map_err(|e| MetaError::PopulationError(format!("insert model_run_config: {e}")))?;
    Ok(())
}
