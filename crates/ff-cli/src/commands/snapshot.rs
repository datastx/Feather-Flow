//! Snapshot command implementation - SCD Type 2 tracking

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ff_core::{discover_snapshots, Project, Snapshot, SnapshotStrategy};
use ff_db::{Database, SnapshotResult};
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::{GlobalArgs, SnapshotArgs};
use crate::commands::common::{self, RunStatus};

/// Snapshot run result for a single snapshot
#[derive(Debug, Clone, Serialize)]
struct SnapshotRunResult {
    snapshot: String,
    status: RunStatus,
    new_records: usize,
    updated_records: usize,
    deleted_records: usize,
    duration_secs: f64,
    error: Option<String>,
}

/// Snapshot results output file format
#[derive(Debug, Serialize)]
struct SnapshotResults {
    timestamp: DateTime<Utc>,
    elapsed_secs: f64,
    success_count: usize,
    failure_count: usize,
    results: Vec<SnapshotRunResult>,
}

/// Execute the snapshot command
pub async fn execute(args: &SnapshotArgs, global: &GlobalArgs) -> Result<()> {
    let start_time = Instant::now();
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    // Get snapshot paths from config
    let snapshot_paths = project.config.snapshot_paths.clone();
    if snapshot_paths.is_empty() {
        println!("No snapshot_paths configured in featherflow.yml");
        return Ok(());
    }

    // Discover snapshots
    let all_snapshots = discover_snapshots(&project.root, &snapshot_paths)
        .context("Failed to discover snapshots")?;

    if all_snapshots.is_empty() {
        println!("No snapshots found in configured paths");
        return Ok(());
    }

    // Filter snapshots if --snapshots or --select provided
    let snapshots_to_run: Vec<&Snapshot> = if let Some(ref names) = args.snapshots {
        let selected: Vec<&str> = names.split(',').map(|s| s.trim()).collect();
        all_snapshots
            .iter()
            .filter(|s| selected.contains(&s.name.as_str()))
            .collect()
    } else if let Some(ref select) = args.select {
        // Simple name matching for now
        let selected: Vec<&str> = select.split(',').map(|s| s.trim()).collect();
        all_snapshots
            .iter()
            .filter(|s| selected.contains(&s.name.as_str()))
            .collect()
    } else {
        all_snapshots.iter().collect()
    };

    if snapshots_to_run.is_empty() {
        println!("No matching snapshots found");
        return Ok(());
    }

    // Create database connection
    let db = common::create_database_connection(&project.config, global.target.as_deref())?;

    println!("Running {} snapshots...\n", snapshots_to_run.len());

    let mut success_count = 0;
    let mut failure_count = 0;
    let mut run_results: Vec<SnapshotRunResult> = Vec::new();

    for snapshot in snapshots_to_run {
        let snapshot_start = Instant::now();

        if global.verbose {
            eprintln!(
                "[verbose] Running snapshot: {} (source: {}, strategy: {})",
                snapshot.name, snapshot.config.source, snapshot.config.strategy
            );
        }

        // Ensure schema exists if specified
        if let Some(ref schema) = snapshot.config.schema {
            db.create_schema_if_not_exists(schema)
                .await
                .context(format!("Failed to create schema: {}", schema))?;
        }

        // Execute the snapshot
        let result = execute_single_snapshot(&db, snapshot, global).await;

        let duration = snapshot_start.elapsed();

        match result {
            Ok(snapshot_result) => {
                success_count += 1;
                println!(
                    "  ✓ {} (new: {}, updated: {}, deleted: {}) [{}ms]",
                    snapshot.name,
                    snapshot_result.new_records,
                    snapshot_result.updated_records,
                    snapshot_result.deleted_records,
                    duration.as_millis()
                );

                run_results.push(SnapshotRunResult {
                    snapshot: snapshot.name.clone(),
                    status: RunStatus::Success,
                    new_records: snapshot_result.new_records,
                    updated_records: snapshot_result.updated_records,
                    deleted_records: snapshot_result.deleted_records,
                    duration_secs: duration.as_secs_f64(),
                    error: None,
                });
            }
            Err(e) => {
                failure_count += 1;
                println!("  ✗ {} - {} [{}ms]", snapshot.name, e, duration.as_millis());

                run_results.push(SnapshotRunResult {
                    snapshot: snapshot.name.clone(),
                    status: RunStatus::Error,
                    new_records: 0,
                    updated_records: 0,
                    deleted_records: 0,
                    duration_secs: duration.as_secs_f64(),
                    error: Some(e.to_string()),
                });
            }
        }
    }

    // Write results to file
    write_snapshot_results(
        &project,
        &run_results,
        start_time,
        success_count,
        failure_count,
    )?;

    println!();
    println!(
        "Completed: {} succeeded, {} failed",
        success_count, failure_count
    );
    println!("Total time: {}ms", start_time.elapsed().as_millis());

    if failure_count > 0 {
        return Err(crate::commands::common::ExitCode(4).into());
    }

    Ok(())
}

/// Execute a single snapshot
async fn execute_single_snapshot(
    db: &Arc<dyn Database>,
    snapshot: &Snapshot,
    global: &GlobalArgs,
) -> Result<SnapshotResult> {
    let snapshot_table = snapshot.config.qualified_name();

    // Get strategy-specific parameters
    let updated_at_column = match snapshot.config.strategy {
        SnapshotStrategy::Timestamp => snapshot.config.updated_at.as_deref(),
        SnapshotStrategy::Check => None,
    };

    let check_cols = match snapshot.config.strategy {
        SnapshotStrategy::Check => {
            if snapshot.config.check_cols.is_empty() {
                None
            } else {
                Some(snapshot.config.check_cols.as_slice())
            }
        }
        SnapshotStrategy::Timestamp => None,
    };

    if global.verbose {
        eprintln!(
            "[verbose] Executing snapshot {} -> {} (strategy: {}, hard_deletes: {})",
            snapshot.config.source,
            snapshot_table,
            snapshot.config.strategy,
            snapshot.config.invalidate_hard_deletes
        );
    }

    let result = db
        .execute_snapshot(
            &snapshot_table,
            &snapshot.config.source,
            &snapshot.config.unique_key,
            updated_at_column,
            check_cols,
            snapshot.config.invalidate_hard_deletes,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Snapshot execution failed: {}", e))?;

    Ok(result)
}

/// Write snapshot results to JSON file
fn write_snapshot_results(
    project: &Project,
    run_results: &[SnapshotRunResult],
    start_time: Instant,
    success_count: usize,
    failure_count: usize,
) -> Result<()> {
    let results = SnapshotResults {
        timestamp: Utc::now(),
        elapsed_secs: start_time.elapsed().as_secs_f64(),
        success_count,
        failure_count,
        results: run_results.to_vec(),
    };

    let target_dir = project.target_dir();
    std::fs::create_dir_all(&target_dir).context("Failed to create target directory")?;
    let results_path = target_dir.join("snapshot_results.json");
    let results_json =
        serde_json::to_string_pretty(&results).context("Failed to serialize snapshot results")?;
    std::fs::write(&results_path, results_json).context("Failed to write snapshot_results.json")?;

    Ok(())
}
