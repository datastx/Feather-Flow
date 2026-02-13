//! Run state tracking for partial execution recovery
//!
//! This module provides functionality to track the state of a run in progress,
//! allowing for resume functionality when a run fails partway through.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use uuid::Uuid;

use crate::error::CoreResult;
use crate::model_name::ModelName;

/// State of a run in progress or completed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    /// Unique identifier for this run
    pub run_id: String,

    /// When the run started
    pub started_at: DateTime<Utc>,

    /// When the state was last updated
    pub last_updated_at: DateTime<Utc>,

    /// Current status of the run
    pub status: RunStatus,

    /// Models that have been successfully completed
    pub completed_models: Vec<CompletedModel>,

    /// Models that failed during execution
    pub failed_models: Vec<FailedModel>,

    /// Models that are still pending execution
    pub pending_models: Vec<ModelName>,

    /// The selection criteria used for this run
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection: Option<String>,

    /// Hash of the project configuration for validation
    pub config_hash: String,
}

/// Status of a run
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// Run is currently in progress
    Running,
    /// Run completed successfully
    Completed,
    /// Run failed with errors
    Failed,
    /// Run was cancelled
    Cancelled,
}

/// A model that completed successfully
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedModel {
    /// Model name
    pub name: ModelName,

    /// When the model completed
    pub completed_at: DateTime<Utc>,

    /// How long the model took to execute (in milliseconds)
    pub duration_ms: u64,
}

/// A model that failed during execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedModel {
    /// Model name
    pub name: ModelName,

    /// When the model failed
    pub failed_at: DateTime<Utc>,

    /// Error message
    pub error: String,
}

impl RunState {
    /// Create a new run state
    pub fn new(
        pending_models: Vec<ModelName>,
        selection: Option<String>,
        config_hash: String,
    ) -> Self {
        Self {
            run_id: Uuid::new_v4().to_string(),
            started_at: Utc::now(),
            last_updated_at: Utc::now(),
            status: RunStatus::Running,
            completed_models: Vec::new(),
            failed_models: Vec::new(),
            pending_models,
            selection,
            config_hash,
        }
    }

    /// Load run state from a file path
    pub fn load(path: &Path) -> CoreResult<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let content =
            fs::read_to_string(path).map_err(|e| crate::error::CoreError::IoWithPath {
                path: path.display().to_string(),
                source: e,
            })?;
        let state: RunState = serde_json::from_str(&content)?;
        Ok(Some(state))
    }

    /// Save run state to a file path atomically
    ///
    /// Uses write-to-temp-then-rename pattern to prevent corruption
    pub fn save(&self, path: &Path) -> CoreResult<()> {
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| crate::error::CoreError::IoWithPath {
                path: parent.display().to_string(),
                source: e,
            })?;
        }

        // Write to a temporary file first
        let temp_path = path.with_extension("json.tmp");
        let json = serde_json::to_string_pretty(self)?;
        fs::write(&temp_path, &json).map_err(|e| crate::error::CoreError::IoWithPath {
            path: temp_path.display().to_string(),
            source: e,
        })?;

        // Atomically rename to the target path
        fs::rename(&temp_path, path).map_err(|e| {
            let _ = fs::remove_file(&temp_path);
            crate::error::CoreError::IoWithPath {
                path: path.display().to_string(),
                source: e,
            }
        })?;

        Ok(())
    }

    /// Mark a model as completed
    pub fn mark_completed(&mut self, name: &str, duration_ms: u64) -> CoreResult<()> {
        self.pending_models.retain(|n| n != name);

        self.completed_models.push(CompletedModel {
            name: ModelName::try_new(name).ok_or_else(|| crate::error::CoreError::EmptyName {
                context: "completed model name".into(),
            })?,
            completed_at: Utc::now(),
            duration_ms,
        });

        self.last_updated_at = Utc::now();
        Ok(())
    }

    /// Mark a model as failed
    pub fn mark_failed(&mut self, name: &str, error: &str) -> CoreResult<()> {
        self.pending_models.retain(|n| n != name);

        self.failed_models.push(FailedModel {
            name: ModelName::try_new(name).ok_or_else(|| crate::error::CoreError::EmptyName {
                context: "failed model name".into(),
            })?,
            failed_at: Utc::now(),
            error: error.to_string(),
        });

        self.last_updated_at = Utc::now();
        Ok(())
    }

    /// Mark the run as completed
    pub fn mark_run_completed(&mut self) {
        self.status = if self.failed_models.is_empty() {
            RunStatus::Completed
        } else {
            RunStatus::Failed
        };
        self.last_updated_at = Utc::now();
    }

    /// Mark the run as cancelled
    pub fn mark_run_cancelled(&mut self) {
        self.status = RunStatus::Cancelled;
        self.last_updated_at = Utc::now();
    }

    /// Check if a model has already been completed
    pub fn is_completed(&self, name: &str) -> bool {
        self.completed_models.iter().any(|m| m.name == name)
    }

    /// Check if a model has failed
    pub fn is_failed(&self, name: &str) -> bool {
        self.failed_models.iter().any(|m| m.name == name)
    }

    /// Get models that need to be run (failed + pending)
    pub fn models_to_run(&self) -> Vec<ModelName> {
        self.failed_models
            .iter()
            .map(|m| m.name.clone())
            .chain(self.pending_models.iter().cloned())
            .collect()
    }

    /// Get only the failed models (for --retry-failed)
    pub fn failed_model_names(&self) -> Vec<ModelName> {
        self.failed_models.iter().map(|m| m.name.clone()).collect()
    }

    /// Get summary statistics
    pub fn summary(&self) -> RunStateSummary {
        RunStateSummary {
            completed: self.completed_models.len(),
            failed: self.failed_models.len(),
            pending: self.pending_models.len(),
            total_duration_ms: self.completed_models.iter().map(|m| m.duration_ms).sum(),
        }
    }
}

/// Summary statistics for a run state
#[derive(Debug, Clone)]
pub struct RunStateSummary {
    /// Number of models that completed successfully
    pub completed: usize,
    /// Number of models that failed during execution
    pub failed: usize,
    /// Number of models still awaiting execution
    pub pending: usize,
    /// Sum of execution times across all completed models, in milliseconds
    pub total_duration_ms: u64,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunStatus::Running => write!(f, "running"),
            RunStatus::Completed => write!(f, "completed"),
            RunStatus::Failed => write!(f, "failed"),
            RunStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

#[cfg(test)]
#[path = "run_state_test.rs"]
mod tests;
