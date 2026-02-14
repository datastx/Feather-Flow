//! State tracking for incremental models

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;

use crate::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use crate::error::{CoreError, CoreResult};
use crate::model_name::ModelName;

/// State file containing all model states
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateFile {
    /// When this state file was last updated
    pub updated_at: DateTime<Utc>,

    /// State for each model, keyed by model name
    pub models: HashMap<ModelName, ModelState>,
}

/// State tracking for a single model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelState {
    /// Model name
    pub name: ModelName,

    /// Last successful run timestamp
    pub last_run: DateTime<Utc>,

    /// Row count after last run (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,

    /// SHA256 hash of compiled SQL
    pub checksum: String,

    /// SHA256 hash of the model's schema YAML
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_checksum: Option<String>,

    /// SHA256 hashes of upstream model SQL (model_name → checksum)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub input_checksums: HashMap<String, String>,

    /// Configuration snapshot
    pub config: ModelStateConfig,
}

/// Snapshot of model configuration at last run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelStateConfig {
    /// Materialization type
    pub materialized: Materialization,

    /// Target schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Unique key for incremental models
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_key: Option<Vec<String>>,

    /// Incremental strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incremental_strategy: Option<IncrementalStrategy>,

    /// On schema change behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_schema_change: Option<OnSchemaChange>,
}

impl StateFile {
    /// Create a new empty state file
    pub fn new() -> Self {
        Self {
            updated_at: Utc::now(),
            models: HashMap::new(),
        }
    }

    /// Load state from a file path
    pub fn load(path: &Path) -> CoreResult<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
        let state: StateFile = serde_json::from_str(&content)?;
        Ok(state)
    }

    /// Save state to a file path atomically
    ///
    /// Uses write-to-temp-then-rename pattern to prevent corruption.
    /// Temp file includes PID to avoid races from concurrent processes.
    pub fn save(&self, path: &Path) -> CoreResult<()> {
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CoreError::IoWithPath {
                path: parent.display().to_string(),
                source: e,
            })?;
        }

        let temp_path = path.with_extension(format!("json.{}.tmp", std::process::id()));
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&temp_path, &json).map_err(|e| CoreError::IoWithPath {
            path: temp_path.display().to_string(),
            source: e,
        })?;
        std::fs::rename(&temp_path, path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            CoreError::IoWithPath {
                path: path.display().to_string(),
                source: e,
            }
        })?;
        Ok(())
    }

    /// Get the state for a specific model
    pub fn get_model(&self, name: &str) -> Option<&ModelState> {
        self.models.get(name)
    }

    /// Update or insert a model's state
    pub fn upsert_model(&mut self, state: ModelState) {
        self.models.insert(state.name.clone(), state);
        self.updated_at = Utc::now();
    }

    /// Check if a model has been modified since last run
    ///
    /// Returns true if:
    /// - Model doesn't exist in state
    /// - SQL checksum has changed
    pub fn is_model_modified(&self, name: &str, current_checksum: &str) -> bool {
        match self.models.get(name) {
            Some(state) => state.checksum != current_checksum,
            None => true,
        }
    }

    /// Check if a model or any of its inputs have been modified since last run
    ///
    /// Returns true if:
    /// - Model doesn't exist in state
    /// - SQL checksum has changed
    /// - Schema checksum has changed
    /// - Any upstream input checksum has changed
    pub fn is_model_or_inputs_modified(
        &self,
        name: &str,
        current_sql_checksum: &str,
        current_schema_checksum: Option<&str>,
        current_input_checksums: &HashMap<String, String>,
    ) -> bool {
        let state = match self.models.get(name) {
            Some(s) => s,
            None => return true,
        };

        if state.checksum != current_sql_checksum {
            return true;
        }

        match (&state.schema_checksum, current_schema_checksum) {
            (Some(old), Some(new)) if old != new => return true,
            (None, Some(_)) => return true,
            (Some(_), None) => return true,
            _ => {}
        }

        for (input_name, current_checksum) in current_input_checksums {
            match state.input_checksums.get(input_name) {
                Some(old_checksum) if old_checksum != current_checksum => return true,
                None => return true,
                _ => {}
            }
        }

        for input_name in state.input_checksums.keys() {
            if !current_input_checksums.contains_key(input_name) {
                return true;
            }
        }

        false
    }
}

impl ModelState {
    /// Create a new model state from current run
    pub fn new(
        name: ModelName,
        compiled_sql: &str,
        row_count: Option<usize>,
        config: ModelStateConfig,
    ) -> Self {
        Self {
            name,
            last_run: Utc::now(),
            row_count,
            checksum: compute_checksum(compiled_sql),
            schema_checksum: None,
            input_checksums: HashMap::new(),
            config,
        }
    }

    /// Create a new model state with full checksums for smart builds
    pub fn new_with_checksums(
        name: ModelName,
        compiled_sql: &str,
        row_count: Option<usize>,
        config: ModelStateConfig,
        schema_checksum: Option<String>,
        input_checksums: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            last_run: Utc::now(),
            row_count,
            checksum: compute_checksum(compiled_sql),
            schema_checksum,
            input_checksums,
            config,
        }
    }
}

impl ModelStateConfig {
    /// Create from a simplified set of fields
    pub fn new(
        materialized: Materialization,
        schema: Option<String>,
        unique_key: Option<Vec<String>>,
        incremental_strategy: Option<IncrementalStrategy>,
        on_schema_change: Option<OnSchemaChange>,
    ) -> Self {
        Self {
            materialized,
            schema,
            unique_key,
            incremental_strategy,
            on_schema_change,
        }
    }
}

/// Compute SHA256 checksum of a string
pub fn compute_checksum(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
