//! State tracking for incremental models

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;

use crate::config::{IncrementalStrategy, Materialization, OnSchemaChange};
use crate::error::CoreResult;

/// State file containing all model states
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateFile {
    /// When this state file was last updated
    pub updated_at: DateTime<Utc>,

    /// State for each model, keyed by model name
    pub models: HashMap<String, ModelState>,
}

/// State tracking for a single model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelState {
    /// Model name
    pub name: String,

    /// Last successful run timestamp
    pub last_run: DateTime<Utc>,

    /// Row count after last run (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,

    /// SHA256 hash of compiled SQL
    pub checksum: String,

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

        let content = std::fs::read_to_string(path)?;
        let state: StateFile = serde_json::from_str(&content)?;
        Ok(state)
    }

    /// Save state to a file path
    pub fn save(&self, path: &Path) -> CoreResult<()> {
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
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
}

impl ModelState {
    /// Create a new model state from current run
    pub fn new(
        name: String,
        compiled_sql: &str,
        row_count: Option<usize>,
        config: ModelStateConfig,
    ) -> Self {
        Self {
            name,
            last_run: Utc::now(),
            row_count,
            checksum: compute_checksum(compiled_sql),
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
mod tests {
    use super::*;

    #[test]
    fn test_compute_checksum() {
        let checksum1 = compute_checksum("SELECT * FROM users");
        let checksum2 = compute_checksum("SELECT * FROM users");
        let checksum3 = compute_checksum("SELECT * FROM customers");

        assert_eq!(checksum1, checksum2);
        assert_ne!(checksum1, checksum3);
        assert_eq!(checksum1.len(), 64); // SHA256 produces 64 hex chars
    }

    #[test]
    fn test_state_file_new() {
        let state = StateFile::new();
        assert!(state.models.is_empty());
    }

    #[test]
    fn test_model_state_new() {
        let config = ModelStateConfig::new(
            Materialization::Incremental,
            Some("staging".to_string()),
            Some(vec!["id".to_string()]),
            Some(IncrementalStrategy::Merge),
            Some(OnSchemaChange::Ignore),
        );

        let state = ModelState::new(
            "my_model".to_string(),
            "SELECT * FROM users",
            Some(100),
            config,
        );

        assert_eq!(state.name, "my_model");
        assert_eq!(state.row_count, Some(100));
        assert!(!state.checksum.is_empty());
    }

    #[test]
    fn test_is_model_modified() {
        let mut state_file = StateFile::new();

        let config = ModelStateConfig::new(Materialization::Table, None, None, None, None);
        let model_state =
            ModelState::new("my_model".to_string(), "SELECT * FROM users", None, config);

        state_file.upsert_model(model_state);

        // Same checksum should not be modified
        let same_checksum = compute_checksum("SELECT * FROM users");
        assert!(!state_file.is_model_modified("my_model", &same_checksum));

        // Different checksum should be modified
        let diff_checksum = compute_checksum("SELECT * FROM customers");
        assert!(state_file.is_model_modified("my_model", &diff_checksum));

        // Unknown model should be modified
        assert!(state_file.is_model_modified("unknown_model", &same_checksum));
    }
}
