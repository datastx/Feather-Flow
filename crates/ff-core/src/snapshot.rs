//! Snapshot types and configuration for SCD Type 2 tracking
//!
//! Snapshots track historical changes to mutable source data, creating a
//! slowly changing dimension (SCD Type 2) table that preserves the history
//! of changes over time.

use crate::error::CoreError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Strategy for detecting changes in snapshots
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategy {
    /// Detect changes using a timestamp column
    #[default]
    Timestamp,
    /// Detect changes by comparing specific columns
    Check,
}

impl std::fmt::Display for SnapshotStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotStrategy::Timestamp => write!(f, "timestamp"),
            SnapshotStrategy::Check => write!(f, "check"),
        }
    }
}

/// Snapshot configuration from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    /// Snapshot name
    pub name: String,

    /// Source table to snapshot (can be schema.table format)
    pub source: String,

    /// Column(s) that uniquely identify a record
    pub unique_key: Vec<String>,

    /// Strategy for detecting changes
    pub strategy: SnapshotStrategy,

    /// Column containing update timestamp (required for timestamp strategy)
    #[serde(default)]
    pub updated_at: Option<String>,

    /// Columns to compare for changes (required for check strategy)
    #[serde(default)]
    pub check_cols: Vec<String>,

    /// Whether to invalidate hard deletes (records missing from source)
    #[serde(default)]
    pub invalidate_hard_deletes: bool,

    /// Target schema for the snapshot table
    #[serde(default)]
    pub schema: Option<String>,

    /// Description of the snapshot
    #[serde(default)]
    pub description: Option<String>,

    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,
}

impl SnapshotConfig {
    /// Validate the snapshot configuration
    pub fn validate(&self) -> Result<(), CoreError> {
        // Check that unique_key is not empty
        if self.unique_key.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: format!(
                    "Snapshot '{}' must have at least one unique_key column",
                    self.name
                ),
            });
        }

        // Validate strategy-specific requirements
        match self.strategy {
            SnapshotStrategy::Timestamp => {
                if self.updated_at.is_none() {
                    return Err(CoreError::ConfigInvalid {
                        message: format!(
                            "Snapshot '{}' with timestamp strategy requires 'updated_at' column",
                            self.name
                        ),
                    });
                }
            }
            SnapshotStrategy::Check => {
                if self.check_cols.is_empty() {
                    return Err(CoreError::ConfigInvalid {
                        message: format!(
                            "Snapshot '{}' with check strategy requires 'check_cols' list",
                            self.name
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Get the qualified name for the snapshot table
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

/// A snapshot file containing one or more snapshot configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile {
    /// Schema version
    #[serde(default = "default_version")]
    pub version: u32,

    /// List of snapshots defined in this file
    pub snapshots: Vec<SnapshotConfig>,
}

fn default_version() -> u32 {
    1
}

impl SnapshotFile {
    /// Load snapshot configuration from a YAML file
    pub fn load(path: &Path) -> Result<Self, CoreError> {
        let content = std::fs::read_to_string(path)?;
        let file: SnapshotFile = serde_yaml::from_str(&content)?;

        // Validate all snapshots
        for snapshot in &file.snapshots {
            snapshot.validate()?;
        }

        Ok(file)
    }
}

/// Represents a loaded snapshot ready for execution
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Snapshot name
    pub name: String,

    /// Path to the snapshot YAML file
    pub path: std::path::PathBuf,

    /// Snapshot configuration
    pub config: SnapshotConfig,
}

impl Snapshot {
    /// Create a new snapshot from config
    pub fn new(config: SnapshotConfig, path: std::path::PathBuf) -> Self {
        Self {
            name: config.name.clone(),
            path,
            config,
        }
    }

    /// Generate the SQL to create the snapshot table if it doesn't exist
    pub fn create_table_sql(&self, source_columns: &[(String, String)]) -> String {
        let qualified_name = self.config.qualified_name();

        // Build column definitions from source columns plus SCD columns
        let mut columns: Vec<String> = source_columns
            .iter()
            .map(|(name, dtype)| format!("    {} {}", name, dtype))
            .collect();

        // Add SCD Type 2 tracking columns
        columns.push("    dbt_scd_id VARCHAR".to_string());
        columns.push("    dbt_updated_at TIMESTAMP".to_string());
        columns.push("    dbt_valid_from TIMESTAMP".to_string());
        columns.push("    dbt_valid_to TIMESTAMP".to_string());

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
            qualified_name,
            columns.join(",\n")
        )
    }

    /// Generate the SQL to get current (active) records from snapshot
    pub fn current_records_sql(&self) -> String {
        format!(
            "SELECT * FROM {} WHERE dbt_valid_to IS NULL",
            self.config.qualified_name()
        )
    }

    /// Generate SQL to detect new records (INSERT)
    pub fn new_records_sql(&self, source_alias: &str, snapshot_alias: &str) -> String {
        let unique_key_conditions: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| format!("{}.{} = {}.{}", snapshot_alias, k, source_alias, k))
            .collect();

        format!(
            "SELECT {source}.* \
             FROM {source} AS {source_alias} \
             LEFT JOIN ({current}) AS {snapshot_alias} \
               ON {conditions} \
             WHERE {snapshot_alias}.{first_key} IS NULL",
            source = self.config.source,
            source_alias = source_alias,
            current = self.current_records_sql(),
            snapshot_alias = snapshot_alias,
            conditions = unique_key_conditions.join(" AND "),
            first_key = self
                .config
                .unique_key
                .first()
                .map(String::as_str)
                .unwrap_or("id"),
        )
    }

    /// Generate SQL to detect changed records based on strategy
    pub fn changed_records_sql(&self, source_alias: &str, snapshot_alias: &str) -> String {
        let unique_key_conditions: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| format!("{}.{} = {}.{}", snapshot_alias, k, source_alias, k))
            .collect();

        let change_condition = match self.config.strategy {
            SnapshotStrategy::Timestamp => {
                let updated_at = self.config.updated_at.as_deref().unwrap_or("updated_at");
                format!(
                    "{}.{} > {}.dbt_updated_at",
                    source_alias, updated_at, snapshot_alias
                )
            }
            SnapshotStrategy::Check => {
                // For check strategy, compare the check_cols
                let col_comparisons: Vec<String> = self
                    .config
                    .check_cols
                    .iter()
                    .map(|c| {
                        format!(
                            "({s}.{c} IS DISTINCT FROM {snap}.{c})",
                            s = source_alias,
                            snap = snapshot_alias,
                            c = c
                        )
                    })
                    .collect();
                col_comparisons.join(" OR ")
            }
        };

        format!(
            "SELECT {source}.* \
             FROM {source} AS {source_alias} \
             INNER JOIN ({current}) AS {snapshot_alias} \
               ON {conditions} \
             WHERE {change_condition}",
            source = self.config.source,
            source_alias = source_alias,
            current = self.current_records_sql(),
            snapshot_alias = snapshot_alias,
            conditions = unique_key_conditions.join(" AND "),
            change_condition = change_condition,
        )
    }

    /// Generate SQL to detect hard deletes (records missing from source)
    pub fn deleted_records_sql(&self, source_alias: &str, snapshot_alias: &str) -> String {
        let unique_key_conditions: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| format!("{}.{} = {}.{}", snapshot_alias, k, source_alias, k))
            .collect();

        format!(
            "SELECT {snapshot_alias}.* \
             FROM ({current}) AS {snapshot_alias} \
             LEFT JOIN {source} AS {source_alias} \
               ON {conditions} \
             WHERE {source_alias}.{first_key} IS NULL",
            current = self.current_records_sql(),
            snapshot_alias = snapshot_alias,
            source = self.config.source,
            source_alias = source_alias,
            conditions = unique_key_conditions.join(" AND "),
            first_key = self
                .config
                .unique_key
                .first()
                .map(String::as_str)
                .unwrap_or("id"),
        )
    }

    /// Generate a unique SCD ID for a record
    pub fn scd_id_expression(&self) -> String {
        // Create a hash of unique key columns plus timestamp
        let key_cols = self.config.unique_key.join(" || '|' || ");
        format!(
            "MD5({} || '|' || CAST(dbt_valid_from AS VARCHAR))",
            key_cols
        )
    }
}

/// Discover snapshots from snapshot paths
pub fn discover_snapshots(
    project_root: &Path,
    snapshot_paths: &[String],
) -> Result<Vec<Snapshot>, CoreError> {
    let mut snapshots = Vec::new();

    for snapshot_path in snapshot_paths {
        let dir = project_root.join(snapshot_path);
        if !dir.exists() {
            continue;
        }

        // Find all .yml and .yaml files
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "yml" || ext == "yaml" {
                        let snapshot_file = SnapshotFile::load(&path)?;
                        for config in snapshot_file.snapshots {
                            snapshots.push(Snapshot::new(config, path.clone()));
                        }
                    }
                }
            }
        }
    }

    Ok(snapshots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_config_validation_timestamp() {
        let config = SnapshotConfig {
            name: "test_snapshot".to_string(),
            source: "raw.customers".to_string(),
            unique_key: vec!["id".to_string()],
            strategy: SnapshotStrategy::Timestamp,
            updated_at: Some("updated_at".to_string()),
            check_cols: Vec::new(),
            invalidate_hard_deletes: false,
            schema: None,
            description: None,
            tags: Vec::new(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_snapshot_config_validation_timestamp_missing_updated_at() {
        let config = SnapshotConfig {
            name: "test_snapshot".to_string(),
            source: "raw.customers".to_string(),
            unique_key: vec!["id".to_string()],
            strategy: SnapshotStrategy::Timestamp,
            updated_at: None,
            check_cols: Vec::new(),
            invalidate_hard_deletes: false,
            schema: None,
            description: None,
            tags: Vec::new(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_snapshot_config_validation_check() {
        let config = SnapshotConfig {
            name: "test_snapshot".to_string(),
            source: "raw.customers".to_string(),
            unique_key: vec!["id".to_string()],
            strategy: SnapshotStrategy::Check,
            updated_at: None,
            check_cols: vec!["name".to_string(), "address".to_string()],
            invalidate_hard_deletes: false,
            schema: None,
            description: None,
            tags: Vec::new(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_snapshot_config_validation_check_missing_cols() {
        let config = SnapshotConfig {
            name: "test_snapshot".to_string(),
            source: "raw.customers".to_string(),
            unique_key: vec!["id".to_string()],
            strategy: SnapshotStrategy::Check,
            updated_at: None,
            check_cols: Vec::new(),
            invalidate_hard_deletes: false,
            schema: None,
            description: None,
            tags: Vec::new(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_snapshot_config_validation_empty_unique_key() {
        let config = SnapshotConfig {
            name: "test_snapshot".to_string(),
            source: "raw.customers".to_string(),
            unique_key: Vec::new(),
            strategy: SnapshotStrategy::Timestamp,
            updated_at: Some("updated_at".to_string()),
            check_cols: Vec::new(),
            invalidate_hard_deletes: false,
            schema: None,
            description: None,
            tags: Vec::new(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_snapshot_qualified_name() {
        let config = SnapshotConfig {
            name: "customer_history".to_string(),
            source: "raw.customers".to_string(),
            unique_key: vec!["id".to_string()],
            strategy: SnapshotStrategy::Timestamp,
            updated_at: Some("updated_at".to_string()),
            check_cols: Vec::new(),
            invalidate_hard_deletes: false,
            schema: Some("snapshots".to_string()),
            description: None,
            tags: Vec::new(),
        };

        assert_eq!(config.qualified_name(), "snapshots.customer_history");
    }

    #[test]
    fn test_snapshot_load_yaml() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("customers.yml");

        let yaml_content = r#"
version: 1
snapshots:
  - name: customer_history
    source: raw.customers
    unique_key:
      - id
    strategy: timestamp
    updated_at: updated_at
    invalidate_hard_deletes: true
    description: Track customer changes
"#;

        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let snapshot_file = SnapshotFile::load(&path).unwrap();
        assert_eq!(snapshot_file.snapshots.len(), 1);

        let snapshot = &snapshot_file.snapshots[0];
        assert_eq!(snapshot.name, "customer_history");
        assert_eq!(snapshot.source, "raw.customers");
        assert_eq!(snapshot.unique_key, vec!["id"]);
        assert_eq!(snapshot.strategy, SnapshotStrategy::Timestamp);
        assert_eq!(snapshot.updated_at, Some("updated_at".to_string()));
        assert!(snapshot.invalidate_hard_deletes);
    }

    #[test]
    fn test_snapshot_create_table_sql() {
        let config = SnapshotConfig {
            name: "customer_history".to_string(),
            source: "raw.customers".to_string(),
            unique_key: vec!["id".to_string()],
            strategy: SnapshotStrategy::Timestamp,
            updated_at: Some("updated_at".to_string()),
            check_cols: Vec::new(),
            invalidate_hard_deletes: false,
            schema: None,
            description: None,
            tags: Vec::new(),
        };

        let snapshot = Snapshot::new(config, std::path::PathBuf::from("test.yml"));
        let source_columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "VARCHAR".to_string()),
        ];

        let sql = snapshot.create_table_sql(&source_columns);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS customer_history"));
        assert!(sql.contains("id INTEGER"));
        assert!(sql.contains("name VARCHAR"));
        assert!(sql.contains("dbt_scd_id VARCHAR"));
        assert!(sql.contains("dbt_valid_from TIMESTAMP"));
        assert!(sql.contains("dbt_valid_to TIMESTAMP"));
    }

    #[test]
    fn test_discover_snapshots() {
        let temp = TempDir::new().unwrap();
        let snapshot_dir = temp.path().join("snapshots");
        std::fs::create_dir(&snapshot_dir).unwrap();

        let yaml_content = r#"
version: 1
snapshots:
  - name: customer_history
    source: raw.customers
    unique_key:
      - id
    strategy: timestamp
    updated_at: updated_at
"#;

        let path = snapshot_dir.join("customers.yml");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let snapshots = discover_snapshots(temp.path(), &["snapshots".to_string()]).unwrap();

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "customer_history");
    }
}
