//! Snapshot types and configuration for SCD Type 2 tracking
//!
//! Snapshots track historical changes to mutable source data, creating a
//! slowly changing dimension (SCD Type 2) table that preserves the history
//! of changes over time.

use crate::error::CoreError;
use crate::sql_utils::{quote_ident, quote_qualified};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ── SCD Type 2 column name constants ──────────────────────────────
//
// Single source of truth for the SCD tracking columns added to every
// snapshot table.  Both `ff-core` snapshot SQL generators and the
// `ff-db` DuckDB implementation reference these.

/// Surrogate key column (MD5 hash of unique keys + timestamp).
pub const SCD_ID: &str = "dbt_scd_id";
/// Tracks when the source row was last updated.
pub const SCD_UPDATED_AT: &str = "dbt_updated_at";
/// Timestamp when this snapshot version became active.
pub const SCD_VALID_FROM: &str = "dbt_valid_from";
/// Timestamp when this snapshot version was superseded (`NULL` = current).
pub const SCD_VALID_TO: &str = "dbt_valid_to";

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

    /// Get the unquoted qualified name for the snapshot table
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
        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
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
        let quoted_name = quote_qualified(&self.config.qualified_name());

        // Build column definitions from source columns plus SCD columns
        let mut columns: Vec<String> = source_columns
            .iter()
            .map(|(name, dtype)| format!("    {} {}", quote_ident(name), dtype))
            .collect();

        // Add SCD Type 2 tracking columns
        columns.push(format!("    {} VARCHAR", quote_ident(SCD_ID)));
        columns.push(format!("    {} TIMESTAMP", quote_ident(SCD_UPDATED_AT)));
        columns.push(format!("    {} TIMESTAMP", quote_ident(SCD_VALID_FROM)));
        columns.push(format!("    {} TIMESTAMP", quote_ident(SCD_VALID_TO)));

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
            quoted_name,
            columns.join(",\n")
        )
    }

    /// Generate the SQL to get current (active) records from snapshot
    pub fn current_records_sql(&self) -> String {
        format!(
            "SELECT * FROM {} WHERE {} IS NULL",
            quote_qualified(&self.config.qualified_name()),
            quote_ident(SCD_VALID_TO)
        )
    }

    /// Generate SQL to detect new records (INSERT)
    pub fn new_records_sql(
        &self,
        source_alias: &str,
        snapshot_alias: &str,
    ) -> Result<String, CoreError> {
        let src_alias = quote_ident(source_alias);
        let snap_alias = quote_ident(snapshot_alias);
        let unique_key_conditions: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("{snap_alias}.{qk} = {src_alias}.{qk}")
            })
            .collect();

        let first_key = quote_ident(self.config.unique_key.first().ok_or_else(|| {
            CoreError::ConfigInvalid {
                message: format!("Snapshot '{}' has empty unique_key", self.config.name),
            }
        })?);

        Ok(format!(
            "SELECT {source}.* \
             FROM {source} AS {src_alias} \
             LEFT JOIN ({current}) AS {snap_alias} \
               ON {conditions} \
             WHERE {snap_alias}.{first_key} IS NULL",
            source = quote_qualified(&self.config.source),
            src_alias = src_alias,
            current = self.current_records_sql(),
            snap_alias = snap_alias,
            conditions = unique_key_conditions.join(" AND "),
            first_key = first_key,
        ))
    }

    /// Generate SQL to detect changed records based on strategy
    pub fn changed_records_sql(&self, source_alias: &str, snapshot_alias: &str) -> String {
        let src_alias = quote_ident(source_alias);
        let snap_alias = quote_ident(snapshot_alias);
        let unique_key_conditions: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("{snap_alias}.{qk} = {src_alias}.{qk}")
            })
            .collect();

        let change_condition = match self.config.strategy {
            SnapshotStrategy::Timestamp => {
                // Timestamp strategy requires updated_at to be set (validated in SnapshotConfig::validate)
                let updated_at_col = self.config.updated_at.as_deref().expect(
                    "Timestamp strategy requires updated_at (should be caught by validation)",
                );
                let updated_at = quote_ident(updated_at_col);
                let dbt_updated_at = quote_ident(SCD_UPDATED_AT);
                format!("{src_alias}.{updated_at} > {snap_alias}.{dbt_updated_at}")
            }
            SnapshotStrategy::Check => {
                let col_comparisons: Vec<String> = self
                    .config
                    .check_cols
                    .iter()
                    .map(|c| {
                        let qc = quote_ident(c);
                        format!("({src_alias}.{qc} IS DISTINCT FROM {snap_alias}.{qc})")
                    })
                    .collect();
                col_comparisons.join(" OR ")
            }
        };

        format!(
            "SELECT {source}.* \
             FROM {source} AS {src_alias} \
             INNER JOIN ({current}) AS {snap_alias} \
               ON {conditions} \
             WHERE {change_condition}",
            source = quote_qualified(&self.config.source),
            src_alias = src_alias,
            current = self.current_records_sql(),
            snap_alias = snap_alias,
            conditions = unique_key_conditions.join(" AND "),
            change_condition = change_condition,
        )
    }

    /// Generate SQL to detect hard deletes (records missing from source)
    pub fn deleted_records_sql(
        &self,
        source_alias: &str,
        snapshot_alias: &str,
    ) -> Result<String, CoreError> {
        let src_alias = quote_ident(source_alias);
        let snap_alias = quote_ident(snapshot_alias);
        let unique_key_conditions: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| {
                let qk = quote_ident(k);
                format!("{snap_alias}.{qk} = {src_alias}.{qk}")
            })
            .collect();

        let first_key = quote_ident(self.config.unique_key.first().ok_or_else(|| {
            CoreError::ConfigInvalid {
                message: format!("Snapshot '{}' has empty unique_key", self.config.name),
            }
        })?);

        Ok(format!(
            "SELECT {snap_alias}.* \
             FROM ({current}) AS {snap_alias} \
             LEFT JOIN {source} AS {src_alias} \
               ON {conditions} \
             WHERE {src_alias}.{first_key} IS NULL",
            current = self.current_records_sql(),
            snap_alias = snap_alias,
            source = quote_qualified(&self.config.source),
            src_alias = src_alias,
            conditions = unique_key_conditions.join(" AND "),
            first_key = first_key,
        ))
    }

    /// Generate a unique SCD ID for a record
    pub fn scd_id_expression(&self) -> String {
        // Create a hash of unique key columns plus timestamp
        let key_cols: Vec<String> = self
            .config
            .unique_key
            .iter()
            .map(|k| quote_ident(k))
            .collect();
        let key_expr = key_cols.join(" || '|' || ");
        format!(
            "MD5({} || '|' || CAST({} AS VARCHAR))",
            key_expr,
            quote_ident(SCD_VALID_FROM)
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
        assert!(sql.contains(r#"CREATE TABLE IF NOT EXISTS "customer_history""#));
        assert!(sql.contains(r#""id" INTEGER"#));
        assert!(sql.contains(r#""name" VARCHAR"#));
        assert!(sql.contains(r#""dbt_scd_id" VARCHAR"#));
        assert!(sql.contains(r#""dbt_valid_from" TIMESTAMP"#));
        assert!(sql.contains(r#""dbt_valid_to" TIMESTAMP"#));
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
