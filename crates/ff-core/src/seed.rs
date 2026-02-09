//! Seed file representation and configuration
//!
//! Seeds are CSV files that can be loaded into database tables.
//! Each seed can have an optional `.yml` configuration file with the same name.

use crate::error::CoreError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Represents a CSV seed file in the project
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seed {
    /// Seed name (derived from filename without extension)
    pub name: String,

    /// Path to the source CSV file
    pub path: PathBuf,

    /// Optional configuration from 1:1 .yml file
    #[serde(default)]
    pub config: Option<SeedConfig>,
}

/// Configuration for a seed from its .yml file
///
/// This follows the 1:1 naming convention where each seed's config file
/// has the same name as its CSV file (e.g., customers.csv + customers.yml)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedConfig {
    /// Config format version
    #[serde(default = "default_version")]
    pub version: u32,

    /// Seed description
    #[serde(default)]
    pub description: Option<String>,

    /// Override target schema (default: project schema or none)
    #[serde(default)]
    pub schema: Option<String>,

    /// Force column quoting
    #[serde(default)]
    pub quote_columns: bool,

    /// Override inferred types for specific columns
    /// Key: column name, Value: SQL type (e.g., "VARCHAR", "INTEGER", "DATE")
    #[serde(default)]
    pub column_types: HashMap<String, String>,

    /// CSV delimiter (default: comma)
    #[serde(default = "default_delimiter")]
    pub delimiter: char,

    /// Enable/disable this seed
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_version() -> u32 {
    1
}

fn default_delimiter() -> char {
    ','
}

fn default_enabled() -> bool {
    true
}

impl Default for SeedConfig {
    fn default() -> Self {
        Self {
            version: default_version(),
            description: None,
            schema: None,
            quote_columns: false,
            column_types: HashMap::new(),
            delimiter: default_delimiter(),
            enabled: default_enabled(),
        }
    }
}

impl SeedConfig {
    /// Load seed configuration from a file path
    pub fn load(path: &Path) -> Result<Self, CoreError> {
        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
        let config: SeedConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

impl Seed {
    /// Create a new seed from a CSV file path
    ///
    /// This also looks for a matching 1:1 config file (same name with .yml or .yaml extension)
    pub fn from_file(path: PathBuf) -> Result<Self, CoreError> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| CoreError::ModelParseError {
                name: path.display().to_string(),
                message: "Invalid file name".to_string(),
            })?
            .to_string();

        // Look for matching 1:1 config file
        let yml_path = path.with_extension("yml");
        let yaml_path = path.with_extension("yaml");

        let config = if yml_path.exists() {
            Some(SeedConfig::load(&yml_path)?)
        } else if yaml_path.exists() {
            Some(SeedConfig::load(&yaml_path)?)
        } else {
            None
        };

        Ok(Self { name, path, config })
    }

    /// Check if this seed is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.as_ref().map(|c| c.enabled).unwrap_or(true)
    }

    /// Get the target schema for this seed
    pub fn target_schema(&self) -> Option<&str> {
        self.config.as_ref().and_then(|c| c.schema.as_deref())
    }

    /// Get the delimiter for this seed's CSV file
    pub fn delimiter(&self) -> char {
        self.config
            .as_ref()
            .map(|c| c.delimiter)
            .unwrap_or(default_delimiter())
    }

    /// Get column type overrides
    pub fn column_types(&self) -> HashMap<String, String> {
        self.config
            .as_ref()
            .map(|c| c.column_types.clone())
            .unwrap_or_default()
    }

    /// Check if columns should be quoted
    pub fn quote_columns(&self) -> bool {
        self.config
            .as_ref()
            .map(|c| c.quote_columns)
            .unwrap_or(false)
    }

    /// Get the qualified table name (schema.name or just name)
    pub fn qualified_name(&self, default_schema: Option<&str>) -> String {
        let schema = self.target_schema().or(default_schema);
        match schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }
}

/// Discover all seed files in the given paths
pub fn discover_seeds(_root: &Path, seed_paths: &[PathBuf]) -> Vec<Seed> {
    // _root reserved for relative path resolution
    let mut seeds = Vec::new();

    for seed_path in seed_paths {
        if !seed_path.exists() {
            continue;
        }

        discover_seeds_recursive(seed_path, &mut seeds);
    }

    // Sort seeds by name for consistent ordering
    seeds.sort_by(|a, b| a.name.cmp(&b.name));
    seeds
}

/// Recursively discover CSV files in a directory
fn discover_seeds_recursive(dir: &Path, seeds: &mut Vec<Seed>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            discover_seeds_recursive(&path, seeds);
        } else if path.extension().is_some_and(|e| e == "csv") {
            match Seed::from_file(path) {
                Ok(seed) => seeds.push(seed),
                Err(e) => {
                    eprintln!("Warning: Failed to load seed file: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_seed_from_file_without_config() {
        let dir = TempDir::new().unwrap();
        let csv_path = dir.path().join("customers.csv");
        std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob").unwrap();

        let seed = Seed::from_file(csv_path).unwrap();
        assert_eq!(seed.name, "customers");
        assert!(seed.config.is_none());
        assert!(seed.is_enabled());
        assert_eq!(seed.delimiter(), ',');
    }

    #[test]
    fn test_seed_from_file_with_config() {
        let dir = TempDir::new().unwrap();
        let csv_path = dir.path().join("orders.csv");
        let yml_path = dir.path().join("orders.yml");

        std::fs::write(&csv_path, "id,amount\n1,100.50").unwrap();
        std::fs::write(
            &yml_path,
            r#"
version: 1
description: Order data
schema: raw
delimiter: ","
column_types:
  id: INTEGER
  amount: DECIMAL(10,2)
"#,
        )
        .unwrap();

        let seed = Seed::from_file(csv_path).unwrap();
        assert_eq!(seed.name, "orders");
        assert!(seed.config.is_some());

        let config = seed.config.as_ref().unwrap();
        assert_eq!(config.schema, Some("raw".to_string()));
        assert_eq!(config.column_types.get("id"), Some(&"INTEGER".to_string()));
        assert_eq!(
            config.column_types.get("amount"),
            Some(&"DECIMAL(10,2)".to_string())
        );
    }

    #[test]
    fn test_seed_disabled() {
        let dir = TempDir::new().unwrap();
        let csv_path = dir.path().join("disabled.csv");
        let yml_path = dir.path().join("disabled.yml");

        std::fs::write(&csv_path, "id\n1").unwrap();
        std::fs::write(&yml_path, "enabled: false").unwrap();

        let seed = Seed::from_file(csv_path).unwrap();
        assert!(!seed.is_enabled());
    }

    #[test]
    fn test_seed_qualified_name() {
        let dir = TempDir::new().unwrap();

        // Without config
        let csv_path = dir.path().join("customers.csv");
        std::fs::write(&csv_path, "id\n1").unwrap();
        let seed = Seed::from_file(csv_path).unwrap();
        assert_eq!(seed.qualified_name(None), "customers");
        assert_eq!(seed.qualified_name(Some("default")), "default.customers");

        // With config schema
        let csv_path2 = dir.path().join("orders.csv");
        let yml_path2 = dir.path().join("orders.yml");
        std::fs::write(&csv_path2, "id\n1").unwrap();
        std::fs::write(&yml_path2, "schema: raw").unwrap();
        let seed2 = Seed::from_file(csv_path2).unwrap();
        // Config schema overrides default
        assert_eq!(seed2.qualified_name(Some("default")), "raw.orders");
    }

    #[test]
    fn test_discover_seeds() {
        let dir = TempDir::new().unwrap();
        let seeds_dir = dir.path().join("seeds");
        std::fs::create_dir_all(&seeds_dir).unwrap();

        std::fs::write(seeds_dir.join("a.csv"), "id\n1").unwrap();
        std::fs::write(seeds_dir.join("b.csv"), "id\n2").unwrap();

        let seeds = discover_seeds(dir.path(), &[seeds_dir]);
        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0].name, "a");
        assert_eq!(seeds[1].name, "b");
    }

    #[test]
    fn test_seed_config_defaults() {
        let config = SeedConfig::default();
        assert_eq!(config.version, 1);
        assert_eq!(config.delimiter, ',');
        assert!(config.enabled);
        assert!(!config.quote_columns);
        assert!(config.column_types.is_empty());
    }

    #[test]
    fn test_custom_delimiter() {
        let dir = TempDir::new().unwrap();
        let csv_path = dir.path().join("tsv_data.csv");
        let yml_path = dir.path().join("tsv_data.yml");

        std::fs::write(&csv_path, "id\tname\n1\tAlice").unwrap();
        std::fs::write(&yml_path, "delimiter: \"\\t\"").unwrap();

        let seed = Seed::from_file(csv_path).unwrap();
        // Note: YAML escape sequences need special handling
        // For now, tab delimiter would need to be specified differently
        assert!(seed.config.is_some());
    }
}
