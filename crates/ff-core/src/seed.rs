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
    pub fn column_types(&self) -> &HashMap<String, String> {
        static EMPTY: std::sync::LazyLock<HashMap<String, String>> =
            std::sync::LazyLock::new(HashMap::new);
        self.config
            .as_ref()
            .map(|c| &c.column_types)
            .unwrap_or(&EMPTY)
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
pub fn discover_seeds(seed_paths: &[PathBuf]) -> Result<Vec<Seed>, CoreError> {
    let mut seeds = Vec::new();

    for seed_path in seed_paths {
        if !seed_path.exists() {
            continue;
        }

        discover_seeds_recursive(seed_path, &mut seeds)?;
    }

    // Sort seeds by name for consistent ordering
    seeds.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(seeds)
}

/// Recursively discover CSV files in a directory
fn discover_seeds_recursive(dir: &Path, seeds: &mut Vec<Seed>) -> Result<(), CoreError> {
    let entries = std::fs::read_dir(dir).map_err(|e| CoreError::IoWithPath {
        path: dir.display().to_string(),
        source: e,
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| CoreError::IoWithPath {
            path: dir.display().to_string(),
            source: e,
        })?;
        let path = entry.path();

        if path.is_dir() {
            discover_seeds_recursive(&path, seeds)?;
        } else if path.extension().is_some_and(|e| e == "csv") {
            match Seed::from_file(path) {
                Ok(seed) => seeds.push(seed),
                Err(e) => {
                    log::warn!("Failed to load seed file: {}", e);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "seed_test.rs"]
mod tests;
