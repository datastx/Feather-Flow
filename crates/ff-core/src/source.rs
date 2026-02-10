//! Source definitions for external data tables
//!
//! Sources represent raw data tables that exist in the database but are not
//! managed by Featherflow (e.g., tables loaded by ETL pipelines).

use crate::error::{CoreError, CoreResult};
use crate::model::{FreshnessConfig, TestDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// A source definition file (from .yml with kind: sources)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFile {
    /// Must be "sources" - enforced during parsing
    pub kind: SourceKind,

    /// Schema format version
    #[serde(default = "default_version")]
    pub version: u32,

    /// Logical name for this source group
    pub name: String,

    /// Description of the source group
    #[serde(default)]
    pub description: Option<String>,

    /// Database name (optional, uses default if not specified)
    #[serde(default)]
    pub database: Option<String>,

    /// Schema name (required)
    pub schema: String,

    /// Owner of the source
    #[serde(default)]
    pub owner: Option<String>,

    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,

    /// Tables in this source
    pub tables: Vec<SourceTable>,
}

fn default_version() -> u32 {
    1
}

/// Enforces kind: sources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Sources,
}

/// A single table within a source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTable {
    /// Logical name used in models
    pub name: String,

    /// Actual table name in database (if different from name)
    #[serde(default)]
    pub identifier: Option<String>,

    /// Description of the table
    #[serde(default)]
    pub description: Option<String>,

    /// Column definitions
    #[serde(default)]
    pub columns: Vec<SourceColumn>,

    /// Freshness configuration (future implementation)
    #[serde(default)]
    pub freshness: Option<FreshnessConfig>,
}

/// Column definition within a source table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceColumn {
    /// Column name
    pub name: String,

    /// Data type
    #[serde(rename = "type")]
    pub data_type: String,

    /// Column description
    #[serde(default)]
    pub description: Option<String>,

    /// Tests to run on this column (supports both simple and parameterized tests)
    #[serde(default)]
    pub tests: Vec<TestDefinition>,
}

// FreshnessConfig, FreshnessThreshold, and FreshnessPeriod are imported from
// crate::model to avoid duplication. Both model and source freshness use the
// same unified types.

impl SourceFile {
    /// Load and validate a source file from a path
    pub fn load(path: &Path) -> CoreResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;

        let source: SourceFile =
            serde_yaml::from_str(&content).map_err(|e| CoreError::SourceParseError {
                path: path.display().to_string(),
                details: e.to_string(),
            })?;

        // Validate tables not empty (SRC004)
        if source.tables.is_empty() {
            return Err(CoreError::SourceEmptyTables {
                name: source.name.clone(),
                path: path.display().to_string(),
            });
        }

        // Validate for duplicate tables within this source (SRC007)
        let mut seen_tables = std::collections::HashSet::new();
        for table in &source.tables {
            if !seen_tables.insert(&table.name) {
                return Err(CoreError::SourceDuplicateTable {
                    table: table.name.clone(),
                    source_name: source.name.clone(),
                });
            }
        }

        Ok(source)
    }

    /// Get the fully qualified name of a table
    pub fn get_qualified_name(&self, table: &SourceTable) -> String {
        let table_name = table.identifier.as_ref().unwrap_or(&table.name);
        format!("{}.{}", self.schema, table_name)
    }

    /// Get all table names (both logical and identifier names)
    pub fn get_all_table_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for table in &self.tables {
            // Add unqualified name
            names.push(table.name.clone());
            // Add qualified name
            names.push(format!("{}.{}", self.schema, table.name));

            // If identifier differs, add those too
            if let Some(ref ident) = table.identifier {
                if ident != &table.name {
                    names.push(ident.clone());
                    names.push(format!("{}.{}", self.schema, ident));
                }
            }
        }
        names
    }
}

/// Discover and load all source files from configured directories
///
/// `source_paths` are expected to be absolute paths (e.g., from
/// `Config::source_paths_absolute`). If a relative path is passed it is
/// used as-is; callers are responsible for resolving paths beforehand.
pub fn discover_sources(source_paths: &[PathBuf]) -> CoreResult<Vec<SourceFile>> {
    let mut sources = Vec::new();

    for source_path in source_paths {
        if !source_path.exists() {
            continue;
        }

        discover_sources_recursive(source_path, &mut sources)?;
    }

    // Detect duplicate source names across files
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (idx, source) in sources.iter().enumerate() {
        if let Some(&prev_idx) = seen.get(&source.name) {
            return Err(CoreError::SourceDuplicateName {
                name: source.name.clone(),
                path1: format!("source #{}", prev_idx + 1),
                path2: format!("source #{}", idx + 1),
            });
        }
        seen.insert(source.name.clone(), idx);
    }

    Ok(sources)
}

/// Minimal YAML probe to check the `kind` field without full deserialization
#[derive(Deserialize)]
struct SourceKindProbe {
    #[serde(default)]
    kind: Option<SourceKind>,
}

/// Recursively discover source files in a directory
fn discover_sources_recursive(dir: &Path, sources: &mut Vec<SourceFile>) -> CoreResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            discover_sources_recursive(&path, sources)?;
        } else if path.extension().is_some_and(|e| e == "yml" || e == "yaml") {
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[warn] Cannot read {}: {}", path.display(), e);
                    continue;
                }
            };

            // Probe the kind field before attempting a full parse
            let probe: SourceKindProbe = match serde_yaml::from_str(&content) {
                Ok(p) => p,
                Err(_) => continue,
            };

            if !matches!(probe.kind, Some(SourceKind::Sources)) {
                continue;
            }

            // Full parse — errors here are real and worth reporting
            match SourceFile::load(&path) {
                Ok(source) => sources.push(source),
                Err(e) => {
                    eprintln!("[warn] Skipping source file {}: {}", path.display(), e);
                    continue;
                }
            }
        }
    }

    Ok(())
}

/// Build lookup of known source tables for dependency categorization
pub fn build_source_lookup(sources: &[SourceFile]) -> HashSet<String> {
    let mut lookup = HashSet::new();

    for source in sources {
        for name in source.get_all_table_names() {
            lookup.insert(name);
        }
    }

    lookup
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_source_file(dir: &Path, name: &str, content: &str) {
        std::fs::write(dir.join(name), content).unwrap();
    }

    #[test]
    fn test_parse_source_file() {
        let yaml = r#"
kind: sources
version: 1
name: raw_ecommerce
description: "Raw e-commerce data"
schema: ecommerce

tables:
  - name: orders
    description: "One record per order"
    columns:
      - name: id
        type: INTEGER
        tests:
          - unique
          - not_null
      - name: amount
        type: DECIMAL(10,2)
"#;

        let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(source.name, "raw_ecommerce");
        assert_eq!(source.schema, "ecommerce");
        assert_eq!(source.tables.len(), 1);
        assert_eq!(source.tables[0].name, "orders");
        assert_eq!(source.tables[0].columns.len(), 2);
    }

    #[test]
    fn test_source_kind_validation() {
        let temp = TempDir::new().unwrap();
        let sources_dir = temp.path().join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        // Invalid kind
        create_source_file(
            &sources_dir,
            "invalid.yml",
            r#"
kind: models
name: test
schema: raw
tables:
  - name: test
"#,
        );

        let result = SourceFile::load(&sources_dir.join("invalid.yml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_source_empty_tables_validation() {
        let temp = TempDir::new().unwrap();
        let sources_dir = temp.path().join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        create_source_file(
            &sources_dir,
            "empty.yml",
            r#"
kind: sources
name: test
schema: raw
tables: []
"#,
        );

        let result = SourceFile::load(&sources_dir.join("empty.yml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_discover_sources() {
        let temp = TempDir::new().unwrap();
        let sources_dir = temp.path().join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        create_source_file(
            &sources_dir,
            "raw_data.yml",
            r#"
kind: sources
name: raw_data
schema: raw
tables:
  - name: orders
  - name: customers
"#,
        );

        create_source_file(
            &sources_dir,
            "external_api.yml",
            r#"
kind: sources
name: external_api
schema: api
tables:
  - name: users
"#,
        );

        let sources = discover_sources(std::slice::from_ref(&sources_dir)).unwrap();
        assert_eq!(sources.len(), 2);
    }

    #[test]
    fn test_build_source_lookup() {
        let yaml = r#"
kind: sources
name: raw
schema: ecommerce
tables:
  - name: orders
    identifier: api_orders
  - name: customers
"#;

        let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
        let lookup = build_source_lookup(&[source]);

        assert!(lookup.contains("orders"));
        assert!(lookup.contains("ecommerce.orders"));
        assert!(lookup.contains("api_orders"));
        assert!(lookup.contains("ecommerce.api_orders"));
        assert!(lookup.contains("customers"));
        assert!(lookup.contains("ecommerce.customers"));
    }

    #[test]
    fn test_get_qualified_name() {
        let yaml = r#"
kind: sources
name: raw
schema: ecommerce
tables:
  - name: orders
    identifier: api_orders
"#;

        let source: SourceFile = serde_yaml::from_str(yaml).unwrap();
        let qualified = source.get_qualified_name(&source.tables[0]);
        assert_eq!(qualified, "ecommerce.api_orders");
    }

    #[test]
    fn test_source_duplicate_table_validation() {
        let temp = TempDir::new().unwrap();
        let sources_dir = temp.path().join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        // Duplicate table name
        create_source_file(
            &sources_dir,
            "duplicate.yml",
            r#"
kind: sources
name: test
schema: raw
tables:
  - name: orders
  - name: orders
"#,
        );

        let result = SourceFile::load(&sources_dir.join("duplicate.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("SRC007"),
            "Expected SRC007 error code, got: {}",
            err_str
        );
        assert!(
            err_str.contains("orders"),
            "Expected table name in error, got: {}",
            err_str
        );
    }

    #[test]
    fn test_source_error_codes() {
        let temp = TempDir::new().unwrap();
        let sources_dir = temp.path().join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        // SRC002: Invalid kind
        create_source_file(
            &sources_dir,
            "invalid_kind.yml",
            r#"
kind: models
name: test
schema: raw
tables:
  - name: test
"#,
        );

        let result = SourceFile::load(&sources_dir.join("invalid_kind.yml"));
        assert!(result.is_err());
        // Note: serde_yaml will fail to parse because SourceKind::Sources is required
        // So this becomes a SRC005 parse error, not SRC002

        // SRC004: Empty tables
        create_source_file(
            &sources_dir,
            "empty_tables.yml",
            r#"
kind: sources
name: empty_source
schema: raw
tables: []
"#,
        );

        let result = SourceFile::load(&sources_dir.join("empty_tables.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("SRC004"),
            "Expected SRC004 error code, got: {}",
            err_str
        );

        // SRC005: Parse error
        create_source_file(&sources_dir, "parse_error.yml", "invalid: yaml: content: [");

        let result = SourceFile::load(&sources_dir.join("parse_error.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("SRC005"),
            "Expected SRC005 error code, got: {}",
            err_str
        );
    }
}
