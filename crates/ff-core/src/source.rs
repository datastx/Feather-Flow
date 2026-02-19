//! Source definitions for external data tables
//!
//! Sources represent raw data tables that exist in the database but are not
//! managed by Featherflow (e.g., tables loaded by ETL pipelines).

use crate::error::{CoreError, CoreResult};
use crate::model::TestDefinition;
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

/// Enforces kind: source (or legacy kind: sources)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    /// Modern singular form
    Source,
    /// Legacy plural form (backward compatible)
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

        if source.tables.is_empty() {
            return Err(CoreError::SourceEmptyTables {
                name: source.name.clone(),
                path: path.display().to_string(),
            });
        }

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
        self.tables
            .iter()
            .flat_map(|table| {
                let schema = &self.schema;
                let mut names = vec![table.name.clone(), format!("{schema}.{}", table.name)];
                if let Some(ref ident) = table.identifier {
                    if ident != &table.name {
                        names.push(ident.clone());
                        names.push(format!("{schema}.{ident}"));
                    }
                }
                names
            })
            .collect()
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

/// Minimal YAML probe that deserializes only the `kind` field.
///
/// Used to cheaply determine whether a YAML file is a sources definition
/// before attempting a full parse with [`SourceFile`].
#[derive(Deserialize)]
struct SourceKindProbe {
    #[serde(default)]
    kind: Option<SourceKind>,
}

/// Recursively discover source files in a directory
fn discover_sources_recursive(dir: &Path, sources: &mut Vec<SourceFile>) -> CoreResult<()> {
    for entry in std::fs::read_dir(dir).map_err(|e| CoreError::IoWithPath {
        path: dir.display().to_string(),
        source: e,
    })? {
        let entry = entry.map_err(|e| CoreError::IoWithPath {
            path: dir.display().to_string(),
            source: e,
        })?;
        let path = entry.path();

        if path.is_dir() {
            discover_sources_recursive(&path, sources)?;
        } else if path.extension().is_some_and(|e| e == "yml" || e == "yaml") {
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("Cannot read {}: {}", path.display(), e);
                    continue;
                }
            };

            // Probe the kind field before attempting a full parse
            let probe: SourceKindProbe = match serde_yaml::from_str(&content) {
                Ok(p) => p,
                Err(_) => continue,
            };

            if !matches!(probe.kind, Some(SourceKind::Sources | SourceKind::Source)) {
                continue;
            }

            // Kind probe confirmed this is a sources file — parse errors are real
            let source = SourceFile::load(&path)?;
            sources.push(source);
        }
    }

    Ok(())
}

/// Build lookup of known source tables for dependency categorization
pub fn build_source_lookup(sources: &[SourceFile]) -> HashSet<String> {
    sources
        .iter()
        .flat_map(|s| s.get_all_table_names())
        .collect()
}

#[cfg(test)]
#[path = "source_test.rs"]
mod tests;
