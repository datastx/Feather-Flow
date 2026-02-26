//! Seed file representation and configuration
//!
//! Seeds are CSV files that can be loaded into database tables.
//! Seeds live in the `nodes/` directory and are identified by `kind: seed`
//! in their YAML schema file.

use crate::error::CoreError;
use crate::model::schema::ModelSchema;
use crate::seed_name::SeedName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Represents a CSV seed file in the project
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seed {
    /// Seed name (derived from filename without extension)
    pub name: SeedName,

    /// Path to the source CSV file
    pub path: PathBuf,

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

fn default_delimiter() -> char {
    ','
}

fn default_enabled() -> bool {
    true
}

impl Seed {
    /// Create a seed from a CSV file path and its already-loaded ModelSchema.
    ///
    /// The schema must have `kind: seed`. Seed-specific configuration
    /// (delimiter, column_types, etc.) is read from the schema fields.
    pub fn from_schema(path: PathBuf, schema: &ModelSchema) -> Result<Self, CoreError> {
        let stem = path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
            CoreError::ModelParseError {
                name: path.display().to_string(),
                message: "Invalid file name".to_string(),
            }
        })?;
        let name = SeedName::new(stem);

        Ok(Self {
            name,
            path,
            description: schema.description.clone(),
            schema: schema.schema.clone(),
            quote_columns: schema.quote_columns,
            column_types: schema.column_types.clone(),
            delimiter: schema.delimiter,
            enabled: schema.enabled,
        })
    }

    /// Check if this seed is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the target schema for this seed
    pub fn target_schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get the delimiter for this seed's CSV file
    pub fn delimiter(&self) -> char {
        self.delimiter
    }

    /// Get column type overrides
    pub fn column_types(&self) -> &HashMap<String, String> {
        &self.column_types
    }

    /// Check if columns should be quoted
    pub fn quote_columns(&self) -> bool {
        self.quote_columns
    }

    /// Get the qualified table name (schema.name or just name)
    pub fn qualified_name(&self, default_schema: Option<&str>) -> String {
        let schema = self.target_schema().or(default_schema);
        match schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.to_string(),
        }
    }
}

#[cfg(test)]
#[path = "seed_test.rs"]
mod tests;
