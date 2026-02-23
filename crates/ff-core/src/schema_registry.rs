//! Schema registry for column metadata lookup
//!
//! Provides a unified view of column names, types, and descriptions across
//! all project nodes (models, sources, seeds). Used by lineage to compute
//! description propagation status.

use crate::project::Project;
use std::collections::HashMap;

/// Metadata for a single column in a node.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// SQL data type
    pub data_type: String,
    /// Optional human-readable description
    pub description: Option<String>,
}

/// Registry of column metadata indexed by node name then column name.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    /// node_name -> { column_name_lowercase -> ColumnInfo }
    nodes: HashMap<String, HashMap<String, ColumnInfo>>,
}

impl SchemaRegistry {
    /// Build a registry from a loaded project.
    ///
    /// Collects columns from:
    /// - Model YAML schemas (`project.models`)
    /// - Source table definitions (`project.sources`)
    pub fn from_project(project: &Project) -> Self {
        let nodes = project
            .models
            .iter()
            .filter_map(|(name, model)| {
                let schema = model.schema.as_ref()?;
                let cols: HashMap<String, ColumnInfo> = schema
                    .columns
                    .iter()
                    .map(|col| {
                        (
                            col.name.to_lowercase(),
                            ColumnInfo {
                                name: col.name.clone(),
                                data_type: col.data_type.clone(),
                                description: col.description.clone(),
                            },
                        )
                    })
                    .collect();
                if cols.is_empty() {
                    None
                } else {
                    Some((name.to_string(), cols))
                }
            })
            .chain(
                project
                    .sources
                    .iter()
                    .flat_map(|sf| &sf.tables)
                    .filter_map(|table| {
                        let cols: HashMap<String, ColumnInfo> = table
                            .columns
                            .iter()
                            .map(|col| {
                                (
                                    col.name.to_lowercase(),
                                    ColumnInfo {
                                        name: col.name.clone(),
                                        data_type: col.data_type.clone(),
                                        description: col.description.clone(),
                                    },
                                )
                            })
                            .collect();
                        if cols.is_empty() {
                            None
                        } else {
                            Some((table.name.clone(), cols))
                        }
                    }),
            )
            .collect();

        Self { nodes }
    }

    /// Look up a single column in a node.
    pub fn get_column(&self, node: &str, column: &str) -> Option<&ColumnInfo> {
        self.nodes
            .get(node)
            .and_then(|cols| cols.get(&column.to_lowercase()))
    }

    /// Get all columns for a node.
    pub fn get_columns(&self, node: &str) -> Option<&HashMap<String, ColumnInfo>> {
        self.nodes.get(node)
    }
}
