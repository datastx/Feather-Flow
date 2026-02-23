//! User-defined function definitions and discovery
//!
//! Functions are `.yml` + `.sql` pairs discovered from `function_paths` directories.
//! They deploy to DuckDB as `CREATE OR REPLACE MACRO` and register as stub UDFs
//! in the DataFusion static analysis engine.

use crate::error::{CoreError, CoreResult};
use crate::function_name::FunctionName;
use crate::serde_helpers::default_true;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Discriminator for function YAML files
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FunctionKind {
    /// Modern singular form
    Function,
    /// Legacy plural form (backward compatible)
    Functions,
}

/// Whether the function is scalar or table-returning
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FunctionType {
    /// Returns a single value
    Scalar,
    /// Returns a table (set of rows)
    Table,
}

impl std::fmt::Display for FunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionType::Scalar => write!(f, "scalar"),
            FunctionType::Table => write!(f, "table"),
        }
    }
}

/// A single function argument
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionArg {
    /// Argument name
    pub name: String,

    /// SQL data type (e.g. "INTEGER", "VARCHAR")
    pub data_type: String,

    /// Default value expression (optional)
    #[serde(default)]
    pub default: Option<String>,

    /// Description of the argument
    #[serde(default)]
    pub description: Option<String>,
}

/// Return type specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FunctionReturn {
    /// Scalar return: single data type
    Scalar {
        /// SQL data type of the return value
        data_type: String,
    },
    /// Table return: set of named columns
    Table {
        /// Output columns for table-returning functions
        columns: Vec<FunctionReturnColumn>,
    },
}

/// A column in a table-returning function's output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionReturnColumn {
    /// Column name
    pub name: String,

    /// SQL data type
    pub data_type: String,
}

/// Configuration options for a function
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FunctionConfig {
    /// Schema to deploy into (overrides project default)
    #[serde(default)]
    pub schema: Option<String>,

    /// Whether the function is deterministic (default: true)
    #[serde(default = "default_true")]
    pub deterministic: bool,
}

/// YAML schema for a function definition file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FunctionSchema {
    /// Must be "functions"
    pub kind: FunctionKind,

    /// Schema format version
    #[serde(default = "default_version")]
    pub version: u32,

    /// Function name (must be a valid SQL identifier)
    pub name: String,

    /// Description of the function
    #[serde(default)]
    pub description: Option<String>,

    /// Whether this is a scalar or table function
    pub function_type: FunctionType,

    /// Function arguments
    #[serde(default)]
    pub args: Vec<FunctionArg>,

    /// Return type specification
    pub returns: FunctionReturn,

    /// Function configuration
    #[serde(default)]
    pub config: FunctionConfig,
}

fn default_version() -> u32 {
    1
}

/// Runtime representation of a user-defined function
#[derive(Debug, Clone)]
pub struct FunctionDef {
    /// Function name
    pub name: FunctionName,

    /// Whether this is scalar or table
    pub function_type: FunctionType,

    /// Function arguments
    pub args: Vec<FunctionArg>,

    /// Return type specification
    pub returns: FunctionReturn,

    /// Description
    pub description: Option<String>,

    /// SQL body (contents of the .sql file)
    pub sql_body: String,

    /// Path to the SQL file
    pub sql_path: PathBuf,

    /// Path to the YAML file
    pub yaml_path: PathBuf,

    /// Function configuration
    pub config: FunctionConfig,
}

/// Placeholder return type for table-returning functions (no single scalar type)
pub const TABLE_RETURN_TYPE: &str = "RECORD";

/// Signature information for static analysis registration
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// Function name
    pub name: FunctionName,

    /// Argument SQL type strings
    pub arg_types: Vec<String>,

    /// Return SQL type string (scalar functions) or [`TABLE_RETURN_TYPE`] (table functions)
    pub return_type: String,

    /// Whether this is a table function
    pub is_table: bool,

    /// Output columns for table functions
    pub return_columns: Vec<(String, String)>,
}

impl FunctionDef {
    /// Load a function definition from a YAML file path.
    ///
    /// Expects a matching `.sql` file in the same directory.
    pub fn load(yaml_path: &Path) -> CoreResult<Self> {
        let content = std::fs::read_to_string(yaml_path).map_err(|e| CoreError::IoWithPath {
            path: yaml_path.display().to_string(),
            source: e,
        })?;
        Self::load_from_str(&content, yaml_path)
    }

    /// Load a function definition from already-read YAML content.
    ///
    /// Expects a matching `.sql` file in the same directory as `yaml_path`.
    pub fn load_from_str(content: &str, yaml_path: &Path) -> CoreResult<Self> {
        let schema: FunctionSchema =
            serde_yaml::from_str(content).map_err(|e| CoreError::FunctionParseError {
                path: yaml_path.display().to_string(),
                details: e.to_string(),
            })?;

        validate_function_schema(&schema, yaml_path)?;

        let sql_path = yaml_path.with_extension("sql");
        let sql_body = load_function_sql_body(&schema.name, &sql_path, yaml_path)?;

        let func_name =
            FunctionName::try_new(&schema.name).ok_or_else(|| CoreError::EmptyName {
                context: "function name".into(),
            })?;

        Ok(FunctionDef {
            name: func_name,
            function_type: schema.function_type,
            args: schema.args,
            returns: schema.returns,
            description: schema.description,
            sql_body,
            sql_path,
            yaml_path: yaml_path.to_path_buf(),
            config: schema.config,
        })
    }

    /// Get the function signature for static analysis registration.
    pub fn signature(&self) -> FunctionSignature {
        let arg_types: Vec<String> = self.args.iter().map(|a| a.data_type.clone()).collect();

        let (return_type, return_columns) = match &self.returns {
            FunctionReturn::Scalar { data_type } => (data_type.clone(), vec![]),
            FunctionReturn::Table { columns } => {
                let cols: Vec<(String, String)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                (TABLE_RETURN_TYPE.to_string(), cols)
            }
        };

        FunctionSignature {
            name: self.name.clone(),
            arg_types,
            return_type,
            is_table: self.function_type == FunctionType::Table,
            return_columns,
        }
    }

    /// Build the optionally schema-qualified function name.
    pub fn qualified_name(&self) -> String {
        if let Some(ref schema) = self.config.schema {
            format!("{}.{}", schema, self.name)
        } else {
            self.name.to_string()
        }
    }

    /// Generate `CREATE OR REPLACE MACRO` SQL from the function definition.
    ///
    /// The `rendered_body` parameter is the SQL body after Jinja rendering.
    pub fn to_create_sql(&self, rendered_body: &str) -> String {
        let name = self.qualified_name();

        let args_sql = self
            .args
            .iter()
            .map(|arg| {
                if let Some(ref default) = arg.default {
                    format!("{} := {}", arg.name, default)
                } else {
                    arg.name.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        match self.function_type {
            FunctionType::Scalar => {
                format!(
                    "CREATE OR REPLACE MACRO {}({}) AS ({})",
                    name,
                    args_sql,
                    rendered_body.trim()
                )
            }
            FunctionType::Table => {
                format!(
                    "CREATE OR REPLACE MACRO {}({}) AS TABLE ({})",
                    name,
                    args_sql,
                    rendered_body.trim()
                )
            }
        }
    }

    /// Generate `DROP MACRO` SQL for this function.
    pub fn to_drop_sql(&self) -> String {
        let name = self.qualified_name();

        match self.function_type {
            FunctionType::Scalar => format!("DROP MACRO IF EXISTS {}", name),
            FunctionType::Table => format!("DROP MACRO TABLE IF EXISTS {}", name),
        }
    }
}

/// Validate the parsed function schema: name, arguments, and return columns.
fn validate_function_schema(schema: &FunctionSchema, yaml_path: &Path) -> CoreResult<()> {
    if !is_valid_sql_identifier(&schema.name) {
        return Err(CoreError::FunctionInvalidName {
            name: schema.name.clone(),
            path: yaml_path.display().to_string(),
        });
    }

    validate_function_args(&schema.name, &schema.args)?;

    // Validate table function has columns (FN006)
    if schema.function_type == FunctionType::Table {
        if let FunctionReturn::Table { ref columns } = schema.returns {
            if columns.is_empty() {
                return Err(CoreError::FunctionTableMissingColumns {
                    name: schema.name.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Validate function arguments: no duplicate names, defaults must trail.
fn validate_function_args(func_name: &str, args: &[FunctionArg]) -> CoreResult<()> {
    let mut seen_args = std::collections::HashSet::new();
    for arg in args {
        if !seen_args.insert(&arg.name) {
            return Err(CoreError::FunctionArgError {
                name: func_name.to_string(),
                details: format!("duplicate argument name '{}'", arg.name),
            });
        }
    }

    let mut seen_default = false;
    for arg in args {
        if arg.default.is_some() {
            seen_default = true;
        } else if seen_default {
            return Err(CoreError::FunctionArgOrderError {
                name: func_name.to_string(),
                arg: arg.name.clone(),
            });
        }
    }

    Ok(())
}

/// Load and validate the companion SQL file for a function.
fn load_function_sql_body(
    func_name: &str,
    sql_path: &Path,
    yaml_path: &Path,
) -> CoreResult<String> {
    if !sql_path.exists() {
        return Err(CoreError::FunctionMissingSqlFile {
            name: func_name.to_string(),
            yaml_path: yaml_path.display().to_string(),
        });
    }

    let sql_body = std::fs::read_to_string(sql_path).map_err(|e| CoreError::IoWithPath {
        path: sql_path.display().to_string(),
        source: e,
    })?;

    if sql_body.trim().is_empty() {
        return Err(CoreError::FunctionEmptySqlFile {
            name: func_name.to_string(),
            sql_path: sql_path.display().to_string(),
        });
    }

    Ok(sql_body)
}

/// Check if a string is a valid SQL identifier
fn is_valid_sql_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

/// Discover and load all function definitions from configured directories.
///
/// `function_paths` are expected to be absolute paths.
pub fn discover_functions(function_paths: &[PathBuf]) -> CoreResult<Vec<FunctionDef>> {
    let mut functions = Vec::new();

    for path in function_paths {
        if !path.exists() {
            continue;
        }

        discover_functions_recursive(path, &mut functions)?;
    }

    // Validate no duplicate names (FN003)
    let mut seen_names: HashMap<String, &Path> = HashMap::new();
    for func in &functions {
        let name = func.name.to_string();
        if let Some(existing_path) = seen_names.get(&name) {
            return Err(CoreError::FunctionDuplicateName {
                name,
                path1: existing_path.display().to_string(),
                path2: func.yaml_path.display().to_string(),
            });
        }
        seen_names.insert(name, &func.yaml_path);
    }

    Ok(functions)
}

/// Minimal YAML probe that deserializes only the `kind` field.
///
/// Used to cheaply determine whether a YAML file is a function definition
/// before attempting a full parse with [`FunctionSchema`].
#[derive(Deserialize)]
struct YamlKindProbe {
    #[serde(default)]
    kind: Option<FunctionKind>,
}

/// Recursively discover function files in a directory
fn discover_functions_recursive(dir: &Path, functions: &mut Vec<FunctionDef>) -> CoreResult<()> {
    crate::project::loading::discover_yaml_recursive(
        dir,
        functions,
        |content| {
            let probe: YamlKindProbe = match serde_yaml::from_str(content) {
                Ok(p) => p,
                Err(_) => return false,
            };
            matches!(
                probe.kind,
                Some(FunctionKind::Functions | FunctionKind::Function)
            )
        },
        FunctionDef::load,
    )
}

/// Build a lookup map from function name to function definition.
pub fn build_function_lookup(functions: &[FunctionDef]) -> HashMap<&str, &FunctionDef> {
    functions.iter().map(|f| (f.name.as_str(), f)).collect()
}

#[cfg(test)]
#[path = "function_test.rs"]
mod tests;
