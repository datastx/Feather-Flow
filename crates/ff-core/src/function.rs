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
    /// User-defined function definition
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
pub struct FunctionSchema {
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

        let schema: FunctionSchema =
            serde_yaml::from_str(&content).map_err(|e| CoreError::FunctionParseError {
                path: yaml_path.display().to_string(),
                details: e.to_string(),
            })?;

        // Validate function name is a valid SQL identifier
        if !is_valid_sql_identifier(&schema.name) {
            return Err(CoreError::FunctionInvalidName {
                name: schema.name.clone(),
                path: yaml_path.display().to_string(),
            });
        }

        // Validate arg names are unique
        let mut seen_args = std::collections::HashSet::new();
        for arg in &schema.args {
            if !seen_args.insert(&arg.name) {
                return Err(CoreError::FunctionArgError {
                    name: schema.name.clone(),
                    details: format!("duplicate argument name '{}'", arg.name),
                });
            }
        }

        // Validate default arg ordering: non-default args cannot follow default args (FN005)
        let mut seen_default = false;
        for arg in &schema.args {
            if arg.default.is_some() {
                seen_default = true;
            } else if seen_default {
                return Err(CoreError::FunctionArgOrderError {
                    name: schema.name.clone(),
                    arg: arg.name.clone(),
                });
            }
        }

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

        // Find matching SQL file
        let sql_path = yaml_path.with_extension("sql");
        if !sql_path.exists() {
            return Err(CoreError::FunctionMissingSqlFile {
                name: schema.name.clone(),
                yaml_path: yaml_path.display().to_string(),
            });
        }

        let sql_body = std::fs::read_to_string(&sql_path).map_err(|e| CoreError::IoWithPath {
            path: sql_path.display().to_string(),
            source: e,
        })?;

        if sql_body.trim().is_empty() {
            return Err(CoreError::FunctionEmptySqlFile {
                name: schema.name.clone(),
                sql_path: sql_path.display().to_string(),
            });
        }

        Ok(FunctionDef {
            name: FunctionName::new(&schema.name),
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
    let mut seen_names: HashMap<String, PathBuf> = HashMap::new();
    for func in &functions {
        let name = func.name.to_string();
        if let Some(existing_path) = seen_names.get(&name) {
            return Err(CoreError::FunctionDuplicateName {
                name,
                path1: existing_path.display().to_string(),
                path2: func.yaml_path.display().to_string(),
            });
        }
        seen_names.insert(name, func.yaml_path.clone());
    }

    Ok(functions)
}

/// Minimal YAML probe to check the `kind` field without full deserialization
#[derive(Deserialize)]
struct YamlKindProbe {
    #[serde(default)]
    kind: Option<FunctionKind>,
}

/// Recursively discover function files in a directory
fn discover_functions_recursive(dir: &Path, functions: &mut Vec<FunctionDef>) -> CoreResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            discover_functions_recursive(&path, functions)?;
        } else if path.extension().is_some_and(|e| e == "yml" || e == "yaml") {
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("Cannot read {}: {}", path.display(), e);
                    continue;
                }
            };

            // Deserialize just the `kind` field to decide whether this is a function YAML
            let probe: YamlKindProbe = match serde_yaml::from_str(&content) {
                Ok(p) => p,
                Err(_) => continue, // Not valid YAML or unrelated file
            };

            if !matches!(probe.kind, Some(FunctionKind::Functions)) {
                continue;
            }

            // Kind probe confirmed this is a function file — parse errors are real
            let func = FunctionDef::load(&path)?;
            functions.push(func);
        }
    }

    Ok(())
}

/// Build a lookup map from function name to function definition.
pub fn build_function_lookup(functions: &[FunctionDef]) -> HashMap<&str, &FunctionDef> {
    functions.iter().map(|f| (f.name.as_str(), f)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_scalar_function(dir: &Path, name: &str) {
        let yml = format!(
            r#"kind: functions
version: 1
name: {name}
description: "Test scalar function"
function_type: scalar
args:
  - name: x
    data_type: DOUBLE
  - name: y
    data_type: DOUBLE
returns:
  data_type: DOUBLE
"#
        );
        let sql = "CASE WHEN y = 0 THEN NULL ELSE x / y END";

        std::fs::write(dir.join(format!("{}.yml", name)), yml).unwrap();
        std::fs::write(dir.join(format!("{}.sql", name)), sql).unwrap();
    }

    fn create_table_function(dir: &Path, name: &str) {
        let yml = format!(
            r#"kind: functions
version: 1
name: {name}
description: "Test table function"
function_type: table
args:
  - name: threshold
    data_type: INTEGER
returns:
  columns:
    - name: id
      data_type: INTEGER
    - name: value
      data_type: DOUBLE
"#
        );
        let sql = "SELECT id, value FROM source WHERE value > threshold";

        std::fs::write(dir.join(format!("{}.yml", name)), yml).unwrap();
        std::fs::write(dir.join(format!("{}.sql", name)), sql).unwrap();
    }

    #[test]
    fn test_load_scalar_function() {
        let temp = TempDir::new().unwrap();
        create_scalar_function(temp.path(), "safe_divide");

        let func = FunctionDef::load(&temp.path().join("safe_divide.yml")).unwrap();
        assert_eq!(func.name, "safe_divide");
        assert_eq!(func.function_type, FunctionType::Scalar);
        assert_eq!(func.args.len(), 2);
        assert_eq!(func.args[0].name, "x");
        assert_eq!(func.args[1].name, "y");
        assert!(func.sql_body.contains("CASE WHEN"));
    }

    #[test]
    fn test_load_table_function() {
        let temp = TempDir::new().unwrap();
        create_table_function(temp.path(), "filter_rows");

        let func = FunctionDef::load(&temp.path().join("filter_rows.yml")).unwrap();
        assert_eq!(func.name, "filter_rows");
        assert_eq!(func.function_type, FunctionType::Table);
        if let FunctionReturn::Table { ref columns } = func.returns {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name, "id");
            assert_eq!(columns[1].name, "value");
        } else {
            panic!("Expected Table return type");
        }
    }

    #[test]
    fn test_missing_sql_file() {
        let temp = TempDir::new().unwrap();
        let yml = r#"kind: functions
version: 1
name: no_sql
function_type: scalar
args: []
returns:
  data_type: INTEGER
"#;
        std::fs::write(temp.path().join("no_sql.yml"), yml).unwrap();
        // No .sql file created

        let result = FunctionDef::load(&temp.path().join("no_sql.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FN001"), "Expected FN001, got: {}", err);
    }

    #[test]
    fn test_invalid_function_name() {
        let temp = TempDir::new().unwrap();
        let yml = r#"kind: functions
version: 1
name: "123invalid"
function_type: scalar
args: []
returns:
  data_type: INTEGER
"#;
        std::fs::write(temp.path().join("123invalid.yml"), yml).unwrap();
        std::fs::write(temp.path().join("123invalid.sql"), "1").unwrap();

        let result = FunctionDef::load(&temp.path().join("123invalid.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FN007"), "Expected FN007, got: {}", err);
    }

    #[test]
    fn test_default_arg_order_error() {
        let temp = TempDir::new().unwrap();
        let yml = r#"kind: functions
version: 1
name: bad_args
function_type: scalar
args:
  - name: x
    data_type: INTEGER
    default: "0"
  - name: y
    data_type: INTEGER
returns:
  data_type: INTEGER
"#;
        std::fs::write(temp.path().join("bad_args.yml"), yml).unwrap();
        std::fs::write(temp.path().join("bad_args.sql"), "x + y").unwrap();

        let result = FunctionDef::load(&temp.path().join("bad_args.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FN005"), "Expected FN005, got: {}", err);
    }

    #[test]
    fn test_table_function_missing_columns() {
        let temp = TempDir::new().unwrap();
        let yml = r#"kind: functions
version: 1
name: empty_table
function_type: table
args: []
returns:
  columns: []
"#;
        std::fs::write(temp.path().join("empty_table.yml"), yml).unwrap();
        std::fs::write(temp.path().join("empty_table.sql"), "SELECT 1").unwrap();

        let result = FunctionDef::load(&temp.path().join("empty_table.yml"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FN006"), "Expected FN006, got: {}", err);
    }

    #[test]
    fn test_discover_functions() {
        let temp = TempDir::new().unwrap();
        let funcs_dir = temp.path().join("functions");
        std::fs::create_dir(&funcs_dir).unwrap();

        create_scalar_function(&funcs_dir, "safe_divide");
        create_scalar_function(&funcs_dir, "cents_to_dollars");

        let functions = discover_functions(&[funcs_dir]).unwrap();
        assert_eq!(functions.len(), 2);

        let names: Vec<&str> = functions.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"safe_divide"));
        assert!(names.contains(&"cents_to_dollars"));
    }

    #[test]
    fn test_discover_functions_duplicate_name() {
        let temp = TempDir::new().unwrap();
        let dir_a = temp.path().join("a");
        let dir_b = temp.path().join("b");
        std::fs::create_dir(&dir_a).unwrap();
        std::fs::create_dir(&dir_b).unwrap();

        create_scalar_function(&dir_a, "same_name");
        create_scalar_function(&dir_b, "same_name");

        let result = discover_functions(&[dir_a, dir_b]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FN003"), "Expected FN003, got: {}", err);
    }

    #[test]
    fn test_discover_functions_empty_dir() {
        let temp = TempDir::new().unwrap();
        let funcs_dir = temp.path().join("functions");
        std::fs::create_dir(&funcs_dir).unwrap();

        let functions = discover_functions(&[funcs_dir]).unwrap();
        assert!(functions.is_empty());
    }

    #[test]
    fn test_discover_functions_missing_dir() {
        let temp = TempDir::new().unwrap();
        let nonexistent = temp.path().join("nonexistent");

        let functions = discover_functions(&[nonexistent]).unwrap();
        assert!(functions.is_empty());
    }

    #[test]
    fn test_scalar_create_sql() {
        let temp = TempDir::new().unwrap();
        create_scalar_function(temp.path(), "safe_divide");
        let func = FunctionDef::load(&temp.path().join("safe_divide.yml")).unwrap();

        let sql = func.to_create_sql("CASE WHEN y = 0 THEN NULL ELSE x / y END");
        assert_eq!(
            sql,
            "CREATE OR REPLACE MACRO safe_divide(x, y) AS (CASE WHEN y = 0 THEN NULL ELSE x / y END)"
        );
    }

    #[test]
    fn test_table_create_sql() {
        let temp = TempDir::new().unwrap();
        create_table_function(temp.path(), "filter_rows");
        let func = FunctionDef::load(&temp.path().join("filter_rows.yml")).unwrap();

        let sql = func.to_create_sql("SELECT id, value FROM source WHERE value > threshold");
        assert_eq!(
            sql,
            "CREATE OR REPLACE MACRO filter_rows(threshold) AS TABLE (SELECT id, value FROM source WHERE value > threshold)"
        );
    }

    #[test]
    fn test_scalar_create_sql_with_defaults() {
        let temp = TempDir::new().unwrap();
        let yml = r#"kind: functions
version: 1
name: add_with_default
function_type: scalar
args:
  - name: x
    data_type: INTEGER
  - name: y
    data_type: INTEGER
    default: "1"
returns:
  data_type: INTEGER
"#;
        std::fs::write(temp.path().join("add_with_default.yml"), yml).unwrap();
        std::fs::write(temp.path().join("add_with_default.sql"), "x + y").unwrap();

        let func = FunctionDef::load(&temp.path().join("add_with_default.yml")).unwrap();
        let sql = func.to_create_sql("x + y");
        assert_eq!(
            sql,
            "CREATE OR REPLACE MACRO add_with_default(x, y := 1) AS (x + y)"
        );
    }

    #[test]
    fn test_create_sql_with_schema() {
        let temp = TempDir::new().unwrap();
        let yml = r#"kind: functions
version: 1
name: my_func
function_type: scalar
args: []
returns:
  data_type: INTEGER
config:
  schema: analytics
"#;
        std::fs::write(temp.path().join("my_func.yml"), yml).unwrap();
        std::fs::write(temp.path().join("my_func.sql"), "42").unwrap();

        let func = FunctionDef::load(&temp.path().join("my_func.yml")).unwrap();
        let sql = func.to_create_sql("42");
        assert_eq!(sql, "CREATE OR REPLACE MACRO analytics.my_func() AS (42)");
    }

    #[test]
    fn test_drop_sql_scalar() {
        let temp = TempDir::new().unwrap();
        create_scalar_function(temp.path(), "safe_divide");
        let func = FunctionDef::load(&temp.path().join("safe_divide.yml")).unwrap();

        assert_eq!(func.to_drop_sql(), "DROP MACRO IF EXISTS safe_divide");
    }

    #[test]
    fn test_drop_sql_table() {
        let temp = TempDir::new().unwrap();
        create_table_function(temp.path(), "filter_rows");
        let func = FunctionDef::load(&temp.path().join("filter_rows.yml")).unwrap();

        assert_eq!(func.to_drop_sql(), "DROP MACRO TABLE IF EXISTS filter_rows");
    }

    #[test]
    fn test_function_signature() {
        let temp = TempDir::new().unwrap();
        create_scalar_function(temp.path(), "safe_divide");
        let func = FunctionDef::load(&temp.path().join("safe_divide.yml")).unwrap();

        let sig = func.signature();
        assert_eq!(sig.name, "safe_divide");
        assert_eq!(sig.arg_types, vec!["DOUBLE", "DOUBLE"]);
        assert_eq!(sig.return_type, "DOUBLE");
        assert!(!sig.is_table);
        assert!(sig.return_columns.is_empty());
    }

    #[test]
    fn test_table_function_signature() {
        let temp = TempDir::new().unwrap();
        create_table_function(temp.path(), "filter_rows");
        let func = FunctionDef::load(&temp.path().join("filter_rows.yml")).unwrap();

        let sig = func.signature();
        assert_eq!(sig.name, "filter_rows");
        assert!(sig.is_table);
        assert_eq!(sig.return_columns.len(), 2);
        assert_eq!(
            sig.return_columns[0],
            ("id".to_string(), "INTEGER".to_string())
        );
    }

    #[test]
    fn test_build_function_lookup() {
        let temp = TempDir::new().unwrap();
        let funcs_dir = temp.path().join("functions");
        std::fs::create_dir(&funcs_dir).unwrap();

        create_scalar_function(&funcs_dir, "safe_divide");
        create_scalar_function(&funcs_dir, "cents_to_dollars");

        let functions = discover_functions(&[funcs_dir]).unwrap();
        let lookup = build_function_lookup(&functions);

        assert_eq!(lookup.len(), 2);
        assert!(lookup.contains_key("safe_divide"));
        assert!(lookup.contains_key("cents_to_dollars"));
    }

    #[test]
    fn test_valid_sql_identifiers() {
        assert!(is_valid_sql_identifier("safe_divide"));
        assert!(is_valid_sql_identifier("_private"));
        assert!(is_valid_sql_identifier("func123"));
        assert!(!is_valid_sql_identifier("123func"));
        assert!(!is_valid_sql_identifier(""));
        assert!(!is_valid_sql_identifier("has spaces"));
        assert!(!is_valid_sql_identifier("has-dashes"));
    }
}
