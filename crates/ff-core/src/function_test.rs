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
