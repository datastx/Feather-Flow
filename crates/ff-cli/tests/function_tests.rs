//! Integration tests for user-defined functions (UDFs)
//!
//! Tests function discovery, listing, validation, deployment, and
//! static analysis integration.

use ff_core::Project;
use std::path::Path;
use std::process::Command;

/// Path to the compiled ff binary
fn ff_bin() -> String {
    env!("CARGO_BIN_EXE_ff").to_string()
}

/// Run an `ff` CLI command and return (stdout, stderr, success).
///
/// Reduces boilerplate for CLI integration tests.
fn run_ff(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(ff_bin())
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("Failed to execute ff with args {:?}: {}", args, e));
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    )
}

// ── Function Discovery Tests ────────────────────────────────────────────

#[test]
fn test_function_discovery_sample_project() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    assert_eq!(
        project.functions.len(),
        3,
        "Sample project should have 3 functions"
    );

    let names: Vec<&str> = project.function_names();
    assert!(
        names.contains(&"cents_to_dollars"),
        "Should find cents_to_dollars function"
    );
    assert!(
        names.contains(&"safe_divide"),
        "Should find safe_divide function"
    );
    assert!(
        names.contains(&"order_volume_by_status"),
        "Should find order_volume_by_status table function"
    );
}

#[test]
fn test_function_lookup_by_name() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let func = project.get_function("cents_to_dollars");
    assert!(func.is_some(), "Should find cents_to_dollars by name");

    let func = func.unwrap();
    assert_eq!(func.args.len(), 1);
    assert_eq!(func.args[0].name, "amount");
    assert_eq!(func.args[0].data_type, "BIGINT");
}

#[test]
fn test_function_sql_body_loaded() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let func = project.get_function("safe_divide").unwrap();
    assert!(
        !func.sql_body.trim().is_empty(),
        "SQL body should be loaded"
    );
    assert!(
        func.sql_body.contains("denominator"),
        "SQL body should contain parameter references"
    );
}

#[test]
fn test_function_signature() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let func = project.get_function("safe_divide").unwrap();
    let sig = func.signature();

    assert_eq!(sig.name, "safe_divide");
    assert_eq!(sig.arg_types.len(), 2);
    assert_eq!(sig.arg_types[0], "DOUBLE");
    assert_eq!(sig.arg_types[1], "DOUBLE");
    assert_eq!(sig.return_type, "DOUBLE");
    assert!(!sig.is_table);
}

#[test]
fn test_function_to_create_sql_scalar() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let func = project.get_function("cents_to_dollars").unwrap();
    let sql = func.to_create_sql(&func.sql_body);

    assert!(
        sql.contains("CREATE OR REPLACE MACRO"),
        "Should generate CREATE MACRO statement, got: {}",
        sql
    );
    assert!(
        sql.contains("cents_to_dollars"),
        "Should reference function name"
    );
    assert!(sql.contains("amount"), "Should include parameter names");
}

#[test]
fn test_function_to_drop_sql() {
    let project = Project::load(Path::new("tests/fixtures/sample_project")).unwrap();

    let func = project.get_function("cents_to_dollars").unwrap();
    let sql = func.to_drop_sql();

    assert!(
        sql.contains("DROP MACRO IF EXISTS"),
        "Should generate DROP MACRO statement, got: {}",
        sql
    );
    assert!(
        sql.contains("cents_to_dollars"),
        "Should reference function name"
    );
}

// ── Duplicate Name Detection ────────────────────────────────────────────

#[test]
fn test_duplicate_function_name_errors() {
    let result = Project::load(Path::new("tests/fixtures/sa_fn_fail_duplicate_name"));

    assert!(
        result.is_err(),
        "Loading project with duplicate function names should fail"
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("my_func") || err.contains("FN003") || err.contains("Duplicate"),
        "Error should mention the duplicate function name, got: {}",
        err
    );
}

// ── CLI: ff function list ───────────────────────────────────────────────

#[test]
fn test_function_list_command() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "deploy",
            "functions",
            "list",
            "--project-dir",
            "tests/fixtures/sample_project",
        ])
        .output()
        .expect("Failed to run ff dt deploy functions list");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "ff deploy functions list should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    assert!(
        stdout.contains("cents_to_dollars"),
        "Should list cents_to_dollars in output"
    );
    assert!(
        stdout.contains("safe_divide"),
        "Should list safe_divide in output"
    );
    assert!(
        stdout.contains("3 functions"),
        "Should show 3 functions count"
    );
}

#[test]
fn test_function_list_json_output() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "deploy",
            "functions",
            "list",
            "--output",
            "json",
            "--project-dir",
            "tests/fixtures/sample_project",
        ])
        .output()
        .expect("Failed to run ff dt deploy functions list --output json");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "JSON output should succeed");

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("Should produce valid JSON");
    assert!(parsed.is_array(), "JSON output should be an array");

    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 3, "Should have 3 functions in JSON output");
}

// ── CLI: ff function show ───────────────────────────────────────────────

#[test]
fn test_function_show_command() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "deploy",
            "functions",
            "show",
            "safe_divide",
            "--project-dir",
            "tests/fixtures/sample_project",
        ])
        .output()
        .expect("Failed to run ff dt deploy functions show");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "ff function show should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    assert!(stdout.contains("safe_divide"), "Should show function name");
    assert!(stdout.contains("DOUBLE"), "Should show argument types");
}

#[test]
fn test_function_show_sql_flag() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "deploy",
            "functions",
            "show",
            "cents_to_dollars",
            "--sql",
            "--project-dir",
            "tests/fixtures/sample_project",
        ])
        .output()
        .expect("Failed to run ff dt deploy functions show --sql");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Show --sql should succeed");
    assert!(
        stdout.contains("CREATE OR REPLACE MACRO"),
        "Should show CREATE MACRO SQL"
    );
}

// ── CLI: ff function validate ───────────────────────────────────────────

#[test]
fn test_function_validate_clean_project() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "deploy",
            "functions",
            "validate",
            "--project-dir",
            "tests/fixtures/sample_project",
        ])
        .output()
        .expect("Failed to run ff dt deploy functions validate");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Clean project function validate should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    assert!(
        stdout.contains("validated successfully"),
        "Should report success"
    );
}

// ── Static Analysis with UDF Stubs ──────────────────────────────────────

#[test]
fn test_compile_with_function_stubs() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            "tests/fixtures/sa_fn_pass_scalar_basic",
        ])
        .output()
        .expect("Failed to run ff dt compile with function stubs");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Compile should succeed when UDF stubs are registered.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

// ── ff ls with --resource-type function ─────────────────────────────────

#[test]
fn test_ls_resource_type_function() {
    let (stdout, stderr, success) = run_ff(&[
        "dt",
        "ls",
        "--resource-type",
        "function",
        "--project-dir",
        "tests/fixtures/sample_project",
    ]);

    assert!(
        success,
        "ff ls --resource-type function should succeed.\nstdout: {}\nstderr: {}",
        stdout, stderr
    );

    assert!(
        stdout.contains("cents_to_dollars"),
        "Should list cents_to_dollars"
    );
    assert!(stdout.contains("safe_divide"), "Should list safe_divide");
}

// ── Edge case: non-existent function show ─────────────────────────────

#[test]
fn test_function_show_nonexistent() {
    let (stdout, stderr, success) = run_ff(&[
        "dt",
        "deploy",
        "functions",
        "show",
        "no_such_function",
        "--project-dir",
        "tests/fixtures/sample_project",
    ]);

    assert!(
        !success,
        "Showing a non-existent function should fail.\nstdout: {}\nstderr: {}",
        stdout, stderr
    );

    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("not found"),
        "Error should say 'not found', got: {}",
        combined
    );
}

// ── Edge case: zero-arg function ──────────────────────────────────────

#[test]
fn test_zero_arg_function() {
    use tempfile::TempDir;

    let temp = TempDir::new().unwrap();
    let func_dir = temp.path().join("functions");
    std::fs::create_dir_all(&func_dir).unwrap();

    // Create a zero-arg scalar function
    std::fs::write(
        func_dir.join("constant_pi.yml"),
        r#"kind: functions
version: 1
name: constant_pi
description: "Returns pi"
function_type: scalar
args: []
returns:
  data_type: DOUBLE
"#,
    )
    .unwrap();
    std::fs::write(func_dir.join("constant_pi.sql"), "3.14159265358979").unwrap();

    // Create minimal project config
    let models_dir = temp.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    std::fs::write(
        temp.path().join("featherflow.yml"),
        r#"name: zero_arg_test
version: "1.0.0"
model_paths: ["models"]
function_paths: ["functions"]
target_path: "target"
materialization: view
dialect: duckdb
database:
  type: duckdb
  path: "target/dev.duckdb"
"#,
    )
    .unwrap();

    let project = Project::load(temp.path()).unwrap();
    assert_eq!(project.functions.len(), 1);

    let func = project.get_function("constant_pi").unwrap();
    assert!(func.args.is_empty(), "Function should have zero args");

    let sql = func.to_create_sql("3.14159265358979");
    assert_eq!(
        sql,
        "CREATE OR REPLACE MACRO constant_pi() AS (3.14159265358979)"
    );
}

// ── Edge case: empty SQL body (FN002) ─────────────────────────────────

#[test]
fn test_empty_sql_body_error() {
    use ff_core::function::FunctionDef;
    use tempfile::TempDir;

    let temp = TempDir::new().unwrap();
    std::fs::write(
        temp.path().join("empty_body.yml"),
        r#"kind: functions
version: 1
name: empty_body
function_type: scalar
args: []
returns:
  data_type: INTEGER
"#,
    )
    .unwrap();
    std::fs::write(temp.path().join("empty_body.sql"), "   \n  ").unwrap();

    let result = FunctionDef::load(&temp.path().join("empty_body.yml"));
    assert!(result.is_err(), "Empty SQL body should fail");

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("FN002"),
        "Should report FN002 for empty SQL, got: {}",
        err
    );
}
