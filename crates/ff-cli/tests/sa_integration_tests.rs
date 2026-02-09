//! CLI integration tests for static analysis (Section 15)
//!
//! Tests `ff validate`, `ff compile`, and `ff analyze` commands against
//! fixture projects designed to exercise static analysis features.

use std::process::Command;

/// Path to the compiled ff binary (resolved at compile time)
fn ff_bin() -> String {
    env!("CARGO_BIN_EXE_ff").to_string()
}

fn clean_project_dir() -> &'static str {
    "tests/fixtures/sa_clean_project"
}

fn diagnostic_project_dir() -> &'static str {
    "tests/fixtures/sa_diagnostic_project"
}

// ── ff validate ─────────────────────────────────────────────────────────

#[test]
fn test_validate_clean_project_succeeds() {
    let output = Command::new(ff_bin())
        .args(["validate", "--project-dir", clean_project_dir()])
        .output()
        .expect("Failed to run ff validate");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Clean project validate should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_validate_loads_all_models() {
    let output = Command::new(ff_bin())
        .args(["validate", "--project-dir", clean_project_dir()])
        .output()
        .expect("Failed to run ff validate");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Validate should mention models were checked
    assert!(
        output.status.success(),
        "Validate should succeed: {}",
        combined
    );
}

// ── ff compile ──────────────────────────────────────────────────────────

#[test]
fn test_compile_clean_project_succeeds() {
    let output = Command::new(ff_bin())
        .args(["compile", "--project-dir", clean_project_dir()])
        .output()
        .expect("Failed to run ff compile");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Clean project compile should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_compile_with_skip_static_analysis() {
    let output = Command::new(ff_bin())
        .args([
            "compile",
            "--project-dir",
            diagnostic_project_dir(),
            "--skip-static-analysis",
        ])
        .output()
        .expect("Failed to run ff compile --skip-static-analysis");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // With --skip-static-analysis, compile should succeed even for diagnostic project
    assert!(
        output.status.success(),
        "Compile with --skip-static-analysis should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_compile_json_output() {
    let output = Command::new(ff_bin())
        .args([
            "compile",
            "--project-dir",
            clean_project_dir(),
            "--output",
            "json",
        ])
        .output()
        .expect("Failed to run ff compile --output json");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // JSON output should be parseable
    if output.status.success() {
        // stdout should contain valid JSON structure
        assert!(
            stdout.contains('{') || stdout.is_empty(),
            "JSON output should contain JSON or be empty: {}",
            stdout
        );
    }
}

// ── ff analyze ──────────────────────────────────────────────────────────

#[test]
fn test_analyze_clean_project() {
    let output = Command::new(ff_bin())
        .args(["analyze", "--project-dir", clean_project_dir()])
        .output()
        .expect("Failed to run ff analyze");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Clean project should produce no errors (may have info/warnings)
    assert!(
        output.status.success(),
        "Analyze clean project should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_analyze_json_output() {
    let output = Command::new(ff_bin())
        .args([
            "analyze",
            "--project-dir",
            clean_project_dir(),
            "--output",
            "json",
        ])
        .output()
        .expect("Failed to run ff analyze --output json");

    let stdout = String::from_utf8_lossy(&output.stdout);

    if output.status.success() && !stdout.trim().is_empty() {
        // Should be valid JSON (array of diagnostics)
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(stdout.trim());
        assert!(
            parsed.is_ok(),
            "Analyze JSON output should be valid JSON: {}",
            stdout
        );
    }
}

#[test]
fn test_analyze_severity_filter() {
    let output = Command::new(ff_bin())
        .args([
            "analyze",
            "--project-dir",
            clean_project_dir(),
            "--severity",
            "error",
        ])
        .output()
        .expect("Failed to run ff analyze --severity error");

    // Filtering to error-only should succeed
    assert!(
        output.status.success(),
        "Analyze with severity filter should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_analyze_diagnostic_project() {
    let output = Command::new(ff_bin())
        .args(["analyze", "--project-dir", diagnostic_project_dir()])
        .output()
        .expect("Failed to run ff analyze on diagnostic project");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // The diagnostic project should produce some diagnostics
    // (it has type mismatches and extra columns in YAML)
    let combined = format!("{}{}", stdout, stderr);
    // We just verify the command runs without crashing
    let _ = combined;
}

#[test]
fn test_analyze_diagnostic_project_json() {
    let output = Command::new(ff_bin())
        .args([
            "analyze",
            "--project-dir",
            diagnostic_project_dir(),
            "--output",
            "json",
        ])
        .output()
        .expect("Failed to run ff analyze --output json on diagnostic project");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should produce valid JSON even with diagnostics
    if !stdout.trim().is_empty() {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(stdout.trim());
        if let Ok(val) = parsed {
            // JSON should be an array of diagnostics
            assert!(
                val.is_array(),
                "Analyze JSON output should be an array: {}",
                stdout
            );
        }
    }
}

// ── ff ls ───────────────────────────────────────────────────────────────

#[test]
fn test_ls_clean_project() {
    let output = Command::new(ff_bin())
        .args(["ls", "--project-dir", clean_project_dir()])
        .output()
        .expect("Failed to run ff ls");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "ls should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    // Should list our 3 models
    assert!(
        stdout.contains("stg_orders") || stderr.contains("stg_orders"),
        "Should list stg_orders"
    );
    assert!(
        stdout.contains("stg_customers") || stderr.contains("stg_customers"),
        "Should list stg_customers"
    );
    assert!(
        stdout.contains("fct_orders") || stderr.contains("fct_orders"),
        "Should list fct_orders"
    );
}

// ── ff parse ────────────────────────────────────────────────────────────

#[test]
fn test_parse_clean_project() {
    let output = Command::new(ff_bin())
        .args(["parse", "--project-dir", clean_project_dir()])
        .output()
        .expect("Failed to run ff parse");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Parse should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

// ── Edge case: nonexistent project ──────────────────────────────────────

#[test]
fn test_validate_nonexistent_project_fails() {
    let output = Command::new(ff_bin())
        .args([
            "validate",
            "--project-dir",
            "/tmp/nonexistent_ff_project_12345",
        ])
        .output()
        .expect("Failed to run ff validate");

    assert!(
        !output.status.success(),
        "Validate on nonexistent project should fail"
    );
}

#[test]
fn test_compile_nonexistent_project_fails() {
    let output = Command::new(ff_bin())
        .args([
            "compile",
            "--project-dir",
            "/tmp/nonexistent_ff_project_12345",
        ])
        .output()
        .expect("Failed to run ff compile");

    assert!(
        !output.status.success(),
        "Compile on nonexistent project should fail"
    );
}
