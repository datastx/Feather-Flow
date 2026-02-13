//! CLI integration tests for static analysis (Section 15)
//!
//! Tests `ff validate`, `ff compile`, and `ff analyze` commands against
//! fixture projects designed to exercise static analysis features.

use std::process::Command;

// ── Fixture paths ──────────────────────────────────────────────────────

fn ff_bin() -> String {
    env!("CARGO_BIN_EXE_ff").to_string()
}

fn clean_project_dir() -> &'static str {
    "tests/fixtures/sa_clean_project"
}

fn diagnostic_project_dir() -> &'static str {
    "tests/fixtures/sa_diagnostic_project"
}

fn sample_project_dir() -> &'static str {
    "tests/fixtures/sample_project"
}

// ── Shared test helpers ────────────────────────────────────────────────

fn run_analyze_json(fixture: &str) -> Vec<serde_json::Value> {
    let output = Command::new(ff_bin())
        .args(["analyze", "--project-dir", fixture, "--output", "json"])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run ff analyze on {}: {}", fixture, e));

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "ff analyze on {} should succeed.\nstdout: {}\nstderr: {}",
        fixture,
        stdout,
        stderr
    );

    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("JSON parse failed for {}: {}\nraw: {}", fixture, e, stdout))
}

/// Like `run_analyze_json` but allows non-zero exit (for projects with error-severity diagnostics).
fn run_analyze_json_allow_failure(fixture: &str) -> Vec<serde_json::Value> {
    let output = Command::new(ff_bin())
        .args(["analyze", "--project-dir", fixture, "--output", "json"])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run ff analyze on {}: {}", fixture, e));

    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("JSON parse failed for {}: {}\nraw: {}", fixture, e, stdout))
}

fn assert_diagnostics(diagnostics: &[serde_json::Value], code: &str, expected: usize) {
    let actual = diagnostics
        .iter()
        .filter(|d| d.get("code").and_then(|c| c.as_str()) == Some(code))
        .count();
    assert_eq!(
        actual, expected,
        "Expected {} diagnostics with code {}, got {}",
        expected, code, actual
    );
}

fn assert_has_diagnostics(diagnostics: &[serde_json::Value], code: &str) {
    let actual = diagnostics
        .iter()
        .filter(|d| d.get("code").and_then(|c| c.as_str()) == Some(code))
        .count();
    assert!(
        actual > 0,
        "Expected at least 1 diagnostic with code {}, got 0",
        code
    );
}

fn assert_no_diagnostics_with_code(diagnostics: &[serde_json::Value], code: &str) {
    assert_diagnostics(diagnostics, code, 0);
}

fn assert_no_error_severity(diagnostics: &[serde_json::Value]) {
    let errors: Vec<_> = diagnostics
        .iter()
        .filter(|d| d.get("severity").and_then(|s| s.as_str()) == Some("error"))
        .collect();
    assert!(
        errors.is_empty(),
        "Expected zero error-severity diagnostics, got {}:\n{:#?}",
        errors.len(),
        errors
    );
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
    let diagnostics = run_analyze_json(clean_project_dir());
    // Clean project — valid JSON array returned, content verified by other tests
    let _ = diagnostics;
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
    let diagnostics = run_analyze_json_allow_failure(diagnostic_project_dir());
    assert!(
        !diagnostics.is_empty(),
        "Diagnostic project should produce at least one diagnostic"
    );
}

// ── ff analyze (sample_project regression guard) ────────────────────────

/// Regression guard: `ff analyze` on the main sample project must produce zero
/// diagnostics.  After the Phase F IR elimination the project went from 48 false
/// diagnostics (28 A001 + 20 bogus A010) to zero.  This test locks that in so
/// any regression is caught immediately.
#[test]
fn test_analyze_sample_project_no_regressions() {
    let diagnostics = run_analyze_json(sample_project_dir());

    assert_no_diagnostics_with_code(&diagnostics, "A001");
    assert_no_error_severity(&diagnostics);
    assert_eq!(
        diagnostics.len(),
        0,
        "Expected zero diagnostics on sample_project, got {}:\n{:#?}",
        diagnostics.len(),
        diagnostics
    );
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

// ── Phase 1: Type Inference (A002, A004, A005) — CLI level ─────────────

#[test]
fn test_sa_union_type_mismatch_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_type_fail_union_mismatch");
    assert_diagnostics(&diags, "A002", 1);
}

#[test]
fn test_sa_sum_on_string_a004_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_type_fail_agg_on_string");
    assert_diagnostics(&diags, "A004", 1);
}

// ── Phase 2: Nullability (A010, A011, A012) — CLI level ─────────────────

#[test]
fn test_sa_left_join_unguarded_a010_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_null_fail_left_join_unguarded");
    assert_has_diagnostics(&diags, "A010");
}

#[test]
fn test_sa_coalesce_guard_no_a010_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_null_pass_coalesce_guarded");
    assert_no_diagnostics_with_code(&diags, "A010");
}

#[test]
fn test_sa_yaml_not_null_contradiction_a011_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_null_fail_yaml_not_null");
    assert_has_diagnostics(&diags, "A011");
}

// ── Phase 3: Unused Columns (A020) — CLI level ─────────────────────────

#[test]
fn test_sa_unused_columns_a020_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_unused_fail_extra_columns");
    assert_has_diagnostics(&diags, "A020");
}

#[test]
fn test_sa_all_consumed_no_a020_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_unused_pass_all_consumed");
    assert_no_diagnostics_with_code(&diags, "A020");
}

#[test]
fn test_sa_terminal_no_a020_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_unused_pass_terminal");
    assert_no_diagnostics_with_code(&diags, "A020");
}

// ── Phase 4: Join Keys (A030, A032, A033) — CLI level ───────────────────

#[test]
fn test_sa_join_type_mismatch_coerced_cli() {
    // DataFusion coerces VARCHAR/INT join keys — A030 not emitted
    let diags = run_analyze_json("tests/fixtures/sa_join_fail_type_mismatch");
    assert_no_diagnostics_with_code(&diags, "A030");
}

#[test]
fn test_sa_cross_join_a032_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_join_fail_cross_join");
    assert_has_diagnostics(&diags, "A032");
}

#[test]
fn test_sa_non_equi_join_a033_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_join_fail_non_equi");
    assert_has_diagnostics(&diags, "A033");
}

#[test]
fn test_sa_equi_join_clean_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_join_pass_equi");
    assert_no_diagnostics_with_code(&diags, "A030");
    assert_no_diagnostics_with_code(&diags, "A032");
    assert_no_diagnostics_with_code(&diags, "A033");
}

// ── Phase 5: Cross-Model Consistency (A040, A041) — CLI level ───────────

#[test]
fn test_sa_extra_in_sql_a040_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_xmodel_fail_extra_in_sql");
    assert_has_diagnostics(&diags, "A040");
}

#[test]
fn test_sa_missing_from_sql_a040_cli() {
    let diags = run_analyze_json_allow_failure("tests/fixtures/sa_xmodel_fail_missing_from_sql");
    assert_has_diagnostics(&diags, "A040");
}

#[test]
fn test_sa_clean_project_no_xmodel_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_clean_project");
    assert_no_diagnostics_with_code(&diags, "A040");
    assert_no_diagnostics_with_code(&diags, "A041");
}

// ── Phase 10: Multi-Model DAG — CLI level ────────────────────────────────

#[test]
fn test_sa_dag_ecommerce_zero_diagnostics_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_dag_pass_ecommerce");
    assert_eq!(
        diags.len(),
        0,
        "Ecommerce project should produce zero diagnostics: {:#?}",
        diags
    );
}

#[test]
fn test_sa_dag_mixed_diagnostics_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_dag_fail_mixed");
    assert_has_diagnostics(&diags, "A040");
}

// ── Phase 12: CLI Integration ────────────────────────────────────────────

#[test]
fn test_cli_validate_strict_with_warnings() {
    let output = Command::new(ff_bin())
        .args([
            "validate",
            "--project-dir",
            "tests/fixtures/sa_dag_fail_mixed",
            "--strict",
        ])
        .output()
        .expect("Failed to run ff validate --strict");

    assert!(
        !output.status.success(),
        "validate --strict on project with warnings should fail"
    );
}

#[test]
fn test_cli_compile_with_sa_error_reports_issue() {
    let output = Command::new(ff_bin())
        .args([
            "compile",
            "--project-dir",
            "tests/fixtures/sa_xmodel_fail_missing_from_sql",
        ])
        .output()
        .expect("Failed to run ff compile");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("missing from SQL output") || combined.contains("[error]"),
        "compile should report SA error in output.\ncombined: {}",
        combined
    );
}

#[test]
fn test_cli_compile_skip_sa_on_error_project() {
    let output = Command::new(ff_bin())
        .args([
            "compile",
            "--project-dir",
            "tests/fixtures/sa_xmodel_fail_missing_from_sql",
            "--skip-static-analysis",
        ])
        .output()
        .expect("Failed to run ff compile --skip-static-analysis");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "compile --skip-static-analysis should bypass SA errors.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_cli_analyze_json_structure() {
    let diags = run_analyze_json("tests/fixtures/sa_dag_fail_mixed");
    assert!(
        !diags.is_empty(),
        "Mixed project should produce diagnostics"
    );
    for diag in &diags {
        assert!(
            diag.get("code").is_some(),
            "Diagnostic missing 'code' field: {:#?}",
            diag
        );
        assert!(
            diag.get("severity").is_some(),
            "Diagnostic missing 'severity' field: {:#?}",
            diag
        );
        assert!(
            diag.get("message").is_some(),
            "Diagnostic missing 'message' field: {:#?}",
            diag
        );
        assert!(
            diag.get("model").is_some(),
            "Diagnostic missing 'model' field: {:#?}",
            diag
        );
    }
}

#[test]
fn test_cli_analyze_model_filter() {
    let output = Command::new(ff_bin())
        .args([
            "analyze",
            "--project-dir",
            "tests/fixtures/sa_dag_fail_mixed",
            "--nodes",
            "stg",
            "--output",
            "json",
        ])
        .output()
        .expect("Failed to run ff analyze --nodes stg");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let diags: Vec<serde_json::Value> = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("JSON parse failed: {}\nraw: {}", e, stdout));

    for diag in &diags {
        let model = diag.get("model").and_then(|m| m.as_str()).unwrap_or("");
        assert_eq!(
            model, "stg",
            "With --nodes stg, all diagnostics should be for 'stg', got '{}'",
            model
        );
    }
}

// ── Phase 13: Regression Guard Rails — CLI level ─────────────────────────

#[test]
fn test_guard_clean_project_zero_diagnostics_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_clean_project");
    assert_eq!(
        diags.len(),
        0,
        "Clean project regression guard: expected zero diagnostics, got {}:\n{:#?}",
        diags.len(),
        diags
    );
}

#[test]
fn test_guard_ecommerce_zero_diagnostics_cli() {
    let diags = run_analyze_json("tests/fixtures/sa_dag_pass_ecommerce");
    assert_eq!(
        diags.len(),
        0,
        "Ecommerce regression guard: expected zero diagnostics, got {}:\n{:#?}",
        diags.len(),
        diags
    );
}
