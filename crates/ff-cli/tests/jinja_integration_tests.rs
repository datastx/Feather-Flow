//! Integration tests verifying Jinja template rendering through the `ff compile` CLI pipeline.
//!
//! Tests against both the existing `sample_project` fixture and dedicated
//! fixtures that exercise user macros, `var()` with defaults, `{% if %}` control
//! flow, built-in macros, error paths, and `--vars` CLI override.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

/// Path to the compiled ff binary.
fn ff_bin() -> String {
    env!("CARGO_BIN_EXE_ff").to_string()
}

/// Run `ff compile` on a fixture project, writing output to a temp dir.
///
/// Returns the `TempDir` whose root contains compiled model SQL files
/// in `<model_name>/<model_name>.sql` layout (the `models/` prefix is stripped
/// by `compute_compiled_path`).
fn compile_to_tempdir(fixture: &str) -> TempDir {
    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            fixture,
            "--output-dir",
            tmp.path().to_str().unwrap(),
            "--skip-static-analysis",
        ])
        .output()
        .expect("Failed to run ff dt compile");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "ff compile should succeed for {}.\nstdout: {}\nstderr: {}",
        fixture,
        stdout,
        stderr
    );

    tmp
}

/// Run `ff compile` with extra CLI args, returning (stdout, stderr, success).
fn compile_with_args(fixture: &str, extra_args: &[&str]) -> (String, String, bool) {
    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    let mut args = vec![
        "dt",
        "compile",
        "--project-dir",
        fixture,
        "--output-dir",
        tmp.path().to_str().unwrap(),
        "--skip-static-analysis",
    ];
    args.extend_from_slice(extra_args);

    let output = Command::new(ff_bin())
        .args(&args)
        .output()
        .expect("Failed to run ff dt compile");

    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    )
}

/// Run `ff compile` with extra CLI args, writing to a temp dir.
/// Returns `(TempDir, stdout, stderr)`. Does NOT assert success.
fn compile_to_tempdir_with_args(fixture: &str, extra_args: &[&str]) -> (TempDir, String, String) {
    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    let mut args = vec![
        "dt",
        "compile",
        "--project-dir",
        fixture,
        "--output-dir",
        tmp.path().to_str().unwrap(),
        "--skip-static-analysis",
    ];
    args.extend_from_slice(extra_args);

    let output = Command::new(ff_bin())
        .args(&args)
        .output()
        .expect("Failed to run ff dt compile");

    (
        tmp,
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// Read the compiled SQL for a model from the temp output directory.
fn read_compiled_model(tmp: &TempDir, model_name: &str) -> String {
    let path = tmp
        .path()
        .join(model_name)
        .join(format!("{}.sql", model_name));
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read compiled SQL at {:?}: {}", path, e))
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests against the existing sample_project fixture
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_sample_project_config_stripped() {
    let tmp = compile_to_tempdir("tests/fixtures/sample_project");
    let sql = read_compiled_model(&tmp, "stg_customers");

    // config() renders to empty string — the call should be gone
    assert!(
        !sql.contains("config("),
        "compiled SQL should not contain config() call, got:\n{}",
        sql
    );
    // The actual SELECT should survive
    assert!(
        sql.contains("SELECT"),
        "compiled SQL should contain SELECT, got:\n{}",
        sql
    );
    assert!(
        sql.contains("customer_id"),
        "compiled SQL should contain customer_id, got:\n{}",
        sql
    );
}

#[test]
fn test_sample_project_var_substitution() {
    let tmp = compile_to_tempdir("tests/fixtures/sample_project");
    let sql = read_compiled_model(&tmp, "stg_orders");

    // var("start_date") → "2024-01-01"
    assert!(
        sql.contains("2024-01-01"),
        "compiled SQL should contain substituted date, got:\n{}",
        sql
    );
    // No raw Jinja var() calls remain
    assert!(
        !sql.contains("var("),
        "compiled SQL should not contain var() call, got:\n{}",
        sql
    );
}

#[test]
fn test_sample_project_var_integer_substitution() {
    let tmp = compile_to_tempdir("tests/fixtures/sample_project");
    let sql = read_compiled_model(&tmp, "rpt_order_volume");

    // var("min_order_count") → 5
    assert!(
        sql.contains("order_volume_by_status(5)"),
        "compiled SQL should contain substituted integer in function call, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("var("),
        "compiled SQL should not contain var() call, got:\n{}",
        sql
    );
}

#[test]
fn test_sample_project_all_models_compile() {
    let tmp = compile_to_tempdir("tests/fixtures/sample_project");

    let models = [
        "stg_customers",
        "stg_orders",
        "stg_payments",
        "stg_payments_star",
        "stg_products",
        "int_customer_metrics",
        "int_orders_enriched",
        "dim_customers",
        "dim_products",
        "fct_orders",
        "rpt_order_volume",
    ];

    for model in &models {
        let sql = read_compiled_model(&tmp, model);
        assert!(
            !sql.is_empty(),
            "compiled SQL for {} should not be empty",
            model
        );
        assert!(
            sql.contains("SELECT"),
            "compiled SQL for {} should contain SELECT, got:\n{}",
            model,
            sql
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests against the jinja_template_project fixture — core features
// ──────────────────────────────────────────────────────────────────────────────

const JINJA_FIXTURE: &str = "tests/fixtures/jinja_template_project";

#[test]
fn test_jinja_user_macro_auto_import() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "stg_events");

    // cents_to_dollars('amount_cents') → (amount_cents / 100.0)
    assert!(
        sql.contains("amount_cents / 100.0"),
        "compiled SQL should contain expanded cents_to_dollars macro, got:\n{}",
        sql
    );
    // The macro name itself should not appear in output
    assert!(
        !sql.contains("cents_to_dollars"),
        "compiled SQL should not contain raw macro name, got:\n{}",
        sql
    );
    // No Jinja syntax should remain in compiled output
    assert!(
        !sql.contains("{{"),
        "compiled SQL should not contain Jinja expression tags, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_var_with_default() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "stg_events");

    // var("default_status", "active") → active (uses default since not in vars)
    assert!(
        sql.contains("'active'"),
        "compiled SQL should contain default var value 'active', got:\n{}",
        sql
    );
    assert!(
        !sql.contains("var("),
        "compiled SQL should not contain var() call, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_if_conditional_rendering() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "int_events_enriched");

    // min_event_count = 10 > 0, so the {% if %} block should be included
    assert!(
        sql.contains("AND user_id IS NOT NULL"),
        "compiled SQL should contain conditional AND clause, got:\n{}",
        sql
    );
    // No Jinja block tags remain
    assert!(
        !sql.contains("{%"),
        "compiled SQL should not contain {{% block tags, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_builtin_date_trunc() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "int_events_enriched");

    // date_trunc('month', 'event_date') → DATE_TRUNC('month', "event_date")
    assert!(
        sql.contains(r#"DATE_TRUNC('month', "event_date")"#),
        "compiled SQL should contain DATE_TRUNC expansion, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_var_string_in_where() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "int_events_enriched");

    // var("event_category") → page_view
    assert!(
        sql.contains("'page_view'"),
        "compiled SQL should contain substituted event_category var, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_builtin_hash() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "fct_events_hashed");

    // hash('user_id') → MD5(CAST("user_id" AS VARCHAR))
    assert!(
        sql.contains(r#"MD5(CAST("user_id" AS VARCHAR))"#),
        "compiled SQL should contain MD5 hash expansion, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_builtin_coalesce_columns() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "fct_events_hashed");

    // coalesce_columns(['amount_dollars', 'status']) → COALESCE("amount_dollars", "status")
    assert!(
        sql.contains(r#"COALESCE("amount_dollars", "status")"#),
        "compiled SQL should contain COALESCE expansion, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_builtin_safe_divide() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "rpt_events_summary");

    // safe_divide('amount_dollars', 'id') expands to CASE WHEN ... END
    assert!(
        sql.contains("CASE WHEN"),
        "compiled SQL should contain CASE WHEN from safe_divide, got:\n{}",
        sql
    );
    assert!(
        sql.contains(r#"CAST("amount_dollars" AS DOUBLE)"#),
        "compiled SQL should contain CAST to DOUBLE, got:\n{}",
        sql
    );
    assert!(
        sql.contains(r#""id" IS NULL"#),
        "compiled SQL should contain NULL guard, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_user_macro_format_event_type() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "rpt_events_summary");

    // format_event_type('event_type') → LOWER(TRIM(event_type))
    assert!(
        sql.contains("LOWER(TRIM(event_type))"),
        "compiled SQL should contain LOWER(TRIM(...)) expansion, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_var_integer_in_having() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "rpt_events_summary");

    // var("min_event_count") → 10 (case-insensitive: formatter may lowercase COUNT)
    let sql_upper = sql.to_uppercase();
    assert!(
        sql_upper.contains("HAVING COUNT(*) >= 10"),
        "compiled SQL should contain HAVING with substituted integer, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_all_models_compile_clean() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);

    let models = [
        "stg_events",
        "int_events_enriched",
        "fct_events_hashed",
        "rpt_events_summary",
        "plain_sql",
        "config_only",
        "conditional_branches",
        "jinja_comments",
        "nested_in_if",
        "var_default_override",
    ];

    for model in &models {
        let sql = read_compiled_model(&tmp, model);
        assert!(
            !sql.is_empty(),
            "compiled SQL for {} should not be empty",
            model
        );
        // No unresolved Jinja syntax in any compiled model
        assert!(
            !sql.contains("{{"),
            "compiled SQL for {} should not contain {{{{ expression tags, got:\n{}",
            model,
            sql
        );
        assert!(
            !sql.contains("}}"),
            "compiled SQL for {} should not contain }}}} expression tags, got:\n{}",
            model,
            sql
        );
        assert!(
            !sql.contains("{%"),
            "compiled SQL for {} should not contain {{%% block tags, got:\n{}",
            model,
            sql
        );
        assert!(
            !sql.contains("%}"),
            "compiled SQL for {} should not contain %%}} block tags, got:\n{}",
            model,
            sql
        );
        assert!(
            !sql.contains("{#"),
            "compiled SQL for {} should not contain {{# comment tags, got:\n{}",
            model,
            sql
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Corner case tests — jinja_template_project
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_jinja_plain_sql_passthrough() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "plain_sql");

    // Plain SQL with no Jinja should pass through unchanged
    assert!(
        sql.contains("SELECT"),
        "plain SQL should contain SELECT, got:\n{}",
        sql
    );
    assert!(
        sql.contains("raw_events"),
        "plain SQL should reference raw_events, got:\n{}",
        sql
    );
    assert!(
        sql.contains("WHERE status = 'active'"),
        "plain SQL should preserve WHERE clause, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_config_only_stripped() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "config_only");

    // config() should render to empty string
    assert!(
        !sql.contains("config("),
        "config() call should be stripped, got:\n{}",
        sql
    );
    // SQL body should survive
    assert!(
        sql.contains("SELECT"),
        "SELECT should survive config stripping, got:\n{}",
        sql
    );
    assert!(
        sql.contains("raw_events"),
        "FROM clause should survive, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_if_elif_else_branches() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "conditional_branches");

    // tier = "gold" → first branch taken: 'premium' AS tier_label
    assert!(
        sql.contains("'premium' AS tier_label"),
        "should contain 'premium' (gold branch), got:\n{}",
        sql
    );
    // Other branches should NOT appear
    assert!(
        !sql.contains("'standard' AS tier_label"),
        "should NOT contain silver branch, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("'basic' AS tier_label"),
        "should NOT contain else branch, got:\n{}",
        sql
    );
    // No Jinja control flow tags remain
    assert!(
        !sql.contains("{%"),
        "no Jinja block tags should remain, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_comments_stripped() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "jinja_comments");

    // All Jinja comments should be stripped
    assert!(
        !sql.contains("{#"),
        "Jinja comment open tags should be stripped, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("#}"),
        "Jinja comment close tags should be stripped, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("This entire comment"),
        "comment text should not appear in output, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("inline comment"),
        "inline comment text should not appear in output, got:\n{}",
        sql
    );
    // SQL body should survive
    assert!(
        sql.contains("SELECT"),
        "SELECT should survive, got:\n{}",
        sql
    );
    assert!(
        sql.contains("event_type"),
        "event_type column should survive, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_nested_macro_in_if() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "nested_in_if");

    // enable_filtering = true, so the if block is included
    // AND the macro inside it should be expanded
    assert!(
        sql.contains("amount_cents / 100.0"),
        "nested macro should be expanded inside if block, got:\n{}",
        sql
    );
    // No Jinja syntax remains
    assert!(
        !sql.contains("{%"),
        "no block tags should remain, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("cents_to_dollars"),
        "macro name should not appear in output, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_var_config_overrides_default() {
    let tmp = compile_to_tempdir(JINJA_FIXTURE);
    let sql = read_compiled_model(&tmp, "var_default_override");

    // event_category is defined in vars as "page_view",
    // so var("event_category", "default_category") should use "page_view"
    assert!(
        sql.contains("'page_view'"),
        "config var should override default, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("default_category"),
        "default value should NOT appear when var is defined, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_cli_vars_override() {
    // Use --vars to override event_category at compile time
    let (tmp, stdout, stderr) = compile_to_tempdir_with_args(
        JINJA_FIXTURE,
        &["--vars", r#"{"event_category":"purchase"}"#],
    );

    assert!(
        stdout.contains("Compiling"),
        "compile should produce output.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    let sql = read_compiled_model(&tmp, "int_events_enriched");

    // --vars should override project vars
    assert!(
        sql.contains("'purchase'"),
        "CLI --vars should override project var, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("'page_view'"),
        "original project var should NOT appear, got:\n{}",
        sql
    );
}

#[test]
fn test_jinja_cli_vars_override_integer() {
    // Override min_event_count from 10 to 25
    let (tmp, stdout, stderr) =
        compile_to_tempdir_with_args(JINJA_FIXTURE, &["--vars", r#"{"min_event_count":25}"#]);

    assert!(
        stdout.contains("Compiling"),
        "compile should produce output.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );

    let sql = read_compiled_model(&tmp, "rpt_events_summary");

    // Case-insensitive: formatter may lowercase COUNT
    let sql_upper = sql.to_uppercase();
    assert!(
        sql_upper.contains("HAVING COUNT(*) >= 25"),
        "CLI --vars should override integer var, got:\n{}",
        sql
    );
    assert!(
        !sql.contains(">= 10"),
        "original integer var should NOT appear, got:\n{}",
        sql
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Error case tests — jinja_error_project
// ──────────────────────────────────────────────────────────────────────────────

const ERROR_FIXTURE: &str = "tests/fixtures/jinja_error_project";

#[test]
fn test_jinja_error_undefined_var() {
    let (stdout, _stderr, success) = compile_with_args(ERROR_FIXTURE, &[]);

    // Compile should fail (exit code 1)
    assert!(
        !success,
        "compile should fail when a model uses an undefined var"
    );

    // Error output should mention the problematic model
    assert!(
        stdout.contains("undefined_var"),
        "error should mention the failing model name, got:\n{}",
        stdout
    );
    // The compile command wraps the error as "Failed to render template"
    assert!(
        stdout.contains("Failed to render template"),
        "error should mention template render failure, got:\n{}",
        stdout
    );
}

#[test]
fn test_jinja_error_undefined_macro() {
    let (stdout, _stderr, success) = compile_with_args(ERROR_FIXTURE, &[]);

    assert!(
        !success,
        "compile should fail when a model calls an undefined macro"
    );

    assert!(
        stdout.contains("missing_import"),
        "error should mention the failing model name, got:\n{}",
        stdout
    );
}

#[test]
fn test_jinja_error_good_model_still_compiles() {
    // Even though two models fail, the good_model should still compile
    let (tmp, stdout, _stderr) = compile_to_tempdir_with_args(ERROR_FIXTURE, &[]);

    // good_model should appear in output with a checkmark
    assert!(
        stdout.contains("good_model"),
        "good_model should appear in compilation output, got:\n{}",
        stdout
    );

    // The compiled file should exist and contain valid SQL
    let sql = read_compiled_model(&tmp, "good_model");
    assert!(
        sql.contains("SELECT"),
        "good_model should compile despite sibling failures, got:\n{}",
        sql
    );
    assert!(
        sql.contains("'hello'"),
        "good_model var should be substituted, got:\n{}",
        sql
    );
}
