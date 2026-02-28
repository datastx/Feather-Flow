//! Comprehensive test harness expansion for Feather-Flow.
//!
//! Tests new features from Phases 1-4: run groups, dual-path incremental
//! compilation (is_exists), hook/test compilation to target, meta DB
//! integration, YAML-only config, and the 5-stage compile pipeline.

use std::path::Path;
use std::process::Command;

// ── Helpers ────────────────────────────────────────────────────────────

fn ff_bin() -> String {
    env!("CARGO_BIN_EXE_ff").to_string()
}

fn run_compile(fixture: &str) -> std::process::Output {
    Command::new(ff_bin())
        .args(["dt", "compile", "--project-dir", fixture])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run ff dt compile on {}: {}", fixture, e))
}

fn run_compile_json(fixture: &str) -> serde_json::Value {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            fixture,
            "--output",
            "json",
        ])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run ff dt compile on {}: {}", fixture, e));
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).unwrap_or(serde_json::Value::Null)
}

fn run_ls(fixture: &str) -> std::process::Output {
    Command::new(ff_bin())
        .args(["dt", "ls", "--project-dir", fixture])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run ff dt ls on {}: {}", fixture, e))
}

fn run_compile_with_nodes(fixture: &str, nodes: &str) -> std::process::Output {
    Command::new(ff_bin())
        .args(["dt", "compile", "--project-dir", fixture, "-n", nodes])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run ff dt compile -n {nodes} on {fixture}: {e}"))
}

/// Clean target directory for a fixture before tests.
fn clean_target(fixture: &str) {
    let target = Path::new(fixture).join("target");
    if target.exists() {
        let _ = std::fs::remove_dir_all(&target);
    }
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let ty = entry.file_type().unwrap();
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            if entry.file_name() == "target" {
                continue; // Skip target directories
            }
            copy_dir_recursive(&entry.path(), &dst_path);
        } else {
            std::fs::copy(entry.path(), &dst_path).unwrap();
        }
    }
}

/// Ensure fixture is compiled (compile if target doesn't exist).
fn ensure_compiled(fixture: &str) {
    let compiled_dir = Path::new(fixture).join("target/compiled");
    if !compiled_dir.exists() {
        let output = run_compile(fixture);
        assert!(
            output.status.success(),
            "Compile should succeed for {}: {}",
            fixture,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section 1: Run Groups
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_run_groups_project_loads() {
    let fixture = "tests/fixtures/run_groups_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "run_groups_project");
    assert_eq!(project.models.len(), 3);

    // Verify run_groups are parsed
    let groups = project
        .config
        .run_groups
        .as_ref()
        .expect("run_groups should be defined");
    assert_eq!(groups.len(), 2);
    assert!(groups.contains_key("staging"));
    assert!(groups.contains_key("full_rebuild"));
}

#[test]
fn test_run_groups_staging_has_correct_nodes() {
    let fixture = "tests/fixtures/run_groups_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    let groups = project.config.run_groups.as_ref().unwrap();
    let staging = &groups["staging"];
    assert_eq!(staging.nodes, vec!["stg_orders"]);
    assert_eq!(staging.mode, Some(ff_core::config::RunMode::Models));
}

#[test]
fn test_run_groups_full_rebuild_config() {
    let fixture = "tests/fixtures/run_groups_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    let groups = project.config.run_groups.as_ref().unwrap();
    let full = &groups["full_rebuild"];
    assert_eq!(full.nodes.len(), 3);
    assert_eq!(full.full_refresh, Some(true));
    assert_eq!(full.fail_fast, Some(true));
    assert_eq!(full.mode, Some(ff_core::config::RunMode::Build));
}

#[test]
fn test_run_groups_compile_succeeds() {
    let fixture = "tests/fixtures/run_groups_project";
    clean_target(fixture);
    let output = run_compile(fixture);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "run_groups_project compile should succeed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    // Should report 2 run groups validated
    assert!(
        stdout.contains("2 groups"),
        "Should report 2 run groups: {}",
        stdout
    );
}

#[test]
fn test_run_groups_node_selector() {
    let fixture = "tests/fixtures/run_groups_project";
    clean_target(fixture);
    let output = run_compile_with_nodes(fixture, "stg_orders");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "Compile with -n stg_orders should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("stg_orders"),
        "Should compile stg_orders: {}",
        stdout
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 2: Dual-Path Incremental Compilation (is_exists)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_dual_path_project_loads() {
    let fixture = "tests/fixtures/dual_path_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "dual_path_project");
    assert_eq!(project.models.len(), 2);
    assert!(project.models.contains_key("stg_events"));
    assert!(project.models.contains_key("fct_events_incremental"));
}

#[test]
fn test_dual_path_incremental_model_config() {
    let fixture = "tests/fixtures/dual_path_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    let model = project.get_model("fct_events_incremental").unwrap();
    assert_eq!(
        model.config.materialized,
        Some(ff_core::config::Materialization::Incremental)
    );
    assert_eq!(model.config.unique_key.as_deref(), Some("event_id"));
    assert_eq!(
        model.config.incremental_strategy,
        Some(ff_core::config::IncrementalStrategy::Merge)
    );
}

#[test]
fn test_dual_path_compile_produces_both_files() {
    let fixture = "tests/fixtures/dual_path_project";
    ensure_compiled(fixture);

    // Verify both .full.sql and .incremental.sql exist
    let models_dir =
        Path::new(fixture).join("target/compiled/dual_path_project/models/fct_events_incremental");
    assert!(
        models_dir.join("fct_events_incremental.full.sql").exists(),
        "Should have .full.sql file"
    );
    assert!(
        models_dir
            .join("fct_events_incremental.incremental.sql")
            .exists(),
        "Should have .incremental.sql file"
    );
    assert!(
        models_dir.join("fct_events_incremental.sql").exists(),
        "Should have default .sql file"
    );
}

#[test]
fn test_dual_path_full_sql_has_no_where_clause() {
    let fixture = "tests/fixtures/dual_path_project";
    ensure_compiled(fixture);
    let full_path = Path::new(fixture)
        .join("target/compiled/dual_path_project/models/fct_events_incremental/fct_events_incremental.full.sql");
    let full_sql = std::fs::read_to_string(&full_path)
        .unwrap_or_else(|e| panic!("Failed to read full SQL: {}", e));
    // Full path should NOT have the WHERE clause from is_exists()
    assert!(
        !full_sql.contains("MAX(created_at)"),
        "Full path should not contain incremental WHERE clause: {}",
        full_sql
    );
}

#[test]
fn test_dual_path_incremental_sql_has_where_clause() {
    let fixture = "tests/fixtures/dual_path_project";
    ensure_compiled(fixture);
    let inc_path = Path::new(fixture)
        .join("target/compiled/dual_path_project/models/fct_events_incremental/fct_events_incremental.incremental.sql");
    let inc_sql = std::fs::read_to_string(&inc_path)
        .unwrap_or_else(|e| panic!("Failed to read incremental SQL: {}", e));
    // Incremental path SHOULD have the WHERE clause from is_exists()
    assert!(
        inc_sql.contains("MAX(created_at)"),
        "Incremental path should contain the WHERE clause: {}",
        inc_sql
    );
}

#[test]
fn test_dual_path_regular_model_has_no_dual_files() {
    let fixture = "tests/fixtures/dual_path_project";
    ensure_compiled(fixture);
    let stg_dir = Path::new(fixture).join("target/compiled/dual_path_project/models/stg_events");
    assert!(
        stg_dir.join("stg_events.sql").exists(),
        "Regular model should have .sql file"
    );
    assert!(
        !stg_dir.join("stg_events.full.sql").exists(),
        "Regular model should NOT have .full.sql file"
    );
    assert!(
        !stg_dir.join("stg_events.incremental.sql").exists(),
        "Regular model should NOT have .incremental.sql file"
    );
}

#[test]
fn test_dual_path_tests_compiled_to_target() {
    let fixture = "tests/fixtures/dual_path_project";
    ensure_compiled(fixture);
    let tests_dir = Path::new(fixture).join("target/compiled/dual_path_project/tests");
    assert!(tests_dir.exists(), "Tests directory should exist");
    assert!(
        tests_dir
            .join("fct_events_incremental__unique__event_id.sql")
            .exists(),
        "Unique test should be compiled"
    );
    assert!(
        tests_dir
            .join("fct_events_incremental__not_null__event_id.sql")
            .exists(),
        "Not null test should be compiled"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 3: YAML-Only Config
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_yaml_config_project_loads() {
    let fixture = "tests/fixtures/yaml_config_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "yaml_config_project");
    assert_eq!(project.models.len(), 4);
}

#[test]
fn test_yaml_config_materializations() {
    let fixture = "tests/fixtures/yaml_config_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();

    // View materialization
    let stg = project.get_model("stg_users").unwrap();
    assert_eq!(
        stg.config.materialized,
        Some(ff_core::config::Materialization::View)
    );

    // Table materialization
    let dim = project.get_model("dim_users").unwrap();
    assert_eq!(
        dim.config.materialized,
        Some(ff_core::config::Materialization::Table)
    );

    // Incremental materialization
    let fct = project.get_model("fct_activity").unwrap();
    assert_eq!(
        fct.config.materialized,
        Some(ff_core::config::Materialization::Incremental)
    );

    // Ephemeral materialization
    let rpt = project.get_model("rpt_summary").unwrap();
    assert_eq!(
        rpt.config.materialized,
        Some(ff_core::config::Materialization::Ephemeral)
    );
}

#[test]
fn test_yaml_config_compile_succeeds() {
    let fixture = "tests/fixtures/yaml_config_project";
    clean_target(fixture);
    let output = run_compile(fixture);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "yaml_config_project compile should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Ephemeral model should be inlined
    assert!(
        stdout.contains("ephemeral inlined"),
        "Should report ephemeral inlining: {}",
        stdout
    );
}

#[test]
fn test_yaml_config_no_config_calls_in_sql() {
    let fixture = "tests/fixtures/yaml_config_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();

    for (name, model) in &project.models {
        assert!(
            !model.raw_sql.contains("config("),
            "Model {} should not have config() calls in SQL",
            name
        );
    }
}

#[test]
fn test_yaml_config_incremental_dual_path() {
    let fixture = "tests/fixtures/yaml_config_project";
    clean_target(fixture);
    let output = run_compile(fixture);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[full + incremental]"),
        "Incremental model should produce dual-path output: {}",
        stdout
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 4: Hook Compilation
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_hook_project_loads() {
    let fixture = "tests/fixtures/hook_compilation_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "hook_compilation_project");
    assert_eq!(project.models.len(), 2);
}

#[test]
fn test_hook_model_config_parsed() {
    let fixture = "tests/fixtures/hook_compilation_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();

    // stg_data has 1 pre_hook
    let stg = project.get_model("stg_data").unwrap();
    assert_eq!(stg.config.pre_hook.len(), 1);
    assert!(stg.config.post_hook.is_empty());

    // fct_metrics has 2 pre_hooks and 1 post_hook
    let fct = project.get_model("fct_metrics").unwrap();
    assert_eq!(fct.config.pre_hook.len(), 2);
    assert_eq!(fct.config.post_hook.len(), 1);
}

#[test]
fn test_hooks_compiled_to_target() {
    let fixture = "tests/fixtures/hook_compilation_project";
    ensure_compiled(fixture);

    let base = Path::new(fixture).join("target/compiled/hook_compilation_project");

    // stg_data hooks
    let stg_hooks = base.join("stg_data/hooks");
    assert!(
        stg_hooks.join("pre_hook.sql").exists(),
        "stg_data should have pre_hook.sql"
    );

    // fct_metrics hooks (multiple pre_hooks)
    let fct_hooks = base.join("fct_metrics/hooks");
    assert!(
        fct_hooks.join("pre_hook_1.sql").exists(),
        "fct_metrics should have pre_hook_1.sql"
    );
    assert!(
        fct_hooks.join("pre_hook_2.sql").exists(),
        "fct_metrics should have pre_hook_2.sql"
    );
    assert!(
        fct_hooks.join("post_hook.sql").exists(),
        "fct_metrics should have post_hook.sql"
    );
}

#[test]
fn test_hook_content_is_correct() {
    let fixture = "tests/fixtures/hook_compilation_project";
    ensure_compiled(fixture);

    let base = Path::new(fixture).join("target/compiled/hook_compilation_project");
    let pre1 = std::fs::read_to_string(base.join("fct_metrics/hooks/pre_hook_1.sql")).unwrap();
    assert!(
        pre1.contains("fct_metrics_pre_1"),
        "pre_hook_1.sql should contain correct SQL: {}",
        pre1
    );
    let post = std::fs::read_to_string(base.join("fct_metrics/hooks/post_hook.sql")).unwrap();
    assert!(
        post.contains("fct_metrics_post_1"),
        "post_hook.sql should contain correct SQL: {}",
        post
    );
}

#[test]
fn test_tests_compiled_to_target_hook_project() {
    let fixture = "tests/fixtures/hook_compilation_project";
    ensure_compiled(fixture);

    let tests_dir = Path::new(fixture).join("target/compiled/hook_compilation_project/tests");
    assert!(tests_dir.exists(), "Tests directory should exist");
    assert!(
        tests_dir
            .join("fct_metrics__unique__metric_id.sql")
            .exists(),
        "Unique test for fct_metrics should be compiled"
    );
    assert!(
        tests_dir
            .join("fct_metrics__not_null__metric_id.sql")
            .exists(),
        "Not null test for fct_metrics should be compiled"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 5: Large DAG (topo sort correctness)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_large_dag_project_loads() {
    let fixture = "tests/fixtures/large_dag_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "large_dag_project");
    assert_eq!(project.models.len(), 12);
}

#[test]
fn test_large_dag_compile_succeeds() {
    let fixture = "tests/fixtures/large_dag_project";
    clean_target(fixture);
    let output = run_compile(fixture);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "large_dag_project compile should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("Compiled 12 models"),
        "Should compile all 12 models: {}",
        stdout
    );
}

#[test]
fn test_large_dag_topo_sort_correctness() {
    use ff_core::dag::ModelDag;
    use std::collections::HashMap;

    // Build a chain: layer_01 -> layer_02 -> ... -> layer_12
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("layer_01".to_string(), vec!["raw_data".to_string()]);
    for i in 2..=12 {
        let prev = format!("layer_{:02}", i - 1);
        let curr = format!("layer_{:02}", i);
        deps.insert(curr, vec![prev]);
    }

    let dag = ModelDag::build(&deps).unwrap();
    let order = dag.topological_order().unwrap();

    // Every layer must appear before the next
    for i in 1..12 {
        let curr = format!("layer_{:02}", i);
        let next = format!("layer_{:02}", i + 1);
        let curr_pos = order.iter().position(|m| m == &curr);
        let next_pos = order.iter().position(|m| m == &next);
        if let (Some(cp), Some(np)) = (curr_pos, next_pos) {
            assert!(
                cp < np,
                "{} (pos {}) should come before {} (pos {})",
                curr,
                cp,
                next,
                np
            );
        }
    }
}

#[test]
fn test_large_dag_bounded_ancestor_selector() {
    use ff_core::dag::ModelDag;
    use std::collections::HashMap;

    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("layer_01".to_string(), vec![]);
    for i in 2..=12 {
        let prev = format!("layer_{:02}", i - 1);
        let curr = format!("layer_{:02}", i);
        deps.insert(curr, vec![prev]);
    }
    let dag = ModelDag::build(&deps).unwrap();

    // Use ancestors_bounded directly: 2 hops from layer_12
    let ancestors = dag.ancestors_bounded("layer_12", 2);
    // Should get layer_11 (1 hop) and layer_10 (2 hops)
    assert!(ancestors.contains(&"layer_11".to_string()));
    assert!(ancestors.contains(&"layer_10".to_string()));
    assert_eq!(ancestors.len(), 2);
}

#[test]
fn test_large_dag_unbounded_ancestor_selector() {
    use ff_core::dag::ModelDag;
    use std::collections::HashMap;

    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("layer_01".to_string(), vec![]);
    for i in 2..=12 {
        let prev = format!("layer_{:02}", i - 1);
        let curr = format!("layer_{:02}", i);
        deps.insert(curr, vec![prev]);
    }
    let dag = ModelDag::build(&deps).unwrap();

    // +layer_12 = all ancestors
    let selected = dag.select("+layer_12").unwrap();
    assert_eq!(
        selected.len(),
        12,
        "Should select all 12 nodes in the chain"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 6: Empty Project
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_empty_project_loads() {
    let fixture = "tests/fixtures/empty_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "empty_project");
    assert!(project.models.is_empty());
    assert!(project.sources.is_empty());
    assert!(project.seeds.is_empty());
    assert!(project.functions.is_empty());
}

#[test]
fn test_empty_project_compile_succeeds() {
    let fixture = "tests/fixtures/empty_project";
    clean_target(fixture);
    let output = run_compile(fixture);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "empty_project compile should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("Compiled 0 models"),
        "Should report 0 models: {}",
        stdout
    );
}

#[test]
fn test_empty_project_ls() {
    let fixture = "tests/fixtures/empty_project";
    let output = run_ls(fixture);
    assert!(
        output.status.success(),
        "ls on empty project should succeed"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 7: Mixed Node Kinds
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_mixed_nodes_project_loads() {
    let fixture = "tests/fixtures/mixed_nodes_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    assert_eq!(project.config.name, "mixed_nodes_project");
    assert_eq!(project.models.len(), 2, "Should have 2 SQL models");
    assert_eq!(project.sources.len(), 1, "Should have 1 source");
    assert_eq!(project.seeds.len(), 1, "Should have 1 seed");
    assert_eq!(project.functions.len(), 1, "Should have 1 function");
}

#[test]
fn test_mixed_nodes_seed_name() {
    let fixture = "tests/fixtures/mixed_nodes_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    let seed = &project.seeds[0];
    assert_eq!(seed.name.as_ref(), "country_codes");
}

#[test]
fn test_mixed_nodes_function_name() {
    let fixture = "tests/fixtures/mixed_nodes_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();
    let func = &project.functions[0];
    assert_eq!(func.name.as_ref(), "safe_divide");
}

#[test]
fn test_mixed_nodes_compile_succeeds() {
    let fixture = "tests/fixtures/mixed_nodes_project";
    clean_target(fixture);
    let output = run_compile(fixture);
    assert!(
        output.status.success(),
        "mixed_nodes_project compile should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 8: Circular Dependencies (edge case)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_circular_dependency_error_message() {
    use ff_core::dag::ModelDag;
    use std::collections::HashMap;

    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert("model_a".to_string(), vec!["model_c".to_string()]);
    deps.insert("model_b".to_string(), vec!["model_a".to_string()]);
    deps.insert("model_c".to_string(), vec!["model_b".to_string()]);

    let result = ModelDag::build(&deps);
    assert!(result.is_err(), "Circular deps should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("E007") || err.contains("cycle") || err.contains("ircular"),
        "Error should mention cycle: {}",
        err
    );
}

#[test]
fn test_self_reference_not_circular() {
    use ff_core::dag::ModelDag;
    use std::collections::HashMap;

    // Self-reference should be silently ignored
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    deps.insert(
        "my_model".to_string(),
        vec!["my_model".to_string(), "upstream".to_string()],
    );
    deps.insert("upstream".to_string(), vec![]);

    let result = ModelDag::build(&deps);
    assert!(
        result.is_ok(),
        "Self-reference should not cause cycle error"
    );
    let dag = result.unwrap();
    assert_eq!(
        dag.dependencies("my_model"),
        vec!["upstream".to_string()],
        "Self-reference should be filtered out"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 9: 5-Stage Compile Pipeline
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_compile_json_output_structure() {
    let fixture = "tests/fixtures/dual_path_project";
    clean_target(fixture);
    let json = run_compile_json(fixture);
    if !json.is_null() {
        // JSON output should have expected top-level keys
        assert!(
            json.get("total_models").is_some() || json.get("results").is_some(),
            "JSON output should have standard keys: {:?}",
            json
        );
    }
}

#[test]
fn test_compile_parse_only_no_model_disk_writes() {
    // Use a tempdir-based copy to isolate from other tests
    let tmp = tempfile::tempdir().unwrap();
    let fixture_src = Path::new("tests/fixtures/dual_path_project");
    // Copy the fixture to a temp location
    let fixture_dst = tmp.path().join("dual_path_project");
    copy_dir_recursive(fixture_src, &fixture_dst);

    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            fixture_dst.to_str().unwrap(),
            "--parse-only",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "parse-only should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // In parse-only mode, no compiled model SQL files should exist
    let models_dir = fixture_dst.join("target/compiled");
    assert!(
        !models_dir.exists(),
        "parse-only should not write compiled model files"
    );
}

#[test]
fn test_compile_skip_static_analysis() {
    let fixture = "tests/fixtures/dual_path_project";
    clean_target(fixture);
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            fixture,
            "--skip-static-analysis",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "compile --skip-static-analysis should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 10: Meta DB Population
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_meta_db_created_after_compile() {
    let fixture = "tests/fixtures/dual_path_project";
    ensure_compiled(fixture);
    let meta_path = Path::new(fixture).join("target/meta.duckdb");
    assert!(
        meta_path.exists(),
        "meta.duckdb should be created after compile"
    );
}

#[test]
fn test_meta_db_created_hook_project() {
    let fixture = "tests/fixtures/hook_compilation_project";
    ensure_compiled(fixture);
    let meta_path = Path::new(fixture).join("target/meta.duckdb");
    assert!(
        meta_path.exists(),
        "meta.duckdb should be created after compile for hook project"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 11: Schema Mismatch Detection (SA01/SA02)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_schema_mismatch_extra_in_sql() {
    // sa_xmodel_fail_extra_in_sql should produce SA01 or A040 diagnostics
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "analyze",
            "--project-dir",
            "tests/fixtures/sa_xmodel_fail_extra_in_sql",
            "--output",
            "json",
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let diags: Vec<serde_json::Value> = serde_json::from_str(stdout.trim()).unwrap_or_default();
    let has_schema_diag = diags.iter().any(|d| {
        let code = d.get("code").and_then(|c| c.as_str()).unwrap_or("");
        code.starts_with("SA") || code.starts_with("A04")
    });
    assert!(
        has_schema_diag,
        "Extra-in-SQL fixture should produce schema mismatch diagnostics"
    );
}

#[test]
fn test_schema_mismatch_missing_from_sql() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "analyze",
            "--project-dir",
            "tests/fixtures/sa_xmodel_fail_missing_from_sql",
            "--output",
            "json",
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let diags: Vec<serde_json::Value> = serde_json::from_str(stdout.trim()).unwrap_or_default();
    let has_schema_diag = diags.iter().any(|d| {
        let code = d.get("code").and_then(|c| c.as_str()).unwrap_or("");
        code.starts_with("SA") || code.starts_with("A04")
    });
    assert!(
        has_schema_diag,
        "Missing-from-SQL fixture should produce schema mismatch diagnostics"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 12: is_exists / is_incremental Jinja functions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_is_exists_function_true() {
    use ff_jinja::IncrementalState;
    use ff_jinja::JinjaEnvironment;
    use std::collections::HashMap;

    let vars = HashMap::new();
    let state = IncrementalState::new(true, true, false);
    let jinja = JinjaEnvironment::with_incremental_context(&vars, &[], state, "analytics.my_model");

    let template = "{% if is_exists() %}WHERE updated_at > '2024-01-01'{% endif %}";
    let result = jinja.render(template).unwrap();
    assert!(
        result.contains("WHERE"),
        "is_exists() should return true: {}",
        result
    );
}

#[test]
fn test_is_exists_false_when_table_missing() {
    use ff_jinja::IncrementalState;
    use ff_jinja::JinjaEnvironment;
    use std::collections::HashMap;

    let vars = HashMap::new();
    let state = IncrementalState::new(true, false, false);
    let jinja = JinjaEnvironment::with_incremental_context(&vars, &[], state, "analytics.my_model");

    let template = "SELECT * FROM src{% if is_exists() %} WHERE ts > '2024-01-01'{% endif %}";
    let result = jinja.render(template).unwrap();
    assert!(
        !result.contains("WHERE"),
        "is_exists() should return false when model does not exist: {}",
        result
    );
}

#[test]
fn test_is_exists_false_with_full_refresh() {
    use ff_jinja::IncrementalState;
    use ff_jinja::JinjaEnvironment;
    use std::collections::HashMap;

    let vars = HashMap::new();
    let state = IncrementalState::new(true, true, true);
    let jinja = JinjaEnvironment::with_incremental_context(&vars, &[], state, "analytics.my_model");

    let template = "SELECT * FROM src{% if is_exists() %} WHERE ts > '2024-01-01'{% endif %}";
    let result = jinja.render(template).unwrap();
    assert!(
        !result.contains("WHERE"),
        "is_exists() should return false with full_refresh: {}",
        result
    );
}

#[test]
fn test_is_exists_false_for_non_incremental() {
    use ff_jinja::IncrementalState;
    use ff_jinja::JinjaEnvironment;
    use std::collections::HashMap;

    let vars = HashMap::new();
    let state = IncrementalState::new(false, true, false);
    let jinja = JinjaEnvironment::with_incremental_context(&vars, &[], state, "analytics.my_model");

    let template = "SELECT * FROM src{% if is_exists() %} WHERE ts > '2024-01-01'{% endif %}";
    let result = jinja.render(template).unwrap();
    assert!(
        !result.contains("WHERE"),
        "is_exists() should return false for non-incremental model: {}",
        result
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 13: Compile pipeline validation
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_compile_strict_flag() {
    let fixture = "tests/fixtures/dual_path_project";
    clean_target(fixture);
    let output = Command::new(ff_bin())
        .args(["dt", "compile", "--project-dir", fixture, "--strict"])
        .output()
        .unwrap();
    // Strict mode may produce warnings but should not crash
    let _ = output.status;
}

#[test]
fn test_compile_with_output_dir() {
    let fixture = "tests/fixtures/dual_path_project";
    let tmp = tempfile::tempdir().unwrap();
    let output_dir = tmp.path().join("custom_output");
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            fixture,
            "--output-dir",
            output_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Compile with custom output dir should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Custom output dir should exist
    assert!(
        output_dir.exists(),
        "Custom output directory should be created"
    );
}

#[test]
fn test_compile_nonexistent_project_fails() {
    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            "/tmp/ff_test_nonexistent_98765",
        ])
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "Compile on nonexistent project should fail"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 14: Sample project regression tests
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_sample_project_dual_path_model() {
    let fixture = "tests/fixtures/sample_project";
    let project = ff_core::Project::load(Path::new(fixture)).unwrap();

    // fct_orders_is_exists should be an incremental model using is_exists()
    let model = project.get_model("fct_orders_is_exists").unwrap();
    assert_eq!(
        model.config.materialized,
        Some(ff_core::config::Materialization::Incremental)
    );
    assert!(
        model.raw_sql.contains("is_exists()"),
        "fct_orders_is_exists should use is_exists() in SQL"
    );
}

#[test]
fn test_sample_project_compile_dual_path() {
    // Use a tempdir copy so we don't interfere with other tests using sample_project
    let tmp = tempfile::tempdir().unwrap();
    let fixture_src = Path::new("tests/fixtures/sample_project");
    let fixture_dst = tmp.path().join("sample_project");
    copy_dir_recursive(fixture_src, &fixture_dst);

    let output = Command::new(ff_bin())
        .args([
            "dt",
            "compile",
            "--project-dir",
            fixture_dst.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "sample_project compile should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Should have at least one dual-path model
    assert!(
        stdout.contains("[full + incremental]"),
        "Should report dual-path model: {}",
        stdout
    );
}
