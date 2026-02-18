//! Tests for MetaDb connection, migration, DDL, constraints, and cascades.

use crate::MetaDb;

// ── Helpers ────────────────────────────────────────────────────────────

/// Query a single i64 value (convenience for COUNT(*) assertions).
fn count(db: &MetaDb, sql: &str) -> i64 {
    db.conn()
        .query_row(sql, [], |row| row.get::<_, i64>(0))
        .unwrap()
}

/// Execute a statement, ignoring the returned row count.
fn exec(db: &MetaDb, sql: &str) {
    db.conn().execute(sql, []).unwrap();
}

/// Expect a statement to fail (constraint violation, etc.).
fn expect_err(db: &MetaDb, sql: &str) {
    assert!(
        db.conn().execute(sql, []).is_err(),
        "Expected error for: {sql}"
    );
}

// ── Connection & migration ─────────────────────────────────────────────

#[test]
fn open_memory_succeeds() {
    let db = MetaDb::open_memory().unwrap();
    assert!(count(&db, "SELECT COUNT(*) FROM ff_meta.schema_version") >= 1);
}

#[test]
fn open_file_creates_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.duckdb");
    assert!(!path.exists());
    let _db = MetaDb::open(&path).unwrap();
    assert!(path.exists());
}

#[test]
fn open_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.duckdb");
    {
        let _db1 = MetaDb::open(&path).unwrap();
        // drop db1 so the file is not held open
    }
    let db2 = MetaDb::open(&path).unwrap();
    let migration_count = crate::ddl::MIGRATIONS.len() as i64;
    assert_eq!(
        count(&db2, "SELECT COUNT(*) FROM ff_meta.schema_version"),
        migration_count,
        "schema_version should have one row per migration"
    );
}

#[test]
fn schema_version_recorded() {
    let db = MetaDb::open_memory().unwrap();
    let version: i32 = db
        .conn()
        .query_row(
            "SELECT MAX(version) FROM ff_meta.schema_version",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let expected = crate::ddl::MIGRATIONS.last().unwrap().version;
    assert_eq!(version, expected);
}

// ── All expected tables exist ──────────────────────────────────────────

#[test]
fn all_tables_exist() {
    let db = MetaDb::open_memory().unwrap();
    let expected_tables = [
        "schema_version",
        "projects",
        "project_hooks",
        "project_vars",
        "models",
        "model_config",
        "model_hooks",
        "model_tags",
        "model_meta",
        "model_columns",
        "model_column_constraints",
        "model_column_references",
        "model_dependencies",
        "model_external_dependencies",
        "sources",
        "source_tags",
        "source_tables",
        "source_columns",
        "functions",
        "function_args",
        "function_return_columns",
        "seeds",
        "seed_column_types",
        "tests",
        "singular_tests",
        "column_lineage",
        "compilation_runs",
        "diagnostics",
        "schema_mismatches",
        "rule_violations",
        "model_run_state",
        "model_run_input_checksums",
        "model_run_config",
    ];

    for table in &expected_tables {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'ff_meta' AND table_name = '{table}'"
        );
        assert_eq!(count(&db, &sql), 1, "Table ff_meta.{table} should exist");
    }
}

#[test]
fn all_views_exist() {
    let db = MetaDb::open_memory().unwrap();
    let expected_views = [
        "model_latest_state",
        "v_models",
        "v_columns",
        "v_lineage",
        "v_diagnostics",
        "v_source_columns",
    ];

    for view in &expected_views {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'ff_meta' AND table_name = '{view}' AND table_type = 'VIEW'"
        );
        assert_eq!(count(&db, &sql), 1, "View ff_meta.{view} should exist");
    }
}

// ── Transaction helper ─────────────────────────────────────────────────

#[test]
fn transaction_commits_on_success() {
    let db = MetaDb::open_memory().unwrap();
    db.transaction(|conn| {
        conn.execute(
            "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('tx_ok', '/tmp', '/tmp/db')",
            [],
        )
        .map_err(|e| crate::MetaError::QueryError(e.to_string()))?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM ff_meta.projects WHERE name = 'tx_ok'"
        ),
        1
    );
}

#[test]
fn transaction_rolls_back_on_error() {
    let db = MetaDb::open_memory().unwrap();
    let result: crate::MetaResult<()> = db.transaction(|conn| {
        conn.execute(
            "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('tx_fail', '/tmp', '/tmp/db')",
            [],
        )
        .map_err(|e| crate::MetaError::QueryError(e.to_string()))?;
        Err(crate::MetaError::QueryError("intentional failure".into()))
    });

    assert!(result.is_err());
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM ff_meta.projects WHERE name = 'tx_fail'"
        ),
        0,
        "Row should have been rolled back"
    );
}

// ── CHECK constraints ──────────────────────────────────────────────────

#[test]
fn check_project_materialization() {
    let db = MetaDb::open_memory().unwrap();
    for m in &["view", "table", "incremental"] {
        exec(
            &db,
            &format!(
                "INSERT INTO ff_meta.projects (name, root_path, db_path, materialization) \
                 VALUES ('p_{m}', '/r', '/d', '{m}')"
            ),
        );
    }
    expect_err(
        &db,
        "INSERT INTO ff_meta.projects (name, root_path, db_path, materialization) \
         VALUES ('p_bad', '/r', '/d', 'ephemeral')",
    );
}

#[test]
fn check_model_materialization() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "p1");
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.models (project_id, name, source_path, raw_sql, materialization) \
             VALUES ({pid}, 'm1', '/m.sql', 'SELECT 1', 'ephemeral')"
        ),
    );
}

#[test]
fn check_model_hook_type() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_hook");
    let model_id = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_hook'",
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_hooks (model_id, hook_type, sql_text, ordinal_position) \
             VALUES ({model_id}, 'invalid_hook', 'SELECT 1', 1)"
        ),
    );
}

#[test]
fn check_column_nullability_nullable() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_null");
    let model_id = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_null'",
    );

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position) \
             VALUES ({model_id}, 'col1', 1)"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position, nullability_declared) \
             VALUES ({model_id}, 'col2', 2, 'not_null')"
        ),
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position, nullability_declared) \
             VALUES ({model_id}, 'col3', 3, 'maybe')"
        ),
    );
}

#[test]
fn check_column_classification() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_cls");
    let model_id = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_cls'",
    );

    for cls in &["pii", "sensitive", "internal", "public"] {
        exec(
            &db,
            &format!(
                "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position, classification) \
                 VALUES ({model_id}, 'c_{cls}', 1, '{cls}')"
            ),
        );
        exec(
            &db,
            &format!("DELETE FROM ff_meta.model_columns WHERE name = 'c_{cls}'"),
        );
    }
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position, classification) \
             VALUES ({model_id}, 'c_bad', 1, 'secret')"
        ),
    );
}

#[test]
fn check_constraint_type() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_model_column(&db, "chk_con");
    let col_id = count(
        &db,
        "SELECT column_id FROM ff_meta.model_columns WHERE name = 'chk_con_col'",
    );

    for ct in &["not_null", "primary_key", "unique"] {
        exec(
            &db,
            &format!(
                "INSERT INTO ff_meta.model_column_constraints (column_id, constraint_type) \
                 VALUES ({col_id}, '{ct}')"
            ),
        );
    }
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_column_constraints (column_id, constraint_type) \
             VALUES ({col_id}, 'foreign_key')"
        ),
    );
}

#[test]
fn check_function_type() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "chk_fn");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.functions (project_id, name, function_type, sql_body, sql_path, yaml_path) \
             VALUES ({pid}, 'fn1', 'scalar', 'x+1', '/fn.sql', '/fn.yml')"
        ),
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.functions (project_id, name, function_type, sql_body, sql_path, yaml_path) \
             VALUES ({pid}, 'fn2', 'aggregate', 'SUM(x)', '/fn.sql', '/fn.yml')"
        ),
    );
}

#[test]
fn check_test_type_and_severity() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "chk_test");

    for tt in &[
        "not_null",
        "unique",
        "accepted_values",
        "relationship",
        "positive",
        "non_negative",
        "min_value",
        "max_value",
        "regex",
        "custom",
    ] {
        exec(
            &db,
            &format!("INSERT INTO ff_meta.tests (project_id, test_type) VALUES ({pid}, '{tt}')"),
        );
    }
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.tests (project_id, test_type) VALUES ({pid}, 'unknown_test')"
        ),
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.tests (project_id, test_type, severity) VALUES ({pid}, 'unique', 'info')"
        ),
    );
}

#[test]
fn check_lineage_kind() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_lk");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_lk'",
    );

    for kind in &["copy", "transform", "inspect"] {
        exec(
            &db,
            &format!(
                "INSERT INTO ff_meta.column_lineage (target_model_id, target_column, source_column, lineage_kind) \
                 VALUES ({mid}, 'tc', 'sc', '{kind}')"
            ),
        );
    }
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.column_lineage (target_model_id, target_column, source_column, lineage_kind) \
             VALUES ({mid}, 'tc', 'sc', 'aggregate')"
        ),
    );
}

#[test]
fn check_compilation_run_type_and_status() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "chk_cr");

    for rt in &["compile", "validate", "run", "analyze", "rules"] {
        exec(
            &db,
            &format!(
                "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, '{rt}')"
            ),
        );
    }
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'build')"
        ),
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES ({pid}, 'run', 'pending')"
        ),
    );
}

#[test]
fn check_model_run_state_status() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_mrs");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_mrs'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'run')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid}, 'success', now())"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'run')"
        ),
    );
    let rid2 = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid2}, 'pending', now())"
        ),
    );
}

#[test]
fn check_mismatch_type() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_mm");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_mm'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'validate')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    for mt in &["extra_in_sql", "type_mismatch", "nullability_mismatch"] {
        exec(
            &db,
            &format!(
                "INSERT INTO ff_meta.schema_mismatches (run_id, model_id, column_name, mismatch_type) \
                 VALUES ({rid}, {mid}, 'col', '{mt}')"
            ),
        );
    }
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.schema_mismatches (run_id, model_id, column_name, mismatch_type) \
             VALUES ({rid}, {mid}, 'col', 'name_mismatch')"
        ),
    );
}

#[test]
fn check_rule_violation_severity() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "chk_rv");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'rules')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.rule_violations (run_id, rule_name, rule_path, severity, message) \
             VALUES ({rid}, 'r1', '/r.sql', 'error', 'bad')"
        ),
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.rule_violations (run_id, rule_name, rule_path, severity, message) \
             VALUES ({rid}, 'r2', '/r.sql', 'info', 'note')"
        ),
    );
}

#[test]
fn check_project_hook_type() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "chk_ph");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.project_hooks (project_id, hook_type, sql_text, ordinal_position) \
             VALUES ({pid}, 'on_run_start', 'SELECT 1', 1)"
        ),
    );
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.project_hooks (project_id, hook_type, sql_text, ordinal_position) \
             VALUES ({pid}, 'before_run', 'SELECT 1', 1)"
        ),
    );
}

#[test]
fn check_model_run_config_materialization() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "chk_mrc");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'chk_mrc'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'run')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid}, 'success', now())"
        ),
    );

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_config (model_id, run_id, materialization) \
             VALUES ({mid}, {rid}, 'table')"
        ),
    );

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'run')"
        ),
    );
    let rid2 = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid2}, 'success', now())"
        ),
    );

    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_config (model_id, run_id, materialization) \
             VALUES ({mid}, {rid2}, 'ephemeral')"
        ),
    );
}

// ── Self-reference CHECK on model_dependencies ─────────────────────────

#[test]
fn model_dependency_self_reference_rejected() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "self_ref");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'self_ref'",
    );

    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_dependencies (model_id, depends_on_model_id) \
             VALUES ({mid}, {mid})"
        ),
    );
}

// ── Application-level cascade deletes via clear_project_data ───────────

#[test]
fn clear_project_data_removes_models_and_children() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "casc_del");
    let pid = count(
        &db,
        "SELECT project_id FROM ff_meta.projects WHERE name = 'casc_del_proj'",
    );
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'casc_del'",
    );

    exec(
        &db,
        &format!("INSERT INTO ff_meta.model_config (model_id) VALUES ({mid})"),
    );
    exec(
        &db,
        &format!("INSERT INTO ff_meta.model_tags (model_id, tag) VALUES ({mid}, 'finance')"),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_meta (model_id, key, value) VALUES ({mid}, 'owner', 'team')"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_hooks (model_id, hook_type, sql_text, ordinal_position) \
             VALUES ({mid}, 'pre_hook', 'SELECT 1', 1)"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position) \
             VALUES ({mid}, 'id', 1)"
        ),
    );
    let col_id = count(
        &db,
        "SELECT column_id FROM ff_meta.model_columns WHERE name = 'id'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_column_constraints (column_id, constraint_type) \
             VALUES ({col_id}, 'primary_key')"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_column_references (column_id, referenced_model_name, referenced_column_name) \
             VALUES ({col_id}, 'other', 'id')"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_external_dependencies (model_id, table_name) \
             VALUES ({mid}, 'raw_orders')"
        ),
    );

    db.clear_project_data(pid).unwrap();

    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.models"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_config"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_tags"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_meta"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_hooks"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_columns"), 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.model_column_constraints"),
        0
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.model_column_references"),
        0
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM ff_meta.model_external_dependencies"
        ),
        0
    );
}

#[test]
fn clear_project_data_removes_sources_and_children() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "casc_src");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.sources (project_id, name, schema_name) \
             VALUES ({pid}, 'raw', 'public')"
        ),
    );
    let sid = count(
        &db,
        "SELECT source_id FROM ff_meta.sources WHERE name = 'raw'",
    );
    exec(
        &db,
        &format!("INSERT INTO ff_meta.source_tags (source_id, tag) VALUES ({sid}, 'external')"),
    );
    exec(
        &db,
        &format!("INSERT INTO ff_meta.source_tables (source_id, name) VALUES ({sid}, 'orders')"),
    );
    let stid = count(
        &db,
        "SELECT source_table_id FROM ff_meta.source_tables WHERE name = 'orders'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.source_columns (source_table_id, name, data_type, ordinal_position) \
             VALUES ({stid}, 'id', 'INTEGER', 1)"
        ),
    );

    db.clear_project_data(pid).unwrap();

    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.sources"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.source_tags"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.source_tables"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.source_columns"), 0);
}

#[test]
fn clear_project_data_removes_compilation_runs_and_diagnostics() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "casc_cr");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'validate')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.diagnostics (run_id, code, severity, message, pass_name) \
             VALUES ({rid}, 'A001', 'warn', 'test diag', 'type_inference')"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.rule_violations (run_id, rule_name, rule_path, severity, message) \
             VALUES ({rid}, 'rule1', '/r.sql', 'error', 'violation')"
        ),
    );

    db.clear_project_data(pid).unwrap();

    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.compilation_runs"),
        0
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.diagnostics"), 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.rule_violations"),
        0
    );
}

#[test]
fn clear_project_data_removes_functions_and_children() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "casc_fn");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.functions (project_id, name, function_type, sql_body, sql_path, yaml_path) \
             VALUES ({pid}, 'my_fn', 'scalar', 'x+1', '/fn.sql', '/fn.yml')"
        ),
    );
    let fid = count(
        &db,
        "SELECT function_id FROM ff_meta.functions WHERE name = 'my_fn'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.function_args (function_id, name, data_type, ordinal_position) \
             VALUES ({fid}, 'x', 'INTEGER', 1)"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.function_return_columns (function_id, name, data_type, ordinal_position) \
             VALUES ({fid}, 'result', 'INTEGER', 1)"
        ),
    );

    db.clear_project_data(pid).unwrap();

    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.functions"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.function_args"), 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.function_return_columns"),
        0
    );
}

#[test]
fn clear_project_data_removes_seeds_and_column_types() {
    let db = MetaDb::open_memory().unwrap();
    let pid = insert_project(&db, "casc_seed");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.seeds (project_id, name, path) VALUES ({pid}, 'users', '/s.csv')"
        ),
    );
    let seed_id = count(
        &db,
        "SELECT seed_id FROM ff_meta.seeds WHERE name = 'users'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.seed_column_types (seed_id, column_name, data_type) \
             VALUES ({seed_id}, 'id', 'INTEGER')"
        ),
    );

    db.clear_project_data(pid).unwrap();

    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.seeds"), 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.seed_column_types"),
        0
    );
}

#[test]
fn clear_project_data_removes_model_run_config() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "casc_cfg");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'casc_cfg'",
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES ({pid}, 'run')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid}, 'success', now())"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_config (model_id, run_id, materialization) \
             VALUES ({mid}, {rid}, 'table')"
        ),
    );

    db.clear_project_data(pid).unwrap();

    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.model_run_state"),
        0
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM ff_meta.model_run_config"),
        0
    );
}

// ── model_latest_state view correctness ────────────────────────────────

#[test]
fn model_latest_state_returns_most_recent_success() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "ls_view");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'ls_view'",
    );

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES ({pid}, 'run', 'success')"
        ),
    );
    let rid1 = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES ({pid}, 'run', 'success')"
        ),
    );
    let rid2 = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid1}, 'success', '2024-01-01 00:00:00')"
        ),
    );
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid2}, 'success', '2024-01-01 00:00:00')"
        ),
    );

    let row_count = count(
        &db,
        &format!("SELECT COUNT(*) FROM ff_meta.model_latest_state WHERE model_id = {mid}"),
    );
    assert_eq!(row_count, 1, "Should return exactly one row per model");

    let latest_rid = count(
        &db,
        &format!("SELECT run_id FROM ff_meta.model_latest_state WHERE model_id = {mid}"),
    );
    assert_eq!(latest_rid, rid2, "Should return the higher run_id");
}

#[test]
fn model_latest_state_excludes_failed_runs() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "ls_fail");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'ls_fail'",
    );

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES ({pid}, 'run', 'success')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid}, 'error', now())"
        ),
    );

    assert_eq!(
        count(
            &db,
            &format!("SELECT COUNT(*) FROM ff_meta.model_latest_state WHERE model_id = {mid}")
        ),
        0,
        "Failed runs should not appear in model_latest_state"
    );
}

#[test]
fn model_latest_state_excludes_non_run_types() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "ls_type");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'ls_type'",
    );

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES ({pid}, 'compile', 'success')"
        ),
    );
    let rid = count(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");
    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, started_at) \
             VALUES ({mid}, {rid}, 'success', now())"
        ),
    );

    assert_eq!(
        count(
            &db,
            &format!("SELECT COUNT(*) FROM ff_meta.model_latest_state WHERE model_id = {mid}")
        ),
        0,
        "'compile' runs should not appear in model_latest_state"
    );
}

// ── Clear-and-repopulate cycle via clear_models ────────────────────────

#[test]
fn clear_and_repopulate_via_clear_models() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "repop_a");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    let mid = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'repop_a'",
    );

    exec(
        &db,
        &format!("INSERT INTO ff_meta.model_tags (model_id, tag) VALUES ({mid}, 'daily')"),
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.models"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_tags"), 1);

    db.clear_models(pid).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.models"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_tags"), 0);

    exec(
        &db,
        &format!(
            "INSERT INTO ff_meta.models (project_id, name, source_path, raw_sql) \
             VALUES ({pid}, 'repop_b', '/m.sql', 'SELECT 2')"
        ),
    );
    let mid2 = count(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'repop_b'",
    );
    exec(
        &db,
        &format!("INSERT INTO ff_meta.model_tags (model_id, tag) VALUES ({mid2}, 'weekly')"),
    );

    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.models"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.model_tags"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ff_meta.projects"), 1);
}

// ── Unique constraints ─────────────────────────────────────────────────

#[test]
fn project_name_unique() {
    let db = MetaDb::open_memory().unwrap();
    insert_project(&db, "unique_proj");
    expect_err(
        &db,
        "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('unique_proj', '/r2', '/d2')",
    );
}

#[test]
fn model_name_unique_per_project() {
    let db = MetaDb::open_memory().unwrap();
    insert_project_and_model(&db, "dup_model");
    let pid = count(&db, "SELECT project_id FROM ff_meta.projects LIMIT 1");
    expect_err(
        &db,
        &format!(
            "INSERT INTO ff_meta.models (project_id, name, source_path, raw_sql) \
             VALUES ({pid}, 'dup_model', '/other.sql', 'SELECT 2')"
        ),
    );
}

// ── Foreign key enforcement ────────────────────────────────────────────

#[test]
fn fk_model_requires_valid_project() {
    let db = MetaDb::open_memory().unwrap();
    expect_err(
        &db,
        "INSERT INTO ff_meta.models (project_id, name, source_path, raw_sql) \
         VALUES (99999, 'orphan', '/m.sql', 'SELECT 1')",
    );
}

#[test]
fn fk_model_config_requires_valid_model() {
    let db = MetaDb::open_memory().unwrap();
    expect_err(
        &db,
        "INSERT INTO ff_meta.model_config (model_id) VALUES (99999)",
    );
}

// ── Test helpers ───────────────────────────────────────────────────────

fn insert_project(db: &MetaDb, name: &str) -> i64 {
    exec(
        db,
        &format!(
            "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('{name}', '/root', '/db')"
        ),
    );
    count(
        db,
        &format!("SELECT project_id FROM ff_meta.projects WHERE name = '{name}'"),
    )
}

fn insert_project_and_model(db: &MetaDb, model_name: &str) {
    let proj_name = format!("{model_name}_proj");
    let pid = insert_project(db, &proj_name);
    exec(
        db,
        &format!(
            "INSERT INTO ff_meta.models (project_id, name, source_path, raw_sql) \
             VALUES ({pid}, '{model_name}', '/m.sql', 'SELECT 1')"
        ),
    );
}

fn insert_project_model_column(db: &MetaDb, prefix: &str) {
    insert_project_and_model(db, prefix);
    let mid = count(
        db,
        &format!("SELECT model_id FROM ff_meta.models WHERE name = '{prefix}'"),
    );
    exec(
        db,
        &format!(
            "INSERT INTO ff_meta.model_columns (model_id, name, ordinal_position) \
             VALUES ({mid}, '{prefix}_col', 1)"
        ),
    );
}
