use super::*;
use crate::MetaDb;

/// Helper to set up a meta DB with a project, model, and a successful run.
fn setup_with_run(
    sql_checksum: &str,
    schema_checksum: Option<&str>,
    input_checksums: &[(&str, &str)], // (upstream_model_name, checksum)
) -> MetaDb {
    let meta = MetaDb::open_memory().unwrap();
    let conn = meta.conn();

    // Insert project
    conn.execute(
        "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('test', '/tmp', '/tmp/dev.duckdb')",
        [],
    ).unwrap();

    let project_id: i64 = conn
        .query_row("SELECT project_id FROM ff_meta.projects", [], |r| r.get(0))
        .unwrap();

    // Insert model_a
    conn.execute(
        "INSERT INTO ff_meta.models (project_id, name, source_path, materialization, raw_sql) VALUES (?, 'model_a', 'models/model_a/model_a.sql', 'table', 'SELECT 1')",
        duckdb::params![project_id],
    ).unwrap();

    let model_a_id: i64 = conn
        .query_row(
            "SELECT model_id FROM ff_meta.models WHERE name = 'model_a'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // Insert upstream models for input checksums
    let mut upstream_ids: HashMap<String, i64> = HashMap::new();
    for (upstream_name, _) in input_checksums {
        conn.execute(
            "INSERT INTO ff_meta.models (project_id, name, source_path, materialization, raw_sql) VALUES (?, ?, ?, 'table', 'SELECT 1')",
            duckdb::params![project_id, upstream_name, format!("models/{0}/{0}.sql", upstream_name)],
        ).unwrap();

        let upstream_id: i64 = conn
            .query_row(
                "SELECT model_id FROM ff_meta.models WHERE name = ?",
                duckdb::params![upstream_name],
                |r| r.get(0),
            )
            .unwrap();
        upstream_ids.insert(upstream_name.to_string(), upstream_id);
    }

    // Create compilation_runs entry (needed for model_latest_state view)
    conn.execute(
        "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status, completed_at) VALUES (?, 'run', 'success', now())",
        duckdb::params![project_id],
    ).unwrap();

    let run_id: i64 = conn
        .query_row(
            "SELECT run_id FROM ff_meta.compilation_runs ORDER BY run_id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // Insert model_run_state
    conn.execute(
        "INSERT INTO ff_meta.model_run_state (model_id, run_id, status, sql_checksum, schema_checksum, started_at, completed_at) VALUES (?, ?, 'success', ?, ?, now(), now())",
        duckdb::params![model_a_id, run_id, sql_checksum, schema_checksum],
    ).unwrap();

    // Insert input checksums
    for (upstream_name, checksum) in input_checksums {
        let upstream_id = upstream_ids[*upstream_name];
        conn.execute(
            "INSERT INTO ff_meta.model_run_input_checksums (model_id, run_id, upstream_model_id, checksum) VALUES (?, ?, ?, ?)",
            duckdb::params![model_a_id, run_id, upstream_id, checksum],
        ).unwrap();
    }

    meta
}

#[test]
fn returns_true_when_no_previous_run() {
    let meta = MetaDb::open_memory().unwrap();
    let conn = meta.conn();

    conn.execute(
        "INSERT INTO ff_meta.projects (name, root_path, db_path) VALUES ('test', '/tmp', '/tmp/dev.duckdb')",
        [],
    ).unwrap();

    let project_id: i64 = conn
        .query_row("SELECT project_id FROM ff_meta.projects", [], |r| r.get(0))
        .unwrap();

    conn.execute(
        "INSERT INTO ff_meta.models (project_id, name, source_path, materialization, raw_sql) VALUES (?, 'model_a', 'models/model_a/model_a.sql', 'table', 'SELECT 1')",
        duckdb::params![project_id],
    ).unwrap();

    let result = is_model_modified(conn, "model_a", "abc123", None, &HashMap::new()).unwrap();
    assert!(result, "should return true when no previous run exists");
}

#[test]
fn returns_false_when_nothing_changed() {
    let meta = setup_with_run("checksum_abc", Some("schema_xyz"), &[]);

    let result = is_model_modified(
        meta.conn(),
        "model_a",
        "checksum_abc",
        Some("schema_xyz"),
        &HashMap::new(),
    )
    .unwrap();
    assert!(!result, "should return false when nothing changed");
}

#[test]
fn returns_true_when_sql_checksum_changed() {
    let meta = setup_with_run("checksum_abc", None, &[]);

    let result = is_model_modified(
        meta.conn(),
        "model_a",
        "checksum_CHANGED",
        None,
        &HashMap::new(),
    )
    .unwrap();
    assert!(result, "should return true when SQL checksum changed");
}

#[test]
fn returns_true_when_schema_checksum_changed() {
    let meta = setup_with_run("checksum_abc", Some("schema_old"), &[]);

    let result = is_model_modified(
        meta.conn(),
        "model_a",
        "checksum_abc",
        Some("schema_new"),
        &HashMap::new(),
    )
    .unwrap();
    assert!(result, "should return true when schema checksum changed");
}

#[test]
fn returns_true_when_schema_added() {
    let meta = setup_with_run("checksum_abc", None, &[]);

    let result = is_model_modified(
        meta.conn(),
        "model_a",
        "checksum_abc",
        Some("schema_new"),
        &HashMap::new(),
    )
    .unwrap();
    assert!(
        result,
        "should return true when schema added (was None, now Some)"
    );
}

#[test]
fn returns_true_when_upstream_checksum_changed() {
    let meta = setup_with_run("checksum_abc", None, &[("upstream_b", "upstream_hash_1")]);

    let mut inputs = HashMap::new();
    inputs.insert(
        "upstream_b".to_string(),
        "upstream_hash_CHANGED".to_string(),
    );

    let result = is_model_modified(meta.conn(), "model_a", "checksum_abc", None, &inputs).unwrap();
    assert!(
        result,
        "should return true when upstream input checksum changed"
    );
}

#[test]
fn returns_false_when_upstream_unchanged() {
    let meta = setup_with_run("checksum_abc", None, &[("upstream_b", "upstream_hash_1")]);

    let mut inputs = HashMap::new();
    inputs.insert("upstream_b".to_string(), "upstream_hash_1".to_string());

    let result = is_model_modified(meta.conn(), "model_a", "checksum_abc", None, &inputs).unwrap();
    assert!(!result, "should return false when upstream input unchanged");
}

#[test]
fn returns_true_when_new_upstream_added() {
    let meta = setup_with_run("checksum_abc", None, &[]);

    let conn = meta.conn();
    let project_id: i64 = conn
        .query_row("SELECT project_id FROM ff_meta.projects", [], |r| r.get(0))
        .unwrap();
    conn.execute(
        "INSERT INTO ff_meta.models (project_id, name, source_path, materialization, raw_sql) VALUES (?, 'new_upstream', 'models/new_upstream/new_upstream.sql', 'table', 'SELECT 1')",
        duckdb::params![project_id],
    ).unwrap();

    let mut inputs = HashMap::new();
    inputs.insert("new_upstream".to_string(), "some_hash".to_string());

    let result = is_model_modified(conn, "model_a", "checksum_abc", None, &inputs).unwrap();
    assert!(
        result,
        "should return true when a new upstream dependency was added"
    );
}

#[test]
fn returns_true_when_upstream_removed() {
    let meta = setup_with_run("checksum_abc", None, &[("upstream_b", "upstream_hash_1")]);

    let result = is_model_modified(
        meta.conn(),
        "model_a",
        "checksum_abc",
        None,
        &HashMap::new(),
    )
    .unwrap();
    assert!(
        result,
        "should return true when an upstream dependency was removed"
    );
}
