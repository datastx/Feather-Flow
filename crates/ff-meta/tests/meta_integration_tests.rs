//! Integration tests for meta database population and querying.
//!
//! These tests use the ff-meta crate API to populate an in-memory meta DB
//! with synthetic project data, then verify correctness via SQL queries.

use ff_core::config::Materialization;
use ff_core::function::{FunctionArg, FunctionConfig, FunctionReturn, FunctionType};
use ff_core::model::testing::TestDefinition;
use ff_core::model::{ModelConfig, ModelSchema, SchemaColumnDef};
use ff_core::model_name::ModelName;
use ff_core::source::{SourceColumn, SourceFile, SourceKind, SourceTable};
use ff_core::{Config, FunctionDef, Model, Project, ProjectParts, Seed, SeedName, SourceName};
use ff_meta::populate::analysis::{Diagnostic, LineageEdge, SchemaMismatch};
use ff_meta::populate::compilation::{populate_dependencies, update_model_compiled};
use ff_meta::populate::execution::{
    record_model_run, ConfigSnapshot, ModelRunRecord, ModelRunStatus,
};
use ff_meta::populate::lifecycle::{begin_population, complete_population};
use ff_meta::populate::populate_project_load;
use ff_meta::query::{execute_query, list_tables, table_row_count};
use ff_meta::MetaDb;
use std::collections::HashMap;
use std::path::PathBuf;

// ── Helpers ────────────────────────────────────────────────────────────

fn test_config() -> Config {
    serde_yaml::from_str(
        r#"
name: integration_test_project
version: "1.0.0"
database:
  default:
    type: duckdb
    path: ":memory:"
    schema: analytics
vars:
  env: test
on_run_start:
- "SELECT 1"
"#,
    )
    .unwrap()
}

fn make_model(name: &str) -> Model {
    Model {
        name: ModelName::new(name),
        path: PathBuf::from(format!("nodes/{name}/{name}.sql")),
        raw_sql: format!("SELECT * FROM raw_{name}"),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: Default::default(),
        external_deps: Default::default(),
        schema: Some(ModelSchema {
            version: 1,
            name: Some(name.to_string()),
            description: Some(format!("{name} model description")),
            owner: Some("data-team".to_string()),
            meta: HashMap::new(),
            tags: vec!["core".to_string()],
            columns: vec![
                SchemaColumnDef {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    description: Some("Primary key".to_string()),
                    description_ai_generated: None,
                    primary_key: true,
                    tests: vec![TestDefinition::Simple("not_null".to_string())],
                    references: None,
                    classification: None,
                },
                SchemaColumnDef {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    description: Some("Entity name".to_string()),
                    description_ai_generated: None,
                    primary_key: false,
                    tests: vec![],
                    references: None,
                    classification: None,
                },
            ],
            deprecated: false,
            deprecation_message: None,
            ..Default::default()
        }),
        base_name: None,
        version: None,
        kind: ff_core::ModelKind::default(),
    }
}

fn make_source() -> SourceFile {
    SourceFile {
        kind: SourceKind::Sources,
        version: 1,
        name: SourceName::new("raw_data"),
        description: Some("Raw data source".to_string()),
        description_ai_generated: None,
        database: Some("main".to_string()),
        schema: "raw".to_string(),
        owner: Some("platform-team".to_string()),
        tags: vec!["raw".to_string()],
        tables: vec![
            SourceTable {
                name: "customers".to_string(),
                identifier: None,
                description: Some("Customer data".to_string()),
                description_ai_generated: None,
                columns: vec![
                    SourceColumn {
                        name: "id".to_string(),
                        data_type: "INTEGER".to_string(),
                        description: Some("Customer ID".to_string()),
                        description_ai_generated: None,
                        tests: vec![],
                    },
                    SourceColumn {
                        name: "email".to_string(),
                        data_type: "VARCHAR".to_string(),
                        description: Some("Email address".to_string()),
                        description_ai_generated: None,
                        tests: vec![],
                    },
                ],
            },
            SourceTable {
                name: "orders".to_string(),
                identifier: None,
                description: Some("Order data".to_string()),
                description_ai_generated: None,
                columns: vec![SourceColumn {
                    name: "order_id".to_string(),
                    data_type: "INTEGER".to_string(),
                    description: Some("Order ID".to_string()),
                    description_ai_generated: None,
                    tests: vec![],
                }],
            },
        ],
    }
}

fn make_seed(name: &str) -> Seed {
    Seed {
        name: SeedName::new(name),
        path: PathBuf::from(format!("nodes/{name}/{name}.csv")),
        description: Some(format!("{name} seed data")),
        schema: None,
        quote_columns: false,
        column_types: HashMap::new(),
        delimiter: ',',
        enabled: true,
    }
}

fn make_function(name: &str) -> FunctionDef {
    FunctionDef {
        name: ff_core::FunctionName::new(name),
        function_type: FunctionType::Scalar,
        description: Some(format!("{name} function")),
        args: vec![FunctionArg {
            name: "x".to_string(),
            data_type: "DOUBLE".to_string(),
            default: None,
            description: Some("Input value".to_string()),
        }],
        returns: FunctionReturn::Scalar {
            data_type: "DOUBLE".to_string(),
        },
        sql_body: format!("{name}(x)"),
        sql_path: PathBuf::from(format!("nodes/{name}/{name}.sql")),
        yaml_path: PathBuf::from(format!("nodes/{name}/{name}.yml")),
        config: FunctionConfig::default(),
    }
}

fn build_test_project() -> Project {
    let config = test_config();
    let mut models = HashMap::new();
    for name in &["stg_customers", "stg_orders", "dim_customers", "fct_orders"] {
        models.insert(ModelName::new(*name), make_model(name));
    }

    Project::new(ProjectParts {
        config,
        root: PathBuf::from("/tmp/integration_test"),
        models,
        sources: vec![make_source()],
        seeds: vec![make_seed("raw_customers"), make_seed("raw_orders")],
        functions: vec![make_function("safe_divide")],
        tests: vec![],
        singular_tests: vec![],
    })
}

/// Count rows in a meta table by name.
fn count(db: &MetaDb, table: &str) -> i64 {
    table_row_count(db.conn(), table).unwrap()
}

/// Query a single i64 value.
fn query_i64(db: &MetaDb, sql: &str) -> i64 {
    db.conn()
        .query_row(sql, [], |row| row.get::<_, i64>(0))
        .unwrap()
}

/// Query a single optional string.
fn query_opt_string(db: &MetaDb, sql: &str) -> Option<String> {
    db.conn()
        .query_row(sql, [], |row| row.get::<_, Option<String>>(0))
        .unwrap()
}

// ============================================================
// Test 1: Full population cycle (project load)
// ============================================================

#[test]
fn test_populate_project_load_models() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    assert!(project_id > 0);

    // Verify project row
    let proj_name = query_opt_string(
        &db,
        &format!("SELECT name FROM ff_meta.projects WHERE project_id = {project_id}"),
    );
    assert_eq!(proj_name.as_deref(), Some("integration_test_project"));

    // Verify all 4 models are present
    assert_eq!(count(&db, "models"), 4);

    // Verify each model by name
    for name in &["stg_customers", "stg_orders", "dim_customers", "fct_orders"] {
        let model_count = query_i64(
            &db,
            &format!("SELECT COUNT(*) FROM ff_meta.models WHERE name = '{name}'"),
        );
        assert_eq!(model_count, 1, "Model '{name}' should exist in meta DB");
    }
}

#[test]
fn test_populate_project_load_sources() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    // Verify source
    assert_eq!(count(&db, "sources"), 1);
    let source_name = query_opt_string(&db, "SELECT name FROM ff_meta.sources LIMIT 1");
    assert_eq!(source_name.as_deref(), Some("raw_data"));

    // Verify source tables
    assert_eq!(count(&db, "source_tables"), 2);

    // Verify source columns
    assert_eq!(count(&db, "source_columns"), 3);
}

#[test]
fn test_populate_project_load_seeds() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    assert_eq!(count(&db, "seeds"), 2);

    let seed_names: Vec<String> = {
        let result =
            execute_query(db.conn(), "SELECT name FROM ff_meta.seeds ORDER BY name").unwrap();
        result
            .rows
            .into_iter()
            .filter_map(|r| r.into_iter().next())
            .collect()
    };
    assert!(seed_names.contains(&"raw_customers".to_string()));
    assert!(seed_names.contains(&"raw_orders".to_string()));
}

#[test]
fn test_populate_project_load_functions() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    assert_eq!(count(&db, "functions"), 1);

    let fn_name = query_opt_string(&db, "SELECT name FROM ff_meta.functions LIMIT 1");
    assert_eq!(fn_name.as_deref(), Some("safe_divide"));

    // Verify function args
    assert_eq!(count(&db, "function_args"), 1);
}

#[test]
fn test_populate_model_columns() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    // Each model has 2 columns, 4 models = 8 columns
    assert_eq!(count(&db, "model_columns"), 8);

    // Verify column details via view
    let result = execute_query(
        db.conn(),
        "SELECT model_name, column_name, declared_type FROM ff_meta.v_columns \
         WHERE model_name = 'stg_customers' ORDER BY ordinal_position",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], "id");
    assert_eq!(result.rows[0][2], "INTEGER");
    assert_eq!(result.rows[1][1], "name");
    assert_eq!(result.rows[1][2], "VARCHAR");
}

#[test]
fn test_populate_project_vars() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    assert_eq!(count(&db, "project_vars"), 1);

    let var_key = query_opt_string(&db, "SELECT key FROM ff_meta.project_vars LIMIT 1");
    assert_eq!(var_key.as_deref(), Some("env"));
}

#[test]
fn test_populate_project_hooks() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    assert_eq!(count(&db, "project_hooks"), 1);
}

// ============================================================
// Test 2: Compilation-phase population (dependencies)
// ============================================================

#[test]
fn test_populate_compilation_dependencies() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    // Get model IDs
    let stg_id = query_i64(
        &db,
        &format!(
            "SELECT model_id FROM ff_meta.models WHERE project_id = {project_id} AND name = 'stg_customers'"
        ),
    );
    let dim_id = query_i64(
        &db,
        &format!(
            "SELECT model_id FROM ff_meta.models WHERE project_id = {project_id} AND name = 'dim_customers'"
        ),
    );

    // Simulate compilation: dim_customers depends on stg_customers
    db.transaction(|conn| {
        update_model_compiled(
            conn,
            dim_id,
            "SELECT * FROM stg_customers",
            "target/compiled/dim_customers.sql",
            "abc123",
        )?;
        populate_dependencies(conn, dim_id, &[stg_id])
    })
    .unwrap();

    // Verify dependency
    let dep_count = query_i64(
        &db,
        &format!(
            "SELECT COUNT(*) FROM ff_meta.model_dependencies \
             WHERE model_id = {dim_id} AND depends_on_model_id = {stg_id}"
        ),
    );
    assert_eq!(dep_count, 1);

    // Verify compiled SQL was stored
    let compiled = query_opt_string(
        &db,
        &format!("SELECT compiled_sql FROM ff_meta.models WHERE model_id = {dim_id}"),
    );
    assert_eq!(compiled.as_deref(), Some("SELECT * FROM stg_customers"));
}

// ============================================================
// Test 3: Analysis diagnostics population
// ============================================================

#[test]
fn test_populate_analysis_diagnostics() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    let model_id = query_i64(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'fct_orders'",
    );

    // Create an analysis compilation run
    db.conn()
        .execute(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES (?, 'analyze')",
            duckdb::params![project_id],
        )
        .unwrap();
    let run_id = query_i64(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    let diagnostics = vec![
        Diagnostic {
            code: "A001".to_string(),
            severity: "warning".to_string(),
            message: "Unknown type for column 'amount'".to_string(),
            model_id: Some(model_id),
            column_name: Some("amount".to_string()),
            hint: Some("Declare type in YAML schema".to_string()),
            pass_name: "type_inference".to_string(),
        },
        Diagnostic {
            code: "A010".to_string(),
            severity: "warning".to_string(),
            message: "Column 'status' may be nullable from LEFT JOIN".to_string(),
            model_id: Some(model_id),
            column_name: Some("status".to_string()),
            hint: None,
            pass_name: "nullability".to_string(),
        },
    ];

    db.transaction(|conn| {
        ff_meta::populate::analysis::populate_diagnostics(conn, run_id, &diagnostics)
    })
    .unwrap();

    // Verify diagnostics were stored
    let diag_count = query_i64(
        &db,
        &format!("SELECT COUNT(*) FROM ff_meta.diagnostics WHERE run_id = {run_id}"),
    );
    assert_eq!(diag_count, 2);

    // Verify via diagnostics table joined with models
    let result = execute_query(
        db.conn(),
        &format!(
            "SELECT d.code, d.severity, d.message, m.name AS model_name, d.pass_name \
             FROM ff_meta.diagnostics d \
             LEFT JOIN ff_meta.models m ON d.model_id = m.model_id \
             WHERE d.run_id = {run_id} ORDER BY d.code"
        ),
    )
    .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], "A001");
    assert_eq!(result.rows[1][0], "A010");
}

// ============================================================
// Test 4: Schema mismatches
// ============================================================

#[test]
fn test_populate_schema_mismatches() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    let model_id = query_i64(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'stg_orders'",
    );

    // Create a compilation run for mismatches
    db.conn()
        .execute(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES (?, 'validate')",
            duckdb::params![project_id],
        )
        .unwrap();
    let run_id = query_i64(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    let mismatches = vec![
        SchemaMismatch {
            model_id,
            column_name: "extra_col".to_string(),
            mismatch_type: "extra_in_sql".to_string(),
            declared_value: None,
            inferred_value: Some("VARCHAR".to_string()),
        },
        SchemaMismatch {
            model_id,
            column_name: "id".to_string(),
            mismatch_type: "type_mismatch".to_string(),
            declared_value: Some("INTEGER".to_string()),
            inferred_value: Some("BIGINT".to_string()),
        },
    ];

    db.transaction(|conn| {
        ff_meta::populate::analysis::populate_schema_mismatches(conn, run_id, &mismatches)
    })
    .unwrap();

    let mismatch_count = query_i64(
        &db,
        &format!("SELECT COUNT(*) FROM ff_meta.schema_mismatches WHERE run_id = {run_id}"),
    );
    assert_eq!(mismatch_count, 2);
}

// ============================================================
// Test 5: Execution state recording
// ============================================================

#[test]
fn test_record_execution_state() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    let model_id = query_i64(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'dim_customers'",
    );

    // Create a run
    db.conn()
        .execute(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES (?, 'run', 'success')",
            duckdb::params![project_id],
        )
        .unwrap();
    let run_id = query_i64(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    // Record model run
    let record = ModelRunRecord {
        model_id,
        run_id,
        status: ModelRunStatus::Success,
        row_count: Some(1000),
        sql_checksum: Some("sha256:abc123".to_string()),
        schema_checksum: Some("sha256:def456".to_string()),
        duration_ms: Some(150),
    };

    db.transaction(|conn| record_model_run(conn, &record))
        .unwrap();

    // Verify run state
    let status = query_opt_string(
        &db,
        &format!(
            "SELECT status FROM ff_meta.model_run_state WHERE model_id = {model_id} AND run_id = {run_id}"
        ),
    );
    assert_eq!(status.as_deref(), Some("success"));

    // Verify via model_latest_state view
    let latest_run = query_i64(
        &db,
        &format!("SELECT run_id FROM ff_meta.model_latest_state WHERE model_id = {model_id}"),
    );
    assert_eq!(latest_run, run_id);

    // Record config snapshot
    let config = ConfigSnapshot {
        materialization: Materialization::View,
        schema_name: Some("analytics".to_string()),
        unique_key: None,
        incremental_strategy: None,
        on_schema_change: None,
    };

    db.transaction(|conn| {
        ff_meta::populate::execution::record_config_snapshot(conn, model_id, run_id, &config)
    })
    .unwrap();

    let mat = query_opt_string(
        &db,
        &format!(
            "SELECT materialization FROM ff_meta.model_run_config \
             WHERE model_id = {model_id} AND run_id = {run_id}"
        ),
    );
    assert_eq!(mat.as_deref(), Some("view"));
}

// ============================================================
// Test 6: Column lineage population
// ============================================================

#[test]
fn test_populate_column_lineage() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    let stg_id = query_i64(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'stg_customers'",
    );
    let dim_id = query_i64(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'dim_customers'",
    );

    let edges = vec![
        LineageEdge {
            target_model_id: dim_id,
            target_column: "id".to_string(),
            source_model_id: Some(stg_id),
            source_table: Some("stg_customers".to_string()),
            source_column: "id".to_string(),
            lineage_kind: "copy".to_string(),
            is_direct: true,
        },
        LineageEdge {
            target_model_id: dim_id,
            target_column: "name".to_string(),
            source_model_id: Some(stg_id),
            source_table: Some("stg_customers".to_string()),
            source_column: "name".to_string(),
            lineage_kind: "transform".to_string(),
            is_direct: true,
        },
    ];

    db.transaction(|conn| ff_meta::populate::analysis::populate_column_lineage(conn, &edges))
        .unwrap();

    assert_eq!(count(&db, "column_lineage"), 2);

    // Verify via lineage view
    let result = execute_query(
        db.conn(),
        "SELECT target_model, target_column, source_model, source_column, lineage_kind \
         FROM ff_meta.v_lineage ORDER BY target_column",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], "dim_customers");
    assert_eq!(result.rows[0][1], "id");
    assert_eq!(result.rows[0][2], "stg_customers");
    assert_eq!(result.rows[0][4], "copy");
}

// ============================================================
// Test 7: Compilation lifecycle (begin + complete)
// ============================================================

#[test]
fn test_compilation_lifecycle() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    // Begin a compile run
    let run_id =
        begin_population(db.conn(), project_id, "compile", Some("-n stg_customers")).unwrap();

    assert!(run_id > 0);

    // Verify running status
    let status = query_opt_string(
        &db,
        &format!("SELECT status FROM ff_meta.compilation_runs WHERE run_id = {run_id}"),
    );
    assert_eq!(status.as_deref(), Some("running"));

    // Verify node selector stored
    let selector = query_opt_string(
        &db,
        &format!("SELECT node_selector FROM ff_meta.compilation_runs WHERE run_id = {run_id}"),
    );
    assert_eq!(selector.as_deref(), Some("-n stg_customers"));

    // Complete the run
    complete_population(db.conn(), run_id, "success").unwrap();

    let final_status = query_opt_string(
        &db,
        &format!("SELECT status FROM ff_meta.compilation_runs WHERE run_id = {run_id}"),
    );
    assert_eq!(final_status.as_deref(), Some("success"));
}

// ============================================================
// Test 8: Query helpers (list_tables, table_row_count, execute_query)
// ============================================================

#[test]
fn test_query_helpers() {
    let db = MetaDb::open_memory().unwrap();

    // list_tables should return all meta tables
    let tables = list_tables(db.conn()).unwrap();
    assert!(tables.len() >= 20, "Should have at least 20 meta tables");
    assert!(tables.contains(&"models".to_string()));
    assert!(tables.contains(&"sources".to_string()));
    assert!(tables.contains(&"functions".to_string()));
    assert!(tables.contains(&"seeds".to_string()));

    // table_row_count on empty table
    assert_eq!(table_row_count(db.conn(), "models").unwrap(), 0);

    // execute_query
    let result = execute_query(db.conn(), "SELECT 1 AS num, 'hello' AS msg").unwrap();
    assert_eq!(result.columns, vec!["num", "msg"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], "hello");
}

// ============================================================
// Test 9: Smart build state query (is_model_modified)
// ============================================================

#[test]
fn test_is_model_modified() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    // No previous run => modified
    let modified = ff_meta::query::is_model_modified(
        db.conn(),
        "stg_customers",
        "checksum_v1",
        Some("schema_v1"),
        &HashMap::new(),
    )
    .unwrap();
    assert!(modified, "Model with no prior run should be modified");

    // Record a successful run
    let model_id = query_i64(
        &db,
        "SELECT model_id FROM ff_meta.models WHERE name = 'stg_customers'",
    );
    db.conn()
        .execute(
            "INSERT INTO ff_meta.compilation_runs (project_id, run_type, status) VALUES (?, 'run', 'success')",
            duckdb::params![project_id],
        )
        .unwrap();
    let run_id = query_i64(&db, "SELECT MAX(run_id) FROM ff_meta.compilation_runs");

    let record = ModelRunRecord {
        model_id,
        run_id,
        status: ModelRunStatus::Success,
        row_count: Some(100),
        sql_checksum: Some("checksum_v1".to_string()),
        schema_checksum: Some("schema_v1".to_string()),
        duration_ms: Some(50),
    };
    record_model_run(db.conn(), &record).unwrap();

    // Same checksums => not modified
    let modified = ff_meta::query::is_model_modified(
        db.conn(),
        "stg_customers",
        "checksum_v1",
        Some("schema_v1"),
        &HashMap::new(),
    )
    .unwrap();
    assert!(
        !modified,
        "Model with same checksums should not be modified"
    );

    // Changed SQL checksum => modified
    let modified = ff_meta::query::is_model_modified(
        db.conn(),
        "stg_customers",
        "checksum_v2",
        Some("schema_v1"),
        &HashMap::new(),
    )
    .unwrap();
    assert!(
        modified,
        "Model with changed SQL checksum should be modified"
    );
}

// ============================================================
// Test 10: Full population + clear + repopulate cycle
// ============================================================

#[test]
fn test_clear_and_repopulate_cycle() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    let project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    assert_eq!(count(&db, "models"), 4);
    assert_eq!(count(&db, "sources"), 1);

    // Clear and repopulate
    db.clear_models(project_id).unwrap();
    assert_eq!(count(&db, "models"), 0);
    assert_eq!(count(&db, "sources"), 0);

    // Repopulate
    db.transaction(|conn| {
        ff_meta::populate::models::populate_models(
            conn,
            project_id,
            &project.models,
            &project.config,
        )?;
        ff_meta::populate::sources::populate_sources(conn, project_id, &project.sources)
    })
    .unwrap();

    assert_eq!(count(&db, "models"), 4);
    assert_eq!(count(&db, "sources"), 1);
}

// ============================================================
// Test 11: Views return expected data
// ============================================================

#[test]
fn test_v_models_view() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    let result = execute_query(
        db.conn(),
        "SELECT name, materialization, description, owner \
         FROM ff_meta.v_models WHERE name = 'dim_customers'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "dim_customers");
    assert_eq!(result.rows[0][1], "view");
    assert_eq!(result.rows[0][2], "dim_customers model description");
    assert_eq!(result.rows[0][3], "data-team");
}

#[test]
fn test_v_source_columns_view() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    let result = execute_query(
        db.conn(),
        "SELECT source_name, table_name, column_name, data_type \
         FROM ff_meta.v_source_columns ORDER BY table_name, column_name",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 3);
    // customers.email, customers.id, orders.order_id
    assert_eq!(result.rows[0][1], "customers");
    assert_eq!(result.rows[0][2], "email");
}

// ============================================================
// Test 12: Model tags population
// ============================================================

#[test]
fn test_populate_model_tags() {
    let db = MetaDb::open_memory().unwrap();
    let project = build_test_project();

    db.transaction(|conn| populate_project_load(conn, &project))
        .unwrap();

    // Each model has 1 tag "core", 4 models = 4 tags
    assert_eq!(count(&db, "model_tags"), 4);
}
