use super::analysis::{Diagnostic, InferredColumn, LineageEdge, SchemaMismatch};
use super::execution::{ConfigSnapshot, InputChecksum, ModelRunRecord, ModelRunStatus};
#[cfg(test)]
use crate::MetaDb;
use ff_core::config::{DatabaseConfig, Materialization};
use ff_core::function::{FunctionArg, FunctionConfig, FunctionReturn, FunctionType};
use ff_core::model::testing::{SchemaTest, SingularTest, TestConfig, TestSeverity, TestType};
use ff_core::model::ModelConfig;
use ff_core::model::{
    ColumnConstraint, ColumnReference, DataClassification, ModelSchema, SchemaColumnDef,
    SchemaContract,
};
use ff_core::model_name::ModelName;
use ff_core::source::{SourceColumn, SourceFile, SourceKind, SourceTable};
use ff_core::{Config, FunctionDef, Model, Seed};
use std::collections::HashMap;
use std::path::PathBuf;

fn test_config() -> Config {
    Config {
        name: "test_project".to_string(),
        version: "1.0.0".to_string(),
        node_paths: vec![],
        model_paths: vec!["models".to_string()],
        macro_paths: vec!["macros".to_string()],
        source_paths: vec!["sources".to_string()],
        test_paths: vec!["tests".to_string()],
        function_paths: vec!["functions".to_string()],
        target_path: "target".to_string(),
        materialization: Materialization::View,
        schema: Some("main_schema".to_string()),
        wap_schema: None,
        dialect: ff_core::config::Dialect::default(),
        database: DatabaseConfig::default(),
        external_tables: vec![],
        vars: HashMap::new(),
        clean_targets: vec![],
        on_run_start: vec![],
        on_run_end: vec![],
        targets: HashMap::new(),
        analysis: Default::default(),
        data_classification: Default::default(),
        documentation: Default::default(),
        query_comment: Default::default(),
        rules: None,
        format: Default::default(),
    }
}

fn test_model(name: &str) -> Model {
    Model {
        name: ModelName::new(name),
        path: PathBuf::from(format!("models/{name}/{name}.sql")),
        raw_sql: format!("SELECT * FROM raw_{name}"),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: Default::default(),
        external_deps: Default::default(),
        schema: Some(ModelSchema {
            version: 1,
            name: Some(name.to_string()),
            description: Some(format!("{name} model")),
            owner: Some("data-team".to_string()),
            meta: HashMap::new(),
            tags: vec!["core".to_string()],
            contract: None,
            columns: vec![SchemaColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                description: Some("Primary key".to_string()),
                primary_key: true,
                constraints: vec![ColumnConstraint::NotNull, ColumnConstraint::PrimaryKey],
                tests: vec![],
                references: None,
                classification: None,
            }],
            deprecated: false,
            deprecation_message: None,
            ..Default::default()
        }),
        base_name: None,
        version: None,
        kind: ff_core::ModelKind::default(),
    }
}

fn test_source() -> SourceFile {
    SourceFile {
        kind: SourceKind::Sources,
        version: 1,
        name: ff_core::SourceName::new("raw_data"),
        description: Some("Raw data source".to_string()),
        database: Some("main".to_string()),
        schema: "raw".to_string(),
        owner: Some("platform-team".to_string()),
        tags: vec!["raw".to_string(), "external".to_string()],
        tables: vec![SourceTable {
            name: "customers".to_string(),
            identifier: None,
            description: Some("Customer table".to_string()),
            columns: vec![
                SourceColumn {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    description: Some("PK".to_string()),
                    tests: vec![],
                },
                SourceColumn {
                    name: "email".to_string(),
                    data_type: "VARCHAR".to_string(),
                    description: None,
                    tests: vec![],
                },
            ],
        }],
    }
}

fn test_function() -> FunctionDef {
    FunctionDef {
        name: ff_core::FunctionName::new("my_add"),
        function_type: FunctionType::Scalar,
        args: vec![
            FunctionArg {
                name: "a".to_string(),
                data_type: "INTEGER".to_string(),
                default: None,
                description: Some("First operand".to_string()),
            },
            FunctionArg {
                name: "b".to_string(),
                data_type: "INTEGER".to_string(),
                default: Some("0".to_string()),
                description: None,
            },
        ],
        returns: FunctionReturn::Scalar {
            data_type: "INTEGER".to_string(),
        },
        description: Some("Add two numbers".to_string()),
        sql_body: "a + b".to_string(),
        sql_path: PathBuf::from("functions/my_add/my_add.sql"),
        yaml_path: PathBuf::from("functions/my_add/my_add.yml"),
        config: FunctionConfig {
            schema: None,
            deterministic: true,
        },
    }
}

fn test_seed() -> Seed {
    Seed {
        name: ff_core::SeedName::new("countries"),
        path: PathBuf::from("models/countries/countries.csv"),
        description: Some("Country codes".to_string()),
        schema: Some("ref".to_string()),
        quote_columns: false,
        column_types: HashMap::from([("code".to_string(), "VARCHAR".to_string())]),
        delimiter: ',',
        enabled: true,
    }
}

fn open_meta() -> MetaDb {
    MetaDb::open_memory().unwrap()
}

fn insert_compilation_run(conn: &duckdb::Connection, project_id: i64, run_type: &str) -> i64 {
    conn.execute(
        "INSERT INTO ff_meta.compilation_runs (project_id, run_type) VALUES (?, ?)",
        duckdb::params![project_id, run_type],
    )
    .unwrap();
    conn.query_row(
        "SELECT run_id FROM ff_meta.compilation_runs WHERE project_id = ? ORDER BY run_id DESC LIMIT 1",
        duckdb::params![project_id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn populate_project_inserts_row() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        assert!(project_id > 0);

        let name: String = conn
            .query_row(
                "SELECT name FROM ff_meta.projects WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "test_project");
        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_project_hooks() {
    let meta = open_meta();
    let mut config = test_config();
    config.on_run_start = vec!["SET threads=4".to_string()];
    config.on_run_end = vec!["VACUUM".to_string(), "CHECKPOINT".to_string()];

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.project_hooks WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_project_vars_serialization() {
    let meta = open_meta();
    let mut config = test_config();
    config.vars.insert(
        "env".to_string(),
        serde_yaml::Value::String("dev".to_string()),
    );
    config.vars.insert(
        "threads".to_string(),
        serde_yaml::Value::Number(serde_yaml::Number::from(4)),
    );
    config
        .vars
        .insert("debug".to_string(), serde_yaml::Value::Bool(true));

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.project_vars WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);

        let (value, vtype): (String, String) = conn
            .query_row(
                "SELECT value, value_type FROM ff_meta.project_vars WHERE project_id = ? AND key = 'env'",
                duckdb::params![project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(value, "dev");
        assert_eq!(vtype, "string");

        let (value, vtype): (String, String) = conn
            .query_row(
                "SELECT value, value_type FROM ff_meta.project_vars WHERE project_id = ? AND key = 'threads'",
                duckdb::params![project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(value, "4");
        assert_eq!(vtype, "number");

        let (value, vtype): (String, String) = conn
            .query_row(
                "SELECT value, value_type FROM ff_meta.project_vars WHERE project_id = ? AND key = 'debug'",
                duckdb::params![project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(value, "true");
        assert_eq!(vtype, "bool");

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_models_returns_id_map() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        models.insert(ModelName::new("customers"), test_model("customers"));

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        assert_eq!(id_map.len(), 2);
        assert!(id_map.contains_key("orders"));
        assert!(id_map.contains_key("customers"));

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.models WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_model_with_config() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let mut model = test_model("inc_orders");
        model.config.materialized = Some(Materialization::Incremental);
        model.config.unique_key = Some("id".to_string());
        model.config.incremental_strategy = Some(ff_core::config::IncrementalStrategy::Merge);
        model.config.pre_hook = vec!["SET threads=1".to_string()];
        model.config.post_hook = vec!["ANALYZE".to_string()];
        model.config.wap = Some(true);

        let mut models = HashMap::new();
        models.insert(ModelName::new("inc_orders"), model);

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["inc_orders"];

        let mat: String = conn
            .query_row(
                "SELECT materialization FROM ff_meta.models WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mat, "incremental");

        let unique_key: Option<String> = conn
            .query_row(
                "SELECT unique_key FROM ff_meta.model_config WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unique_key.unwrap(), "id");

        let hook_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_hooks WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hook_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_model_tags_dedup() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let mut model = test_model("tagged");
        model.schema.as_mut().unwrap().tags = vec!["core".to_string(), "finance".to_string()];
        model.config.tags = vec!["finance".to_string(), "reporting".to_string()];

        let mut models = HashMap::new();
        models.insert(ModelName::new("tagged"), model);

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["tagged"];

        let tag_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_tags WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tag_count, 3, "finance should be deduplicated");

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_model_columns_with_constraints_and_references() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let mut model = test_model("orders");
        model.schema.as_mut().unwrap().columns = vec![
            SchemaColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                description: None,
                primary_key: true,
                constraints: vec![ColumnConstraint::NotNull, ColumnConstraint::PrimaryKey],
                tests: vec![],
                references: None,
                classification: Some(DataClassification::Internal),
            },
            SchemaColumnDef {
                name: "customer_id".to_string(),
                data_type: "INTEGER".to_string(),
                description: Some("FK to customers".to_string()),
                primary_key: false,
                constraints: vec![ColumnConstraint::NotNull],
                tests: vec![],
                references: Some(ColumnReference {
                    model: ModelName::new("customers"),
                    column: "id".to_string(),
                }),
                classification: None,
            },
            SchemaColumnDef {
                name: "email".to_string(),
                data_type: "VARCHAR".to_string(),
                description: None,
                primary_key: false,
                constraints: vec![],
                tests: vec![],
                references: None,
                classification: Some(DataClassification::Pii),
            },
        ];
        model.schema.as_mut().unwrap().contract = Some(SchemaContract { enforced: true });

        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), model);

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];

        let col_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_columns WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(col_count, 3);

        let constraint_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_column_constraints mc
                 JOIN ff_meta.model_columns c ON mc.column_id = c.column_id
                 WHERE c.model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(constraint_count, 3);

        let ref_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_column_references mr
                 JOIN ff_meta.model_columns c ON mr.column_id = c.column_id
                 WHERE c.model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ref_count, 1);

        let enforced: bool = conn
            .query_row(
                "SELECT contract_enforced FROM ff_meta.models WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(enforced);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_model_meta() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let mut model = test_model("meta_test");
        model.schema.as_mut().unwrap().meta = HashMap::from([
            (
                "team".to_string(),
                serde_yaml::Value::String("analytics".to_string()),
            ),
            (
                "priority".to_string(),
                serde_yaml::Value::Number(serde_yaml::Number::from(1)),
            ),
        ]);

        let mut models = HashMap::new();
        models.insert(ModelName::new("meta_test"), model);

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["meta_test"];

        let meta_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_meta WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(meta_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_sources_with_tables_and_columns() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let source = test_source();
        super::sources::populate_sources(conn, project_id, &[source])?;

        let source_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.sources WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 1);

        let tag_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.source_tags st
                 JOIN ff_meta.sources s ON st.source_id = s.source_id
                 WHERE s.project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tag_count, 2);

        let table_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.source_tables st
                 JOIN ff_meta.sources s ON st.source_id = s.source_id
                 WHERE s.project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 1);

        let col_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.source_columns sc
                 JOIN ff_meta.source_tables st ON sc.source_table_id = st.source_table_id
                 JOIN ff_meta.sources s ON st.source_id = s.source_id
                 WHERE s.project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(col_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_functions_with_args_and_return_columns() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let func = test_function();
        super::functions::populate_functions(conn, project_id, &[func])?;

        let func_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.functions WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(func_count, 1);

        let arg_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.function_args fa
                 JOIN ff_meta.functions f ON fa.function_id = f.function_id
                 WHERE f.project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(arg_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_table_function_with_return_columns() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let func = FunctionDef {
            name: ff_core::FunctionName::new("get_records"),
            function_type: FunctionType::Table,
            args: vec![],
            returns: FunctionReturn::Table {
                columns: vec![
                    ff_core::function::FunctionReturnColumn {
                        name: "id".to_string(),
                        data_type: "INTEGER".to_string(),
                    },
                    ff_core::function::FunctionReturnColumn {
                        name: "value".to_string(),
                        data_type: "VARCHAR".to_string(),
                    },
                ],
            },
            description: None,
            sql_body: "SELECT 1 AS id, 'test' AS value".to_string(),
            sql_path: PathBuf::from("functions/get_records/get_records.sql"),
            yaml_path: PathBuf::from("functions/get_records/get_records.yml"),
            config: FunctionConfig {
                schema: Some("analytics".to_string()),
                deterministic: false,
            },
        };
        super::functions::populate_functions(conn, project_id, &[func])?;

        let ret_col_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.function_return_columns frc
                 JOIN ff_meta.functions f ON frc.function_id = f.function_id
                 WHERE f.project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ret_col_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_seeds_with_column_types() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let seed = test_seed();
        super::seeds::populate_seeds(conn, project_id, &[seed])?;

        let seed_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.seeds WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(seed_count, 1);

        let type_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.seed_column_types sct
                 JOIN ff_meta.seeds s ON sct.seed_id = s.seed_id
                 WHERE s.project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(type_count, 1);

        let schema_name: Option<String> = conn
            .query_row(
                "SELECT schema_name FROM ff_meta.seeds WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(schema_name.unwrap(), "ref");

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_schema_tests() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;

        let tests = vec![
            SchemaTest {
                test_type: TestType::NotNull,
                column: "id".to_string(),
                model: ModelName::new("orders"),
                config: TestConfig::default(),
            },
            SchemaTest {
                test_type: TestType::Unique,
                column: "id".to_string(),
                model: ModelName::new("orders"),
                config: TestConfig {
                    severity: TestSeverity::Warn,
                    where_clause: Some("id > 0".to_string()),
                    ..Default::default()
                },
            },
            SchemaTest {
                test_type: TestType::AcceptedValues {
                    values: vec!["active".to_string(), "inactive".to_string()],
                    quote: true,
                },
                column: "status".to_string(),
                model: ModelName::new("orders"),
                config: TestConfig::default(),
            },
        ];

        super::tests::populate_schema_tests(conn, project_id, &tests, &id_map)?;

        let test_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.tests WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(test_count, 3);

        let (sev, where_cl): (String, Option<String>) = conn
            .query_row(
                "SELECT severity, where_clause FROM ff_meta.tests WHERE project_id = ? AND test_type = 'unique'",
                duckdb::params![project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(sev, "warn");
        assert_eq!(where_cl.unwrap(), "id > 0");

        let config_json: Option<String> = conn
            .query_row(
                "SELECT config_json FROM ff_meta.tests WHERE project_id = ? AND test_type = 'accepted_values'",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&config_json.unwrap()).unwrap();
        assert_eq!(parsed["values"], serde_json::json!(["active", "inactive"]));
        assert_eq!(parsed["quote"], serde_json::json!(true));

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_singular_tests() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let tests = vec![SingularTest {
            name: ff_core::TestName::new("assert_no_orphans"),
            path: PathBuf::from("tests/assert_no_orphans.sql"),
            sql: "SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)"
                .to_string(),
        }];

        super::tests::populate_singular_tests(conn, project_id, &tests)?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.singular_tests WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_empty_project() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        assert!(project_id > 0);

        let empty_models: HashMap<ModelName, Model> = HashMap::new();
        let id_map = super::models::populate_models(conn, project_id, &empty_models, &config)?;
        assert!(id_map.is_empty());

        super::sources::populate_sources(conn, project_id, &[])?;
        super::functions::populate_functions(conn, project_id, &[])?;
        super::seeds::populate_seeds(conn, project_id, &[])?;
        super::tests::populate_schema_tests(conn, project_id, &[], &id_map)?;
        super::tests::populate_singular_tests(conn, project_id, &[])?;

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_model_no_schema() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let model = Model {
            name: ModelName::new("bare"),
            path: PathBuf::from("models/bare/bare.sql"),
            raw_sql: "SELECT 1".to_string(),
            compiled_sql: None,
            config: ModelConfig::default(),
            depends_on: Default::default(),
            external_deps: Default::default(),
            schema: None,
            base_name: None,
            version: None,
            kind: ff_core::ModelKind::default(),
        };

        let mut models = HashMap::new();
        models.insert(ModelName::new("bare"), model);

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["bare"];

        let col_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_columns WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(col_count, 0);

        let meta_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_meta WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(meta_count, 0);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_project_load_orchestrator() {
    let meta = open_meta();

    let project = ff_core::Project::new(ff_core::project::ProjectParts {
        root: PathBuf::from("/tmp/project"),
        config: test_config(),
        models: {
            let mut m = HashMap::new();
            m.insert(ModelName::new("orders"), test_model("orders"));
            m
        },
        seeds: vec![test_seed()],
        tests: vec![SchemaTest {
            test_type: TestType::NotNull,
            column: "id".to_string(),
            model: ModelName::new("orders"),
            config: TestConfig::default(),
        }],
        singular_tests: vec![],
        sources: vec![test_source()],
        functions: vec![test_function()],
    });

    meta.transaction(|conn| {
        let project_id = super::populate_project_load(conn, &project)?;
        assert!(project_id > 0);

        let model_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.models WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(model_count, 1);

        let test_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.tests WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(test_count, 1);

        let source_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.sources WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 1);

        let seed_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.seeds WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(seed_count, 1);

        Ok(())
    })
    .unwrap();
}

#[test]
fn idempotent_repopulate() {
    let meta = open_meta();
    let config = test_config();

    let mut models = HashMap::new();
    models.insert(ModelName::new("orders"), test_model("orders"));

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        super::models::populate_models(conn, project_id, &models, &config)?;
        super::sources::populate_sources(conn, project_id, &[test_source()])?;
        Ok(())
    })
    .unwrap();

    meta.clear_project_data(1).unwrap();

    meta.transaction(|conn| {
        conn.execute(
            "DELETE FROM ff_meta.projects WHERE project_id = ?",
            duckdb::params![1i64],
        )
        .map_err(|e| crate::error::MetaError::PopulationError(e.to_string()))?;
        Ok(())
    })
    .unwrap();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        assert_eq!(id_map.len(), 1);
        super::sources::populate_sources(conn, project_id, &[test_source()])?;

        let source_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.sources WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 1);

        Ok(())
    })
    .unwrap();
}

// ─── Compilation (Batch 3) ───────────────────────────────────────

#[test]
fn update_model_compiled_sets_fields() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];

        let before: Option<String> = conn
            .query_row(
                "SELECT compiled_sql FROM ff_meta.models WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(before.is_none());

        super::compilation::update_model_compiled(
            conn,
            model_id,
            "SELECT id, name FROM raw_orders",
            "target/compiled/orders.sql",
            "abc123",
        )?;

        let (compiled, path, checksum): (Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT compiled_sql, compiled_path, sql_checksum FROM ff_meta.models WHERE model_id = ?",
                duckdb::params![model_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(compiled.unwrap(), "SELECT id, name FROM raw_orders");
        assert_eq!(path.unwrap(), "target/compiled/orders.sql");
        assert_eq!(checksum.unwrap(), "abc123");

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_model_dependencies() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("stg_orders"), test_model("stg_orders"));
        models.insert(ModelName::new("stg_customers"), test_model("stg_customers"));
        models.insert(ModelName::new("fct_orders"), test_model("fct_orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;

        let fct_id = id_map["fct_orders"];
        let stg_orders_id = id_map["stg_orders"];
        let stg_customers_id = id_map["stg_customers"];

        super::compilation::populate_dependencies(
            conn,
            fct_id,
            &[stg_orders_id, stg_customers_id],
        )?;

        let dep_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_dependencies WHERE model_id = ?",
                duckdb::params![fct_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dep_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_external_dependencies() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("stg_orders"), test_model("stg_orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["stg_orders"];

        super::compilation::populate_external_dependencies(
            conn,
            model_id,
            &["raw.orders", "raw.customers"],
        )?;

        let ext_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_external_dependencies WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ext_count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn self_reference_dependency_rejected() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];

        let result = super::compilation::populate_dependencies(conn, model_id, &[model_id]);
        assert!(result.is_err());

        Ok(())
    })
    .unwrap();
}

#[test]
fn clear_and_repopulate_dependencies() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("stg_orders"), test_model("stg_orders"));
        models.insert(ModelName::new("stg_customers"), test_model("stg_customers"));
        models.insert(ModelName::new("fct_orders"), test_model("fct_orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;

        let fct_id = id_map["fct_orders"];
        let stg_orders_id = id_map["stg_orders"];
        let stg_customers_id = id_map["stg_customers"];

        super::compilation::populate_dependencies(conn, fct_id, &[stg_orders_id])?;
        super::compilation::populate_external_dependencies(conn, fct_id, &["raw.legacy"])?;

        super::compilation::clear_model_dependencies(conn, fct_id)?;
        super::compilation::populate_dependencies(
            conn,
            fct_id,
            &[stg_orders_id, stg_customers_id],
        )?;

        let dep_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_dependencies WHERE model_id = ?",
                duckdb::params![fct_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dep_count, 2);

        let ext_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_external_dependencies WHERE model_id = ?",
                duckdb::params![fct_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ext_count, 0);

        Ok(())
    })
    .unwrap();
}

// ─── Analysis (Batch 4) ─────────────────────────────────────────

#[test]
fn populate_inferred_schemas_updates_columns() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];

        let inferred = vec![InferredColumn {
            model_id,
            column_name: "id".to_string(),
            inferred_type: Some("BIGINT".to_string()),
            nullability_inferred: Some("not_null".to_string()),
        }];

        super::analysis::populate_inferred_schemas(conn, &inferred)?;

        let (inf_type, inf_null): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT inferred_type, nullability_inferred FROM ff_meta.model_columns WHERE model_id = ? AND name = 'id'",
                duckdb::params![model_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(inf_type.unwrap(), "BIGINT");
        assert_eq!(inf_null.unwrap(), "not_null");

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_column_lineage_edges() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("stg_orders"), test_model("stg_orders"));
        models.insert(ModelName::new("fct_orders"), test_model("fct_orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;

        let stg_id = id_map["stg_orders"];
        let fct_id = id_map["fct_orders"];

        let edges = vec![
            LineageEdge {
                target_model_id: fct_id,
                target_column: "order_id".to_string(),
                source_model_id: Some(stg_id),
                source_table: None,
                source_column: "id".to_string(),
                lineage_kind: "copy".to_string(),
                is_direct: true,
            },
            LineageEdge {
                target_model_id: fct_id,
                target_column: "total".to_string(),
                source_model_id: Some(stg_id),
                source_table: None,
                source_column: "amount".to_string(),
                lineage_kind: "transform".to_string(),
                is_direct: true,
            },
            LineageEdge {
                target_model_id: stg_id,
                target_column: "id".to_string(),
                source_model_id: None,
                source_table: Some("raw.orders".to_string()),
                source_column: "order_id".to_string(),
                lineage_kind: "copy".to_string(),
                is_direct: true,
            },
        ];

        super::analysis::populate_column_lineage(conn, &edges)?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.column_lineage WHERE target_model_id = ?",
                duckdb::params![fct_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        let ext_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.column_lineage WHERE source_model_id IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ext_count, 1);

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_diagnostics_records() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];
        let run_id = insert_compilation_run(conn, project_id, "analyze");

        let diags = vec![
            Diagnostic {
                code: "A001".to_string(),
                severity: "warning".to_string(),
                message: "Unknown type for column foo".to_string(),
                model_id: Some(model_id),
                column_name: Some("foo".to_string()),
                hint: Some("Add type annotation".to_string()),
                pass_name: "plan_type_inference".to_string(),
            },
            Diagnostic {
                code: "A010".to_string(),
                severity: "warning".to_string(),
                message: "Nullable from LEFT JOIN".to_string(),
                model_id: Some(model_id),
                column_name: Some("bar".to_string()),
                hint: None,
                pass_name: "plan_nullability".to_string(),
            },
        ];

        super::analysis::populate_diagnostics(conn, run_id, &diags)?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.diagnostics WHERE run_id = ?",
                duckdb::params![run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        let (code, hint): (String, Option<String>) = conn
            .query_row(
                "SELECT code, hint FROM ff_meta.diagnostics WHERE run_id = ? AND code = 'A001'",
                duckdb::params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(code, "A001");
        assert_eq!(hint.unwrap(), "Add type annotation");

        Ok(())
    })
    .unwrap();
}

#[test]
fn populate_schema_mismatches_records() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];
        let run_id = insert_compilation_run(conn, project_id, "validate");

        let mismatches = vec![
            SchemaMismatch {
                model_id,
                column_name: "amount".to_string(),
                mismatch_type: "type_mismatch".to_string(),
                declared_value: Some("DECIMAL(10,2)".to_string()),
                inferred_value: Some("DOUBLE".to_string()),
            },
            SchemaMismatch {
                model_id,
                column_name: "extra_col".to_string(),
                mismatch_type: "extra_in_sql".to_string(),
                declared_value: None,
                inferred_value: Some("VARCHAR".to_string()),
            },
        ];

        super::analysis::populate_schema_mismatches(conn, run_id, &mismatches)?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.schema_mismatches WHERE run_id = ?",
                duckdb::params![run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        let (declared, inferred): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT declared_value, inferred_value FROM ff_meta.schema_mismatches WHERE run_id = ? AND mismatch_type = 'type_mismatch'",
                duckdb::params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(declared.unwrap(), "DECIMAL(10,2)");
        assert_eq!(inferred.unwrap(), "DOUBLE");

        Ok(())
    })
    .unwrap();
}

// ─── Execution (Batch 5) ────────────────────────────────────────

#[test]
fn record_model_run_success() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];
        let run_id = insert_compilation_run(conn, project_id, "run");

        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id,
                status: ModelRunStatus::Success,
                row_count: Some(42),
                sql_checksum: Some("sha256abc".to_string()),
                schema_checksum: Some("sha256def".to_string()),
                duration_ms: Some(150),
            },
        )?;

        let (status, rows, duration): (String, Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT status, row_count, duration_ms FROM ff_meta.model_run_state WHERE model_id = ? AND run_id = ?",
                duckdb::params![model_id, run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "success");
        assert_eq!(rows.unwrap(), 42);
        assert_eq!(duration.unwrap(), 150);

        Ok(())
    })
    .unwrap();
}

#[test]
fn multiple_runs_accumulate() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];

        let run1 = insert_compilation_run(conn, project_id, "run");
        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id: run1,
                status: ModelRunStatus::Success,
                row_count: Some(10),
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: Some(100),
            },
        )?;

        let run2 = insert_compilation_run(conn, project_id, "run");
        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id: run2,
                status: ModelRunStatus::Success,
                row_count: Some(20),
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: Some(200),
            },
        )?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_run_state WHERE model_id = ?",
                duckdb::params![model_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn model_latest_state_returns_most_recent_success() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];

        let run1 = insert_compilation_run(conn, project_id, "run");
        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id: run1,
                status: ModelRunStatus::Success,
                row_count: Some(10),
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: Some(100),
            },
        )?;

        let run2 = insert_compilation_run(conn, project_id, "run");
        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id: run2,
                status: ModelRunStatus::Error,
                row_count: None,
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: Some(50),
            },
        )?;

        let run3 = insert_compilation_run(conn, project_id, "run");
        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id: run3,
                status: ModelRunStatus::Success,
                row_count: Some(30),
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: Some(300),
            },
        )?;

        let (latest_run, rows): (i64, Option<i64>) = conn
            .query_row(
                "SELECT run_id, row_count FROM ff_meta.model_latest_state WHERE model_id = ?",
                duckdb::params![model_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(latest_run, run3);
        assert_eq!(rows.unwrap(), 30);

        Ok(())
    })
    .unwrap();
}

#[test]
fn record_input_checksums() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("stg_a"), test_model("stg_a"));
        models.insert(ModelName::new("stg_b"), test_model("stg_b"));
        models.insert(ModelName::new("fct"), test_model("fct"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;

        let fct_id = id_map["fct"];
        let stg_a_id = id_map["stg_a"];
        let stg_b_id = id_map["stg_b"];
        let run_id = insert_compilation_run(conn, project_id, "run");

        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id: fct_id,
                run_id,
                status: ModelRunStatus::Success,
                row_count: Some(100),
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: None,
            },
        )?;

        super::execution::record_input_checksums(
            conn,
            fct_id,
            run_id,
            &[
                InputChecksum {
                    upstream_model_id: stg_a_id,
                    checksum: "aaa111".to_string(),
                },
                InputChecksum {
                    upstream_model_id: stg_b_id,
                    checksum: "bbb222".to_string(),
                },
            ],
        )?;

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.model_run_input_checksums WHERE model_id = ? AND run_id = ?",
                duckdb::params![fct_id, run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        Ok(())
    })
    .unwrap();
}

#[test]
fn record_config_snapshot_for_drift_detection() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        let id_map = super::models::populate_models(conn, project_id, &models, &config)?;
        let model_id = id_map["orders"];
        let run_id = insert_compilation_run(conn, project_id, "run");

        super::execution::record_model_run(
            conn,
            &ModelRunRecord {
                model_id,
                run_id,
                status: ModelRunStatus::Success,
                row_count: Some(42),
                sql_checksum: None,
                schema_checksum: None,
                duration_ms: None,
            },
        )?;

        super::execution::record_config_snapshot(
            conn,
            model_id,
            run_id,
            &ConfigSnapshot {
                materialization: Materialization::Incremental,
                schema_name: Some("analytics".to_string()),
                unique_key: Some("id".to_string()),
                incremental_strategy: Some(ff_core::config::IncrementalStrategy::Merge),
                on_schema_change: Some(ff_core::config::OnSchemaChange::Fail),
            },
        )?;

        let (mat, uk): (String, Option<String>) = conn
            .query_row(
                "SELECT materialization, unique_key FROM ff_meta.model_run_config WHERE model_id = ? AND run_id = ?",
                duckdb::params![model_id, run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(mat, "incremental");
        assert_eq!(uk.unwrap(), "id");

        Ok(())
    })
    .unwrap();
}

// ─── Lifecycle (Batch 6) ────────────────────────────────────────

#[test]
fn begin_population_creates_run_and_clears_data() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let mut models = HashMap::new();
        models.insert(ModelName::new("orders"), test_model("orders"));
        super::models::populate_models(conn, project_id, &models, &config)?;
        super::sources::populate_sources(conn, project_id, &[test_source()])?;

        let model_count_before: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.models WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(model_count_before, 1);

        let run_id = super::lifecycle::begin_population(conn, project_id, "compile", None)?;
        assert!(run_id > 0);

        let model_count_after: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.models WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(model_count_after, 0);

        let source_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.sources WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 0);

        let (run_type, status): (String, String) = conn
            .query_row(
                "SELECT run_type, status FROM ff_meta.compilation_runs WHERE run_id = ?",
                duckdb::params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(run_type, "compile");
        assert_eq!(status, "running");

        Ok(())
    })
    .unwrap();
}

#[test]
fn complete_population_updates_status() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let run_id = super::lifecycle::begin_population(conn, project_id, "validate", None)?;

        super::lifecycle::complete_population(conn, run_id, "success")?;

        let (status, completed): (String, Option<String>) = conn
            .query_row(
                "SELECT status, completed_at::VARCHAR FROM ff_meta.compilation_runs WHERE run_id = ?",
                duckdb::params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "success");
        assert!(completed.is_some());

        Ok(())
    })
    .unwrap();
}

#[test]
fn complete_population_error_status() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let run_id = super::lifecycle::begin_population(conn, project_id, "run", None)?;

        super::lifecycle::complete_population(conn, run_id, "error")?;

        let status: String = conn
            .query_row(
                "SELECT status FROM ff_meta.compilation_runs WHERE run_id = ?",
                duckdb::params![run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "error");

        Ok(())
    })
    .unwrap();
}

#[test]
fn begin_population_with_node_selector() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;
        let run_id = super::lifecycle::begin_population(
            conn,
            project_id,
            "compile",
            Some("orders,+customers"),
        )?;

        let selector: Option<String> = conn
            .query_row(
                "SELECT node_selector FROM ff_meta.compilation_runs WHERE run_id = ?",
                duckdb::params![run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(selector.unwrap(), "orders,+customers");

        Ok(())
    })
    .unwrap();
}

#[test]
fn all_run_types_accepted() {
    let meta = open_meta();
    let config = test_config();

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        for run_type in &["compile", "validate", "run", "analyze", "rules"] {
            let run_id = super::lifecycle::begin_population(conn, project_id, run_type, None)?;
            assert!(run_id > 0);
        }

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.compilation_runs WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 5);

        Ok(())
    })
    .unwrap();
}

#[test]
fn project_hooks_survive_begin_population() {
    let meta = open_meta();
    let mut config = test_config();
    config.on_run_start = vec!["SET threads=4".to_string()];

    meta.transaction(|conn| {
        let project_id =
            super::project::populate_project(conn, &config, &PathBuf::from("/tmp/project"))?;

        let hook_count_before: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.project_hooks WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hook_count_before, 1);

        super::lifecycle::begin_population(conn, project_id, "compile", None)?;

        let hook_count_after: i64 = conn
            .query_row(
                "SELECT count(*) FROM ff_meta.project_hooks WHERE project_id = ?",
                duckdb::params![project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hook_count_after, 1, "project_hooks should survive clear");

        Ok(())
    })
    .unwrap();
}
