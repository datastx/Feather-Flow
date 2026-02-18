use super::*;

/// Legacy schema.yml container type (only used in tests)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaYml {
    version: u32,
    #[serde(default)]
    models: Vec<SchemaModelDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaModelDef {
    name: String,
    #[serde(default)]
    columns: Vec<SchemaColumnDef>,
}

impl SchemaYml {
    fn parse(content: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(content)
    }

    fn extract_tests(&self) -> Vec<SchemaTest> {
        let mut tests = Vec::new();
        for model_def in &self.models {
            for column_def in &model_def.columns {
                for test_def in &column_def.tests {
                    if let Some(test_type) = parse_test_definition(test_def) {
                        tests.push(SchemaTest {
                            test_type,
                            column: column_def.name.clone(),
                            model: crate::model_name::ModelName::new(&model_def.name),
                            config: TestConfig::default(),
                        });
                    }
                }
            }
        }
        tests
    }
}

#[test]
fn test_parse_schema_yml() {
    let yaml = r#"
version: 1

models:
  - name: stg_orders
    columns:
      - name: order_id
        type: INTEGER
        tests:
          - unique
          - not_null
      - name: customer_id
        type: INTEGER
        tests:
          - not_null
"#;
    let schema = SchemaYml::parse(yaml).unwrap();
    assert_eq!(schema.version, 1);
    assert_eq!(schema.models.len(), 1);
    assert_eq!(schema.models[0].columns.len(), 2);
}

#[test]
fn test_extract_tests() {
    let yaml = r#"
version: 1
models:
  - name: stg_orders
    columns:
      - name: order_id
        type: INTEGER
        tests:
          - unique
          - not_null
"#;
    let schema = SchemaYml::parse(yaml).unwrap();
    let tests = schema.extract_tests();

    assert_eq!(tests.len(), 2);
    assert_eq!(tests[0].model, "stg_orders");
    assert_eq!(tests[0].column, "order_id");
    assert_eq!(tests[0].test_type, TestType::Unique);
}

#[test]
fn test_config_precedence_sql_wins() {
    use crate::config::Materialization;

    // Create a model with SQL config set
    let model = Model {
        name: ModelName::new("test"),
        path: std::path::PathBuf::from("test.sql"),
        raw_sql: String::new(),
        compiled_sql: None,
        config: ModelConfig {
            materialized: Some(Materialization::Table),
            schema: Some("sql_schema".to_string()),
            tags: vec![],
            unique_key: None,
            incremental_strategy: None,
            on_schema_change: None,
            pre_hook: vec![],
            post_hook: vec![],
            wap: None,
        },
        depends_on: HashSet::new(),
        external_deps: HashSet::new(),
        schema: Some(ModelSchema {
            ..Default::default()
        }),
        base_name: None,
        version: None,
        kind: ModelKind::default(),
    };

    // SQL config should win over project default
    assert_eq!(
        model.materialization(Materialization::View),
        Materialization::Table
    );
    assert_eq!(model.target_schema(None), Some("sql_schema"));
}

#[test]
fn test_config_precedence_falls_back_to_project_default() {
    use crate::config::Materialization;

    // Create a model with no SQL config — should fall back to project default
    let model = Model {
        name: ModelName::new("test"),
        path: std::path::PathBuf::from("test.sql"),
        raw_sql: String::new(),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: HashSet::new(),
        external_deps: HashSet::new(),
        schema: Some(ModelSchema {
            ..Default::default()
        }),
        base_name: None,
        version: None,
        kind: ModelKind::default(),
    };

    // Should use the passed-in project default
    assert_eq!(
        model.materialization(Materialization::View),
        Materialization::View
    );
    assert_eq!(model.target_schema(None), None);
    assert_eq!(
        model.target_schema(Some("default_schema")),
        Some("default_schema")
    );
}

#[test]
fn test_config_precedence_project_default() {
    use crate::config::Materialization;

    // Create a model with no config
    let model = Model {
        name: ModelName::new("test"),
        path: std::path::PathBuf::from("test.sql"),
        raw_sql: String::new(),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: HashSet::new(),
        external_deps: HashSet::new(),
        schema: None,
        base_name: None,
        version: None,
        kind: ModelKind::default(),
    };

    // Project default should be used
    assert_eq!(
        model.materialization(Materialization::View),
        Materialization::View
    );
    assert_eq!(
        model.target_schema(Some("default_schema")),
        Some("default_schema")
    );
}

#[test]
fn test_model_config_hooks_default() {
    let config = ModelConfig::default();
    assert!(config.pre_hook.is_empty());
    assert!(config.post_hook.is_empty());
}

#[test]
fn test_model_config_with_hooks() {
    let config = ModelConfig {
        materialized: None,
        schema: None,
        tags: vec![],
        unique_key: None,
        incremental_strategy: None,
        on_schema_change: None,
        pre_hook: vec!["CREATE INDEX IF NOT EXISTS idx_id ON {{ this }}(id)".to_string()],
        post_hook: vec![
            "ANALYZE {{ this }}".to_string(),
            "GRANT SELECT ON {{ this }} TO analyst".to_string(),
        ],
        wap: None,
    };
    assert_eq!(config.pre_hook.len(), 1);
    assert_eq!(config.post_hook.len(), 2);
    assert!(config.pre_hook[0].contains("CREATE INDEX"));
    assert!(config.post_hook[0].contains("ANALYZE"));
}

#[test]
fn test_parse_version_suffix() {
    // Standard version suffix
    let (base, version) = Model::parse_version("fct_orders_v2");
    assert_eq!(base, Some("fct_orders".to_string()));
    assert_eq!(version, Some(2));

    // Larger version number
    let (base, version) = Model::parse_version("stg_customers_v10");
    assert_eq!(base, Some("stg_customers".to_string()));
    assert_eq!(version, Some(10));

    // No version suffix
    let (base, version) = Model::parse_version("dim_products");
    assert_eq!(base, None);
    assert_eq!(version, None);

    // v at start (should NOT match)
    let (base, version) = Model::parse_version("v2_model");
    assert_eq!(base, None);
    assert_eq!(version, None);

    // Underscore but no number
    let (base, version) = Model::parse_version("model_vx");
    assert_eq!(base, None);
    assert_eq!(version, None);

    // Multiple underscores
    let (base, version) = Model::parse_version("my_cool_model_v3");
    assert_eq!(base, Some("my_cool_model".to_string()));
    assert_eq!(version, Some(3));
}

#[test]
fn test_model_version_methods() {
    // Create a versioned model
    let mut model = Model {
        name: ModelName::new("fct_orders_v2"),
        path: std::path::PathBuf::from("models/fct_orders_v2.sql"),
        raw_sql: "SELECT 1".to_string(),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: std::collections::HashSet::new(),
        external_deps: std::collections::HashSet::new(),
        schema: None,
        base_name: Some("fct_orders".to_string()),
        version: Some(2),
        kind: ModelKind::default(),
    };

    assert!(model.is_versioned());
    assert_eq!(model.get_version(), Some(2));
    assert_eq!(model.get_base_name(), "fct_orders");

    // Non-versioned model
    model.base_name = None;
    model.version = None;
    model.name = ModelName::new("dim_products");

    assert!(!model.is_versioned());
    assert_eq!(model.get_version(), None);
    assert_eq!(model.get_base_name(), "dim_products");
}

#[test]
fn test_deprecated_model_via_model() {
    let mut model = Model {
        name: ModelName::new("fct_orders_v1"),
        path: std::path::PathBuf::from("models/fct_orders_v1.sql"),
        raw_sql: "SELECT 1".to_string(),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: std::collections::HashSet::new(),
        external_deps: std::collections::HashSet::new(),
        schema: Some(ModelSchema {
            deprecated: true,
            deprecation_message: Some("Use v2".to_string()),
            ..Default::default()
        }),
        base_name: Some("fct_orders".to_string()),
        version: Some(1),
        kind: ModelKind::default(),
    };

    assert!(model.is_deprecated());
    assert_eq!(model.get_deprecation_message(), Some("Use v2"));

    // Non-deprecated model
    model.schema.as_mut().unwrap().deprecated = false;
    assert!(!model.is_deprecated());
}
