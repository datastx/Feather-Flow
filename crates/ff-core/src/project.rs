//! Project discovery and loading

use crate::config::Config;
use crate::error::{CoreError, CoreResult};
use crate::exposure::{discover_exposures, Exposure};
use crate::metric::{discover_metrics, Metric};
use crate::model::{Model, SchemaTest, SingularTest};
use crate::source::{discover_sources, SourceFile};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Represents a Featherflow project
#[derive(Debug)]
pub struct Project {
    /// Project root directory
    pub root: PathBuf,

    /// Project configuration
    pub config: Config,

    /// Models discovered in the project
    pub models: HashMap<String, Model>,

    /// Schema tests from schema.yml files
    pub tests: Vec<SchemaTest>,

    /// Singular tests (standalone SQL test files)
    pub singular_tests: Vec<SingularTest>,

    /// Source definitions
    pub sources: Vec<SourceFile>,

    /// Exposure definitions
    pub exposures: Vec<Exposure>,

    /// Metric definitions
    pub metrics: Vec<Metric>,
}

impl Project {
    /// Load a project from a directory
    pub fn load(path: &Path) -> CoreResult<Self> {
        let root = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };

        if !root.exists() {
            return Err(CoreError::ProjectNotFound {
                path: root.display().to_string(),
            });
        }

        let config = Config::load_from_dir(&root)?;

        // Emit deprecation warning if external_tables is used
        if !config.external_tables.is_empty() {
            eprintln!(
                "Warning: 'external_tables' is deprecated. Use source_paths and source files (kind: sources) instead."
            );
        }

        let models = Self::discover_models(&root, &config)?;

        // Discover source definitions
        let source_paths = config.source_paths_absolute(&root);
        let sources = discover_sources(&root, &source_paths).unwrap_or_default();

        // Collect tests from 1:1 schema files loaded with each model
        let mut tests = Vec::new();
        for model in models.values() {
            tests.extend(model.get_schema_tests());
        }

        // Discover singular tests from test_paths
        let singular_tests = Self::discover_singular_tests(&root, &config)?;

        // Discover exposure definitions
        let exposure_paths = config.exposure_paths_absolute(&root);
        let exposures = discover_exposures(&exposure_paths);

        // Discover metric definitions
        let metric_paths = config.metric_paths_absolute(&root);
        let metrics = discover_metrics(&metric_paths);

        Ok(Self {
            root,
            config,
            models,
            tests,
            singular_tests,
            sources,
            exposures,
            metrics,
        })
    }

    /// Discover all SQL model files in the project using flat directory-per-model layout
    ///
    /// Each model lives in `models/<model_name>/<model_name>.sql + .yml`.
    /// Loose SQL files at the root of a model_path are rejected.
    fn discover_models(root: &Path, config: &Config) -> CoreResult<HashMap<String, Model>> {
        let mut models = HashMap::new();

        for model_path in config.model_paths_absolute(root) {
            if !model_path.exists() {
                continue;
            }

            Self::discover_models_flat(&model_path, &mut models)?;
        }

        Ok(models)
    }

    /// Discover models using flat directory-per-model layout
    ///
    /// Direct children of the models root MUST be directories. Each directory
    /// must contain exactly one `.sql` file whose stem matches the directory name.
    fn discover_models_flat(dir: &Path, models: &mut HashMap<String, Model>) -> CoreResult<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let dir_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                // Find SQL files in this directory (non-recursive)
                let sql_files: Vec<PathBuf> = std::fs::read_dir(&path)?
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "sql"))
                    .collect();

                if sql_files.is_empty() {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "directory contains no .sql files".to_string(),
                    });
                }

                if sql_files.len() > 1 {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!(
                            "directory contains {} .sql files (expected exactly 1)",
                            sql_files.len()
                        ),
                    });
                }

                let sql_path = &sql_files[0];
                let sql_stem = sql_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();

                if sql_stem != dir_name {
                    return Err(CoreError::ModelDirectoryMismatch {
                        directory: dir_name,
                        sql_file: sql_stem,
                    });
                }

                let model = Model::from_file(sql_path.clone())?;

                if models.contains_key(&model.name) {
                    return Err(CoreError::DuplicateModel {
                        name: model.name.clone(),
                    });
                }

                models.insert(model.name.clone(), model);
            } else if path.extension().is_some_and(|e| e == "sql") {
                return Err(CoreError::InvalidModelDirectory {
                    path: path.display().to_string(),
                    reason: "loose .sql files are not allowed at the model root — each model must be in its own directory (models/<name>/<name>.sql)".to_string(),
                });
            }
            // Ignore non-SQL files at root level (e.g., .gitkeep, README)
        }

        Ok(())
    }

    /// Discover singular tests from test_paths
    fn discover_singular_tests(root: &Path, config: &Config) -> CoreResult<Vec<SingularTest>> {
        let mut tests = Vec::new();

        for test_path in config.test_paths_absolute(root) {
            if !test_path.exists() {
                continue;
            }

            Self::discover_singular_tests_recursive(&test_path, &mut tests)?;
        }

        Ok(tests)
    }

    /// Recursively discover singular test SQL files
    fn discover_singular_tests_recursive(
        dir: &Path,
        tests: &mut Vec<SingularTest>,
    ) -> CoreResult<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                Self::discover_singular_tests_recursive(&path, tests)?;
            } else if path.extension().is_some_and(|e| e == "sql") {
                match SingularTest::from_file(path) {
                    Ok(test) => tests.push(test),
                    Err(e) => {
                        // Log warning but continue - don't fail on a single bad test file
                        eprintln!("Warning: Failed to load test file: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Get a model by name
    pub fn get_model(&self, name: &str) -> Option<&Model> {
        self.models.get(name)
    }

    /// Get a mutable model by name
    pub fn get_model_mut(&mut self, name: &str) -> Option<&mut Model> {
        self.models.get_mut(name)
    }

    /// Get all model names
    pub fn model_names(&self) -> Vec<&str> {
        self.models.keys().map(|s| s.as_str()).collect()
    }

    /// Resolve a model reference, handling version resolution
    ///
    /// If the reference is unversioned (e.g., "fct_orders"), resolves to the latest version.
    /// If the reference is versioned (e.g., "fct_orders_v1"), resolves to that specific version.
    ///
    /// Returns (resolved_name, warnings) where warnings contains any deprecation warnings.
    pub fn resolve_model_reference(&self, reference: &str) -> (Option<&Model>, Vec<String>) {
        let mut warnings = Vec::new();

        // First try exact match
        if let Some(model) = self.models.get(reference) {
            // Check for deprecation
            if model.is_deprecated() {
                let msg = model
                    .get_deprecation_message()
                    .unwrap_or("This model is deprecated");
                warnings.push(format!(
                    "Warning: Model '{}' is deprecated. {}",
                    reference, msg
                ));
            }
            return (Some(model), warnings);
        }

        // If no exact match and reference doesn't look versioned, try to find latest version
        let (parsed_base, _) = Model::parse_version(reference);
        if parsed_base.is_none() {
            // Unversioned reference - find all versions and return latest
            if let Some((name, model)) = self.get_latest_version(reference) {
                if model.is_versioned() {
                    // Resolved to a versioned model - this is normal, no warning needed
                }
                // Check for deprecation on resolved model
                if model.is_deprecated() {
                    let msg = model
                        .get_deprecation_message()
                        .unwrap_or("This model is deprecated");
                    warnings.push(format!("Warning: Model '{}' is deprecated. {}", name, msg));
                }
                return (Some(model), warnings);
            }
        }

        (None, warnings)
    }

    /// Get the latest version of a model by base name
    ///
    /// Returns the model with the highest version number, or the unversioned model if no versions exist.
    pub fn get_latest_version(&self, base_name: &str) -> Option<(&str, &Model)> {
        let mut candidates: Vec<(&str, &Model, Option<u32>)> = Vec::new();

        for (name, model) in &self.models {
            // Check if this model matches the base name
            let model_base = model.get_base_name();
            if model_base == base_name || name == base_name {
                candidates.push((name.as_str(), model, model.version));
            }
        }

        if candidates.is_empty() {
            return None;
        }

        // Sort by version (None treated as 0, so unversioned comes before v1)
        candidates.sort_by(|a, b| {
            let va = a.2.unwrap_or(0);
            let vb = b.2.unwrap_or(0);
            vb.cmp(&va) // Descending order, highest version first
        });

        candidates.first().map(|(name, model, _)| (*name, *model))
    }

    /// Get all versions of a model by base name
    pub fn get_all_versions(&self, base_name: &str) -> Vec<(&str, &Model)> {
        let mut versions: Vec<(&str, &Model)> = self
            .models
            .iter()
            .filter(|(_, model)| model.get_base_name() == base_name)
            .map(|(name, model)| (name.as_str(), model))
            .collect();

        // Sort by version ascending
        versions.sort_by(|a, b| {
            let va = a.1.version.unwrap_or(0);
            let vb = b.1.version.unwrap_or(0);
            va.cmp(&vb)
        });

        versions
    }

    /// Check if a model reference is to a non-latest version and return a warning if so
    pub fn check_version_warning(&self, reference: &str) -> Option<String> {
        if let Some(model) = self.models.get(reference) {
            if model.is_versioned() {
                let base_name = model.get_base_name();
                if let Some((latest_name, _)) = self.get_latest_version(base_name) {
                    if latest_name != reference {
                        return Some(format!(
                            "Warning: Model '{}' depends on '{}' which is not the latest version. Latest is '{}'.",
                            reference, reference, latest_name
                        ));
                    }
                }
            }
        }
        None
    }

    /// Get tests for a specific model
    pub fn tests_for_model(&self, model: &str) -> Vec<&SchemaTest> {
        self.tests.iter().filter(|t| t.model == model).collect()
    }

    /// Get the target directory path
    pub fn target_dir(&self) -> PathBuf {
        self.config.target_path_absolute(&self.root)
    }

    /// Get the compiled directory path
    pub fn compiled_dir(&self) -> PathBuf {
        self.target_dir()
            .join("compiled")
            .join(&self.config.name)
            .join("models")
    }

    /// Get the manifest path
    pub fn manifest_path(&self) -> PathBuf {
        self.target_dir().join("manifest.json")
    }

    /// Get source table names for dependency categorization
    pub fn source_table_names(&self) -> std::collections::HashSet<String> {
        crate::source::build_source_lookup(&self.sources)
    }

    /// Get all source names
    pub fn source_names(&self) -> Vec<&str> {
        self.sources.iter().map(|s| s.name.as_str()).collect()
    }

    /// Get all exposure names
    pub fn exposure_names(&self) -> Vec<&str> {
        self.exposures.iter().map(|e| e.name.as_str()).collect()
    }

    /// Get an exposure by name
    pub fn get_exposure(&self, name: &str) -> Option<&Exposure> {
        self.exposures.iter().find(|e| e.name == name)
    }

    /// Get exposures that depend on a specific model
    pub fn exposures_for_model(&self, model_name: &str) -> Vec<&Exposure> {
        self.exposures
            .iter()
            .filter(|e| e.depends_on_model(model_name))
            .collect()
    }

    /// Get all metric names
    pub fn metric_names(&self) -> Vec<&str> {
        self.metrics.iter().map(|m| m.name.as_str()).collect()
    }

    /// Get a metric by name
    pub fn get_metric(&self, name: &str) -> Option<&Metric> {
        self.metrics.iter().find(|m| m.name == name)
    }

    /// Get metrics that are based on a specific model
    pub fn metrics_for_model(&self, model_name: &str) -> Vec<&Metric> {
        self.metrics
            .iter()
            .filter(|m| m.model == model_name)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_project() -> TempDir {
        let dir = TempDir::new().unwrap();

        // Create featherflow.yml
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
external_tables:
  - raw.orders
"#,
        )
        .unwrap();

        // Create directory-per-model layout
        std::fs::create_dir_all(dir.path().join("models/stg_orders")).unwrap();

        // Create a model file
        std::fs::write(
            dir.path().join("models/stg_orders/stg_orders.sql"),
            "SELECT * FROM raw.orders",
        )
        .unwrap();

        // Create 1:1 schema file for the model
        std::fs::write(
            dir.path().join("models/stg_orders/stg_orders.yml"),
            r#"
version: 1
description: "Staged orders"
columns:
  - name: order_id
    tests:
      - unique
"#,
        )
        .unwrap();

        dir
    }

    #[test]
    fn test_load_project() {
        let dir = setup_test_project();
        let project = Project::load(dir.path()).unwrap();

        assert_eq!(project.config.name, "test_project");
        assert_eq!(project.models.len(), 1);
        assert!(project.models.contains_key("stg_orders"));
        assert!(project.sources.is_empty()); // No sources in this test
    }

    #[test]
    fn test_discover_tests() {
        let dir = setup_test_project();
        let project = Project::load(dir.path()).unwrap();

        // Tests come exclusively from 1:1 schema files
        assert_eq!(project.tests.len(), 1);
        assert!(project.tests.iter().all(|t| t.model == "stg_orders"));
        assert!(project.tests.iter().all(|t| t.column == "order_id"));
    }

    #[test]
    fn test_duplicate_model_detection() {
        let dir = TempDir::new().unwrap();

        // Create featherflow.yml with TWO model_paths roots
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models_a", "models_b"]
"#,
        )
        .unwrap();

        // Create directory-per-model in both roots with the same model name
        std::fs::create_dir_all(dir.path().join("models_a/orders")).unwrap();
        std::fs::create_dir_all(dir.path().join("models_b/orders")).unwrap();

        std::fs::write(
            dir.path().join("models_a/orders/orders.sql"),
            "SELECT * FROM raw_orders",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models_a/orders/orders.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models_b/orders/orders.sql"),
            "SELECT * FROM staging_orders",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models_b/orders/orders.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();

        // Should fail with DuplicateModel error
        let result = Project::load(dir.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CoreError::DuplicateModel { ref name } if name == "orders"),
            "Expected DuplicateModel error for 'orders', got: {:?}",
            err
        );
    }

    #[test]
    fn test_exposure_discovery() {
        let dir = TempDir::new().unwrap();

        // Create minimal featherflow.yml
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
exposure_paths: ["exposures"]
"#,
        )
        .unwrap();

        // Create models directory with a model (directory-per-model)
        std::fs::create_dir_all(dir.path().join("models/orders")).unwrap();
        std::fs::write(
            dir.path().join("models/orders/orders.sql"),
            "SELECT 1 AS id",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/orders/orders.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();

        // Create exposures directory
        std::fs::create_dir_all(dir.path().join("exposures")).unwrap();

        // Create exposure files
        std::fs::write(
            dir.path().join("exposures/revenue_dashboard.yml"),
            r#"
version: "1"
kind: exposure
name: revenue_dashboard
type: dashboard
owner:
  name: Analytics Team
depends_on:
  - orders
"#,
        )
        .unwrap();

        std::fs::write(
            dir.path().join("exposures/ml_model.yml"),
            r#"
version: "1"
kind: exposure
name: churn_predictor
type: ml_model
owner:
  name: Data Science Team
  email: ds@example.com
depends_on:
  - orders
"#,
        )
        .unwrap();

        // Load the project
        let project = Project::load(dir.path()).unwrap();

        // Verify exposures were discovered
        assert_eq!(project.exposures.len(), 2);

        // Verify exposure names
        let names: Vec<&str> = project.exposure_names();
        assert!(names.contains(&"revenue_dashboard"));
        assert!(names.contains(&"churn_predictor"));

        // Test get_exposure
        let dashboard = project.get_exposure("revenue_dashboard").unwrap();
        assert_eq!(dashboard.owner.name, "Analytics Team");

        // Test exposures_for_model
        let exposures = project.exposures_for_model("orders");
        assert_eq!(exposures.len(), 2);
    }

    #[test]
    fn test_exposure_discovery_empty_dir() {
        let dir = TempDir::new().unwrap();

        // Create minimal featherflow.yml
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        // Create models directory (directory-per-model)
        std::fs::create_dir_all(dir.path().join("models/orders")).unwrap();
        std::fs::write(
            dir.path().join("models/orders/orders.sql"),
            "SELECT 1 AS id",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/orders/orders.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();

        // Note: not creating exposures directory

        // Load the project - should succeed with empty exposures
        let project = Project::load(dir.path()).unwrap();
        assert!(project.exposures.is_empty());
    }

    #[test]
    fn test_versioned_model_discovery() {
        let dir = TempDir::new().unwrap();

        // Create minimal featherflow.yml
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        // Create models directory with versioned models (directory-per-model)
        std::fs::create_dir_all(dir.path().join("models/fct_orders")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders/fct_orders.sql"),
            "SELECT 1 AS id",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders/fct_orders.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("models/fct_orders_v2")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v2/fct_orders_v2.sql"),
            "SELECT 2 AS id",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v2/fct_orders_v2.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("models/fct_orders_v3")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v3/fct_orders_v3.sql"),
            "SELECT 3 AS id",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v3/fct_orders_v3.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();

        let project = Project::load(dir.path()).unwrap();

        // All models should be discovered
        assert_eq!(project.models.len(), 3);

        // Check version parsing
        let v2 = project.get_model("fct_orders_v2").unwrap();
        assert!(v2.is_versioned());
        assert_eq!(v2.get_version(), Some(2));
        assert_eq!(v2.get_base_name(), "fct_orders");

        let v3 = project.get_model("fct_orders_v3").unwrap();
        assert_eq!(v3.get_version(), Some(3));

        // Original model should not be versioned
        let original = project.get_model("fct_orders").unwrap();
        assert!(!original.is_versioned());
        assert_eq!(original.get_base_name(), "fct_orders");
    }

    #[test]
    fn test_resolve_latest_version() {
        let dir = TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("models/fct_orders_v1")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v1/fct_orders_v1.sql"),
            "SELECT 1",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v1/fct_orders_v1.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("models/fct_orders_v2")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v2/fct_orders_v2.sql"),
            "SELECT 2",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v2/fct_orders_v2.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();

        let project = Project::load(dir.path()).unwrap();

        // get_latest_version should return v2
        let (name, model) = project.get_latest_version("fct_orders").unwrap();
        assert_eq!(name, "fct_orders_v2");
        assert_eq!(model.get_version(), Some(2));

        // get_all_versions should return both in order
        let versions = project.get_all_versions("fct_orders");
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].0, "fct_orders_v1");
        assert_eq!(versions[1].0, "fct_orders_v2");
    }

    #[test]
    fn test_resolve_model_reference() {
        let dir = TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("models/fct_orders_v1")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v1/fct_orders_v1.sql"),
            "SELECT 1",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v1/fct_orders_v1.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("models/fct_orders_v2")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v2/fct_orders_v2.sql"),
            "SELECT 2",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v2/fct_orders_v2.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("models/dim_products")).unwrap();
        std::fs::write(
            dir.path().join("models/dim_products/dim_products.sql"),
            "SELECT 1",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/dim_products/dim_products.yml"),
            "version: 1\ncolumns:\n  - name: id\n",
        )
        .unwrap();

        let project = Project::load(dir.path()).unwrap();

        // Exact match for versioned model
        let (model, warnings) = project.resolve_model_reference("fct_orders_v1");
        assert!(model.is_some());
        assert_eq!(model.unwrap().name, "fct_orders_v1");
        assert!(warnings.is_empty());

        // Unversioned reference resolves to latest
        let (model, warnings) = project.resolve_model_reference("fct_orders");
        assert!(model.is_some());
        assert_eq!(model.unwrap().name, "fct_orders_v2");
        assert!(warnings.is_empty());

        // Exact match for unversioned model
        let (model, warnings) = project.resolve_model_reference("dim_products");
        assert!(model.is_some());
        assert_eq!(model.unwrap().name, "dim_products");
        assert!(warnings.is_empty());

        // Non-existent model
        let (model, _) = project.resolve_model_reference("non_existent");
        assert!(model.is_none());
    }

    #[test]
    fn test_missing_schema_file_enforcement() {
        let dir = TempDir::new().unwrap();

        // Create featherflow.yml (schema files are always required)
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        // Create a model directory without a matching .yml file
        std::fs::create_dir_all(dir.path().join("models/no_schema_model")).unwrap();
        std::fs::write(
            dir.path()
                .join("models/no_schema_model/no_schema_model.sql"),
            "SELECT 1 AS id",
        )
        .unwrap();

        // Should fail with MissingSchemaFile error
        let result = Project::load(dir.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CoreError::MissingSchemaFile { ref model, .. } if model == "no_schema_model"),
            "Expected MissingSchemaFile error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_deprecation_warning_in_resolution() {
        let dir = TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("models/fct_orders_v1")).unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v1/fct_orders_v1.sql"),
            "SELECT 1",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/fct_orders_v1/fct_orders_v1.yml"),
            r#"
version: 1
deprecated: true
deprecation_message: "Use fct_orders_v2 instead"
"#,
        )
        .unwrap();

        let project = Project::load(dir.path()).unwrap();

        // Reference to deprecated model should generate warning
        let (model, warnings) = project.resolve_model_reference("fct_orders_v1");
        assert!(model.is_some());
        assert!(!warnings.is_empty());
        assert!(warnings[0].contains("deprecated"));
        assert!(warnings[0].contains("Use fct_orders_v2 instead"));
    }
}
