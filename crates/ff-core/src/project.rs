//! Project discovery and loading

use crate::config::Config;
use crate::error::{CoreError, CoreResult};
use crate::model::{Model, SchemaTest, SchemaYml};
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
        let models = Self::discover_models(&root, &config)?;

        // Collect tests from both:
        // 1. Legacy schema.yml files (backward compatibility)
        // 2. 1:1 schema files (.yml files matching model names)
        let mut tests = Self::discover_tests(&root, &config)?;

        // Also collect tests from 1:1 schema files loaded with each model
        for model in models.values() {
            tests.extend(model.get_schema_tests());
        }

        Ok(Self {
            root,
            config,
            models,
            tests,
        })
    }

    /// Discover all SQL model files in the project
    fn discover_models(root: &Path, config: &Config) -> CoreResult<HashMap<String, Model>> {
        let mut models = HashMap::new();

        for model_path in config.model_paths_absolute(root) {
            if !model_path.exists() {
                continue;
            }

            Self::discover_models_recursive(&model_path, &mut models)?;
        }

        Ok(models)
    }

    /// Recursively discover SQL files in a directory
    fn discover_models_recursive(
        dir: &Path,
        models: &mut HashMap<String, Model>,
    ) -> CoreResult<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                Self::discover_models_recursive(&path, models)?;
            } else if path.extension().is_some_and(|e| e == "sql") {
                let model = Model::from_file(path)?;

                if models.contains_key(&model.name) {
                    return Err(CoreError::DuplicateModel {
                        name: model.name.clone(),
                    });
                }

                models.insert(model.name.clone(), model);
            }
        }

        Ok(())
    }

    /// Discover schema tests from schema.yml files
    fn discover_tests(root: &Path, config: &Config) -> CoreResult<Vec<SchemaTest>> {
        let mut tests = Vec::new();

        for model_path in config.model_paths_absolute(root) {
            if !model_path.exists() {
                continue;
            }

            Self::discover_tests_recursive(&model_path, &mut tests)?;
        }

        Ok(tests)
    }

    /// Recursively discover schema.yml files
    fn discover_tests_recursive(dir: &Path, tests: &mut Vec<SchemaTest>) -> CoreResult<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                Self::discover_tests_recursive(&path, tests)?;
            } else if path
                .file_name()
                .is_some_and(|n| n == "schema.yml" || n == "schema.yaml")
            {
                let schema = SchemaYml::load(&path)?;
                tests.extend(schema.extract_tests());
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

        // Create models directory
        std::fs::create_dir_all(dir.path().join("models/staging")).unwrap();

        // Create a model file
        std::fs::write(
            dir.path().join("models/staging/stg_orders.sql"),
            "SELECT * FROM raw.orders",
        )
        .unwrap();

        // Create schema.yml
        std::fs::write(
            dir.path().join("models/schema.yml"),
            r#"
version: 1
models:
  - name: stg_orders
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
    }

    #[test]
    fn test_discover_tests() {
        let dir = setup_test_project();
        let project = Project::load(dir.path()).unwrap();

        assert_eq!(project.tests.len(), 1);
        assert_eq!(project.tests[0].model, "stg_orders");
        assert_eq!(project.tests[0].column, "order_id");
    }

    #[test]
    fn test_duplicate_model_detection() {
        let dir = TempDir::new().unwrap();

        // Create featherflow.yml
        std::fs::write(
            dir.path().join("featherflow.yml"),
            r#"
name: test_project
model_paths: ["models"]
"#,
        )
        .unwrap();

        // Create models directory with subdirectories
        std::fs::create_dir_all(dir.path().join("models/staging")).unwrap();
        std::fs::create_dir_all(dir.path().join("models/marts")).unwrap();

        // Create two model files with the same name in different directories
        std::fs::write(
            dir.path().join("models/staging/orders.sql"),
            "SELECT * FROM raw_orders",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/marts/orders.sql"),
            "SELECT * FROM staging_orders",
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
}
