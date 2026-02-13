//! Project discovery and model loading

use crate::config::Config;
use crate::error::{CoreError, CoreResult};
use crate::exposure::discover_exposures;
use crate::function::discover_functions;
use crate::model::{Model, SingularTest};
use crate::model_name::ModelName;
use crate::source::discover_sources;
use std::collections::HashMap;
use std::path::Path;

use super::{Project, ProjectParts};

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
            log::warn!("'external_tables' is deprecated. Use source_paths and source files (kind: sources) instead.");
        }

        let models = Self::discover_models(&root, &config)?;

        let source_paths = config.source_paths_absolute(&root);
        let sources = discover_sources(&source_paths)?;

        let mut tests = Vec::new();
        for model in models.values() {
            tests.extend(model.get_schema_tests());
        }

        let singular_tests = Self::discover_singular_tests(&root, &config)?;

        let exposure_paths = config.exposure_paths_absolute(&root);
        let exposures = discover_exposures(&exposure_paths)?;

        let function_paths = config.function_paths_absolute(&root);
        let functions = discover_functions(&function_paths)?;

        Ok(Self::new(ProjectParts {
            root,
            config,
            models,
            tests,
            singular_tests,
            sources,
            exposures,
            functions,
        }))
    }

    /// Discover all SQL model files in the project using flat directory-per-model layout
    ///
    /// Each model lives in `models/<model_name>/<model_name>.sql + .yml`.
    /// Loose SQL files at the root of a model_path are rejected.
    fn discover_models(root: &Path, config: &Config) -> CoreResult<HashMap<ModelName, Model>> {
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
    fn discover_models_flat(dir: &Path, models: &mut HashMap<ModelName, Model>) -> CoreResult<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_dir() {
                if path.extension().is_some_and(|e| e == "sql") {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "loose .sql files are not allowed at the model root — each model must be in its own directory (models/<name>/<name>.sql)".to_string(),
                    });
                }
                continue;
            }

            let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) if !name.is_empty() => name.to_string(),
                _ => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "directory name is not valid UTF-8".to_string(),
                    });
                }
            };

            let all_visible_files: Vec<std::path::PathBuf> = std::fs::read_dir(&path)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.is_file()
                        && !p
                            .file_name()
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| n.starts_with('.'))
                })
                .collect();

            let sql_files: Vec<&std::path::PathBuf> = all_visible_files
                .iter()
                .filter(|p| p.extension().is_some_and(|e| e == "sql"))
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

            let sql_path = sql_files[0];
            let sql_stem = match sql_path.file_stem().and_then(|s| s.to_str()) {
                Some(stem) if !stem.is_empty() => stem.to_string(),
                _ => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: sql_path.display().to_string(),
                        reason: "SQL file name is not valid UTF-8".to_string(),
                    });
                }
            };

            if sql_stem != dir_name {
                return Err(CoreError::ModelDirectoryMismatch {
                    directory: dir_name,
                    sql_file: sql_stem,
                });
            }

            let extra_files: Vec<String> = all_visible_files
                .iter()
                .filter(|p| {
                    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                    ext != "sql" && ext != "yml" && ext != "yaml"
                })
                .map(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown")
                        .to_string()
                })
                .collect();

            if !extra_files.is_empty() {
                return Err(CoreError::ExtraFilesInModelDirectory {
                    directory: dir_name,
                    files: extra_files.join(", "),
                });
            }

            let model = Model::from_file(sql_path.clone())?;

            if models.contains_key(model.name.as_str()) {
                return Err(CoreError::DuplicateModel {
                    name: model.name.to_string(),
                });
            }

            models.insert(model.name.clone(), model);
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
                let test = SingularTest::from_file(path)?;
                tests.push(test);
            }
        }

        Ok(())
    }
}
