//! Project discovery and model loading

use crate::config::Config;
use crate::error::{CoreError, CoreResult};
use crate::function::discover_functions;
use crate::model::schema::ModelKind;
use crate::model::{Model, ModelSchema, SingularTest};
use crate::model_name::ModelName;
use crate::seed::Seed;
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

        let (models, seeds) = Self::discover_models(&root, &config)?;

        let source_paths = config.source_paths_absolute(&root);
        let sources = discover_sources(&source_paths)?;

        let mut tests = Vec::new();
        for model in models.values() {
            tests.extend(model.get_schema_tests());
        }

        let singular_tests = Self::discover_singular_tests(&root, &config)?;

        let function_paths = config.function_paths_absolute(&root);
        let functions = discover_functions(&function_paths)?;

        Ok(Self::new(ProjectParts {
            root,
            config,
            models,
            seeds,
            tests,
            singular_tests,
            sources,
            functions,
        }))
    }

    /// Discover all model and seed directories in the project using flat directory-per-resource layout
    ///
    /// Each resource lives in `models/<name>/<name>.sql + .yml` (model) or
    /// `models/<name>/<name>.csv + .yml` (seed, with `kind: seed` in YAML).
    /// Loose files at the root of a model_path are rejected.
    fn discover_models(
        root: &Path,
        config: &Config,
    ) -> CoreResult<(HashMap<ModelName, Model>, Vec<Seed>)> {
        let mut models = HashMap::new();
        let mut seeds = Vec::new();

        for model_path in config.model_paths_absolute(root) {
            if !model_path.exists() {
                continue;
            }

            Self::discover_models_flat(&model_path, &mut models, &mut seeds)?;
        }

        // Sort seeds by name for consistent ordering
        seeds.sort_by(|a, b| a.name.cmp(&b.name));

        Ok((models, seeds))
    }

    /// Discover models and seeds using flat directory-per-resource layout
    ///
    /// Direct children of the models root MUST be directories. Each directory
    /// must contain exactly one `.sql` file (model), one `.py` file (python model),
    /// or one `.csv` file (seed), whose stem matches the directory name.
    /// A 1:1 YAML schema file is required.
    /// Seeds are identified by `kind: seed` in their YAML.
    /// Python models are identified by `kind: python` in their YAML.
    fn discover_models_flat(
        dir: &Path,
        models: &mut HashMap<ModelName, Model>,
        seeds: &mut Vec<Seed>,
    ) -> CoreResult<()> {
        for entry in std::fs::read_dir(dir).map_err(|e| CoreError::IoWithPath {
            path: dir.display().to_string(),
            source: e,
        })? {
            let entry = entry.map_err(|e| CoreError::IoWithPath {
                path: dir.display().to_string(),
                source: e,
            })?;
            let path = entry.path();

            if !path.is_dir() {
                let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                if ext == "sql" || ext == "csv" || ext == "py" {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!(
                            "loose .{ext} files are not allowed at the model root — each resource must be in its own directory (models/<name>/<name>.{ext})"
                        ),
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

            let all_visible_files: Vec<std::path::PathBuf> = std::fs::read_dir(&path)
                .map_err(|e| CoreError::IoWithPath {
                    path: path.display().to_string(),
                    source: e,
                })?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_file() && !is_hidden_file(p))
                .collect();

            let sql_files: Vec<&std::path::PathBuf> = all_visible_files
                .iter()
                .filter(|p| p.extension().is_some_and(|e| e == "sql"))
                .collect();

            let csv_files: Vec<&std::path::PathBuf> = all_visible_files
                .iter()
                .filter(|p| p.extension().is_some_and(|e| e == "csv"))
                .collect();

            let py_files: Vec<&std::path::PathBuf> = all_visible_files
                .iter()
                .filter(|p| p.extension().is_some_and(|e| e == "py"))
                .collect();

            /// Resource type detected from the files in a model directory
            enum ResourceType<'a> {
                Sql(&'a std::path::PathBuf),
                Csv(&'a std::path::PathBuf),
                Python(&'a std::path::PathBuf),
            }

            // Determine resource type based on files present (exactly one data file required)
            let resource = match (sql_files.len(), csv_files.len(), py_files.len()) {
                (1, 0, 0) => ResourceType::Sql(sql_files[0]),
                (0, 1, 0) => ResourceType::Csv(csv_files[0]),
                (0, 0, 1) => ResourceType::Python(py_files[0]),
                (0, 0, 0) => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "directory contains no .sql, .py, or .csv files".to_string(),
                    });
                }
                (s, 0, 0) if s > 1 => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!("directory contains {} .sql files (expected exactly 1)", s),
                    });
                }
                (0, c, 0) if c > 1 => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!("directory contains {} .csv files (expected exactly 1)", c),
                    });
                }
                (0, 0, p) if p > 1 => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!("directory contains {} .py files (expected exactly 1)", p),
                    });
                }
                _ => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "directory contains multiple data file types (.sql, .csv, .py) — each directory must contain exactly one data file".to_string(),
                    });
                }
            };

            let data_path = match &resource {
                ResourceType::Sql(p) | ResourceType::Csv(p) | ResourceType::Python(p) => *p,
            };

            let data_stem = match data_path.file_stem().and_then(|s| s.to_str()) {
                Some(stem) if !stem.is_empty() => stem.to_string(),
                _ => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: data_path.display().to_string(),
                        reason: "file name is not valid UTF-8".to_string(),
                    });
                }
            };

            if data_stem != dir_name {
                return Err(CoreError::ModelDirectoryMismatch {
                    directory: dir_name,
                    sql_file: data_stem,
                });
            }

            // Check for extra files (only .sql/.csv/.py + .yml/.yaml are allowed)
            let extra_files: Vec<String> = all_visible_files
                .iter()
                .filter(|p| {
                    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                    ext != "sql" && ext != "csv" && ext != "py" && ext != "yml" && ext != "yaml"
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

            match resource {
                ResourceType::Csv(_) => {
                    // Load YAML to confirm kind: seed and build Seed from it
                    let yml_path = data_path.with_extension("yml");
                    let yaml_path = data_path.with_extension("yaml");

                    let schema = if yml_path.exists() {
                        ModelSchema::load(&yml_path)?
                    } else if yaml_path.exists() {
                        ModelSchema::load(&yaml_path)?
                    } else {
                        return Err(CoreError::MissingSchemaFile {
                            model: dir_name,
                            expected_path: yml_path.display().to_string(),
                        });
                    };

                    if schema.kind != ModelKind::Seed {
                        return Err(CoreError::InvalidModelDirectory {
                            path: path.display().to_string(),
                            reason: format!(
                                "directory contains a .csv file but YAML declares kind: {} (expected kind: seed)",
                                schema.kind
                            ),
                        });
                    }

                    let seed = Seed::from_schema(data_path.to_path_buf(), &schema)?;
                    seeds.push(seed);
                }
                ResourceType::Python(_) => {
                    // Load YAML and confirm kind: python
                    let yml_path = data_path.with_extension("yml");
                    let yaml_path = data_path.with_extension("yaml");

                    let schema = if yml_path.exists() {
                        ModelSchema::load(&yml_path)?
                    } else if yaml_path.exists() {
                        ModelSchema::load(&yaml_path)?
                    } else {
                        return Err(CoreError::MissingSchemaFile {
                            model: dir_name,
                            expected_path: yml_path.display().to_string(),
                        });
                    };

                    if schema.kind != ModelKind::Python {
                        return Err(CoreError::InvalidModelDirectory {
                            path: path.display().to_string(),
                            reason: format!(
                                "directory contains a .py file but YAML declares kind: {} (expected kind: python)",
                                schema.kind
                            ),
                        });
                    }

                    let model = Model::from_python_file(data_path.to_path_buf(), schema)?;

                    if models.contains_key(model.name.as_str()) {
                        return Err(CoreError::DuplicateModel {
                            name: model.name.to_string(),
                        });
                    }

                    models.insert(model.name.clone(), model);
                }
                ResourceType::Sql(_) => {
                    // Peek at YAML to ensure kind is not seed/python for a SQL directory
                    let yml_path = data_path.with_extension("yml");
                    let yaml_path = data_path.with_extension("yaml");

                    let schema_path = if yml_path.exists() {
                        Some(&yml_path)
                    } else if yaml_path.exists() {
                        Some(&yaml_path)
                    } else {
                        None
                    };

                    if let Some(sp) = schema_path {
                        let schema = ModelSchema::load(sp)?;
                        if schema.kind == ModelKind::Seed {
                            return Err(CoreError::InvalidModelDirectory {
                                path: path.display().to_string(),
                                reason: "directory contains a .sql file but YAML declares kind: seed (expected kind: model or no kind field)".to_string(),
                            });
                        }
                        if schema.kind == ModelKind::Python {
                            return Err(CoreError::InvalidModelDirectory {
                                path: path.display().to_string(),
                                reason: "directory contains a .sql file but YAML declares kind: python (expected kind: model or no kind field)".to_string(),
                            });
                        }
                    }

                    let model = Model::from_file(data_path.clone())?;

                    if models.contains_key(model.name.as_str()) {
                        return Err(CoreError::DuplicateModel {
                            name: model.name.to_string(),
                        });
                    }

                    models.insert(model.name.clone(), model);
                }
            }
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
        for entry in std::fs::read_dir(dir).map_err(|e| CoreError::IoWithPath {
            path: dir.display().to_string(),
            source: e,
        })? {
            let entry = entry.map_err(|e| CoreError::IoWithPath {
                path: dir.display().to_string(),
                source: e,
            })?;
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

/// Returns `true` if the file's name starts with a dot (hidden on Unix)
fn is_hidden_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with('.'))
}
