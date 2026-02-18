//! Project discovery and model loading

use crate::config::Config;
use crate::error::{CoreError, CoreResult};
use crate::function::{discover_functions, FunctionDef};
use crate::model::schema::ModelKind;
use crate::model::{Model, ModelSchema, SingularTest};
use crate::model_name::ModelName;
use crate::node::{NodeKind, NodeKindProbe};
use crate::seed::Seed;
use crate::source::{discover_sources, SourceFile};
use std::collections::HashMap;
use std::path::Path;

use super::{Project, ProjectParts};

/// Collected results from unified node discovery.
struct DiscoveredNodes {
    models: HashMap<ModelName, Model>,
    seeds: Vec<Seed>,
    sources: Vec<SourceFile>,
    functions: Vec<FunctionDef>,
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
            log::warn!("'external_tables' is deprecated. Use source files (kind: source) instead.");
        }

        let (models, seeds, sources, functions) = if config.uses_node_paths() {
            // ── Unified node_paths discovery ──────────────────────────
            let nodes = Self::discover_all_nodes(&root, &config)?;

            // Also pick up any legacy-path sources/functions that aren't
            // covered by node_paths (allows gradual migration).
            let legacy_source_paths = config.source_paths_absolute(&root);
            let mut sources = nodes.sources;
            if !legacy_source_paths.is_empty() {
                let extra = discover_sources(&legacy_source_paths)?;
                sources.extend(extra);
            }

            let legacy_fn_paths = config.function_paths_absolute(&root);
            let mut functions = nodes.functions;
            if !legacy_fn_paths.is_empty() {
                let extra = discover_functions(&legacy_fn_paths)?;
                functions.extend(extra);
            }

            (nodes.models, nodes.seeds, sources, functions)
        } else {
            // ── Legacy per-type discovery ─────────────────────────────
            let (models, seeds) = Self::discover_models(&root, &config)?;
            let source_paths = config.source_paths_absolute(&root);
            let sources = discover_sources(&source_paths)?;
            let function_paths = config.function_paths_absolute(&root);
            let functions = discover_functions(&function_paths)?;
            (models, seeds, sources, functions)
        };

        let mut tests = Vec::new();
        for model in models.values() {
            tests.extend(model.get_schema_tests());
        }

        let singular_tests = Self::discover_singular_tests(&root, &config)?;

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

    // ── Unified node discovery ───────────────────────────────────────

    /// Discover all nodes from `node_paths` directories.
    ///
    /// Each direct child of a node path must be a directory containing a
    /// `.yml`/`.yaml` file with a `kind` field.  The kind determines the
    /// resource type and which companion files are expected:
    ///
    /// | kind       | companion file |
    /// |------------|----------------|
    /// | `sql`      | `<name>.sql`   |
    /// | `seed`     | `<name>.csv`   |
    /// | `source`   | *(none)*       |
    /// | `function` | `<name>.sql`   |
    fn discover_all_nodes(root: &Path, config: &Config) -> CoreResult<DiscoveredNodes> {
        let mut models = HashMap::new();
        let mut seeds = Vec::new();
        let mut sources = Vec::new();
        let mut functions = Vec::new();

        for node_path in config.node_paths_absolute(root) {
            if !node_path.exists() {
                continue;
            }

            Self::discover_nodes_flat(
                &node_path,
                &mut models,
                &mut seeds,
                &mut sources,
                &mut functions,
            )?;
        }

        seeds.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(DiscoveredNodes {
            models,
            seeds,
            sources,
            functions,
        })
    }

    /// Walk one node-path root and dispatch each directory by kind.
    fn discover_nodes_flat(
        dir: &Path,
        models: &mut HashMap<ModelName, Model>,
        seeds: &mut Vec<Seed>,
        sources: &mut Vec<SourceFile>,
        functions: &mut Vec<FunctionDef>,
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

            // Only directories are allowed at the node root
            if !path.is_dir() {
                let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                if matches!(ext, "sql" | "csv" | "yml" | "yaml" | "py") {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!(
                            "loose .{ext} files are not allowed at the node root — each resource must be in its own directory (nodes/<name>/<name>.{ext})"
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

            // Find the YAML config file
            let yml_path = path.join(format!("{}.yml", dir_name));
            let yaml_path = path.join(format!("{}.yaml", dir_name));

            let config_path = if yml_path.exists() {
                yml_path
            } else if yaml_path.exists() {
                yaml_path
            } else {
                return Err(CoreError::NodeMissingYaml {
                    directory: dir_name,
                });
            };

            // Probe the kind field
            let content =
                std::fs::read_to_string(&config_path).map_err(|e| CoreError::IoWithPath {
                    path: config_path.display().to_string(),
                    source: e,
                })?;

            let probe: NodeKindProbe =
                serde_yaml::from_str(&content).map_err(|_| CoreError::NodeMissingKind {
                    directory: dir_name.clone(),
                })?;

            let kind = match probe.kind {
                Some(k) => k.normalize(),
                None => {
                    return Err(CoreError::NodeMissingKind {
                        directory: dir_name,
                    });
                }
            };

            match kind {
                NodeKind::Sql => {
                    Self::load_sql_node(&path, &dir_name, models)?;
                }
                NodeKind::Seed => {
                    Self::load_seed_node(&path, &dir_name, seeds)?;
                }
                NodeKind::Source => {
                    Self::load_source_node(&config_path, sources)?;
                }
                NodeKind::Function => {
                    Self::load_function_node(&config_path, functions)?;
                }
                NodeKind::Python => {
                    return Err(CoreError::NodeUnsupportedKind {
                        directory: dir_name,
                        kind: "python".to_string(),
                    });
                }
                // Legacy variants are normalized away — unreachable after normalize()
                _ => {
                    return Err(CoreError::NodeUnsupportedKind {
                        directory: dir_name,
                        kind: kind.to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Load a `kind: sql` node as a [`Model`].
    fn load_sql_node(
        dir: &Path,
        dir_name: &str,
        models: &mut HashMap<ModelName, Model>,
    ) -> CoreResult<()> {
        let sql_path = dir.join(format!("{}.sql", dir_name));
        if !sql_path.exists() {
            return Err(CoreError::NodeMissingDataFile {
                directory: dir_name.to_string(),
                kind: "sql".to_string(),
                extension: "sql".to_string(),
            });
        }

        let model = Model::from_file(sql_path)?;

        if models.contains_key(model.name.as_str()) {
            return Err(CoreError::DuplicateModel {
                name: model.name.to_string(),
            });
        }

        models.insert(model.name.clone(), model);
        Ok(())
    }

    /// Load a `kind: seed` node as a [`Seed`].
    fn load_seed_node(dir: &Path, dir_name: &str, seeds: &mut Vec<Seed>) -> CoreResult<()> {
        let csv_path = dir.join(format!("{}.csv", dir_name));
        if !csv_path.exists() {
            return Err(CoreError::NodeMissingDataFile {
                directory: dir_name.to_string(),
                kind: "seed".to_string(),
                extension: "csv".to_string(),
            });
        }

        let yml_path = csv_path.with_extension("yml");
        let yaml_path = csv_path.with_extension("yaml");

        let schema = if yml_path.exists() {
            ModelSchema::load(&yml_path)?
        } else if yaml_path.exists() {
            ModelSchema::load(&yaml_path)?
        } else {
            return Err(CoreError::MissingSchemaFile {
                model: dir_name.to_string(),
                expected_path: yml_path.display().to_string(),
            });
        };

        let seed = Seed::from_schema(csv_path, &schema)?;
        seeds.push(seed);
        Ok(())
    }

    /// Load a `kind: source` node as a [`SourceFile`].
    fn load_source_node(yaml_path: &Path, sources: &mut Vec<SourceFile>) -> CoreResult<()> {
        let source = SourceFile::load(yaml_path)?;
        sources.push(source);
        Ok(())
    }

    /// Load a `kind: function` node as a [`FunctionDef`].
    fn load_function_node(yaml_path: &Path, functions: &mut Vec<FunctionDef>) -> CoreResult<()> {
        let func = FunctionDef::load(yaml_path)?;
        functions.push(func);
        Ok(())
    }

    // ── Legacy per-type discovery (backward compat) ──────────────────

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
    /// must contain exactly one `.sql` file (model) or one `.csv` file (seed),
    /// whose stem matches the directory name. A 1:1 YAML schema file is required.
    /// Seeds are identified by `kind: seed` in their YAML.
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
                if ext == "sql" || ext == "csv" {
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

            // Determine resource type based on files present
            let (data_path, is_seed) = match (sql_files.len(), csv_files.len()) {
                (1, 0) => (sql_files[0], false),
                (0, 1) => (csv_files[0], true),
                (0, 0) => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "directory contains no .sql or .csv files".to_string(),
                    });
                }
                (s, 0) if s > 1 => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!("directory contains {} .sql files (expected exactly 1)", s),
                    });
                }
                (0, c) if c > 1 => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!("directory contains {} .csv files (expected exactly 1)", c),
                    });
                }
                _ => {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: "directory contains both .sql and .csv files — each directory must contain exactly one data file".to_string(),
                    });
                }
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

            // Check for extra files (only .sql/.csv + .yml/.yaml are allowed)
            let extra_files: Vec<String> = all_visible_files
                .iter()
                .filter(|p| {
                    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                    ext != "sql" && ext != "csv" && ext != "yml" && ext != "yaml"
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

            if is_seed {
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
            } else {
                // Peek at YAML to ensure kind is not seed for a SQL directory
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

        Ok(())
    }

    // ── Tests ────────────────────────────────────────────────────────

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
