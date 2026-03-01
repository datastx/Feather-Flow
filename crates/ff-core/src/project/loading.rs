//! Project discovery and model loading

use crate::config::Config;
use crate::error::{CoreError, CoreResult};
use crate::function::FunctionDef;
use crate::model::{Model, ModelSchema, SchemaTest, SingularTest};
use crate::model_name::ModelName;
use crate::node::{NodeKind, NodeKindProbe};
use crate::seed::Seed;
use crate::source::SourceFile;
use std::collections::HashMap;
use std::path::Path;

use super::{Project, ProjectParts};

/// Recursively discover YAML files in a directory, probing each for a matching
/// `kind` and loading matches with a caller-supplied loader.
///
/// This is the shared logic behind `discover_functions_recursive` and
/// `discover_sources_recursive`.
pub(crate) fn discover_yaml_recursive<T, P, L>(
    dir: &Path,
    items: &mut Vec<T>,
    probe: P,
    load: L,
) -> CoreResult<()>
where
    P: Fn(&str) -> bool + Copy,
    L: Fn(&Path) -> CoreResult<T> + Copy,
{
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
            discover_yaml_recursive(&path, items, probe, load)?;
            continue;
        }
        if !path.extension().is_some_and(|e| e == "yml" || e == "yaml") {
            continue;
        }
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Cannot read {}: {}", path.display(), e);
                continue;
            }
        };
        if !probe(&content) {
            continue;
        }
        let item = load(&path)?;
        items.push(item);
    }
    Ok(())
}

/// Extract the file extension as a `&str`, returning `""` for paths without one.
fn file_extension_str(path: &Path) -> &str {
    path.extension().and_then(|e| e.to_str()).unwrap_or("")
}

/// Validate the directory name is valid UTF-8 and non-empty.
fn validate_dir_name(path: &Path) -> CoreResult<String> {
    match path.file_name().and_then(|n| n.to_str()) {
        Some(name) if !name.is_empty() => Ok(name.to_string()),
        _ => Err(CoreError::InvalidModelDirectory {
            path: path.display().to_string(),
            reason: "directory name is not valid UTF-8".to_string(),
        }),
    }
}

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

        if !config.external_tables.is_empty() {
            log::warn!("'external_tables' is deprecated. Use source files (kind: source) instead.");
        }

        let nodes = Self::discover_all_nodes(&root, &config)?;
        let (models, seeds, sources, functions) =
            (nodes.models, nodes.seeds, nodes.sources, nodes.functions);

        let tests: Vec<SchemaTest> = models.values().flat_map(|m| m.get_schema_tests()).collect();

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

            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with('.'))
            {
                continue;
            }

            if !path.is_dir() {
                let ext = file_extension_str(&path);
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

            Self::process_node_dir(&path, models, seeds, sources, functions)?;
        }

        Ok(())
    }

    /// Process a single node directory: probe its YAML kind and dispatch to
    /// the appropriate loader.
    fn process_node_dir(
        path: &Path,
        models: &mut HashMap<ModelName, Model>,
        seeds: &mut Vec<Seed>,
        sources: &mut Vec<SourceFile>,
        functions: &mut Vec<FunctionDef>,
    ) -> CoreResult<()> {
        let dir_name = validate_dir_name(path)?;

        let config_path = match find_yaml_path(&path.join(&dir_name)) {
            Some(p) => p,
            None => {
                return Err(CoreError::NodeMissingYaml {
                    directory: dir_name,
                });
            }
        };

        let content = std::fs::read_to_string(&config_path).map_err(|e| CoreError::IoWithPath {
            path: config_path.display().to_string(),
            source: e,
        })?;

        let probe: NodeKindProbe =
            serde_yaml::from_str(&content).map_err(|_| CoreError::NodeMissingKind {
                directory: dir_name.clone(),
            })?;

        let Some(raw_kind) = probe.kind else {
            return Err(CoreError::NodeMissingKind {
                directory: dir_name,
            });
        };

        match raw_kind.normalize() {
            NodeKind::Sql => Self::load_sql_node(path, &dir_name, &content, &config_path, models),
            NodeKind::Seed => Self::load_seed_node(path, &dir_name, seeds),
            NodeKind::Source => Self::load_source_node(&content, &config_path, sources),
            NodeKind::Function => Self::load_function_node(&content, &config_path, functions),
            NodeKind::Python => {
                Self::load_python_node(path, &dir_name, &content, &config_path, models)
            }
            kind => Err(CoreError::NodeUnsupportedKind {
                directory: dir_name,
                kind: kind.to_string(),
            }),
        }
    }

    /// Load a `kind: sql` node as a [`Model`].
    fn load_sql_node(
        dir: &Path,
        dir_name: &str,
        yaml_content: &str,
        yaml_path: &Path,
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

        let model = Model::from_file_with_schema_content(sql_path, yaml_content, yaml_path)?;

        if models.contains_key(model.name.as_str()) {
            return Err(CoreError::DuplicateModel {
                name: model.name.to_string(),
            });
        }

        models.insert(model.name.clone(), model);
        Ok(())
    }

    /// Load a `kind: python` node as a [`Model`].
    fn load_python_node(
        dir: &Path,
        dir_name: &str,
        yaml_content: &str,
        _yaml_path: &Path,
        models: &mut HashMap<ModelName, Model>,
    ) -> CoreResult<()> {
        let py_path = dir.join(format!("{}.py", dir_name));
        if !py_path.exists() {
            return Err(CoreError::NodeMissingDataFile {
                directory: dir_name.to_string(),
                kind: "python".to_string(),
                extension: "py".to_string(),
            });
        }

        let schema: ModelSchema =
            serde_yaml::from_str(yaml_content).map_err(|e| CoreError::ModelParseError {
                name: dir_name.to_string(),
                message: e.to_string(),
            })?;

        let model = Model::from_python_file(py_path, schema)?;

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

        let schema = match find_yaml_path(&csv_path) {
            Some(p) => ModelSchema::load(&p)?,
            None => {
                return Err(CoreError::MissingSchemaFile {
                    model: dir_name.to_string(),
                    expected_path: csv_path.with_extension("yml").display().to_string(),
                });
            }
        };

        let seed = Seed::from_schema(csv_path, &schema)?;
        seeds.push(seed);
        Ok(())
    }

    /// Load a `kind: source` node as a [`SourceFile`].
    fn load_source_node(
        yaml_content: &str,
        yaml_path: &Path,
        sources: &mut Vec<SourceFile>,
    ) -> CoreResult<()> {
        let source = SourceFile::load_from_str(yaml_content, yaml_path)?;
        sources.push(source);
        Ok(())
    }

    /// Load a `kind: function` node as a [`FunctionDef`].
    fn load_function_node(
        yaml_content: &str,
        yaml_path: &Path,
        functions: &mut Vec<FunctionDef>,
    ) -> CoreResult<()> {
        let func = FunctionDef::load_from_str(yaml_content, yaml_path)?;
        functions.push(func);
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

/// Locate a YAML config file by testing `.yml` then `.yaml` extensions.
///
/// `base` is the path *without* the final extension (or whose extension will
/// be replaced).  Returns `Some(path)` for the first variant that exists on
/// disk, or `None` if neither is found.
pub(crate) fn find_yaml_path(base: &Path) -> Option<std::path::PathBuf> {
    let yml = base.with_extension("yml");
    if yml.exists() {
        return Some(yml);
    }
    let yaml = base.with_extension("yaml");
    if yaml.exists() {
        return Some(yaml);
    }
    None
}
