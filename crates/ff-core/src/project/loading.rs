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

/// Categorized files found in a model/node directory.
struct CategorizedFiles {
    /// All visible (non-hidden) files in the directory
    all_visible: Vec<std::path::PathBuf>,
    /// `.sql` files
    sql: Vec<std::path::PathBuf>,
    /// `.csv` files
    csv: Vec<std::path::PathBuf>,
    /// `.py` files
    py: Vec<std::path::PathBuf>,
}

/// Scan a directory and categorize its visible files by extension.
fn categorize_dir_files(dir: &Path) -> CoreResult<CategorizedFiles> {
    let all_visible: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| CoreError::IoWithPath {
            path: dir.display().to_string(),
            source: e,
        })?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && !is_hidden_file(p))
        .collect();

    let mut sql = Vec::new();
    let mut csv = Vec::new();
    let mut py = Vec::new();
    for p in &all_visible {
        match file_extension_str(p) {
            "sql" => sql.push(p.clone()),
            "csv" => csv.push(p.clone()),
            "py" => py.push(p.clone()),
            _ => {}
        }
    }

    Ok(CategorizedFiles {
        all_visible,
        sql,
        csv,
        py,
    })
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

        let tests: Vec<_> = models.values().flat_map(|m| m.get_schema_tests()).collect();

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
                let ext = file_extension_str(&path);
                if matches!(ext, "sql" | "csv" | "py") {
                    return Err(CoreError::InvalidModelDirectory {
                        path: path.display().to_string(),
                        reason: format!(
                            "loose .{ext} files are not allowed at the model root — each resource must be in its own directory (models/<name>/<name>.{ext})"
                        ),
                    });
                }
                continue;
            }

            Self::process_legacy_model_dir(&path, models, seeds)?;
        }

        Ok(())
    }

    /// Process a single legacy model directory: detect resource type from
    /// file extensions, validate naming, and dispatch to the appropriate loader.
    fn process_legacy_model_dir(
        path: &Path,
        models: &mut HashMap<ModelName, Model>,
        seeds: &mut Vec<Seed>,
    ) -> CoreResult<()> {
        let dir_name = validate_dir_name(path)?;
        let files = categorize_dir_files(path)?;
        let resource = classify_resource_type(path, &files)?;

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

        check_no_extra_files(&files.all_visible, &dir_name)?;

        match resource {
            ResourceType::Csv(_) => {
                let seed = Self::load_seed_resource(data_path, &dir_name, path)?;
                seeds.push(seed);
            }
            ResourceType::Python(_) => {
                let model = Self::load_python_resource(data_path, &dir_name, path)?;
                Self::insert_model(models, model)?;
            }
            ResourceType::Sql(_) => {
                let model = Self::load_sql_resource(data_path, path)?;
                Self::insert_model(models, model)?;
            }
        }

        Ok(())
    }

    // ── Resource loaders for discover_models_flat ────────────────────

    fn load_required_schema(data_path: &Path, dir_name: &str) -> CoreResult<ModelSchema> {
        match find_yaml_path(data_path) {
            Some(p) => ModelSchema::load(&p),
            None => Err(CoreError::MissingSchemaFile {
                model: dir_name.to_string(),
                expected_path: data_path.with_extension("yml").display().to_string(),
            }),
        }
    }

    fn load_seed_resource(data_path: &Path, dir_name: &str, dir_path: &Path) -> CoreResult<Seed> {
        let schema = Self::load_required_schema(data_path, dir_name)?;

        if schema.kind != ModelKind::Seed {
            return Err(CoreError::InvalidModelDirectory {
                path: dir_path.display().to_string(),
                reason: format!(
                    "directory contains a .csv file but YAML declares kind: {} (expected kind: seed)",
                    schema.kind
                ),
            });
        }

        Seed::from_schema(data_path.to_path_buf(), &schema)
    }

    fn load_python_resource(
        data_path: &Path,
        dir_name: &str,
        dir_path: &Path,
    ) -> CoreResult<Model> {
        let schema = Self::load_required_schema(data_path, dir_name)?;

        if schema.kind != ModelKind::Python {
            return Err(CoreError::InvalidModelDirectory {
                path: dir_path.display().to_string(),
                reason: format!(
                    "directory contains a .py file but YAML declares kind: {} (expected kind: python)",
                    schema.kind
                ),
            });
        }

        Model::from_python_file(data_path.to_path_buf(), schema)
    }

    fn load_sql_resource(data_path: &Path, dir_path: &Path) -> CoreResult<Model> {
        if let Some(sp) = find_yaml_path(data_path) {
            let schema = ModelSchema::load(&sp)?;
            if schema.kind == ModelKind::Seed {
                return Err(CoreError::InvalidModelDirectory {
                    path: dir_path.display().to_string(),
                    reason: "directory contains a .sql file but YAML declares kind: seed (expected kind: model or no kind field)".to_string(),
                });
            }
            if schema.kind == ModelKind::Python {
                return Err(CoreError::InvalidModelDirectory {
                    path: dir_path.display().to_string(),
                    reason: "directory contains a .sql file but YAML declares kind: python (expected kind: model or no kind field)".to_string(),
                });
            }
        }

        Model::from_file(data_path.to_path_buf())
    }

    fn insert_model(models: &mut HashMap<ModelName, Model>, model: Model) -> CoreResult<()> {
        if models.contains_key(model.name.as_str()) {
            return Err(CoreError::DuplicateModel {
                name: model.name.to_string(),
            });
        }
        models.insert(model.name.clone(), model);
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

/// What kind of data file a legacy model directory contains.
enum ResourceType<'a> {
    Sql(&'a Path),
    Csv(&'a Path),
    Python(&'a Path),
}

/// Determine the single resource type in a legacy model directory.
///
/// Exactly one of `.sql`, `.csv`, or `.py` must be present. Returns an error
/// if the directory has zero data files, multiples of one kind, or a mix of kinds.
fn classify_resource_type<'a>(
    dir_path: &Path,
    files: &'a CategorizedFiles,
) -> CoreResult<ResourceType<'a>> {
    match (files.sql.len(), files.csv.len(), files.py.len()) {
        (1, 0, 0) => Ok(ResourceType::Sql(&files.sql[0])),
        (0, 1, 0) => Ok(ResourceType::Csv(&files.csv[0])),
        (0, 0, 1) => Ok(ResourceType::Python(&files.py[0])),
        (0, 0, 0) => Err(CoreError::InvalidModelDirectory {
            path: dir_path.display().to_string(),
            reason: "directory contains no .sql, .py, or .csv files".to_string(),
        }),
        (s, 0, 0) if s > 1 => Err(CoreError::InvalidModelDirectory {
            path: dir_path.display().to_string(),
            reason: format!("directory contains {} .sql files (expected exactly 1)", s),
        }),
        (0, c, 0) if c > 1 => Err(CoreError::InvalidModelDirectory {
            path: dir_path.display().to_string(),
            reason: format!("directory contains {} .csv files (expected exactly 1)", c),
        }),
        (0, 0, p) if p > 1 => Err(CoreError::InvalidModelDirectory {
            path: dir_path.display().to_string(),
            reason: format!("directory contains {} .py files (expected exactly 1)", p),
        }),
        _ => Err(CoreError::InvalidModelDirectory {
            path: dir_path.display().to_string(),
            reason: "directory contains multiple data file types (.sql, .csv, .py) — each directory must contain exactly one data file".to_string(),
        }),
    }
}

/// Reject directories that contain files with unexpected extensions.
fn check_no_extra_files(all_visible: &[std::path::PathBuf], dir_name: &str) -> CoreResult<()> {
    let extra_files: Vec<String> = all_visible
        .iter()
        .filter(|p| {
            let ext = file_extension_str(p);
            !matches!(ext, "sql" | "csv" | "py" | "yml" | "yaml")
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
            directory: dir_name.to_string(),
            files: extra_files.join(", "),
        });
    }

    Ok(())
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

/// Returns `true` if the file's name starts with a dot (hidden on Unix)
fn is_hidden_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with('.'))
}
