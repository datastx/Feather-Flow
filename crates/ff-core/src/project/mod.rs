//! Project discovery and loading

mod loading;
mod versioning;

pub(crate) use loading::find_yaml_path;

use crate::config::Config;
use crate::function::FunctionDef;
use crate::function_name::FunctionName;
use crate::model::{Model, SchemaTest, SingularTest};
use crate::model_name::ModelName;
use crate::seed::Seed;
use crate::source::SourceFile;
use std::collections::HashMap;
use std::path::PathBuf;

/// All fields needed to construct a [`Project`].
///
/// Avoids a 9-parameter constructor and makes call sites self-documenting
/// via named fields.
#[derive(Debug)]
pub struct ProjectParts {
    /// Project root directory
    pub root: PathBuf,
    /// Project configuration
    pub config: Config,
    /// Models discovered in the project
    pub models: HashMap<ModelName, Model>,
    /// Seeds discovered in the project (kind: seed directories)
    pub seeds: Vec<Seed>,
    /// Schema tests from YAML files
    pub tests: Vec<SchemaTest>,
    /// Singular tests (standalone SQL test files)
    pub singular_tests: Vec<SingularTest>,
    /// Source definitions
    pub sources: Vec<SourceFile>,
    /// User-defined function definitions
    pub functions: Vec<FunctionDef>,
}

/// Represents a Featherflow project
#[derive(Debug)]
pub struct Project {
    /// Project root directory
    pub root: PathBuf,

    /// Project configuration
    pub config: Config,

    /// Models discovered in the project
    pub models: HashMap<ModelName, Model>,

    /// Seeds discovered in the project (kind: seed directories in model_paths)
    pub seeds: Vec<Seed>,

    /// Schema tests from model YAML files
    pub tests: Vec<SchemaTest>,

    /// Singular tests (standalone SQL test files)
    pub singular_tests: Vec<SingularTest>,

    /// Source definitions
    pub sources: Vec<SourceFile>,

    /// User-defined function definitions
    pub functions: Vec<FunctionDef>,

    /// Function lookup by name (O(1) access, index into `functions` vec)
    functions_by_name: HashMap<FunctionName, usize>,
}

impl Project {
    /// Create a new project from [`ProjectParts`].
    ///
    /// Builds the `functions_by_name` index from the `functions` vec automatically.
    pub fn new(parts: ProjectParts) -> Self {
        let functions_by_name: HashMap<FunctionName, usize> = parts
            .functions
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        Self {
            root: parts.root,
            config: parts.config,
            models: parts.models,
            seeds: parts.seeds,
            tests: parts.tests,
            singular_tests: parts.singular_tests,
            sources: parts.sources,
            functions: parts.functions,
            functions_by_name,
        }
    }

    /// Get the function name index for O(1) lookup
    pub fn get_function_index(&self) -> &HashMap<FunctionName, usize> {
        &self.functions_by_name
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

    /// Get source table names for dependency categorization
    pub fn source_table_names(&self) -> std::collections::HashSet<String> {
        crate::source::build_source_lookup(&self.sources)
    }

    /// Get all source names
    pub fn source_names(&self) -> Vec<&str> {
        self.sources.iter().map(|s| s.name.as_str()).collect()
    }

    /// Get all function names
    pub fn function_names(&self) -> Vec<&str> {
        self.functions.iter().map(|f| f.name.as_str()).collect()
    }

    /// Get a function by name (O(1) lookup)
    pub fn get_function(&self, name: &str) -> Option<&FunctionDef> {
        self.functions_by_name
            .get(name)
            .map(|&idx| &self.functions[idx])
    }
}

#[cfg(test)]
#[path = "project_test.rs"]
mod tests;
