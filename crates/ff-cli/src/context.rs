//! Runtime context for CLI commands

use anyhow::{Context, Result};
use ff_core::{Config, Project};
use ff_db::{Database, DuckDbBackend};
use std::path::Path;
use std::sync::Arc;

use crate::cli::GlobalArgs;

/// Runtime context containing loaded project and database connection
#[allow(dead_code)]
pub struct RuntimeContext {
    /// The loaded project
    pub project: Project,

    /// Database connection
    pub db: Arc<dyn Database>,

    /// Verbose output enabled
    pub verbose: bool,
}

#[allow(dead_code)]
impl RuntimeContext {
    /// Create a new runtime context from global arguments
    pub async fn new(args: &GlobalArgs) -> Result<Self> {
        let project_path = Path::new(&args.project_dir);

        // Load config from custom path or project directory
        let _config = if let Some(config_path) = &args.config {
            Config::load(Path::new(config_path)).context("Failed to load configuration file")?
        } else {
            Config::load_from_dir(project_path).context("Failed to load project configuration")?
        };

        // Load the full project
        let project = Project::load(project_path).context("Failed to load project")?;

        // Create database connection
        let db_path = args
            .target
            .as_deref()
            .unwrap_or(&project.config.database.path);
        let db: Arc<dyn Database> =
            Arc::new(DuckDbBackend::new(db_path).context("Failed to connect to database")?);

        Ok(Self {
            project,
            db,
            verbose: args.verbose,
        })
    }

    /// Print verbose output if enabled
    pub fn verbose(&self, msg: &str) {
        if self.verbose {
            eprintln!("[verbose] {}", msg);
        }
    }

    /// Get filtered model names from a comma-separated string
    pub fn filter_models(&self, models_arg: &Option<String>) -> Vec<String> {
        match models_arg {
            Some(models) => models
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            None => self
                .project
                .model_names()
                .into_iter()
                .map(String::from)
                .collect(),
        }
    }
}
