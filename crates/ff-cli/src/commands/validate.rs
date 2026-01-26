//! Validate command implementation

use anyhow::{Context, Result};
use ff_core::dag::ModelDag;
use ff_core::Project;
use ff_jinja::JinjaEnvironment;
use ff_sql::SqlParser;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::cli::{GlobalArgs, ValidateArgs};

/// Validation result severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
enum Severity {
    Info,
    Warning,
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARNING"),
            Severity::Error => write!(f, "ERROR"),
        }
    }
}

/// A single validation issue
struct ValidationIssue {
    severity: Severity,
    code: String,
    message: String,
    file: Option<String>,
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.file {
            Some(file) => write!(
                f,
                "[{}] {}: {} ({})",
                self.severity, self.code, self.message, file
            ),
            None => write!(f, "[{}] {}: {}", self.severity, self.code, self.message),
        }
    }
}

/// Collect validation issues
struct ValidationContext {
    issues: Vec<ValidationIssue>,
}

impl ValidationContext {
    fn new() -> Self {
        Self { issues: Vec::new() }
    }

    fn error(&mut self, code: &str, message: impl Into<String>, file: Option<String>) {
        self.issues.push(ValidationIssue {
            severity: Severity::Error,
            code: code.to_string(),
            message: message.into(),
            file,
        });
    }

    fn warning(&mut self, code: &str, message: impl Into<String>, file: Option<String>) {
        self.issues.push(ValidationIssue {
            severity: Severity::Warning,
            code: code.to_string(),
            message: message.into(),
            file,
        });
    }

    fn error_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .count()
    }

    fn warning_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Warning)
            .count()
    }
}

/// Execute the validate command
pub async fn execute(args: &ValidateArgs, global: &GlobalArgs) -> Result<()> {
    let project_path = Path::new(&global.project_dir);
    let project = Project::load(project_path).context("Failed to load project")?;

    println!("Validating project: {}\n", project.config.name);

    let mut ctx = ValidationContext::new();

    // Get models to validate
    let models_to_validate: Vec<String> = if let Some(filter) = &args.models {
        filter.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        project
            .model_names()
            .into_iter()
            .map(String::from)
            .collect()
    };

    // Create SQL parser and Jinja environment
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = JinjaEnvironment::new(&project.config.vars);

    // Collect known models and external tables
    let external_tables: HashSet<String> = project.config.external_tables.iter().cloned().collect();
    let known_models: HashSet<String> = project.models.keys().cloned().collect();

    // Track dependencies for cycle detection
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    // Validate SQL syntax
    print!("Checking SQL syntax... ");
    let mut sql_errors = 0;
    for name in &models_to_validate {
        if let Some(model) = project.get_model(name) {
            // First render Jinja template
            let rendered = match jinja.render(&model.raw_sql) {
                Ok(sql) => sql,
                Err(e) => {
                    ctx.error(
                        "E001",
                        format!("Jinja render error: {}", e),
                        Some(model.path.display().to_string()),
                    );
                    sql_errors += 1;
                    continue;
                }
            };

            // Then parse SQL
            match parser.parse(&rendered) {
                Ok(stmts) => {
                    // Extract dependencies for later cycle detection
                    let deps = ff_sql::extract_dependencies(&stmts);
                    let (model_deps, _) = ff_sql::extractor::categorize_dependencies(
                        deps,
                        &known_models,
                        &external_tables,
                    );
                    dependencies.insert(name.clone(), model_deps);
                }
                Err(e) => {
                    ctx.error(
                        "E002",
                        format!("SQL parse error: {}", e),
                        Some(model.path.display().to_string()),
                    );
                    sql_errors += 1;
                }
            }
        }
    }
    if sql_errors == 0 {
        println!("OK");
    } else {
        println!("FAILED ({} errors)", sql_errors);
    }

    // Check for undefined Jinja variables
    print!("Checking Jinja variables... ");
    let mut var_warnings = 0;
    for name in &models_to_validate {
        if let Some(model) = project.get_model(name) {
            // Check for var() calls without defaults
            if let Err(e) = jinja.render(&model.raw_sql) {
                let err_str = e.to_string();
                if err_str.contains("undefined variable") {
                    ctx.warning(
                        "W001",
                        format!("Undefined variable: {}", err_str),
                        Some(model.path.display().to_string()),
                    );
                    var_warnings += 1;
                }
            }
        }
    }
    if var_warnings == 0 {
        println!("OK");
    } else {
        println!("{} warnings", var_warnings);
    }

    // Check for circular dependencies
    print!("Checking for cycles... ");
    match ModelDag::build(&dependencies) {
        Ok(_) => println!("OK"),
        Err(e) => {
            ctx.error("E003", format!("Circular dependency: {}", e), None);
            println!("FAILED");
        }
    }

    // Check for duplicate model names (already handled by Project::load, but verify)
    print!("Checking for duplicates... ");
    let model_names: Vec<&String> = project.models.keys().collect();
    let unique_count = model_names.iter().collect::<HashSet<_>>().len();
    if model_names.len() == unique_count {
        println!("OK");
    } else {
        ctx.error("E004", "Duplicate model names detected", None);
        println!("FAILED");
    }

    // Check schema files
    print!("Checking schema files... ");
    let mut schema_warnings = 0;

    // Find orphaned schema files (schema without matching model)
    let models_with_tests: HashSet<&str> = project.tests.iter().map(|t| t.model.as_str()).collect();
    for model_ref in &models_with_tests {
        if !known_models.contains(*model_ref) {
            ctx.warning(
                "W002",
                format!("Schema references unknown model: {}", model_ref),
                None,
            );
            schema_warnings += 1;
        }
    }

    if schema_warnings == 0 {
        println!("OK");
    } else {
        println!("{} warnings", schema_warnings);
    }

    // Print all issues
    println!();
    for issue in &ctx.issues {
        println!("{}", issue);
    }

    // Summary
    let error_count = ctx.error_count();
    let warning_count = ctx.warning_count();

    println!();
    if error_count == 0 && (warning_count == 0 || !args.strict) {
        println!(
            "Validation passed: {} errors, {} warnings",
            error_count, warning_count
        );
        Ok(())
    } else if args.strict && warning_count > 0 {
        println!(
            "Validation failed (strict mode): {} errors, {} warnings",
            error_count, warning_count
        );
        std::process::exit(1);
    } else {
        println!(
            "Validation failed: {} errors, {} warnings",
            error_count, warning_count
        );
        std::process::exit(1);
    }
}
