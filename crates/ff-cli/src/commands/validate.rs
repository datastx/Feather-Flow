//! Validate command implementation

use anyhow::{Context, Result};
use ff_core::dag::ModelDag;
use ff_core::model::TestDefinition;
use ff_core::source::build_source_lookup;
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

    fn has_circular_dependency(&self) -> bool {
        self.issues
            .iter()
            .any(|i| i.severity == Severity::Error && i.code == "E003")
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

    // Create SQL parser and Jinja environment with macro support
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);

    // Collect known models and external tables (including sources)
    let mut external_tables: HashSet<String> =
        project.config.external_tables.iter().cloned().collect();
    // Add source tables to external tables lookup
    let source_tables = build_source_lookup(&project.sources);
    external_tables.extend(source_tables);
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
                    let err_str = e.to_string();
                    // Check for macro import errors (M003)
                    if err_str.contains("unable to load template")
                        || err_str.contains("template not found")
                        || (err_str.contains("import") && err_str.contains("not found"))
                    {
                        ctx.error(
                            "M003",
                            format!("Macro import error: {}", e),
                            Some(model.path.display().to_string()),
                        );
                    } else {
                        ctx.error(
                            "E001",
                            format!("Jinja render error: {}", e),
                            Some(model.path.display().to_string()),
                        );
                    }
                    sql_errors += 1;
                    continue;
                }
            };

            // Then parse SQL
            match parser.parse(&rendered) {
                Ok(stmts) => {
                    // Extract dependencies for later cycle detection
                    let deps = ff_sql::extract_dependencies(&stmts);
                    let (model_deps, _, unknown_deps) =
                        ff_sql::extractor::categorize_dependencies_with_unknown(
                            deps,
                            &known_models,
                            &external_tables,
                        );

                    // Warn about unknown dependencies
                    for unknown in &unknown_deps {
                        ctx.warning(
                            "W003",
                            format!(
                                "Unknown dependency '{}' in model '{}'. Not defined as a model or source.",
                                unknown, name
                            ),
                            Some(model.path.display().to_string()),
                        );
                    }

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
        println!("✓");
    } else {
        println!("✗ ({} errors)", sql_errors);
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
        println!("✓");
    } else {
        println!("{} warnings", var_warnings);
    }

    // Check for circular dependencies
    print!("Checking for cycles... ");
    match ModelDag::build(&dependencies) {
        Ok(_) => println!("✓"),
        Err(e) => {
            ctx.error("E003", format!("Circular dependency: {}", e), None);
            println!("✗");
        }
    }

    // Check for duplicate model names (already handled by Project::load, but verify)
    print!("Checking for duplicates... ");
    let model_names: Vec<&String> = project.models.keys().collect();
    let unique_count = model_names.iter().collect::<HashSet<_>>().len();
    if model_names.len() == unique_count {
        println!("✓");
    } else {
        ctx.error("E004", "Duplicate model names detected", None);
        println!("✗");
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

    // Validate that test columns exist in the schema definition
    for test in &project.tests {
        if let Some(model) = project.get_model(&test.model) {
            if let Some(schema) = &model.schema {
                let schema_columns: HashSet<&str> =
                    schema.columns.iter().map(|c| c.name.as_str()).collect();
                if !schema_columns.contains(test.column.as_str()) {
                    ctx.warning(
                        "W006",
                        format!(
                            "Test references column '{}' not defined in schema for model '{}'",
                            test.column, test.model
                        ),
                        Some(model.path.with_extension("yml").display().to_string()),
                    );
                    schema_warnings += 1;
                }
            }
        }
    }

    // Check reference integrity and type compatibility
    for name in &models_to_validate {
        if let Some(model) = project.get_model(name) {
            if let Some(schema) = &model.schema {
                for column in &schema.columns {
                    // Check if referenced model exists
                    if let Some(refs) = &column.references {
                        if !known_models.contains(&refs.model) {
                            ctx.warning(
                                "W004",
                                format!(
                                    "Column '{}' references unknown model '{}' in model '{}'",
                                    column.name, refs.model, name
                                ),
                                Some(model.path.with_extension("yml").display().to_string()),
                            );
                            schema_warnings += 1;
                        }
                    }

                    // Check test/type compatibility
                    if let Some(data_type) = &column.data_type {
                        let data_type_upper = data_type.to_uppercase();
                        for test in &column.tests {
                            if let Some(warning) = check_test_type_compatibility(
                                test,
                                &data_type_upper,
                                &column.name,
                                name,
                            ) {
                                ctx.warning(
                                    "W005",
                                    warning,
                                    Some(model.path.with_extension("yml").display().to_string()),
                                );
                                schema_warnings += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    if schema_warnings == 0 {
        println!("✓");
    } else {
        println!("{} warnings", schema_warnings);
    }

    // Check source files
    print!("Checking source files... ");
    let source_count = project.sources.len();
    let source_table_count: usize = project.sources.iter().map(|s| s.tables.len()).sum();
    if source_count == 0 {
        println!("(none found)");
    } else {
        println!(
            "✓ ({} sources, {} tables)",
            source_count, source_table_count
        );
    }

    // Check macro files
    print!("Checking macro files... ");
    let mut macro_errors = 0;
    let mut macro_count = 0;
    for macro_dir in &macro_paths {
        if macro_dir.exists() && macro_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(macro_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|e| e == "sql") {
                        macro_count += 1;
                        // Try to read and parse the macro file
                        if let Ok(content) = std::fs::read_to_string(&path) {
                            // Use a simple Jinja env to test syntax
                            let test_env = JinjaEnvironment::new(&project.config.vars);
                            if let Err(e) = test_env.render(&content) {
                                let err_str = e.to_string();
                                // Only report if it's a real syntax error, not undefined variable
                                if !err_str.contains("undefined") {
                                    ctx.error(
                                        "M002",
                                        format!("Macro parse error: {}", e),
                                        Some(path.display().to_string()),
                                    );
                                    macro_errors += 1;
                                }
                            }
                        } else {
                            ctx.error(
                                "M001",
                                "Failed to read macro file",
                                Some(path.display().to_string()),
                            );
                            macro_errors += 1;
                        }
                    }
                }
            }
        }
    }
    if macro_count == 0 {
        println!("(none found)");
    } else if macro_errors == 0 {
        println!("✓ ({} macros)", macro_count);
    } else {
        println!("✗ ({} errors in {} macros)", macro_errors, macro_count);
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
        // Exit code 3 = Circular dependency (per spec)
        if ctx.has_circular_dependency() {
            std::process::exit(3);
        }
        std::process::exit(1);
    }
}

/// Check if a test makes sense for the given data type
/// Returns Some(warning_message) if there's a type incompatibility
fn check_test_type_compatibility(
    test: &TestDefinition,
    data_type: &str,
    column_name: &str,
    model_name: &str,
) -> Option<String> {
    // Tests that require numeric types
    let numeric_tests = ["positive", "non_negative", "min_value", "max_value"];

    // String/text types
    let string_types = [
        "VARCHAR", "CHAR", "TEXT", "STRING", "NVARCHAR", "NCHAR", "CLOB",
    ];

    // Check if type is a string type
    let is_string_type = string_types
        .iter()
        .any(|t| data_type.starts_with(t) || data_type.contains(t));

    let test_name = match test {
        TestDefinition::Simple(name) => name.as_str(),
        TestDefinition::Parameterized(map) => map.keys().next().map(|s| s.as_str()).unwrap_or(""),
    };

    // Numeric tests on string types
    if numeric_tests.contains(&test_name) && is_string_type {
        return Some(format!(
            "Test '{}' on column '{}' (type {}) in model '{}' - numeric test on string type",
            test_name, column_name, data_type, model_name
        ));
    }

    // Regex test on non-string types
    if test_name == "regex" && !is_string_type {
        // Only warn if it's clearly a non-string type
        let numeric_types = [
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
            "REAL",
            "BOOLEAN",
            "BOOL",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "DATETIME",
        ];
        let is_non_string_type = numeric_types
            .iter()
            .any(|t| data_type.starts_with(t) || data_type.contains(t));

        if is_non_string_type {
            return Some(format!(
                "Test 'regex' on column '{}' (type {}) in model '{}' - regex test on non-string type",
                column_name, data_type, model_name
            ));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_numeric_test_on_string_type() {
        let test = TestDefinition::Simple("positive".to_string());
        let result = check_test_type_compatibility(&test, "VARCHAR", "amount", "test_model");
        assert!(result.is_some());
        assert!(result.unwrap().contains("numeric test on string type"));
    }

    #[test]
    fn test_numeric_test_on_numeric_type() {
        let test = TestDefinition::Simple("positive".to_string());
        let result = check_test_type_compatibility(&test, "INTEGER", "amount", "test_model");
        assert!(result.is_none());
    }

    #[test]
    fn test_regex_on_integer_type() {
        let mut params = HashMap::new();
        params.insert(
            "regex".to_string(),
            ff_core::model::TestParams {
                values: vec![],
                quote: false,
                value: None,
                pattern: Some(".*".to_string()),
            },
        );
        let test = TestDefinition::Parameterized(params);
        let result = check_test_type_compatibility(&test, "INTEGER", "code", "test_model");
        assert!(result.is_some());
        assert!(result.unwrap().contains("regex test on non-string type"));
    }

    #[test]
    fn test_regex_on_varchar_type() {
        let mut params = HashMap::new();
        params.insert(
            "regex".to_string(),
            ff_core::model::TestParams {
                values: vec![],
                quote: false,
                value: None,
                pattern: Some(".*".to_string()),
            },
        );
        let test = TestDefinition::Parameterized(params);
        let result = check_test_type_compatibility(&test, "VARCHAR", "email", "test_model");
        assert!(result.is_none());
    }

    #[test]
    fn test_min_value_on_text_type() {
        let mut params = HashMap::new();
        params.insert(
            "min_value".to_string(),
            ff_core::model::TestParams {
                values: vec![],
                quote: false,
                value: Some(0.0),
                pattern: None,
            },
        );
        let test = TestDefinition::Parameterized(params);
        let result = check_test_type_compatibility(&test, "TEXT", "count", "test_model");
        assert!(result.is_some());
    }

    #[test]
    fn test_not_null_on_any_type() {
        let test = TestDefinition::Simple("not_null".to_string());
        // not_null should work on any type
        let result1 = check_test_type_compatibility(&test, "VARCHAR", "name", "test_model");
        let result2 = check_test_type_compatibility(&test, "INTEGER", "id", "test_model");
        assert!(result1.is_none());
        assert!(result2.is_none());
    }

    #[test]
    fn test_unique_on_any_type() {
        let test = TestDefinition::Simple("unique".to_string());
        // unique should work on any type
        let result1 = check_test_type_compatibility(&test, "VARCHAR", "name", "test_model");
        let result2 = check_test_type_compatibility(&test, "INTEGER", "id", "test_model");
        assert!(result1.is_none());
        assert!(result2.is_none());
    }
}
