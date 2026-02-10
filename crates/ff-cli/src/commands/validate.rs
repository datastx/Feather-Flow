//! Validate command implementation

use crate::commands::common::{self, load_project};
use anyhow::{Context, Result};
use ff_core::dag::ModelDag;
use ff_core::manifest::Manifest;
use ff_core::model::TestDefinition;
use ff_core::{ModelName, Project};
use ff_jinja::JinjaEnvironment;
use ff_sql::SqlParser;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::cli::{GlobalArgs, ValidateArgs};

/// Validation result severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Severity {
    Warning,
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
    let project = load_project(global)?;

    println!("Validating project: {}\n", project.config.name);

    let mut ctx = ValidationContext::new();

    let models_to_validate = get_models_to_validate(&project, &args.models);
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let macro_paths = project.config.macro_paths_absolute(&project.root);
    let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);
    let external_tables = common::build_external_tables_lookup(&project);
    let known_models: HashSet<String> = project.models.keys().map(|k| k.to_string()).collect();

    let dependencies = validate_sql_syntax(
        &project,
        &models_to_validate,
        &parser,
        &jinja,
        &external_tables,
        &known_models,
        &mut ctx,
    );
    validate_jinja_variables(&project, &models_to_validate, &jinja, &mut ctx);
    validate_dag(&dependencies, &mut ctx);
    validate_duplicates(&project, &mut ctx);
    validate_schemas(&project, &models_to_validate, &known_models, &mut ctx);
    validate_sources(&project);
    validate_macros(&project.config.vars, &macro_paths, &mut ctx);

    // Run DataFusion static analysis (unless DAG has circular deps, which would prevent topo sort)
    if !ctx.has_circular_dependency() {
        validate_static_analysis(
            &project,
            &models_to_validate,
            &jinja,
            &external_tables,
            &dependencies,
            global,
            &mut ctx,
        );
    }

    // Validate contracts if --contracts flag is set
    if args.contracts {
        validate_contracts(&project, &models_to_validate, &args.state, &mut ctx)?;
    }

    // Validate governance if --governance flag is set
    if args.governance {
        validate_governance(&project, &models_to_validate, &mut ctx);
    }

    print_issues_and_summary(&ctx, args.strict)
}

/// Get list of models to validate based on CLI filter
fn get_models_to_validate(project: &Project, filter: &Option<String>) -> Vec<String> {
    if let Some(f) = filter {
        f.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        project
            .model_names()
            .into_iter()
            .map(String::from)
            .collect()
    }
}

/// Validate SQL syntax and extract dependencies
fn validate_sql_syntax(
    project: &Project,
    models: &[String],
    parser: &SqlParser,
    jinja: &JinjaEnvironment,
    external_tables: &HashSet<String>,
    known_models: &HashSet<String>,
    ctx: &mut ValidationContext,
) -> HashMap<String, Vec<String>> {
    print!("Checking SQL syntax... ");
    let mut sql_errors = 0;
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    for name in models {
        if let Some(model) = project.get_model(name) {
            let rendered = match jinja.render(&model.raw_sql) {
                Ok(sql) => sql,
                Err(e) => {
                    let err_str = e.to_string();
                    let code = if is_macro_import_error(&err_str) {
                        "M003"
                    } else {
                        "E001"
                    };
                    let msg = if code == "M003" {
                        format!("Macro import error: {}", e)
                    } else {
                        format!("Jinja render error: {}", e)
                    };
                    ctx.error(code, msg, Some(model.path.display().to_string()));
                    sql_errors += 1;
                    continue;
                }
            };

            match parser.parse(&rendered) {
                Ok(stmts) => {
                    // Reject CTEs and derived tables — each transform must be its own model
                    if let Err(e) = ff_sql::validate_no_complex_queries(&stmts) {
                        let code = match &e {
                            ff_sql::SqlError::CteNotAllowed { .. } => "S005",
                            ff_sql::SqlError::DerivedTableNotAllowed => "S006",
                            ff_sql::SqlError::SelectStarNotAllowed => "S009",
                            _ => "S004",
                        };
                        ctx.error(code, e.to_string(), Some(model.path.display().to_string()));
                        sql_errors += 1;
                        continue;
                    }

                    let deps = ff_sql::extract_dependencies(&stmts);
                    let (model_deps, _, unknown_deps) =
                        ff_sql::extractor::categorize_dependencies_with_unknown(
                            deps,
                            known_models,
                            external_tables,
                        );

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

    dependencies
}

/// Check if error message indicates a macro import failure
fn is_macro_import_error(err_str: &str) -> bool {
    err_str.contains("unable to load template")
        || err_str.contains("template not found")
        || (err_str.contains("import") && err_str.contains("not found"))
}

/// Validate Jinja variables are defined
fn validate_jinja_variables(
    project: &Project,
    models: &[String],
    jinja: &JinjaEnvironment,
    ctx: &mut ValidationContext,
) {
    print!("Checking Jinja variables... ");
    let mut var_warnings = 0;

    for name in models {
        if let Some(model) = project.get_model(name) {
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
}

/// Validate DAG for circular dependencies
fn validate_dag(dependencies: &HashMap<String, Vec<String>>, ctx: &mut ValidationContext) {
    print!("Checking for cycles... ");
    match ModelDag::build(dependencies) {
        Ok(_) => println!("✓"),
        Err(e) => {
            ctx.error("E003", format!("Circular dependency: {}", e), None);
            println!("✗");
        }
    }
}

/// Validate no duplicate model names (case-insensitive for DuckDB compatibility)
fn validate_duplicates(project: &Project, ctx: &mut ValidationContext) {
    print!("Checking for duplicates... ");
    let model_names: Vec<&ModelName> = project.models.keys().collect();

    // Check case-insensitive duplicates (DuckDB identifiers are case-insensitive)
    let mut seen: HashMap<String, &ModelName> = HashMap::new();
    let mut found_dup = false;
    for name in &model_names {
        let lower = name.as_ref().to_lowercase();
        if let Some(existing) = seen.get(&lower) {
            ctx.error(
                "E004",
                format!(
                    "Duplicate model names (case-insensitive): '{}' and '{}'",
                    existing, name
                ),
                None,
            );
            found_dup = true;
        } else {
            seen.insert(lower, name);
        }
    }

    if found_dup {
        println!("✗");
    } else {
        println!("✓");
    }
}

/// Validate schema files and test definitions
fn validate_schemas(
    project: &Project,
    models: &[String],
    known_models: &HashSet<String>,
    ctx: &mut ValidationContext,
) {
    print!("Checking schema files... ");
    let mut schema_issues = 0;

    // Check orphaned schema references
    let models_with_tests: HashSet<&str> = project.tests.iter().map(|t| t.model.as_str()).collect();
    for model_ref in &models_with_tests {
        if !known_models.contains(*model_ref) {
            ctx.warning(
                "W002",
                format!("Schema references unknown model: {}", model_ref),
                None,
            );
            schema_issues += 1;
        }
    }

    // Validate test columns exist in schema
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
                    schema_issues += 1;
                }
            }
        }
    }

    // Check for missing schema files (1:1 YAML per model is always enforced)
    for name in models {
        if let Some(model) = project.get_model(name) {
            if model.schema.is_none() {
                let expected = model.path.with_extension("yml");
                ctx.error(
                    "E010",
                    format!(
                        "Model '{}' is missing a required schema file ({})",
                        name,
                        expected.display()
                    ),
                    Some(model.path.display().to_string()),
                );
                schema_issues += 1;
            }
        }
    }

    // Check references and type compatibility
    for name in models {
        let Some(model) = project.get_model(name) else {
            continue;
        };
        let Some(schema) = &model.schema else {
            continue;
        };
        for column in &schema.columns {
            if let Some(refs) = &column.references {
                if !known_models.contains(refs.model.as_str()) {
                    ctx.warning(
                        "W004",
                        format!(
                            "Column '{}' references unknown model '{}' in model '{}'",
                            column.name, refs.model, name
                        ),
                        Some(model.path.with_extension("yml").display().to_string()),
                    );
                    schema_issues += 1;
                }
            }

            for test in &column.tests {
                if let Some(warning) = check_test_type_compatibility(
                    test,
                    &column.data_type.to_uppercase(),
                    &column.name,
                    name,
                ) {
                    ctx.warning(
                        "W005",
                        warning,
                        Some(model.path.with_extension("yml").display().to_string()),
                    );
                    schema_issues += 1;
                }
            }
        }
    }

    if schema_issues == 0 {
        println!("✓");
    } else {
        println!("{} warnings", schema_issues);
    }
}

/// Validate source files
fn validate_sources(project: &Project) {
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
}

/// Validate macro files
fn validate_macros(
    vars: &HashMap<String, serde_yaml::Value>,
    macro_paths: &[PathBuf],
    ctx: &mut ValidationContext,
) {
    print!("Checking macro files... ");
    let mut macro_errors = 0;
    let mut macro_count = 0;

    for macro_dir in macro_paths {
        if macro_dir.exists() && macro_dir.is_dir() {
            let Ok(entries) = std::fs::read_dir(macro_dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_none_or(|e| e != "sql") {
                    continue;
                }
                macro_count += 1;
                let Ok(content) = std::fs::read_to_string(&path) else {
                    ctx.error(
                        "M001",
                        "Failed to read macro file",
                        Some(path.display().to_string()),
                    );
                    macro_errors += 1;
                    continue;
                };
                let test_env = JinjaEnvironment::new(vars);
                if let Err(e) = test_env.render(&content) {
                    let err_str = e.to_string();
                    if !err_str.contains("undefined") {
                        ctx.error(
                            "M002",
                            format!("Macro parse error: {}", e),
                            Some(path.display().to_string()),
                        );
                        macro_errors += 1;
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
}

/// Run DataFusion-based static analysis on SQL models
///
/// Builds LogicalPlans for each model and checks for schema mismatches
/// between YAML declarations and inferred SQL output.
fn validate_static_analysis(
    project: &Project,
    models: &[String],
    jinja: &JinjaEnvironment,
    external_tables: &HashSet<String>,
    dependencies: &HashMap<String, Vec<String>>,
    global: &GlobalArgs,
    ctx: &mut ValidationContext,
) {
    print!("Running static analysis... ");

    // Build topological order from dependencies
    let dag = match ModelDag::build(dependencies) {
        Ok(d) => d,
        Err(_) => {
            println!("(skipped — DAG error)");
            return;
        }
    };
    let topo_order = match dag.topological_order() {
        Ok(o) => o,
        Err(_) => {
            println!("(skipped — cycle detected)");
            return;
        }
    };

    // Filter to requested models
    let model_filter: HashSet<&str> = models.iter().map(|s| s.as_str()).collect();

    // Build SQL sources from rendered models
    let sql_sources: HashMap<String, String> = topo_order
        .iter()
        .filter(|n| model_filter.contains(n.as_str()))
        .filter_map(|name| {
            let model = project.get_model(name)?;
            let rendered = jinja.render(&model.raw_sql).ok()?;
            Some((name.clone(), rendered))
        })
        .collect();

    if sql_sources.is_empty() {
        println!("(no models to analyze)");
        return;
    }

    // Use the shared static analysis pipeline
    let output = match common::run_static_analysis_pipeline(
        project,
        &sql_sources,
        &topo_order,
        external_tables,
    ) {
        Ok(o) => o,
        Err(e) => {
            println!("(skipped — {})", e);
            return;
        }
    };
    let result = &output.result;

    let mut issue_count = 0;

    // Report failures
    for (model, err) in &result.failures {
        if global.verbose {
            eprintln!("[verbose] Static analysis failed for '{}': {}", model, err);
        }
    }

    // Report schema mismatches (all mismatches are errors)
    for (model_name, plan_result) in &result.model_plans {
        for mismatch in &plan_result.mismatches {
            ctx.error("SA01", format!("{}: {}", model_name, mismatch), None);
            issue_count += 1;
        }
    }

    let plan_count = result.model_plans.len();
    let failure_count = result.failures.len();
    if issue_count == 0 && failure_count == 0 {
        println!("✓ ({} models analyzed)", plan_count);
    } else if issue_count == 0 {
        println!(
            "✓ ({} analyzed, {} could not be planned)",
            plan_count, failure_count
        );
    } else {
        println!("{} issues ({} analyzed)", issue_count, plan_count);
    }
}

/// Validate schema contracts
///
/// Without --state: Reports models with contracts defined and validates structure
/// With --state: Reports models with contracts and checks against manifest
///
/// Note: Full contract validation (column types) happens at runtime during `ff run`
/// because we need to query the actual database schema.
fn validate_contracts(
    project: &Project,
    models: &[String],
    state_path: &Option<String>,
    ctx: &mut ValidationContext,
) -> Result<()> {
    print!("Checking schema contracts... ");

    // Load reference manifest if provided
    let reference_manifest = if let Some(path) = state_path {
        let manifest_path = Path::new(path);
        if !manifest_path.exists() {
            ctx.error(
                "C001",
                format!("Reference manifest not found: {}", path),
                None,
            );
            println!("✗");
            return Ok(());
        }
        Some(Manifest::load(manifest_path).context("Failed to load reference manifest")?)
    } else {
        None
    };

    let mut contract_warnings = 0;
    let mut models_with_contracts = 0;
    let mut enforced_count = 0;

    for name in models {
        let Some(model) = project.get_model(name) else {
            continue;
        };
        let Some(schema) = &model.schema else {
            continue;
        };
        if let Some(contract) = &schema.contract {
            models_with_contracts += 1;
            if contract.enforced {
                enforced_count += 1;
            }

            let file_path = model.path.with_extension("yml").display().to_string();

            // Validate contract structure
            if schema.columns.is_empty() {
                ctx.warning(
                    "C006",
                    format!(
                        "Model '{}' has contract defined but no columns specified",
                        name
                    ),
                    Some(file_path.clone()),
                );
                contract_warnings += 1;
            }

            // If we have a reference manifest, verify model exists
            if let Some(ref manifest) = reference_manifest {
                if manifest.get_model(name).is_none() {
                    ctx.warning(
                        "C005",
                        format!(
                            "Model '{}' has contract but not found in reference manifest (new model?)",
                            name
                        ),
                        Some(model.path.display().to_string()),
                    );
                    contract_warnings += 1;
                }
            }
        }
    }

    if models_with_contracts == 0 {
        println!("(no contracts defined)");
    } else if contract_warnings == 0 {
        println!(
            "✓ ({} contracts, {} enforced)",
            models_with_contracts, enforced_count
        );
    } else {
        println!(
            "{} warnings ({} contracts, {} enforced)",
            contract_warnings, models_with_contracts, enforced_count
        );
    }

    Ok(())
}

/// Validate data governance rules
///
/// Checks:
/// - G001: Column missing classification (when `require_classification` is true)
/// - G002: PII column has no description
/// - G003: PII column missing not_null test
fn validate_governance(project: &Project, models: &[String], ctx: &mut ValidationContext) {
    print!("Checking data governance... ");
    let require_classification = project.config.data_classification.require_classification;
    let mut governance_issues = 0;

    for name in models {
        let Some(model) = project.get_model(name) else {
            continue;
        };
        let Some(schema) = &model.schema else {
            continue;
        };
        let file_path = model.path.with_extension("yml").display().to_string();

        for column in &schema.columns {
            // G001: Missing classification (when require_classification is true)
            if require_classification && column.classification.is_none() {
                ctx.warning(
                    "G001",
                    format!(
                        "Column '{}' in model '{}' has no data classification",
                        column.name, name
                    ),
                    Some(file_path.clone()),
                );
                governance_issues += 1;
            }

            // G002: PII column has no description
            if column.classification == Some(ff_core::model::DataClassification::Pii)
                && column.description.is_none()
            {
                ctx.warning(
                    "G002",
                    format!(
                        "PII column '{}' in model '{}' has no description",
                        column.name, name
                    ),
                    Some(file_path.clone()),
                );
                governance_issues += 1;
            }

            // G003: PII column missing not_null test
            if column.classification == Some(ff_core::model::DataClassification::Pii) {
                let has_not_null = column.tests.iter().any(|t| {
                    matches!(
                        t,
                        ff_core::model::TestDefinition::Simple(name) if name == "not_null"
                    )
                });
                if !has_not_null {
                    ctx.warning(
                        "G003",
                        format!(
                            "PII column '{}' in model '{}' is missing not_null test",
                            column.name, name
                        ),
                        Some(file_path.clone()),
                    );
                    governance_issues += 1;
                }
            }
        }
    }

    if governance_issues == 0 {
        if require_classification {
            println!("✓");
        } else {
            println!("✓ (classification optional)");
        }
    } else {
        println!("{} issues", governance_issues);
    }
}

/// Print all issues and final summary
fn print_issues_and_summary(ctx: &ValidationContext, strict: bool) -> Result<()> {
    println!();
    for issue in &ctx.issues {
        println!("{}", issue);
    }

    let error_count = ctx.error_count();
    let warning_count = ctx.warning_count();

    println!();
    if error_count == 0 && (warning_count == 0 || !strict) {
        println!(
            "Validation passed: {} errors, {} warnings",
            error_count, warning_count
        );
        Ok(())
    } else if strict && warning_count > 0 {
        println!(
            "Validation failed (strict mode): {} errors, {} warnings",
            error_count, warning_count
        );
        Err(crate::commands::common::ExitCode(1).into())
    } else {
        println!(
            "Validation failed: {} errors, {} warnings",
            error_count, warning_count
        );
        if ctx.has_circular_dependency() {
            Err(crate::commands::common::ExitCode(3).into())
        } else {
            Err(crate::commands::common::ExitCode(1).into())
        }
    }
}

/// Check if a test makes sense for the given data type
fn check_test_type_compatibility(
    test: &TestDefinition,
    data_type: &str,
    column_name: &str,
    model_name: &str,
) -> Option<String> {
    let numeric_tests = ["positive", "non_negative", "min_value", "max_value"];
    let string_types = [
        "VARCHAR", "CHAR", "TEXT", "STRING", "NVARCHAR", "NCHAR", "CLOB",
    ];

    let is_string_type = string_types
        .iter()
        .any(|t| data_type.starts_with(t) || data_type.contains(t));

    let test_name = match test {
        TestDefinition::Simple(name) => name.as_str(),
        TestDefinition::Parameterized(map) => map.keys().next().map(|s| s.as_str()).unwrap_or(""),
    };

    if numeric_tests.contains(&test_name) && is_string_type {
        return Some(format!(
            "Test '{}' on column '{}' (type {}) in model '{}' - numeric test on string type",
            test_name, column_name, data_type, model_name
        ));
    }

    if test_name == "regex" && !is_string_type {
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
                to: None,
                field: None,
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
                to: None,
                field: None,
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
                to: None,
                field: None,
            },
        );
        let test = TestDefinition::Parameterized(params);
        let result = check_test_type_compatibility(&test, "TEXT", "count", "test_model");
        assert!(result.is_some());
    }

    #[test]
    fn test_not_null_on_any_type() {
        let test = TestDefinition::Simple("not_null".to_string());
        let result1 = check_test_type_compatibility(&test, "VARCHAR", "name", "test_model");
        let result2 = check_test_type_compatibility(&test, "INTEGER", "id", "test_model");
        assert!(result1.is_none());
        assert!(result2.is_none());
    }

    #[test]
    fn test_unique_on_any_type() {
        let test = TestDefinition::Simple("unique".to_string());
        let result1 = check_test_type_compatibility(&test, "VARCHAR", "name", "test_model");
        let result2 = check_test_type_compatibility(&test, "INTEGER", "id", "test_model");
        assert!(result1.is_none());
        assert!(result2.is_none());
    }
}
