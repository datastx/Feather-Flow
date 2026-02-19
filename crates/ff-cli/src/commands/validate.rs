//! Validate command implementation

use crate::commands::common::{self, load_project};
use anyhow::{Context, Result};
use ff_core::dag::ModelDag;
use ff_core::model::TestDefinition;
use ff_core::{ModelName, Project};
use ff_jinja::JinjaEnvironment;
use ff_meta::manifest::Manifest;
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
pub(crate) async fn execute(args: &ValidateArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    println!("Validating project: {}\n", project.config.name);

    let mut ctx = ValidationContext::new();

    let models_to_validate = get_models_to_validate(&project, &args.nodes)?;
    let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())
        .context("Invalid SQL dialect")?;
    let jinja = common::build_jinja_env_with_context(&project, global.target.as_deref(), false);
    let external_tables = common::build_external_tables_lookup(&project);
    let known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();
    let macro_paths = project.config.macro_paths_absolute(&project.root);

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

    // Validate documentation completeness (driven by featherflow.yml config)
    validate_documentation(&project, &models_to_validate, &mut ctx);

    // Populate meta database (non-fatal)
    if let Some(meta_db) = common::open_meta_db(&project) {
        if let Some((_project_id, run_id, _model_id_map)) =
            common::populate_meta_phase1(&meta_db, &project, "validate", args.nodes.as_deref())
        {
            // Run SQL rules if configured
            validate_rules(&project, &meta_db, &mut ctx);

            let status = if ctx.error_count() > 0 {
                "error"
            } else {
                "success"
            };
            common::complete_meta_run(&meta_db, run_id, status);
        }
    }

    print_issues_and_summary(&ctx, args.strict)
}

/// Get list of models to validate based on CLI filter.
///
/// When a filter is provided, builds the project DAG and resolves selectors
/// (supporting `+model`, `tag:X`, `path:X`, `N+model`, etc.).
/// When no filter is given, returns all model names without building a DAG.
fn get_models_to_validate(project: &Project, filter: &Option<String>) -> Result<Vec<String>> {
    if filter.is_some() {
        let (_, dag) =
            crate::commands::common::build_project_dag(project).context("building DAG")?;
        crate::commands::common::resolve_nodes(project, &dag, filter)
    } else {
        Ok(project
            .model_names()
            .into_iter()
            .map(String::from)
            .collect())
    }
}

/// Validate SQL syntax and extract dependencies
fn validate_sql_syntax(
    project: &Project,
    models: &[String],
    parser: &SqlParser,
    jinja: &JinjaEnvironment,
    external_tables: &HashSet<String>,
    known_models: &HashSet<&str>,
    ctx: &mut ValidationContext,
) -> HashMap<String, Vec<String>> {
    print!("Checking SQL syntax... ");
    let mut sql_errors = 0;
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    for name in models {
        let Some(model) = project.get_model(name) else {
            continue;
        };

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

        let stmts = match parser.parse(&rendered) {
            Ok(stmts) => stmts,
            Err(e) => {
                ctx.error(
                    "E002",
                    format!("SQL parse error: {}", e),
                    Some(model.path.display().to_string()),
                );
                sql_errors += 1;
                continue;
            }
        };

        if let Err(e) = ff_sql::validate_no_complex_queries(&stmts) {
            let code = match &e {
                ff_sql::SqlError::CteNotAllowed { .. } => "S005",
                ff_sql::SqlError::DerivedTableNotAllowed => "S006",
                _ => "S004",
            };
            ctx.error(code, e.to_string(), Some(model.path.display().to_string()));
            sql_errors += 1;
            continue;
        }

        let deps = ff_sql::extract_dependencies(&stmts);
        let (mut model_deps, _, unknown_deps) =
            ff_sql::extractor::categorize_dependencies_with_unknown(
                deps,
                known_models,
                external_tables,
            );

        // Resolve table function references (same as compile.rs) to avoid
        // false W003 warnings for known functions like table-valued UDFs.
        let (func_model_deps, remaining_unknown) = common::resolve_function_dependencies(
            &unknown_deps,
            project,
            parser,
            known_models,
            external_tables,
        );
        model_deps.extend(func_model_deps);
        model_deps.sort();
        model_deps.dedup();

        for unknown in &remaining_unknown {
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
    known_models: &HashSet<&str>,
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
        let Some(model) = project.get_model(&test.model) else {
            continue;
        };
        let Some(schema) = &model.schema else {
            continue;
        };
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
        let file_path = model.path.with_extension("yml").display().to_string();
        for column in &schema.columns {
            schema_issues += check_column_schema(column, name, known_models, &file_path, ctx);
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
        if !macro_dir.exists() || !macro_dir.is_dir() {
            continue;
        }
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
    _global: &GlobalArgs,
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

    // Collect issues into vecs first to avoid double-borrow of ctx
    let mut mismatch_issues: Vec<(String, String, bool)> = Vec::new();
    let mut failure_issues: Vec<String> = Vec::new();

    let (mismatch_count, plan_count, failure_count) = common::report_static_analysis_results(
        result,
        &output.overrides,
        |model_name, mismatch, is_error| {
            mismatch_issues.push((
                mismatch.code().to_string(),
                format!("{}: {}", model_name, mismatch),
                is_error,
            ));
        },
        |model, err| {
            failure_issues.push(format!("{}: planning failed: {}", model, err));
        },
    );

    for (code, msg, is_error) in mismatch_issues {
        if is_error {
            ctx.error(&code, msg, None);
        } else {
            ctx.warning(&code, msg, None);
        }
    }
    for msg in failure_issues {
        ctx.error("SA01", msg, None);
    }
    issue_count += mismatch_count;
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
        let Some(contract) = &schema.contract else {
            continue;
        };

        models_with_contracts += 1;
        if contract.enforced {
            enforced_count += 1;
        }

        let file_path = model.path.with_extension("yml").display().to_string();

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

/// Check a single column for reference validity (W004) and test-type compatibility (W005).
fn check_column_schema(
    column: &ff_core::model::SchemaColumnDef,
    model_name: &str,
    known_models: &HashSet<&str>,
    file_path: &str,
    ctx: &mut ValidationContext,
) -> usize {
    let mut issues = 0;

    if let Some(refs) = &column.references {
        if !known_models.contains(refs.model.as_str()) {
            ctx.warning(
                "W004",
                format!(
                    "Column '{}' references unknown model '{}' in model '{}'",
                    column.name, refs.model, model_name
                ),
                Some(file_path.to_string()),
            );
            issues += 1;
        }
    }

    for test in &column.tests {
        if let Some(warning) = check_test_type_compatibility(
            test,
            &column.data_type.to_uppercase(),
            &column.name,
            model_name,
        ) {
            ctx.warning("W005", warning, Some(file_path.to_string()));
            issues += 1;
        }
    }

    issues
}

/// Check a single column for governance violations (G001/G002/G003).
fn check_column_governance(
    column: &ff_core::model::SchemaColumnDef,
    model_name: &str,
    file_path: &str,
    require_classification: bool,
    ctx: &mut ValidationContext,
) -> usize {
    let mut issues = 0;

    if require_classification && column.classification.is_none() {
        ctx.warning(
            "G001",
            format!(
                "Column '{}' in model '{}' has no data classification",
                column.name, model_name
            ),
            Some(file_path.to_string()),
        );
        issues += 1;
    }

    if column.classification == Some(ff_core::model::DataClassification::Pii)
        && column.description.is_none()
    {
        ctx.warning(
            "G002",
            format!(
                "PII column '{}' in model '{}' has no description",
                column.name, model_name
            ),
            Some(file_path.to_string()),
        );
        issues += 1;
    }

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
                    column.name, model_name
                ),
                Some(file_path.to_string()),
            );
            issues += 1;
        }
    }

    issues
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
            governance_issues +=
                check_column_governance(column, name, &file_path, require_classification, ctx);
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

/// Validate documentation completeness
///
/// Checks:
/// - D001: Model missing description (when `require_model_descriptions` is true)
/// - D002: Column missing description (when `require_column_descriptions` is true)
fn validate_documentation(project: &Project, models: &[String], ctx: &mut ValidationContext) {
    let doc_config = &project.config.documentation;
    let require_models = doc_config.require_model_descriptions;
    let require_columns = doc_config.require_column_descriptions;

    if !require_models && !require_columns {
        return;
    }

    print!("Checking documentation... ");
    let mut doc_issues = 0;

    for name in models {
        let Some(model) = project.get_model(name) else {
            continue;
        };
        let Some(schema) = &model.schema else {
            continue;
        };
        let file_path = model.path.with_extension("yml").display().to_string();

        if require_models {
            doc_issues += check_model_description(schema, name, &file_path, ctx);
        }

        if require_columns {
            for column in &schema.columns {
                doc_issues += check_column_description(column, name, &file_path, ctx);
            }
        }
    }

    if doc_issues == 0 {
        println!("✓");
    } else {
        println!("{} issues", doc_issues);
    }
}

/// Check that a model has a non-empty description (D001).
fn check_model_description(
    schema: &ff_core::model::ModelSchema,
    model_name: &str,
    file_path: &str,
    ctx: &mut ValidationContext,
) -> usize {
    let has_description = schema
        .description
        .as_ref()
        .is_some_and(|d| !d.trim().is_empty());

    if !has_description {
        ctx.error(
            "D001",
            format!("Model '{}' is missing a description", model_name),
            Some(file_path.to_string()),
        );
        return 1;
    }
    0
}

/// Check that a column has a non-empty description (D002).
fn check_column_description(
    column: &ff_core::model::SchemaColumnDef,
    model_name: &str,
    file_path: &str,
    ctx: &mut ValidationContext,
) -> usize {
    let has_description = column
        .description
        .as_ref()
        .is_some_and(|d| !d.trim().is_empty());

    if !has_description {
        ctx.error(
            "D002",
            format!(
                "Column '{}' in model '{}' is missing a description",
                column.name, model_name
            ),
            Some(file_path.to_string()),
        );
        return 1;
    }
    0
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

/// Process a single rule result, recording issues into the validation context.
///
/// Returns `true` if the rule had a failure or error.
fn process_rule_result(result: &ff_meta::rules::RuleResult, ctx: &mut ValidationContext) -> bool {
    if let Some(ref err) = result.error {
        ctx.warning(
            "R003",
            format!("Rule '{}' SQL error: {}", result.name, err),
            Some(result.path.clone()),
        );
        return true;
    }

    if !result.passed {
        let code = match result.severity {
            ff_core::rules::RuleSeverity::Error => "R010",
            ff_core::rules::RuleSeverity::Warn => "R011",
        };
        let msg = format!(
            "Rule '{}' found {} violations",
            result.name, result.violation_count
        );
        match result.severity {
            ff_core::rules::RuleSeverity::Error => ctx.error(code, msg, Some(result.path.clone())),
            ff_core::rules::RuleSeverity::Warn => ctx.warning(code, msg, Some(result.path.clone())),
        }
        return true;
    }

    false
}

/// Run SQL rules against the meta database during validation.
fn validate_rules(project: &Project, meta_db: &ff_meta::MetaDb, ctx: &mut ValidationContext) {
    let rules_config = match &project.config.rules {
        Some(rc) if !rc.paths.is_empty() => rc,
        _ => return,
    };

    print!("Running SQL rules... ");

    let rule_dirs = ff_core::rules::resolve_rule_paths(&rules_config.paths, &project.root);
    let rules = match ff_core::rules::discover_rules(&rule_dirs, rules_config.severity) {
        Ok(r) => r,
        Err(e) => {
            ctx.warning("R001", format!("Failed to discover rules: {e}"), None);
            println!("(skipped)");
            return;
        }
    };

    if rules.is_empty() {
        println!("(none found)");
        return;
    }

    let (results, _violations) = match ff_meta::rules::execute_all_rules(meta_db.conn(), &rules) {
        Ok(r) => r,
        Err(e) => {
            ctx.warning("R002", format!("Failed to execute rules: {e}"), None);
            println!("(skipped)");
            return;
        }
    };

    let mut fail_count = 0;
    for result in &results {
        if process_rule_result(result, ctx) {
            fail_count += 1;
        }
    }

    if fail_count == 0 {
        println!("\u{2713} ({} rules)", rules.len());
    } else {
        println!("{} issues ({} rules)", fail_count, rules.len());
    }
}

#[cfg(test)]
#[path = "validate_test.rs"]
mod tests;
