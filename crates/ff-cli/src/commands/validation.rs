//! Shared validation logic used by the `compile` command.
//!
//! Contains project-level checks that go beyond SQL parsing and DAG construction:
//! duplicate detection, schema validation, source validation, macro validation,
//! contracts, governance, documentation, and SQL rules.

use anyhow::{Context, Result};
use ff_core::model::TestDefinition;
use ff_core::{ModelName, Project};
use ff_jinja::JinjaEnvironment;
use ff_meta::manifest::Manifest;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

// ── Validation types ───────────────────────────────────────────────────

/// Validation result severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Severity {
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
pub(crate) struct ValidationIssue {
    pub severity: Severity,
    pub code: String,
    pub message: String,
    pub file: Option<String>,
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
pub(crate) struct ValidationContext {
    pub issues: Vec<ValidationIssue>,
}

impl ValidationContext {
    pub fn new() -> Self {
        Self { issues: Vec::new() }
    }

    pub fn error(&mut self, code: &str, message: impl Into<String>, file: Option<String>) {
        self.issues.push(ValidationIssue {
            severity: Severity::Error,
            code: code.to_string(),
            message: message.into(),
            file,
        });
    }

    pub fn warning(&mut self, code: &str, message: impl Into<String>, file: Option<String>) {
        self.issues.push(ValidationIssue {
            severity: Severity::Warning,
            code: code.to_string(),
            message: message.into(),
            file,
        });
    }

    pub fn error_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .count()
    }

    pub fn warning_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Warning)
            .count()
    }
}

// ── Validation checks (used by compile) ────────────────────────────────

/// Validate no duplicate model names (case-insensitive for DuckDB compatibility)
pub(crate) fn validate_duplicates(project: &Project, ctx: &mut ValidationContext) {
    print!("Checking for duplicates... ");
    let model_names: Vec<&ModelName> = project.models.keys().collect();

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
pub(crate) fn validate_schemas(
    project: &Project,
    models: &[String],
    known_models: &HashSet<&str>,
    ctx: &mut ValidationContext,
) {
    print!("Checking schema files... ");
    let mut schema_issues = 0;

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
pub(crate) fn validate_sources(project: &Project) {
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
pub(crate) fn validate_macros(
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

/// Validate schema contracts
pub(crate) fn validate_contracts(
    project: &Project,
    models: &[String],
    state_path: &Option<String>,
    ctx: &mut ValidationContext,
) -> Result<()> {
    print!("Checking schema contracts... ");

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

/// Validate data governance rules
pub(crate) fn validate_governance(
    project: &Project,
    models: &[String],
    ctx: &mut ValidationContext,
) {
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
pub(crate) fn validate_documentation(
    project: &Project,
    models: &[String],
    ctx: &mut ValidationContext,
) {
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

/// Run SQL rules against the meta database during validation.
pub(crate) fn validate_rules(
    project: &Project,
    meta_db: &ff_meta::MetaDb,
    ctx: &mut ValidationContext,
) {
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

// ── Helper functions ───────────────────────────────────────────────────

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

/// Check that a model has a non-empty description (D001).
pub(crate) fn check_model_description(
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
pub(crate) fn check_column_description(
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

/// Check if a test makes sense for the given data type
pub(crate) fn check_test_type_compatibility(
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

#[cfg(test)]
#[path = "validation_test.rs"]
mod tests;
