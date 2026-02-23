//! Rules command implementation

use crate::cli::{GlobalArgs, RulesArgs};
use crate::commands::common::{self, load_project, print_table};
use anyhow::{Context, Result};
use ff_core::rules::{discover_rules, resolve_rule_paths, OnRuleFailure, RuleSeverity};

/// Execute the rules command.
pub(crate) async fn execute(args: &RulesArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;
    let rules_config = project.config.rules.clone().unwrap_or_default();

    if rules_config.paths.is_empty() {
        println!("No rule paths configured. Add `rules.paths` to featherflow.yml.");
        return Ok(());
    }

    let rule_dirs = resolve_rule_paths(&rules_config.paths, &project.root);
    let rules =
        discover_rules(&rule_dirs, rules_config.severity).context("Failed to discover rules")?;

    if rules.is_empty() {
        println!("No rule files found in configured paths.");
        return Ok(());
    }

    if args.list {
        return list_rules(&rules);
    }

    println!("Running {} rules against meta database...\n", rules.len());

    let Some(meta_db) = common::open_meta_db(&project) else {
        anyhow::bail!(
            "Meta database not found. Run `ff compile` or `ff run` first to populate it."
        );
    };

    let (results, violations) = ff_meta::rules::execute_all_rules(meta_db.conn(), &rules)
        .context("Failed to execute rules")?;

    {
        if let Some((_project_id, run_id, _model_id_map)) = common::populate_meta_phase1(
            &meta_db,
            &project,
            ff_meta::populate::lifecycle::RunType::Rules,
            None,
        ) {
            let _ = meta_db.transaction(|conn| {
                ff_meta::rules::populate_rule_violations(conn, run_id, &violations)
            });
            let status = if results
                .iter()
                .any(|r| !r.passed && r.severity == RuleSeverity::Error)
            {
                ff_meta::populate::lifecycle::PopulationStatus::Error
            } else {
                ff_meta::populate::lifecycle::PopulationStatus::Success
            };
            common::complete_meta_run(&meta_db, run_id, status);
        }
    }

    print_rule_results(&results, &violations);

    let error_count = results
        .iter()
        .filter(|r| !r.passed && r.severity == RuleSeverity::Error)
        .count();
    let warn_count = results
        .iter()
        .filter(|r| !r.passed && r.severity == RuleSeverity::Warn)
        .count();
    let error_with_sql = results.iter().filter(|r| r.error.is_some()).count();

    println!();
    if error_count == 0 && error_with_sql == 0 {
        println!(
            "Rules passed: {} passed, {} warnings",
            results.iter().filter(|r| r.passed).count(),
            warn_count
        );
        Ok(())
    } else if rules_config.on_failure == OnRuleFailure::Warn {
        println!(
            "Rules completed (warn mode): {} errors, {} warnings",
            error_count + error_with_sql,
            warn_count
        );
        Ok(())
    } else {
        println!(
            "Rules failed: {} errors, {} warnings",
            error_count + error_with_sql,
            warn_count
        );
        Err(common::ExitCode(1).into())
    }
}

fn list_rules(rules: &[ff_core::rules::RuleFile]) -> Result<()> {
    let rows: Vec<Vec<String>> = rules
        .iter()
        .map(|r| {
            vec![
                r.name.clone(),
                r.severity.to_string(),
                r.description.clone().unwrap_or_default(),
                r.path.display().to_string(),
            ]
        })
        .collect();
    print_table(&["NAME", "SEVERITY", "DESCRIPTION", "PATH"], &rows);
    Ok(())
}

fn print_rule_results(
    results: &[ff_meta::rules::RuleResult],
    violations: &[ff_meta::rules::RuleViolation],
) {
    for result in results {
        let icon = if result.passed {
            "\u{2713}"
        } else {
            "\u{2717}"
        };
        let severity_label = match result.severity {
            RuleSeverity::Error => "ERROR",
            RuleSeverity::Warn => "WARN",
        };

        if result.passed {
            println!("  {} [{}] {} - passed", icon, severity_label, result.name);
        } else if let Some(ref err) = result.error {
            println!(
                "  {} [{}] {} - SQL error: {}",
                icon, severity_label, result.name, err
            );
        } else {
            println!(
                "  {} [{}] {} - {} violations",
                icon, severity_label, result.name, result.violation_count
            );
        }
    }

    if !violations.is_empty() {
        println!("\nViolations:");
        for v in violations {
            let entity = v
                .entity_name
                .as_deref()
                .map(|e| format!(" ({})", e))
                .unwrap_or_default();
            println!(
                "  [{}] {}{}: {}",
                v.severity, v.rule_name, entity, v.message
            );
        }
    }
}
