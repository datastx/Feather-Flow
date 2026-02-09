//! Clean command implementation

use anyhow::Result;
use std::fs;

use crate::cli::{CleanArgs, GlobalArgs};
use crate::commands::common::load_project;

/// Execute the clean command
pub async fn execute(args: &CleanArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    // Default clean targets if not specified in config
    let clean_targets = if project.config.clean_targets.is_empty() {
        vec!["target".to_string()]
    } else {
        project.config.clean_targets.clone()
    };

    if args.dry_run {
        println!("Dry run - would clean the following directories:");
    } else {
        println!("Cleaning project: {}", project.config.name);
    }

    let mut cleaned_count = 0;
    let mut skipped_count = 0;

    for target in &clean_targets {
        let target_path = project.root.join(target);

        if target_path.exists() {
            if args.dry_run {
                println!("  Would remove: {}", target_path.display());
                cleaned_count += 1;
            } else {
                match fs::remove_dir_all(&target_path) {
                    Ok(_) => {
                        println!("  Removed: {}", target_path.display());
                        cleaned_count += 1;
                    }
                    Err(e) => {
                        eprintln!("  Failed to remove {}: {}", target_path.display(), e);
                    }
                }
            }
        } else {
            if global.verbose {
                println!("  Skipping (not found): {}", target_path.display());
            }
            skipped_count += 1;
        }
    }

    println!();
    if args.dry_run {
        println!(
            "Would clean {} director{}, {} not found",
            cleaned_count,
            if cleaned_count == 1 { "y" } else { "ies" },
            skipped_count
        );
    } else {
        println!(
            "Cleaned {} director{}, {} skipped",
            cleaned_count,
            if cleaned_count == 1 { "y" } else { "ies" },
            skipped_count
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_clean_removes_target_directory() {
        let temp_dir = tempdir().unwrap();
        let project_path = temp_dir.path();

        // Create a minimal featherflow.yml
        let config_content = r#"
name: test_project
model_paths:
  - models
database:
  type: duckdb
  path: ":memory:"
"#;
        fs::write(project_path.join("featherflow.yml"), config_content).unwrap();
        fs::create_dir_all(project_path.join("models")).unwrap();

        // Create target directory with some files
        let target_dir = project_path.join("target");
        fs::create_dir_all(&target_dir).unwrap();
        File::create(target_dir.join("test.txt")).unwrap();

        assert!(target_dir.exists());

        let args = CleanArgs { dry_run: false };
        let global = GlobalArgs {
            verbose: false,
            project_dir: project_path.to_path_buf(),
            config: None,
            target: None,
        };

        execute(&args, &global).await.unwrap();

        assert!(!target_dir.exists());
    }

    #[tokio::test]
    async fn test_clean_dry_run_does_not_remove() {
        let temp_dir = tempdir().unwrap();
        let project_path = temp_dir.path();

        // Create a minimal featherflow.yml
        let config_content = r#"
name: test_project
model_paths:
  - models
database:
  type: duckdb
  path: ":memory:"
"#;
        fs::write(project_path.join("featherflow.yml"), config_content).unwrap();
        fs::create_dir_all(project_path.join("models")).unwrap();

        // Create target directory
        let target_dir = project_path.join("target");
        fs::create_dir_all(&target_dir).unwrap();

        let args = CleanArgs { dry_run: true };
        let global = GlobalArgs {
            verbose: false,
            project_dir: project_path.to_path_buf(),
            config: None,
            target: None,
        };

        execute(&args, &global).await.unwrap();

        // Directory should still exist after dry run
        assert!(target_dir.exists());
    }

    #[tokio::test]
    async fn test_clean_handles_missing_directory() {
        let temp_dir = tempdir().unwrap();
        let project_path = temp_dir.path();

        // Create a minimal featherflow.yml
        let config_content = r#"
name: test_project
model_paths:
  - models
database:
  type: duckdb
  path: ":memory:"
"#;
        fs::write(project_path.join("featherflow.yml"), config_content).unwrap();
        fs::create_dir_all(project_path.join("models")).unwrap();

        // Don't create target directory - it should be handled gracefully
        let target_dir = project_path.join("target");
        assert!(!target_dir.exists());

        let args = CleanArgs { dry_run: false };
        let global = GlobalArgs {
            verbose: true,
            project_dir: project_path.to_path_buf(),
            config: None,
            target: None,
        };

        // Should not error
        let result = execute(&args, &global).await;
        assert!(result.is_ok());
    }
}
