use super::*;
use std::fs;
use std::fs::File;
use tempfile::tempdir;

#[tokio::test]
async fn test_clean_removes_target_directory() {
    let temp_dir = tempdir().unwrap();
    let project_path = temp_dir.path();

    let config_content = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ":memory:"
"#;
    fs::write(project_path.join("featherflow.yml"), config_content).unwrap();

    // Create target directory with some files
    let target_dir = project_path.join("target");
    fs::create_dir_all(&target_dir).unwrap();
    File::create(target_dir.join("test.txt")).unwrap();

    assert!(target_dir.exists());

    let args = CleanArgs { dry_run: false };
    let global = GlobalArgs {
        verbose: false,
        project_dir: project_path.to_path_buf(),
        database: None,
    };

    execute(&args, &global).await.unwrap();

    assert!(!target_dir.exists());
}

#[tokio::test]
async fn test_clean_dry_run_does_not_remove() {
    let temp_dir = tempdir().unwrap();
    let project_path = temp_dir.path();

    let config_content = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ":memory:"
"#;
    fs::write(project_path.join("featherflow.yml"), config_content).unwrap();

    // Create target directory
    let target_dir = project_path.join("target");
    fs::create_dir_all(&target_dir).unwrap();

    let args = CleanArgs { dry_run: true };
    let global = GlobalArgs {
        verbose: false,
        project_dir: project_path.to_path_buf(),
        database: None,
    };

    execute(&args, &global).await.unwrap();

    // Directory should still exist after dry run
    assert!(target_dir.exists());
}

#[tokio::test]
async fn test_clean_handles_missing_directory() {
    let temp_dir = tempdir().unwrap();
    let project_path = temp_dir.path();

    let config_content = r#"
name: test_project
database:
  default:
    type: duckdb
    path: ":memory:"
"#;
    fs::write(project_path.join("featherflow.yml"), config_content).unwrap();

    // Don't create target directory - it should be handled gracefully
    let target_dir = project_path.join("target");
    assert!(!target_dir.exists());

    let args = CleanArgs { dry_run: false };
    let global = GlobalArgs {
        verbose: true,
        project_dir: project_path.to_path_buf(),
        database: None,
    };

    // Should not error
    let result = execute(&args, &global).await;
    assert!(result.is_ok());
}
