//! Init command implementation - scaffolds a new Featherflow project

use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::cli::InitArgs;

/// Execute the init command
pub(crate) async fn execute(args: &InitArgs) -> Result<()> {
    // Reject names that could cause path traversal or confusing directory names
    if args.name.contains('/')
        || args.name.contains('\\')
        || args.name.contains("..")
        || args.name.starts_with('.')
        || args.name.starts_with('-')
    {
        anyhow::bail!(
            "Invalid project name '{}': must not contain '/', '\\', '..', or start with '.' or '-'",
            args.name
        );
    }

    let project_dir = Path::new(&args.name);

    if project_dir.exists() {
        anyhow::bail!(
            "Directory '{}' already exists. Choose a different project name.",
            args.name
        );
    }

    println!("Creating new Featherflow project: {}\n", args.name);

    // Create directory structure
    let dirs = [
        "",
        "models",
        "models/my_first_model",
        "sources",
        "macros",
        "tests",
        "functions",
    ];
    for dir in &dirs {
        let path = project_dir.join(dir);
        fs::create_dir_all(&path)
            .with_context(|| format!("Failed to create directory: {}", path.display()))?;
    }

    // Generate featherflow.yml
    // Escape YAML special characters in interpolated values
    let safe_name = args.name.replace('"', "\\\"");
    let safe_db_path = args.database_path.replace('"', "\\\"");
    let config_content = format!(
        r#"name: "{name}"
version: "1.0.0"

model_paths: ["models"]
source_paths: ["sources"]
macro_paths: ["macros"]
test_paths: ["tests"]
function_paths: ["functions"]
target_path: "target"

materialization: view
dialect: duckdb

database:
  type: duckdb
  path: "{db_path}"

vars:
  environment: dev

# analysis:
#   severity_overrides:
#     A020: warning    # promote unused columns from info to warning
#     A032: off        # disable cross join diagnostics
"#,
        name = safe_name,
        db_path = safe_db_path,
    );
    fs::write(project_dir.join("featherflow.yml"), config_content)
        .context("Failed to write featherflow.yml")?;

    // Generate example model SQL
    let example_sql = r#"-- Example model: transforms raw data into a clean format
SELECT
    id,
    name,
    created_at
FROM raw_example
WHERE id IS NOT NULL
"#;
    fs::write(
        project_dir.join("models/my_first_model/my_first_model.sql"),
        example_sql,
    )
    .context("Failed to write example model SQL")?;

    // Generate example model YAML schema (1:1 with SQL)
    let example_yml = r#"version: 1
description: "Example model that transforms raw data"

columns:
  - name: id
    type: INTEGER
    description: "Primary key"
    tests:
      - unique
      - not_null
  - name: name
    type: VARCHAR
    description: "Entity name"
    tests:
      - not_null
  - name: created_at
    type: TIMESTAMP
    description: "Timestamp when the record was created"
"#;
    fs::write(
        project_dir.join("models/my_first_model/my_first_model.yml"),
        example_yml,
    )
    .context("Failed to write example model YAML")?;

    // Generate example function
    let example_fn_yml = r#"kind: functions
version: 1
name: safe_divide
description: "Division that returns NULL on zero denominator"
function_type: scalar
args:
  - name: numerator
    data_type: DOUBLE
  - name: denominator
    data_type: DOUBLE
returns:
  data_type: DOUBLE
"#;
    let example_fn_sql = "CASE WHEN denominator = 0 THEN NULL ELSE numerator / denominator END\n";
    fs::write(
        project_dir.join("functions/safe_divide.yml"),
        example_fn_yml,
    )
    .context("Failed to write example function YAML")?;
    fs::write(
        project_dir.join("functions/safe_divide.sql"),
        example_fn_sql,
    )
    .context("Failed to write example function SQL")?;

    // Generate .gitignore
    let gitignore = "target/\n*.duckdb\n*.duckdb.wal\n";
    fs::write(project_dir.join(".gitignore"), gitignore).context("Failed to write .gitignore")?;

    println!("  Created featherflow.yml");
    println!("  Created models/my_first_model/my_first_model.sql");
    println!("  Created models/my_first_model/my_first_model.yml");
    println!("  Created sources/");
    println!("  Created macros/");
    println!("  Created tests/");
    println!("  Created functions/safe_divide.yml");
    println!("  Created functions/safe_divide.sql");
    println!("  Created .gitignore");
    println!();
    println!("Project '{}' initialized successfully!", args.name);
    println!();
    println!("Next steps:");
    println!("  cd {}", args.name);
    println!("  ff validate    # Validate the project");
    println!("  ff run          # Run all models");

    Ok(())
}
