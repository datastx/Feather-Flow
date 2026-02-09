//! Exposure definitions for Featherflow
//!
//! Exposures represent downstream dependencies of data models, such as
//! dashboards, reports, machine learning models, or applications that
//! consume data from the warehouse.

use crate::error::{CoreError, CoreResult};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Exposure type indicating what kind of downstream consumer this is
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExposureType {
    /// A dashboard or BI report
    #[default]
    Dashboard,
    /// A notebook for analysis
    Notebook,
    /// A machine learning model
    MlModel,
    /// An application consuming the data
    Application,
    /// An analysis or report
    Analysis,
    /// Other type of exposure
    Other,
}

impl std::fmt::Display for ExposureType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExposureType::Dashboard => write!(f, "dashboard"),
            ExposureType::Notebook => write!(f, "notebook"),
            ExposureType::MlModel => write!(f, "ml_model"),
            ExposureType::Application => write!(f, "application"),
            ExposureType::Analysis => write!(f, "analysis"),
            ExposureType::Other => write!(f, "other"),
        }
    }
}

/// Maturity level of an exposure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExposureMaturity {
    /// High maturity - well established, critical
    High,
    /// Medium maturity - established but not critical
    #[default]
    Medium,
    /// Low maturity - experimental or new
    Low,
}

impl std::fmt::Display for ExposureMaturity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExposureMaturity::High => write!(f, "high"),
            ExposureMaturity::Medium => write!(f, "medium"),
            ExposureMaturity::Low => write!(f, "low"),
        }
    }
}

/// Owner information for an exposure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposureOwner {
    /// Name of the owner (person or team)
    pub name: String,

    /// Email address of the owner
    #[serde(default)]
    pub email: Option<String>,
}

/// An exposure definition representing a downstream dependency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exposure {
    /// Version of the exposure format
    #[serde(default = "default_version")]
    pub version: u32,

    /// Kind must be "exposure"
    #[serde(default = "default_kind")]
    pub kind: String,

    /// Unique name of the exposure
    pub name: String,

    /// Type of exposure (dashboard, notebook, ml_model, etc.)
    #[serde(rename = "type", default)]
    pub exposure_type: ExposureType,

    /// Owner information
    pub owner: ExposureOwner,

    /// List of model names this exposure depends on
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// URL to the exposure (e.g., dashboard link)
    #[serde(default)]
    pub url: Option<String>,

    /// Description of the exposure
    #[serde(default)]
    pub description: Option<String>,

    /// Maturity level
    #[serde(default)]
    pub maturity: ExposureMaturity,

    /// Additional metadata tags
    #[serde(default)]
    pub tags: Vec<String>,

    /// Path to the source file (populated during loading)
    #[serde(skip)]
    pub source_path: Option<String>,
}

fn default_version() -> u32 {
    1
}

fn default_kind() -> String {
    "exposure".to_string()
}

impl Exposure {
    /// Load an exposure from a YAML file
    pub fn from_file(path: &Path) -> CoreResult<Self> {
        if !path.exists() {
            return Err(CoreError::ConfigNotFound {
                path: path.display().to_string(),
            });
        }

        let content = std::fs::read_to_string(path)?;
        let mut exposure: Exposure =
            serde_yaml::from_str(&content).map_err(|e| CoreError::ConfigParseError {
                message: format!("Failed to parse exposure file {}: {}", path.display(), e),
            })?;

        // Store the source path
        exposure.source_path = Some(path.display().to_string());

        // Validate the exposure
        exposure.validate()?;

        Ok(exposure)
    }

    /// Parse an exposure from a YAML string
    pub fn from_yaml(yaml: &str) -> CoreResult<Self> {
        let exposure: Exposure =
            serde_yaml::from_str(yaml).map_err(|e| CoreError::ConfigParseError {
                message: format!("Failed to parse exposure YAML: {}", e),
            })?;
        exposure.validate()?;
        Ok(exposure)
    }

    /// Validate the exposure configuration
    fn validate(&self) -> CoreResult<()> {
        if self.name.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: "Exposure name cannot be empty".to_string(),
            });
        }

        if self.kind != "exposure" {
            return Err(CoreError::ConfigInvalid {
                message: format!(
                    "Invalid kind '{}' for exposure '{}', expected 'exposure'",
                    self.kind, self.name
                ),
            });
        }

        if self.owner.name.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: format!("Exposure '{}' must have an owner name", self.name),
            });
        }

        Ok(())
    }

    /// Check if this exposure depends on a specific model
    pub fn depends_on_model(&self, model_name: &str) -> bool {
        self.depends_on.iter().any(|dep| dep == model_name)
    }
}

/// Discover all exposure files in the given paths
pub fn discover_exposures(paths: &[impl AsRef<Path>]) -> Vec<Exposure> {
    let mut exposures = Vec::new();

    for path in paths {
        let path = path.as_ref();
        if !path.exists() || !path.is_dir() {
            continue;
        }

        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let file_path = entry.path();
                if file_path
                    .extension()
                    .is_some_and(|e| e == "yml" || e == "yaml")
                {
                    match Exposure::from_file(&file_path) {
                        Ok(exposure) => exposures.push(exposure),
                        Err(e) => {
                            // Log warning but don't fail - file might not be an exposure
                            eprintln!(
                                "Warning: Failed to load exposure from {}: {}",
                                file_path.display(),
                                e
                            );
                        }
                    }
                }
            }
        }
    }

    exposures
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_parse_minimal_exposure() {
        let yaml = r#"
version: 1
kind: exposure
name: revenue_dashboard
owner:
  name: Analytics Team
"#;
        let exposure = Exposure::from_yaml(yaml).unwrap();
        assert_eq!(exposure.name, "revenue_dashboard");
        assert_eq!(exposure.owner.name, "Analytics Team");
        assert_eq!(exposure.exposure_type, ExposureType::Dashboard);
        assert_eq!(exposure.maturity, ExposureMaturity::Medium);
    }

    #[test]
    fn test_parse_full_exposure() {
        let yaml = r#"
version: 1
kind: exposure
name: revenue_dashboard
type: dashboard
owner:
  name: Analytics Team
  email: analytics@company.com
depends_on:
  - fct_orders
  - dim_customers
url: https://bi.company.com/dashboard/123
description: Executive revenue dashboard
maturity: high
tags:
  - executive
  - revenue
"#;
        let exposure = Exposure::from_yaml(yaml).unwrap();
        assert_eq!(exposure.name, "revenue_dashboard");
        assert_eq!(exposure.exposure_type, ExposureType::Dashboard);
        assert_eq!(exposure.owner.name, "Analytics Team");
        assert_eq!(
            exposure.owner.email,
            Some("analytics@company.com".to_string())
        );
        assert_eq!(exposure.depends_on.len(), 2);
        assert!(exposure.depends_on.contains(&"fct_orders".to_string()));
        assert!(exposure.depends_on.contains(&"dim_customers".to_string()));
        assert_eq!(
            exposure.url,
            Some("https://bi.company.com/dashboard/123".to_string())
        );
        assert_eq!(
            exposure.description,
            Some("Executive revenue dashboard".to_string())
        );
        assert_eq!(exposure.maturity, ExposureMaturity::High);
        assert_eq!(exposure.tags.len(), 2);
    }

    #[test]
    fn test_exposure_types() {
        let cases = vec![
            ("dashboard", ExposureType::Dashboard),
            ("notebook", ExposureType::Notebook),
            ("ml_model", ExposureType::MlModel),
            ("application", ExposureType::Application),
            ("analysis", ExposureType::Analysis),
            ("other", ExposureType::Other),
        ];

        for (type_str, expected) in cases {
            let yaml = format!(
                r#"
version: 1
kind: exposure
name: test_exposure
type: {}
owner:
  name: Test
"#,
                type_str
            );
            let exposure = Exposure::from_yaml(&yaml).unwrap();
            assert_eq!(exposure.exposure_type, expected);
        }
    }

    #[test]
    fn test_maturity_levels() {
        let cases = vec![
            ("high", ExposureMaturity::High),
            ("medium", ExposureMaturity::Medium),
            ("low", ExposureMaturity::Low),
        ];

        for (maturity_str, expected) in cases {
            let yaml = format!(
                r#"
version: 1
kind: exposure
name: test_exposure
maturity: {}
owner:
  name: Test
"#,
                maturity_str
            );
            let exposure = Exposure::from_yaml(&yaml).unwrap();
            assert_eq!(exposure.maturity, expected);
        }
    }

    #[test]
    fn test_invalid_kind() {
        let yaml = r#"
version: 1
kind: model
name: not_an_exposure
owner:
  name: Test
"#;
        let result = Exposure::from_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid kind"));
    }

    #[test]
    fn test_missing_name() {
        let yaml = r#"
version: 1
kind: exposure
name: ""
owner:
  name: Test
"#;
        let result = Exposure::from_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("name cannot be empty"));
    }

    #[test]
    fn test_missing_owner_name() {
        let yaml = r#"
version: 1
kind: exposure
name: test_exposure
owner:
  name: ""
"#;
        let result = Exposure::from_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("owner name"));
    }

    #[test]
    fn test_depends_on_model() {
        let yaml = r#"
version: 1
kind: exposure
name: test_exposure
depends_on:
  - fct_orders
  - dim_customers
owner:
  name: Test
"#;
        let exposure = Exposure::from_yaml(yaml).unwrap();
        assert!(exposure.depends_on_model("fct_orders"));
        assert!(exposure.depends_on_model("dim_customers"));
        assert!(!exposure.depends_on_model("stg_orders"));
    }

    #[test]
    fn test_from_file() {
        let temp = TempDir::new().unwrap();
        let exposure_path = temp.path().join("revenue_dashboard.yml");

        fs::write(
            &exposure_path,
            r#"
version: 1
kind: exposure
name: revenue_dashboard
type: dashboard
owner:
  name: Analytics Team
depends_on:
  - fct_orders
"#,
        )
        .unwrap();

        let exposure = Exposure::from_file(&exposure_path).unwrap();
        assert_eq!(exposure.name, "revenue_dashboard");
        assert_eq!(exposure.exposure_type, ExposureType::Dashboard);
        assert!(exposure.source_path.is_some());
    }

    #[test]
    fn test_discover_exposures() {
        let temp = TempDir::new().unwrap();
        let exposures_dir = temp.path().join("exposures");
        fs::create_dir(&exposures_dir).unwrap();

        // Create two exposure files
        fs::write(
            exposures_dir.join("dashboard1.yml"),
            r#"
version: 1
kind: exposure
name: dashboard_one
owner:
  name: Team A
depends_on:
  - model_a
"#,
        )
        .unwrap();

        fs::write(
            exposures_dir.join("dashboard2.yaml"),
            r#"
version: 1
kind: exposure
name: dashboard_two
owner:
  name: Team B
depends_on:
  - model_b
"#,
        )
        .unwrap();

        // Create a non-exposure YAML file that should be skipped
        fs::write(
            exposures_dir.join("not_exposure.yml"),
            r#"
name: some_model
"#,
        )
        .unwrap();

        let exposures = discover_exposures(&[&exposures_dir]);
        assert_eq!(exposures.len(), 2);

        let names: Vec<&str> = exposures.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"dashboard_one"));
        assert!(names.contains(&"dashboard_two"));
    }

    #[test]
    fn test_exposure_display_types() {
        assert_eq!(format!("{}", ExposureType::Dashboard), "dashboard");
        assert_eq!(format!("{}", ExposureType::MlModel), "ml_model");
        assert_eq!(format!("{}", ExposureMaturity::High), "high");
    }
}
