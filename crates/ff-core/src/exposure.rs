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

/// Kind discriminator for exposure files (must be "exposure")
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExposureKind {
    /// The only valid kind value
    #[default]
    Exposure,
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
    #[serde(default)]
    pub kind: ExposureKind,

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

impl Exposure {
    /// Load an exposure from a YAML file
    pub fn from_file(path: &Path) -> CoreResult<Self> {
        if !path.exists() {
            return Err(CoreError::ConfigNotFound {
                path: path.display().to_string(),
            });
        }

        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
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
///
/// Returns an error if duplicate exposure names are found across files.
pub fn discover_exposures(paths: &[impl AsRef<Path>]) -> CoreResult<Vec<Exposure>> {
    let mut exposures = Vec::new();

    for path in paths {
        let path = path.as_ref();
        if !path.exists() || !path.is_dir() {
            continue;
        }

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();
            if !file_path
                .extension()
                .is_some_and(|e| e == "yml" || e == "yaml")
            {
                continue;
            }

            match Exposure::from_file(&file_path) {
                Ok(exposure) => exposures.push(exposure),
                Err(e) => {
                    log::warn!(
                        "Failed to load exposure from {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }
    }

    // Detect duplicate exposure names
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (idx, exposure) in exposures.iter().enumerate() {
        if let Some(&prev_idx) = seen.get(&exposure.name) {
            return Err(CoreError::ExposureDuplicateName {
                name: exposure.name.clone(),
                path1: exposures[prev_idx]
                    .source_path
                    .clone()
                    .unwrap_or_else(|| format!("exposure #{}", prev_idx + 1)),
                path2: exposure
                    .source_path
                    .clone()
                    .unwrap_or_else(|| format!("exposure #{}", idx + 1)),
            });
        }
        seen.insert(exposure.name.clone(), idx);
    }

    Ok(exposures)
}

#[cfg(test)]
#[path = "exposure_test.rs"]
mod tests;
