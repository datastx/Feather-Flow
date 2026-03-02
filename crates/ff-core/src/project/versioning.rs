//! Model version resolution

use crate::model::Model;

use super::Project;

impl Project {
    /// Resolve a model reference, handling version resolution
    ///
    /// If the reference is unversioned (e.g., "fct_orders"), resolves to the latest version.
    /// If the reference is versioned (e.g., "fct_orders_v1"), resolves to that specific version.
    ///
    /// Returns (resolved_name, warnings) where warnings contains any deprecation warnings.
    pub fn resolve_model_reference(&self, reference: &str) -> (Option<&Model>, Vec<String>) {
        let mut warnings = Vec::new();

        if let Some(model) = self.models.get(reference) {
            if model.is_deprecated() {
                let msg = model
                    .get_deprecation_message()
                    .unwrap_or("This model is deprecated");
                warnings.push(format!(
                    "Warning: Model '{}' is deprecated. {}",
                    reference, msg
                ));
            }
            return (Some(model), warnings);
        }

        let (parsed_base, _) = Model::parse_version(reference);
        if parsed_base.is_some() {
            return (None, warnings);
        }

        let Some((name, model)) = self.get_latest_version(reference) else {
            return (None, warnings);
        };

        if model.is_deprecated() {
            let msg = model
                .get_deprecation_message()
                .unwrap_or("This model is deprecated");
            warnings.push(format!("Warning: Model '{}' is deprecated. {}", name, msg));
        }
        (Some(model), warnings)
    }

    /// Get the latest version of a model by base name
    ///
    /// Returns the model with the highest version number, or the unversioned model if no versions exist.
    pub fn get_latest_version(&self, base_name: &str) -> Option<(&str, &Model)> {
        self.models
            .iter()
            .filter(|(name, model)| model.get_base_name() == base_name || *name == base_name)
            .max_by_key(|(_, model)| model.version.unwrap_or(0))
            .map(|(name, model)| (name.as_str(), model))
    }

    /// Get all versions of a model by base name
    pub fn get_all_versions(&self, base_name: &str) -> Vec<(&str, &Model)> {
        let mut versions: Vec<(&str, &Model)> = self
            .models
            .iter()
            .filter(|(_, model)| model.get_base_name() == base_name)
            .map(|(name, model)| (name.as_str(), model))
            .collect();

        versions.sort_by(|a, b| {
            let va = a.1.version.unwrap_or(0);
            let vb = b.1.version.unwrap_or(0);
            va.cmp(&vb)
        });

        versions
    }

    /// Check if a model reference is to a non-latest version and return a warning if so
    pub fn check_version_warning(&self, reference: &str) -> Option<String> {
        let model = self.models.get(reference)?;
        if !model.is_versioned() {
            return None;
        }
        let base_name = model.get_base_name();
        let (latest_name, _) = self.get_latest_version(base_name)?;
        (latest_name != reference).then(|| {
            format!(
                "Warning: '{}' is not the latest version. Latest is '{}'.",
                reference, latest_name
            )
        })
    }
}
