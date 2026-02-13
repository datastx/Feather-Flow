//! Selector parsing and model filtering
//!
//! Supports dbt-style selectors:
//! - `model_name` - exact model name
//! - `+model_name` - model and all ancestors
//! - `model_name+` - model and all descendants
//! - `+model_name+` - model, ancestors, and descendants
//! - `path:models/staging/*` - models matching path pattern
//! - `tag:daily` - models with the specified tag
//! - `state:modified` - models with changed SQL (requires --state)
//! - `state:new` - models not in reference manifest (requires --state)
//! - `state:modified+` - modified models and their descendants

use crate::dag::ModelDag;
use crate::error::{CoreError, CoreResult};
use crate::manifest::Manifest;
use crate::Model;
use crate::ModelName;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Traversal depth for ancestor/descendant graph walks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversalDepth {
    /// No traversal
    None,
    /// Traverse up to N hops
    Bounded(usize),
    /// Traverse all ancestors/descendants
    Unlimited,
}

/// State comparison type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateType {
    /// Models that have been modified (SQL changed)
    Modified,
    /// Models that are new (not in reference manifest)
    New,
}

/// Parsed selector type
#[derive(Debug, Clone)]
pub enum SelectorType {
    /// Model name with optional ancestor/descendant depth
    Model {
        name: String,
        ancestor_depth: TraversalDepth,
        descendant_depth: TraversalDepth,
    },
    /// Path-based selection with glob pattern
    Path { pattern: String },
    /// Tag-based selection
    Tag { tag: String },
    /// Owner-based selection (matches owner field or meta.owner)
    Owner { owner: String },
    /// State-based selection (requires reference manifest)
    State {
        state_type: StateType,
        include_descendants: bool,
    },
}

/// A selector that can filter models
#[derive(Debug)]
pub struct Selector {
    selector_type: SelectorType,
}

impl Selector {
    /// Parse a selector string
    pub fn parse(selector: &str) -> CoreResult<Self> {
        let selector = selector.trim();

        if let Some(pattern) = selector.strip_prefix("path:") {
            let pattern = pattern.to_string();
            if pattern.is_empty() {
                return Err(CoreError::InvalidSelector {
                    selector: selector.to_string(),
                    reason: "path: selector requires a pattern".to_string(),
                });
            }
            return Ok(Self {
                selector_type: SelectorType::Path { pattern },
            });
        }

        if let Some(tag) = selector.strip_prefix("tag:") {
            let tag = tag.to_string();
            if tag.is_empty() {
                return Err(CoreError::InvalidSelector {
                    selector: selector.to_string(),
                    reason: "tag: selector requires a tag name".to_string(),
                });
            }
            return Ok(Self {
                selector_type: SelectorType::Tag { tag },
            });
        }

        if let Some(owner) = selector.strip_prefix("owner:") {
            let owner = owner.to_string();
            if owner.is_empty() {
                return Err(CoreError::InvalidSelector {
                    selector: selector.to_string(),
                    reason: "owner: selector requires an owner name".to_string(),
                });
            }
            return Ok(Self {
                selector_type: SelectorType::Owner { owner },
            });
        }

        if let Some(state_str) = selector.strip_prefix("state:") {
            let include_descendants = state_str.ends_with('+');
            let state_name = state_str.trim_end_matches('+');

            let state_type = match state_name {
                "modified" => StateType::Modified,
                "new" => StateType::New,
                _ => {
                    return Err(CoreError::InvalidSelector {
                        selector: selector.to_string(),
                        reason: format!(
                            "unknown state type '{}', expected 'modified' or 'new'",
                            state_name
                        ),
                    });
                }
            };

            return Ok(Self {
                selector_type: SelectorType::State {
                    state_type,
                    include_descendants,
                },
            });
        }

        // Parse model selector with optional +prefix/suffix and bounded depth
        // Supported forms: model, +model, model+, +model+,
        //   N+model, model+N, N+model+N, N+model+, +model+N
        let (ancestor_depth, descendant_depth, name) = parse_model_selector(selector)?;

        if name.is_empty() {
            return Err(CoreError::InvalidSelector {
                selector: selector.to_string(),
                reason: "model name cannot be empty".to_string(),
            });
        }

        Ok(Self {
            selector_type: SelectorType::Model {
                name,
                ancestor_depth,
                descendant_depth,
            },
        })
    }

    /// Check if this selector requires a reference manifest (state-based)
    pub fn requires_state(&self) -> bool {
        matches!(self.selector_type, SelectorType::State { .. })
    }

    /// Apply this selector to filter models
    ///
    /// Returns a list of model names that match the selector
    pub fn apply(
        &self,
        models: &HashMap<ModelName, Model>,
        dag: &ModelDag,
    ) -> CoreResult<Vec<String>> {
        self.apply_with_state(models, dag, None)
    }

    /// Apply this selector with an optional reference manifest for state comparison
    ///
    /// Returns a list of model names that match the selector
    pub fn apply_with_state(
        &self,
        models: &HashMap<ModelName, Model>,
        dag: &ModelDag,
        reference_manifest: Option<&Manifest>,
    ) -> CoreResult<Vec<String>> {
        let matched = self.apply_unordered(models, dag, reference_manifest)?;
        // Return in topological order
        let order = dag.topological_order()?;
        Ok(order.into_iter().filter(|m| matched.contains(m)).collect())
    }

    /// Apply this selector returning an unordered set of matched model names.
    ///
    /// Used internally to avoid redundant topo-sorts when combining multiple
    /// selectors.
    fn apply_unordered(
        &self,
        models: &HashMap<ModelName, Model>,
        dag: &ModelDag,
        reference_manifest: Option<&Manifest>,
    ) -> CoreResult<HashSet<String>> {
        match &self.selector_type {
            SelectorType::Model {
                name,
                ancestor_depth,
                descendant_depth,
            } => self.select_by_model_unordered(name, *ancestor_depth, *descendant_depth, dag),
            SelectorType::Path { pattern } => {
                Ok(self.select_by_path(pattern, models)?.into_iter().collect())
            }
            SelectorType::Tag { tag } => Ok(self.select_by_tag(tag, models)?.into_iter().collect()),
            SelectorType::Owner { owner } => {
                Ok(self.select_by_owner(owner, models)?.into_iter().collect())
            }
            SelectorType::State {
                state_type,
                include_descendants,
            } => {
                let manifest = reference_manifest.ok_or_else(|| CoreError::InvalidSelector {
                    selector: format!("state:{:?}", state_type),
                    reason: "state: selector requires --state flag with path to reference manifest"
                        .to_string(),
                })?;
                self.select_by_state_unordered(
                    state_type,
                    *include_descendants,
                    models,
                    dag,
                    manifest,
                )
            }
        }
    }

    /// Select models by name with optional ancestor/descendant depth.
    ///
    /// Returns the matched set (unordered). Callers that need topological
    /// order should sort after collecting all selectors.
    fn select_by_model_unordered(
        &self,
        name: &str,
        ancestor_depth: TraversalDepth,
        descendant_depth: TraversalDepth,
        dag: &ModelDag,
    ) -> CoreResult<HashSet<String>> {
        if !dag.contains(name) {
            return Err(CoreError::ModelNotFound {
                name: name.to_string(),
            });
        }

        let mut selected = HashSet::new();
        selected.insert(name.to_string());

        match ancestor_depth {
            TraversalDepth::None => {}
            TraversalDepth::Unlimited => selected.extend(dag.ancestors(name)),
            TraversalDepth::Bounded(n) => selected.extend(dag.ancestors_bounded(name, n)),
        }

        match descendant_depth {
            TraversalDepth::None => {}
            TraversalDepth::Unlimited => selected.extend(dag.descendants(name)),
            TraversalDepth::Bounded(n) => selected.extend(dag.descendants_bounded(name, n)),
        }

        Ok(selected)
    }

    /// Select models by path pattern (supports glob wildcards)
    fn select_by_path(
        &self,
        pattern: &str,
        models: &HashMap<ModelName, Model>,
    ) -> CoreResult<Vec<String>> {
        let mut selected = Vec::new();

        for (name, model) in models {
            if Self::matches_path_pattern(&model.path, pattern) {
                selected.push(name.to_string());
            }
        }

        Ok(selected)
    }

    /// Check if a path matches a glob-like pattern
    fn matches_path_pattern(path: &Path, pattern: &str) -> bool {
        let path_str = path.to_string_lossy();

        if pattern.contains("**") {
            return Self::matches_double_star(&path_str, pattern);
        }

        if pattern.contains('*') {
            return Self::matches_single_star(&path_str, pattern);
        }

        path_str.contains(pattern)
    }

    fn matches_double_star(path_str: &str, pattern: &str) -> bool {
        let parts: Vec<&str> = pattern.split("**").collect();
        if parts.len() != 2 {
            return path_str.contains(pattern);
        }

        let prefix = parts[0].trim_end_matches('/');
        let suffix = parts[1].trim_start_matches('/');

        let matches_prefix = prefix.is_empty() || path_str.contains(prefix);
        let matches_suffix = suffix.is_empty()
            || suffix == "*"
            || path_str.ends_with(suffix)
            || (suffix.starts_with("*.") && {
                let ext = suffix.trim_start_matches("*.");
                path_str.ends_with(&format!(".{}", ext))
            });

        matches_prefix && matches_suffix
    }

    fn matches_single_star(path_str: &str, pattern: &str) -> bool {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() != 2 {
            return path_str.contains(pattern);
        }

        let prefix = parts[0];
        let suffix = parts[1];

        let matches_prefix = prefix.is_empty() || path_str.contains(prefix);
        let matches_suffix = suffix.is_empty() || path_str.ends_with(suffix);

        matches_prefix && matches_suffix
    }

    /// Select models by tag
    ///
    /// Checks both `model.config.tags` (from SQL config()) and
    /// `model.schema.tags` (from the model's YAML schema file).
    fn select_by_tag(
        &self,
        tag: &str,
        models: &HashMap<ModelName, Model>,
    ) -> CoreResult<Vec<String>> {
        let mut selected = Vec::new();
        let tag_str = tag.to_string();

        for (name, model) in models {
            let in_config = model.config.tags.contains(&tag_str);
            let in_schema = model
                .schema
                .as_ref()
                .map(|s| s.tags.contains(&tag_str))
                .unwrap_or(false);
            if in_config || in_schema {
                selected.push(name.to_string());
            }
        }

        Ok(selected)
    }

    /// Select models by owner (matches owner field or meta.owner, supports partial matching)
    fn select_by_owner(
        &self,
        owner: &str,
        models: &HashMap<ModelName, Model>,
    ) -> CoreResult<Vec<String>> {
        let mut selected = Vec::new();
        let owner_lower = owner.to_lowercase();

        for (name, model) in models {
            // Use the get_owner() method which checks both direct owner and meta.owner
            if let Some(model_owner) = model.get_owner() {
                // Support partial matching (e.g., "data-team" matches "data-team@company.com")
                if model_owner.to_lowercase().contains(&owner_lower) {
                    selected.push(name.to_string());
                }
            }
        }

        Ok(selected)
    }

    /// Select models by state comparison (unordered)
    fn select_by_state_unordered(
        &self,
        state_type: &StateType,
        include_descendants: bool,
        models: &HashMap<ModelName, Model>,
        dag: &ModelDag,
        reference_manifest: &Manifest,
    ) -> CoreResult<HashSet<String>> {
        let mut selected: HashSet<String> = HashSet::new();

        for (name, model) in models {
            let should_select = match state_type {
                StateType::New => {
                    // Model is new if it doesn't exist in reference manifest
                    !reference_manifest.models.contains_key(name.as_str())
                }
                StateType::Modified => {
                    // Model is modified if SQL content differs from reference
                    if let Some(ref_model) = reference_manifest.models.get(name.as_str()) {
                        Self::is_model_modified(model, ref_model)
                    } else {
                        // If not in reference, it's also considered "modified" (new)
                        true
                    }
                }
            };

            if should_select {
                selected.insert(name.to_string());
            }
        }

        // If include_descendants, add all downstream models
        if include_descendants {
            let descendants: Vec<String> = selected
                .iter()
                .flat_map(|name| dag.descendants(name))
                .collect();
            selected.extend(descendants);
        }

        Ok(selected)
    }

    /// Check if a model has been modified compared to reference
    fn is_model_modified(current: &Model, reference: &crate::manifest::ManifestModel) -> bool {
        // Compare by checking if dependencies changed
        let current_deps: HashSet<String> =
            current.depends_on.iter().map(|m| m.to_string()).collect();
        let ref_deps: HashSet<String> =
            reference.depends_on.iter().map(|m| m.to_string()).collect();

        if current_deps != ref_deps {
            return true;
        }

        // Compare materialization (treat None as "unchanged" — only detect actual differences)
        if let Some(ref current_mat) = current.config.materialized {
            if current_mat != &reference.materialized {
                return true;
            }
        }

        // Compare schema
        let current_schema = current.config.schema.as_deref();
        if current_schema != reference.schema.as_deref() {
            return true;
        }

        // Compare tags
        let current_tags: HashSet<_> = current.config.tags.iter().collect();
        let ref_tags: HashSet<_> = reference.tags.iter().collect();
        if current_tags != ref_tags {
            return true;
        }

        // Compare SQL content via checksum if available in the manifest
        if let Some(ref ref_checksum) = reference.sql_checksum {
            let current_checksum = current.sql_checksum();
            if current_checksum != *ref_checksum {
                return true;
            }
        }

        false
    }
}

/// Parse a model selector string into `(ancestor_depth, descendant_depth, name)`.
///
/// Handles: `model`, `+model`, `model+`, `+model+`,
///   `N+model`, `model+N`, `N+model+N`, `N+model+`, `+model+N`.
///
/// **Limitation**: Purely numeric model names (e.g. `123`) cannot use the
/// bounded `N+model` syntax because the leading digits are parsed as a
/// depth prefix. Such models can still be selected with plain `123` or
/// `+123` / `123+` (unbounded) forms.
fn parse_model_selector(s: &str) -> CoreResult<(TraversalDepth, TraversalDepth, String)> {
    let parts: Vec<&str> = s.split('+').collect();
    match parts.len() {
        // No '+' at all → plain model name
        1 => Ok((
            TraversalDepth::None,
            TraversalDepth::None,
            parts[0].to_string(),
        )),
        // One '+' → either prefix or suffix
        2 => {
            let (left, right) = (parts[0], parts[1]);
            if left.is_empty() && !right.is_empty() {
                // +model
                Ok((
                    TraversalDepth::Unlimited,
                    TraversalDepth::None,
                    right.to_string(),
                ))
            } else if !left.is_empty() && right.is_empty() {
                // Could be "model+" or "N+"
                // If left is all digits, that's invalid (e.g. "3+")
                if left.chars().all(|c| c.is_ascii_digit()) {
                    Err(CoreError::InvalidSelector {
                        selector: s.to_string(),
                        reason: "model name cannot be empty".to_string(),
                    })
                } else {
                    // model+
                    Ok((
                        TraversalDepth::None,
                        TraversalDepth::Unlimited,
                        left.to_string(),
                    ))
                }
            } else if !left.is_empty() && !right.is_empty() {
                // N+model or model+N
                if left.chars().all(|c| c.is_ascii_digit()) {
                    let n: usize = left.parse().map_err(|_| CoreError::InvalidSelector {
                        selector: s.to_string(),
                        reason: format!("invalid depth '{}'", left),
                    })?;
                    Ok((
                        TraversalDepth::Bounded(n),
                        TraversalDepth::None,
                        right.to_string(),
                    ))
                } else if right.chars().all(|c| c.is_ascii_digit()) {
                    let n: usize = right.parse().map_err(|_| CoreError::InvalidSelector {
                        selector: s.to_string(),
                        reason: format!("invalid depth '{}'", right),
                    })?;
                    Ok((
                        TraversalDepth::None,
                        TraversalDepth::Bounded(n),
                        left.to_string(),
                    ))
                } else {
                    Err(CoreError::InvalidSelector {
                        selector: s.to_string(),
                        reason: "ambiguous selector: expected N+model or model+N".to_string(),
                    })
                }
            } else {
                // both empty → just "+"
                Err(CoreError::InvalidSelector {
                    selector: s.to_string(),
                    reason: "model name cannot be empty".to_string(),
                })
            }
        }
        // Two '+' → three parts
        3 => {
            let (left, middle, right) = (parts[0], parts[1], parts[2]);
            if middle.is_empty() {
                return Err(CoreError::InvalidSelector {
                    selector: s.to_string(),
                    reason: "model name cannot be empty".to_string(),
                });
            }
            let ancestor_depth = if left.is_empty() {
                TraversalDepth::Unlimited
            } else if left.chars().all(|c| c.is_ascii_digit()) {
                let n: usize = left.parse().map_err(|_| CoreError::InvalidSelector {
                    selector: s.to_string(),
                    reason: format!("invalid depth '{}'", left),
                })?;
                TraversalDepth::Bounded(n)
            } else {
                return Err(CoreError::InvalidSelector {
                    selector: s.to_string(),
                    reason: format!("expected a number before '+', got '{}'", left),
                });
            };
            let descendant_depth = if right.is_empty() {
                TraversalDepth::Unlimited
            } else if right.chars().all(|c| c.is_ascii_digit()) {
                let n: usize = right.parse().map_err(|_| CoreError::InvalidSelector {
                    selector: s.to_string(),
                    reason: format!("invalid depth '{}'", right),
                })?;
                TraversalDepth::Bounded(n)
            } else {
                return Err(CoreError::InvalidSelector {
                    selector: s.to_string(),
                    reason: format!("expected a number after '+', got '{}'", right),
                });
            };
            Ok((ancestor_depth, descendant_depth, middle.to_string()))
        }
        _ => Err(CoreError::InvalidSelector {
            selector: s.to_string(),
            reason: "too many '+' characters in selector".to_string(),
        }),
    }
}

/// Parse comma-separated selectors and return the union of matched models in
/// topological order.
///
/// If `selectors_str` is `None`, returns all models in topological order.
pub fn apply_selectors(
    selectors_str: &Option<String>,
    models: &HashMap<ModelName, Model>,
    dag: &ModelDag,
) -> CoreResult<Vec<String>> {
    let Some(raw) = selectors_str else {
        return dag.topological_order();
    };

    let mut combined: HashSet<String> = HashSet::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let sel = Selector::parse(token)?;
        let matched = sel.apply_unordered(models, dag, None)?;
        combined.extend(matched);
    }

    // Single topo-sort at the end
    let order = dag.topological_order()?;
    Ok(order.into_iter().filter(|m| combined.contains(m)).collect())
}

#[cfg(test)]
#[path = "selector_test.rs"]
mod tests;
