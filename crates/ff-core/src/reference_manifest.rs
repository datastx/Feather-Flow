//! Trait-based abstraction for reference manifests used in state selectors.
//!
//! This trait decouples the selector logic from the concrete `Manifest` type,
//! allowing different implementations (JSON file, meta database) to serve as
//! reference manifests for `state:modified` and `state:new` selectors.

use crate::config::Materialization;
use crate::model_name::ModelName;

/// Snapshot of a single model from a reference manifest, containing only the
/// fields needed for state comparison.
pub struct ReferenceModelRef {
    pub depends_on: Vec<ModelName>,
    pub materialized: Materialization,
    pub schema: Option<String>,
    pub tags: Vec<String>,
    pub sql_checksum: Option<String>,
}

/// A reference manifest that can be queried for model existence and metadata.
///
/// Implemented by `Manifest` (JSON file format) and potentially by meta
/// database queries in the future.
pub trait ReferenceManifest {
    /// Check whether a model exists in the reference manifest.
    fn contains_model(&self, name: &str) -> bool;

    /// Retrieve a model snapshot for comparison against current project state.
    fn get_model_ref(&self, name: &str) -> Option<ReferenceModelRef>;
}
