//! Unified node kind for all resource types in a Featherflow project.
//!
//! Every node directory contains a `.yml` configuration file whose `kind`
//! field identifies the resource type.  The [`NodeKind`] enum is the
//! canonical discriminator used during project discovery.

use serde::{Deserialize, Serialize};

/// Canonical resource kind that unifies all node types.
///
/// Featherflow projects organise resources under `node_paths` directories.
/// Each resource lives in its own sub-directory with a mandatory `.yml` file.
/// The `kind` field in that YAML file determines how the resource is loaded:
///
/// | kind         | data file   | description                          |
/// |--------------|-------------|--------------------------------------|
/// | `sql`        | `.sql`      | SQL transformation model             |
/// | `seed`       | `.csv`      | CSV seed data                        |
/// | `source`     | *(none)*    | External data source definition      |
/// | `function`   | `.sql`      | User-defined SQL function / macro    |
/// | `python`     | `.py`       | Python transformation *(reserved)*   |
///
/// Legacy values (`model`, `sources`, `functions`) are accepted during
/// deserialization and normalised to their modern equivalents via
/// [`NodeKind::normalize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    // ── Modern names ─────────────────────────────────────────────────
    /// SQL transformation model
    Sql,
    /// CSV seed data
    Seed,
    /// External data source definition
    Source,
    /// User-defined SQL function / macro
    Function,
    /// Python transformation (reserved for future use)
    Python,

    // ── Legacy aliases (accepted on read, normalised away) ───────────
    /// Legacy alias for [`NodeKind::Sql`]
    Model,
    /// Legacy alias for [`NodeKind::Source`] (plural form from v1 YAML)
    Sources,
    /// Legacy alias for [`NodeKind::Function`] (plural form from v1 YAML)
    Functions,
}

impl NodeKind {
    /// Collapse legacy aliases to their canonical form.
    ///
    /// ```
    /// # use ff_core::node::NodeKind;
    /// assert_eq!(NodeKind::Model.normalize(), NodeKind::Sql);
    /// assert_eq!(NodeKind::Sources.normalize(), NodeKind::Source);
    /// assert_eq!(NodeKind::Functions.normalize(), NodeKind::Function);
    /// assert_eq!(NodeKind::Sql.normalize(), NodeKind::Sql);
    /// ```
    pub fn normalize(self) -> Self {
        match self {
            NodeKind::Model => NodeKind::Sql,
            NodeKind::Sources => NodeKind::Source,
            NodeKind::Functions => NodeKind::Function,
            other => other,
        }
    }

    /// Returns the expected data-file extension for this kind, if any.
    ///
    /// Kinds that have no companion data file (e.g. `source`) return `None`.
    pub fn expected_extension(&self) -> Option<&'static str> {
        match self.normalize() {
            NodeKind::Sql => Some("sql"),
            NodeKind::Seed => Some("csv"),
            NodeKind::Function => Some("sql"),
            NodeKind::Python => Some("py"),
            NodeKind::Source => None,
            // Legacy variants are handled by normalize(); unreachable after normalize
            _ => None,
        }
    }

    /// Human-readable label for error messages and display.
    pub fn label(&self) -> &'static str {
        match self.normalize() {
            NodeKind::Sql => "sql model",
            NodeKind::Seed => "seed",
            NodeKind::Source => "source",
            NodeKind::Function => "function",
            NodeKind::Python => "python model",
            _ => "unknown",
        }
    }
}

impl std::fmt::Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.normalize() {
            NodeKind::Sql => write!(f, "sql"),
            NodeKind::Seed => write!(f, "seed"),
            NodeKind::Source => write!(f, "source"),
            NodeKind::Function => write!(f, "function"),
            NodeKind::Python => write!(f, "python"),
            _ => write!(f, "unknown"),
        }
    }
}

/// Lightweight probe that deserializes only the `kind` field from a YAML file.
///
/// Used by the unified node discovery to cheaply determine the resource type
/// before committing to a full parse with the type-specific schema.
#[derive(Debug, Deserialize)]
pub struct NodeKindProbe {
    /// The `kind` field from the YAML file, if present.
    #[serde(default)]
    pub kind: Option<NodeKind>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_legacy_kinds() {
        assert_eq!(NodeKind::Model.normalize(), NodeKind::Sql);
        assert_eq!(NodeKind::Sources.normalize(), NodeKind::Source);
        assert_eq!(NodeKind::Functions.normalize(), NodeKind::Function);
    }

    #[test]
    fn normalize_modern_kinds_are_identity() {
        assert_eq!(NodeKind::Sql.normalize(), NodeKind::Sql);
        assert_eq!(NodeKind::Seed.normalize(), NodeKind::Seed);
        assert_eq!(NodeKind::Source.normalize(), NodeKind::Source);
        assert_eq!(NodeKind::Function.normalize(), NodeKind::Function);
        assert_eq!(NodeKind::Python.normalize(), NodeKind::Python);
    }

    #[test]
    fn display_uses_modern_names() {
        assert_eq!(NodeKind::Model.to_string(), "sql");
        assert_eq!(NodeKind::Sources.to_string(), "source");
        assert_eq!(NodeKind::Functions.to_string(), "function");
        assert_eq!(NodeKind::Sql.to_string(), "sql");
    }

    #[test]
    fn expected_extensions() {
        assert_eq!(NodeKind::Sql.expected_extension(), Some("sql"));
        assert_eq!(NodeKind::Seed.expected_extension(), Some("csv"));
        assert_eq!(NodeKind::Source.expected_extension(), None);
        assert_eq!(NodeKind::Function.expected_extension(), Some("sql"));
        assert_eq!(NodeKind::Python.expected_extension(), Some("py"));
    }

    #[test]
    fn deserialize_modern_kinds() {
        let probe: NodeKindProbe = serde_yaml::from_str("kind: sql").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Sql);

        let probe: NodeKindProbe = serde_yaml::from_str("kind: seed").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Seed);

        let probe: NodeKindProbe = serde_yaml::from_str("kind: source").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Source);

        let probe: NodeKindProbe = serde_yaml::from_str("kind: function").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Function);
    }

    #[test]
    fn deserialize_legacy_kinds() {
        let probe: NodeKindProbe = serde_yaml::from_str("kind: model").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Model);

        let probe: NodeKindProbe = serde_yaml::from_str("kind: sources").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Sources);

        let probe: NodeKindProbe = serde_yaml::from_str("kind: functions").unwrap();
        assert_eq!(probe.kind.unwrap(), NodeKind::Functions);
    }

    #[test]
    fn probe_missing_kind() {
        let probe: NodeKindProbe = serde_yaml::from_str("version: 1").unwrap();
        assert!(probe.kind.is_none());
    }
}
