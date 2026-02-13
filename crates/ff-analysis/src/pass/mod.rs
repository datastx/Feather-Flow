//! Pass infrastructure — composable analysis passes over DataFusion LogicalPlans

pub(crate) mod expr_utils;
pub mod plan_cross_model;
pub(crate) mod plan_join_keys;
pub(crate) mod plan_nullability;
pub mod plan_pass;
pub(crate) mod plan_type_inference;
pub(crate) mod plan_unused_columns;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Strongly-typed diagnostic codes emitted by analysis passes.
///
/// Each variant corresponds to a specific diagnostic rule (e.g. A001 = unknown type).
/// Using an enum instead of a bare `String` prevents typos and enables exhaustive matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiagnosticCode {
    /// A001: Unknown type for column (no longer emitted — DataFusion resolves all types)
    A001,
    /// A002: Type mismatch in UNION columns
    A002,
    /// A003: UNION column count mismatch
    A003,
    /// A004: SUM/AVG on string column
    A004,
    /// A005: Lossy cast
    A005,
    /// A010: Nullable from JOIN without guard
    A010,
    /// A011: YAML NOT NULL vs JOIN nullable
    A011,
    /// A012: Redundant IS NULL check
    A012,
    /// A020: Unused column
    A020,
    // A021: Reserved/retired — SELECT * is now allowed; DataFusion expands wildcards transparently.
    // Do not reuse this code to avoid confusion with historical diagnostics.
    /// A030: Join key type mismatch
    A030,
    /// A032: Cross join
    A032,
    /// A033: Non-equi join
    A033,
    /// A040: Cross-model schema mismatch (extra/missing/type)
    A040,
    /// A041: Cross-model nullability mismatch
    A041,
}

impl std::fmt::Display for DiagnosticCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Diagnostic severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational — no action required
    Info,
    /// Warning — potential issue worth reviewing
    Warning,
    /// Error — likely bug or incorrect behavior
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "info"),
            Severity::Warning => write!(f, "warning"),
            Severity::Error => write!(f, "error"),
        }
    }
}

/// A diagnostic message produced by an analysis pass
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    /// Diagnostic code (e.g. A001)
    pub code: DiagnosticCode,
    /// Severity level
    pub severity: Severity,
    /// Human-readable message
    pub message: String,
    /// Model that produced this diagnostic
    pub model: String,
    /// Optional column reference
    pub column: Option<String>,
    /// Optional hint for how to fix
    pub hint: Option<String>,
    /// Name of the pass that produced this diagnostic.
    ///
    /// Uses `Cow<'static, str>` because all built-in pass names are string
    /// literals (`"type_inference"`, `"nullability"`, etc.) and can be
    /// borrowed at zero cost via `"name".into()`. If a future extension
    /// needs a dynamic pass name, use `Cow::Owned(name)`.
    pub pass_name: Cow<'static, str>,
}

/// Check if a pass should run given an optional filter list
pub(crate) fn should_run_pass(name: &str, filter: Option<&[String]>) -> bool {
    match filter {
        Some(allowed) => allowed.iter().any(|f| f == name),
        None => true,
    }
}
