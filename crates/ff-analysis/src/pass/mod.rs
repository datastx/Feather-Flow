//! Pass infrastructure — composable analysis passes over DataFusion LogicalPlans

pub(crate) mod expr_utils;
pub(crate) mod plan_cross_model;
pub(crate) mod plan_join_keys;
pub(crate) mod plan_nullability;
pub mod plan_pass;
pub(crate) mod plan_type_inference;
pub(crate) mod plan_unused_columns;

use ff_core::config::ConfigSeverity;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;

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

/// Resolved severity for a diagnostic code override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverriddenSeverity {
    /// Override to a specific severity level
    Level(Severity),
    /// Suppress the diagnostic entirely
    Off,
}

/// User-configured severity overrides for diagnostic codes.
///
/// Built from `Config.analysis.severity_overrides` and used to remap or
/// suppress diagnostics after passes have run. Uses separate maps for
/// typed `DiagnosticCode` keys (direct lookup, no formatting) and
/// string SA-codes.
#[derive(Debug, Clone, Default)]
pub struct SeverityOverrides {
    code_overrides: HashMap<DiagnosticCode, OverriddenSeverity>,
    sa_overrides: HashMap<String, OverriddenSeverity>,
}

impl SeverityOverrides {
    /// Build overrides from the config map.
    ///
    /// Keys that parse as a `DiagnosticCode` (e.g. `"A020"`) go into the
    /// typed map for O(1) lookup; everything else (e.g. `"SA01"`) stays
    /// as a string key.
    pub fn from_config(config_map: &HashMap<String, ConfigSeverity>) -> Self {
        let mut code_overrides = HashMap::new();
        let mut sa_overrides = HashMap::new();

        for (key, sev) in config_map {
            let resolved = match sev {
                ConfigSeverity::Info => OverriddenSeverity::Level(Severity::Info),
                ConfigSeverity::Warning => OverriddenSeverity::Level(Severity::Warning),
                ConfigSeverity::Error => OverriddenSeverity::Level(Severity::Error),
                ConfigSeverity::Off => OverriddenSeverity::Off,
            };
            if let Some(dc) = parse_diagnostic_code(key) {
                code_overrides.insert(dc, resolved);
            } else {
                sa_overrides.insert(key.clone(), resolved);
            }
        }

        Self {
            code_overrides,
            sa_overrides,
        }
    }

    /// Look up an override for a `DiagnosticCode` (e.g. `A020`).
    pub fn get_for_code(&self, code: DiagnosticCode) -> Option<OverriddenSeverity> {
        self.code_overrides.get(&code).copied()
    }

    /// Look up an override for an SA-code string (e.g. `"SA01"`, `"SA02"`).
    pub fn get_for_sa(&self, code: &str) -> Option<OverriddenSeverity> {
        self.sa_overrides.get(code).copied()
    }
}

/// Try to parse a string key into a DiagnosticCode.
fn parse_diagnostic_code(s: &str) -> Option<DiagnosticCode> {
    match s {
        "A001" => Some(DiagnosticCode::A001),
        "A002" => Some(DiagnosticCode::A002),
        "A003" => Some(DiagnosticCode::A003),
        "A004" => Some(DiagnosticCode::A004),
        "A005" => Some(DiagnosticCode::A005),
        "A010" => Some(DiagnosticCode::A010),
        "A011" => Some(DiagnosticCode::A011),
        "A012" => Some(DiagnosticCode::A012),
        "A020" => Some(DiagnosticCode::A020),
        "A030" => Some(DiagnosticCode::A030),
        "A032" => Some(DiagnosticCode::A032),
        "A033" => Some(DiagnosticCode::A033),
        "A040" => Some(DiagnosticCode::A040),
        "A041" => Some(DiagnosticCode::A041),
        _ => None,
    }
}

/// Apply severity overrides to a list of diagnostics.
///
/// - `Off` overrides remove the diagnostic from the output.
/// - `Level(s)` overrides change the diagnostic's severity to `s`.
/// - Diagnostics without overrides are passed through unchanged.
pub fn apply_severity_overrides(
    diagnostics: Vec<Diagnostic>,
    overrides: &SeverityOverrides,
) -> Vec<Diagnostic> {
    diagnostics
        .into_iter()
        .filter_map(|mut d| match overrides.get_for_code(d.code) {
            Some(OverriddenSeverity::Off) => None,
            Some(OverriddenSeverity::Level(s)) => {
                d.severity = s;
                Some(d)
            }
            None => Some(d),
        })
        .collect()
}

#[cfg(test)]
#[path = "severity_override_test.rs"]
mod severity_override_tests;
