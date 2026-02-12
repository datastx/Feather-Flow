//! Shared serde helper functions used across multiple modules.

/// Serde default function that returns `true`.
///
/// Used for boolean fields that should default to enabled/active.
pub fn default_true() -> bool {
    true
}
