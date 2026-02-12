//! Strongly-typed model name wrapper.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// Strongly-typed wrapper for model names.
    ///
    /// Prevents accidental mixing of model names with table names, column names,
    /// or other string types. Guaranteed non-empty after construction.
    pub struct ModelName;
}

#[cfg(test)]
#[path = "model_name_test.rs"]
mod tests;
