//! Strongly-typed function name wrapper.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// Strongly-typed wrapper for user-defined function names.
    ///
    /// Prevents accidental mixing of function names with model names, table names,
    /// or other string types. Guaranteed non-empty after construction.
    pub struct FunctionName;
}

#[cfg(test)]
#[path = "function_name_test.rs"]
mod tests;
