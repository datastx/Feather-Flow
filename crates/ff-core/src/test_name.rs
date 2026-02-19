//! Strongly-typed singular test name.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// A non-empty singular test name.
    pub struct TestName;
}
