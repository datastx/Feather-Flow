//! Strongly-typed source name.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// A non-empty source group name.
    pub struct SourceName;
}
