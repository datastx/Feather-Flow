//! Strongly-typed seed name.

use crate::newtype_string::define_newtype_string;

define_newtype_string! {
    /// A non-empty seed name.
    pub struct SeedName;
}
