//! Macro for defining strongly-typed string newtypes.
//!
//! All newtypes share the same invariant (non-empty string) and the same set of
//! trait impls (Display, Deref, AsRef, Borrow, TryFrom, PartialEq, Serialize,
//! Deserialize). This macro generates all of that from a single invocation.

/// Define a strongly-typed, non-empty string newtype.
///
/// Generates:
/// - The struct with `Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize`
/// - Custom `Deserialize` (rejects empty strings)
/// - `new()` (panics on empty), `try_new()` (returns Option), `as_str()`, `into_inner()`
/// - `Display`, `AsRef<str>`, `Deref<Target=str>`, `Borrow<str>`
/// - `TryFrom<String>`, `TryFrom<&str>`
/// - `PartialEq<str>`, `PartialEq<&str>`, `PartialEq<String>`
macro_rules! define_newtype_string {
    (
        $(#[$meta:meta])*
        $vis:vis struct $Name:ident;
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
        $vis struct $Name(String);

        impl<'de> serde::Deserialize<'de> for $Name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                $Name::try_new(s)
                    .ok_or_else(|| serde::de::Error::custom(concat!(stringify!($Name), " must not be empty")))
            }
        }

        impl $Name {
            /// Create a new instance, panicking if the name is empty.
            ///
            /// Prefer [`try_new`](Self::try_new) when handling untrusted input.
            pub fn new(name: impl Into<String>) -> Self {
                let s = name.into();
                assert!(!s.is_empty(), concat!(stringify!($Name), " must not be empty"));
                Self(s)
            }

            /// Try to create a new instance, returning `None` if the name is empty.
            pub fn try_new(name: impl Into<String>) -> Option<Self> {
                let s = name.into();
                if s.is_empty() { None } else { Some(Self(s)) }
            }

            /// Return the underlying name as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the wrapper and return the inner `String`.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $Name {
            fn as_ref(&self) -> &str { &self.0 }
        }

        impl std::ops::Deref for $Name {
            type Target = str;
            fn deref(&self) -> &str { &self.0 }
        }

        impl std::borrow::Borrow<str> for $Name {
            fn borrow(&self) -> &str { &self.0 }
        }

        impl TryFrom<String> for $Name {
            type Error = &'static str;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                if s.is_empty() {
                    Err(concat!(stringify!($Name), " must not be empty"))
                } else {
                    Ok(Self(s))
                }
            }
        }

        impl TryFrom<&str> for $Name {
            type Error = &'static str;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                if s.is_empty() {
                    Err(concat!(stringify!($Name), " must not be empty"))
                } else {
                    Ok(Self(s.to_string()))
                }
            }
        }

        impl PartialEq<str> for $Name {
            fn eq(&self, other: &str) -> bool { self.0 == other }
        }

        impl PartialEq<&str> for $Name {
            fn eq(&self, other: &&str) -> bool { self.0 == *other }
        }

        impl PartialEq<String> for $Name {
            fn eq(&self, other: &String) -> bool { self.0 == *other }
        }
    };
}

pub(crate) use define_newtype_string;
