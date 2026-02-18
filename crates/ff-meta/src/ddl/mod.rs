//! Embedded DDL migrations for the meta database.
//!
//! Each migration is a numbered `.sql` file embedded via `include_str!`.
//! The [`MIGRATIONS`] array is ordered by version number and consumed by
//! [`crate::migration::run_migrations`].

/// A single DDL migration.
pub struct Migration {
    /// Sequential version number (1-based).
    pub version: i32,
    /// Raw SQL to execute.
    pub sql: &'static str,
}

/// All known migrations, in order.
pub static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: include_str!("v001_initial.sql"),
    },
    Migration {
        version: 2,
        sql: include_str!("v002_effective_classification.sql"),
    },
];
