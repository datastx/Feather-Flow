//! ff-jinja - Jinja templating layer for Featherflow
//!
//! This crate provides a simplified Jinja templating environment
//! with `var()`, `env()`, `log()`, `error()`, `warn()`,
//! `from_json()`, `to_json()`, `is_exists()`, `is_incremental()` (deprecated),
//! and `this` functions, as well as built-in macros for common SQL operations.
//!
//! Context variables (`project_name`, `target`, `run_id`, `run_started_at`,
//! `ff_version`, `executing`, `model`) are available when constructed with
//! [`JinjaEnvironment::with_context`].

pub(crate) mod builtins;
pub(crate) mod context;
pub(crate) mod custom_tests;
pub(crate) mod environment;
pub(crate) mod error;
pub(crate) mod functions;

pub use builtins::{
    get_builtin_macros, get_macro_by_name, get_macro_categories, get_macros_by_category,
    MacroMetadata, MacroParam,
};
pub use context::{ModelContext, TargetContext, TemplateContext};
pub use custom_tests::{
    discover_custom_test_macros, generate_custom_test_sql, CustomTestMacro, CustomTestRegistry,
};
pub use environment::JinjaEnvironment;
pub use error::JinjaError;
pub use functions::IncrementalState;
