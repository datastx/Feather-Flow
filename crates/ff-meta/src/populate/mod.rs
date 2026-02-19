//! Population functions for the meta database.
//!
//! Each submodule handles one entity type. All functions take `&Connection`
//! so callers can wrap them in a single transaction via `MetaDb::transaction`.

pub mod analysis;
pub mod compilation;
pub mod execution;
pub mod functions;
pub mod lifecycle;
pub mod models;
pub mod project;
pub mod seeds;
pub mod sources;
pub mod tests;

use crate::error::MetaResult;
use duckdb::Connection;
use ff_core::Project;

use self::functions::populate_functions;
use self::models::populate_models;
use self::project::populate_project;
use self::seeds::populate_seeds;
use self::sources::populate_sources;
use self::tests::{populate_schema_tests, populate_singular_tests};

#[cfg(test)]
#[path = "populate_test.rs"]
mod populate_tests;

/// Populate all phase-1 data (project load) into the meta database.
///
/// Inserts the project row plus all discovered models, sources, functions,
/// seeds, and tests. Returns the generated `project_id`.
pub fn populate_project_load(conn: &Connection, project: &Project) -> MetaResult<i64> {
    let project_id = populate_project(conn, &project.config, &project.root)?;
    let model_id_map = populate_models(conn, project_id, &project.models, &project.config)?;
    populate_sources(conn, project_id, &project.sources)?;
    populate_functions(conn, project_id, &project.functions)?;
    populate_seeds(conn, project_id, &project.seeds)?;
    populate_schema_tests(conn, project_id, &project.tests, &model_id_map)?;
    populate_singular_tests(conn, project_id, &project.singular_tests)?;
    Ok(project_id)
}
