//! Bridge between Feather-Flow's type system and DataFusion/Arrow types

pub(crate) mod functions;
pub(crate) mod lineage;
pub(crate) mod planner;
pub(crate) mod propagation;
pub(crate) mod provider;
pub(crate) mod types;
