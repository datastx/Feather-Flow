//! DataFusion ContextProvider for Feather-Flow schema catalog
//!
//! Implements the `ContextProvider` trait so that `SqlToRel` can resolve
//! table names, UDF signatures, and aggregate functions during planning.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result as DFResult};
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::planner::ContextProvider;
use datafusion_sql::TableReference;

use crate::datafusion_bridge::functions;
use crate::datafusion_bridge::types::sql_type_to_arrow;
use crate::schema::{RelSchema, SchemaCatalog};
use crate::types::Nullability;

/// Metadata for a user-defined scalar function stub to register in the DataFusion context.
///
/// Function names are case-insensitive — internally normalized to uppercase for lookup.
/// For table-returning functions, see [`UserTableFunctionStub`].
#[derive(Debug, Clone)]
pub struct UserFunctionStub {
    name: String,
    arg_types: Vec<String>,
    return_type: String,
}

impl UserFunctionStub {
    /// Create a new user function stub.
    ///
    /// Returns `None` if the name is empty.
    pub fn new(
        name: impl Into<String>,
        arg_types: Vec<String>,
        return_type: impl Into<String>,
    ) -> Option<Self> {
        let name = name.into();
        if name.is_empty() {
            return None;
        }
        Some(Self {
            name,
            arg_types,
            return_type: return_type.into(),
        })
    }

    /// Function name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Argument SQL type strings
    pub fn arg_types(&self) -> &[String] {
        &self.arg_types
    }

    /// Return SQL type string
    pub fn return_type(&self) -> &str {
        &self.return_type
    }
}

/// Metadata for a user-defined table function stub to register in the DataFusion context.
///
/// Table functions return a set of rows with a fixed schema. They are registered
/// via `get_table_function_source` so DataFusion can resolve `SELECT * FROM func(args)`.
#[derive(Debug, Clone)]
pub struct UserTableFunctionStub {
    name: String,
    /// Each entry is `(column_name, sql_type_string)`.
    columns: Vec<(String, String)>,
}

impl UserTableFunctionStub {
    /// Create a new user table function stub.
    ///
    /// Returns `None` if the name is empty or columns are empty.
    pub fn new(name: impl Into<String>, columns: Vec<(String, String)>) -> Option<Self> {
        let name = name.into();
        if name.is_empty() || columns.is_empty() {
            return None;
        }
        Some(Self { name, columns })
    }

    /// Function name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Output columns as `(name, sql_type)` pairs
    pub fn columns(&self) -> &[(String, String)] {
        &self.columns
    }
}

/// Pre-built function registries for reuse across multiple provider instances.
///
/// Building DuckDB scalar/aggregate stubs is expensive (47+ entries). This struct
/// captures the result so that [`FeatherFlowProvider`] instances sharing the same
/// function set can borrow it instead of rebuilding on every construction.
pub struct FunctionRegistry {
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    table_functions: HashMap<String, Arc<dyn TableSource>>,
}

impl FunctionRegistry {
    /// Build the base DuckDB function registry (no user-defined functions).
    pub fn new() -> Self {
        Self::with_user_functions(&[], &[])
    }

    /// Build a function registry with additional user-defined function stubs.
    pub fn with_user_functions(
        user_functions: &[UserFunctionStub],
        user_table_functions: &[UserTableFunctionStub],
    ) -> Self {
        let mut scalar_functions: HashMap<String, Arc<ScalarUDF>> = functions::duckdb_scalar_udfs()
            .into_iter()
            .map(|f| (f.name().to_uppercase(), f))
            .collect();
        let aggregate_functions: HashMap<String, Arc<AggregateUDF>> =
            functions::duckdb_aggregate_udfs()
                .into_iter()
                .map(|f| (f.name().to_uppercase(), f))
                .collect();

        for uf in user_functions {
            let udf = functions::make_user_scalar_udf(uf.name(), uf.arg_types(), uf.return_type());
            scalar_functions.insert(uf.name().to_uppercase(), udf);
        }

        let mut table_functions: HashMap<String, Arc<dyn TableSource>> =
            HashMap::with_capacity(user_table_functions.len());
        for tf in user_table_functions {
            let fields: Vec<Field> = tf
                .columns()
                .iter()
                .map(|(col_name, col_type)| {
                    let arrow_type = sql_type_to_arrow(&crate::types::parse_sql_type(col_type));
                    Field::new(col_name, arrow_type, true)
                })
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let source: Arc<dyn TableSource> = Arc::new(LogicalTableSource { schema });
            table_functions.insert(tf.name().to_uppercase(), source);
        }

        Self {
            scalar_functions,
            aggregate_functions,
            table_functions,
        }
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// A ContextProvider backed by a Feather-Flow SchemaCatalog.
///
/// Maps model/source names to Arrow schemas so DataFusion's `SqlToRel`
/// can resolve table references during SQL-to-LogicalPlan conversion.
/// Borrows the function registry to avoid cloning during DAG-wide
/// propagation. Arrow schemas are pre-computed at construction time
/// from the catalog to avoid repeated conversion in `get_table_source`.
pub struct FeatherFlowProvider<'a> {
    arrow_schemas: HashMap<String, SchemaRef>,
    config: ConfigOptions,
    registry: &'a FunctionRegistry,
}

impl<'a> FeatherFlowProvider<'a> {
    /// Create a provider from a schema catalog and function registry.
    ///
    /// Eagerly converts every `RelSchema` in the catalog to an Arrow
    /// `SchemaRef` so that `get_table_source` lookups are O(1) HashMap
    /// hits with no per-call allocation.
    pub fn new(catalog: &SchemaCatalog, registry: &'a FunctionRegistry) -> Self {
        let arrow_schemas: HashMap<String, SchemaRef> = catalog
            .iter()
            .map(|(name, schema)| (name.clone(), Self::rel_schema_to_arrow(schema)))
            .collect();
        Self {
            arrow_schemas,
            config: ConfigOptions::default(),
            registry,
        }
    }

    /// Convert a RelSchema to an Arrow SchemaRef
    fn rel_schema_to_arrow(schema: &RelSchema) -> SchemaRef {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| {
                let arrow_type = sql_type_to_arrow(&col.sql_type);
                let nullable = !matches!(col.nullability, Nullability::NotNull);
                Field::new(&col.name, arrow_type, nullable)
            })
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// Minimal table source backed by an Arrow schema
struct LogicalTableSource {
    schema: SchemaRef,
}

impl TableSource for LogicalTableSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ContextProvider for FeatherFlowProvider<'_> {
    fn get_table_source(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        let table_name = name.table();
        if let Some(arrow_schema) = self.arrow_schemas.get(table_name) {
            return Ok(Arc::new(LogicalTableSource {
                schema: arrow_schema.clone(),
            }));
        }

        let lower = table_name.to_lowercase();
        for (key, arrow_schema) in &self.arrow_schemas {
            if key.to_lowercase() == lower {
                return Ok(Arc::new(LogicalTableSource {
                    schema: arrow_schema.clone(),
                }));
            }
        }

        plan_err!("Table not found: {table_name}")
    }

    fn get_table_function_source(
        &self,
        name: &str,
        _args: Vec<Expr>,
    ) -> DFResult<Arc<dyn TableSource>> {
        if let Some(source) = self.registry.table_functions.get(&name.to_uppercase()) {
            return Ok(source.clone());
        }
        plan_err!("Table function not found: {name}")
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.registry
            .scalar_functions
            .get(&name.to_uppercase())
            .cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.registry
            .aggregate_functions
            .get(&name.to_uppercase())
            .cloned()
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<arrow::datatypes::DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config
    }

    fn udf_names(&self) -> Vec<String> {
        self.registry.scalar_functions.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.registry.aggregate_functions.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
    }
}

/// Build user function stubs for static analysis from project functions.
///
/// Converts each scalar [`ff_core::function::FunctionDef`] into a
/// [`UserFunctionStub`] and each table function into a
/// [`UserTableFunctionStub`] that can be registered in a
/// [`FunctionRegistry`].
pub fn build_user_function_stubs(
    project: &ff_core::Project,
) -> (Vec<UserFunctionStub>, Vec<UserTableFunctionStub>) {
    use ff_core::function::FunctionReturn;

    let mut scalar_stubs = Vec::new();
    let mut table_stubs = Vec::new();

    for f in &project.functions {
        match &f.returns {
            FunctionReturn::Scalar { data_type } => {
                let sig = f.signature();
                if let Some(stub) =
                    UserFunctionStub::new(sig.name.to_string(), sig.arg_types, data_type.clone())
                {
                    scalar_stubs.push(stub);
                }
            }
            FunctionReturn::Table { columns } => {
                let cols: Vec<(String, String)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                if let Some(stub) = UserTableFunctionStub::new(f.name.to_string(), cols) {
                    table_stubs.push(stub);
                }
            }
        }
    }

    (scalar_stubs, table_stubs)
}

#[cfg(test)]
#[path = "provider_test.rs"]
mod tests;
