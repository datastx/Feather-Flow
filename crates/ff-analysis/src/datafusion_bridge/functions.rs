//! DuckDB function stubs for DataFusion planning
//!
//! Registers stub UDFs with correct signatures so that DataFusion's SqlToRel
//! can plan queries containing DuckDB-specific functions. These stubs are
//! never executed — they only provide type information for static analysis.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::Result as DFResult;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    Accumulator, AggregateUDF, ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Register all DuckDB scalar UDFs
pub fn duckdb_scalar_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        // Date/time functions
        make_scalar(
            "date_trunc",
            vec![
                DataType::Utf8,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ],
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        make_scalar(
            "date_part",
            vec![
                DataType::Utf8,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ],
            DataType::Int64,
        ),
        make_scalar(
            "date_diff",
            vec![
                DataType::Utf8,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ],
            DataType::Int64,
        ),
        make_scalar(
            "date_add",
            vec![
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            ],
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        make_scalar(
            "datediff",
            vec![
                DataType::Utf8,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ],
            DataType::Int64,
        ),
        make_scalar(
            "dateadd",
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ],
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        // String formatting
        make_scalar(
            "strftime",
            vec![
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                DataType::Utf8,
            ],
            DataType::Utf8,
        ),
        make_scalar(
            "strptime",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        // Epoch conversions
        make_scalar(
            "epoch",
            vec![DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            )],
            DataType::Int64,
        ),
        make_scalar(
            "epoch_ms",
            vec![DataType::Int64],
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        // Regex
        make_scalar(
            "regexp_matches",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Boolean,
        ),
        make_scalar(
            "regexp_replace",
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        ),
        make_scalar(
            "regexp_extract",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        ),
        // Type conversion (type-preserving: returns first non-Null arg type)
        make_type_preserving_variadic_scalar("coalesce"),
        make_type_preserving_variadic_scalar("ifnull"),
        make_type_preserving_variadic_scalar("nullif"),
        // Struct functions
        make_variadic_scalar("struct_pack", DataType::Utf8),
        make_scalar(
            "struct_extract",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        ),
        // List functions
        make_variadic_scalar("list_value", DataType::Utf8),
        make_scalar(
            "list_extract",
            vec![DataType::Utf8, DataType::Int64],
            DataType::Utf8,
        ),
        make_scalar("unnest", vec![DataType::Utf8], DataType::Utf8),
        // Utility
        make_scalar(
            "generate_series",
            vec![DataType::Int64, DataType::Int64],
            DataType::Int64,
        ),
        make_scalar("hash", vec![DataType::Utf8], DataType::Int64),
        make_scalar("md5", vec![DataType::Utf8], DataType::Utf8),
        // String functions
        make_scalar(
            "format",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        ),
        make_scalar(
            "printf",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        ),
        make_scalar(
            "string_split",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        ),
    ]
}

/// Register all DuckDB aggregate UDFs
///
/// Includes standard SQL aggregates (SUM, AVG, etc.) that DataFusion's
/// planning-only context does not provide by default, plus DuckDB-specific ones.
pub fn duckdb_aggregate_udfs() -> Vec<Arc<AggregateUDF>> {
    vec![
        // Standard SQL aggregates (type-preserving: return type matches input type)
        make_type_preserving_aggregate("sum"),
        make_type_preserving_aggregate("avg"),
        make_type_preserving_aggregate("min"),
        make_type_preserving_aggregate("max"),
        make_aggregate("count", DataType::Utf8, DataType::Int64),
        // DuckDB-specific aggregates
        make_aggregate("string_agg", DataType::Utf8, DataType::Utf8),
        make_aggregate("group_concat", DataType::Utf8, DataType::Utf8),
        make_aggregate("list_agg", DataType::Utf8, DataType::Utf8),
        make_aggregate("array_agg", DataType::Utf8, DataType::Utf8),
        make_aggregate("bool_and", DataType::Boolean, DataType::Boolean),
        make_aggregate("bool_or", DataType::Boolean, DataType::Boolean),
        make_aggregate("every", DataType::Boolean, DataType::Boolean),
        make_aggregate("listagg", DataType::Utf8, DataType::Utf8),
        make_aggregate("approx_count_distinct", DataType::Utf8, DataType::Int64),
        make_aggregate("approx_quantile", DataType::Float64, DataType::Float64),
        make_aggregate("median", DataType::Float64, DataType::Float64),
        make_aggregate("mode", DataType::Utf8, DataType::Utf8),
        make_aggregate("arg_min", DataType::Utf8, DataType::Utf8),
        make_aggregate("arg_max", DataType::Utf8, DataType::Utf8),
    ]
}

/// Create a scalar UDF with exact signature
fn make_scalar(name: &str, args: Vec<DataType>, ret: DataType) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StubScalarUDF {
        name: name.to_string(),
        signature: Signature::new(TypeSignature::Exact(args), Volatility::Immutable),
        return_type: ret,
    }))
}

/// Create a variadic scalar UDF
fn make_variadic_scalar(name: &str, ret: DataType) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StubScalarUDF {
        name: name.to_string(),
        signature: Signature::variadic_any(Volatility::Immutable),
        return_type: ret,
    }))
}

/// Create a type-preserving variadic scalar UDF (returns first non-Null arg type)
fn make_type_preserving_variadic_scalar(name: &str) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(TypePreservingScalarUDF {
        name: name.to_string(),
        signature: Signature::variadic_any(Volatility::Immutable),
    }))
}

/// Create a type-preserving aggregate UDF (return type matches input type)
fn make_type_preserving_aggregate(name: &str) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::from(TypePreservingAggregateUDF {
        name: name.to_string(),
        signature: Signature::variadic_any(Volatility::Immutable),
    }))
}

/// Create an aggregate UDF
fn make_aggregate(name: &str, input: DataType, ret: DataType) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::from(StubAggregateUDF {
        name: name.to_string(),
        signature: Signature::new(TypeSignature::Variadic(vec![input]), Volatility::Immutable),
        return_type: ret,
    }))
}

// --- Stub implementations ---

#[derive(Debug, Hash, PartialEq, Eq)]
struct StubScalarUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl ScalarUDFImpl for StubScalarUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        // Stubs are never executed
        unreachable!("Stub UDF should not be executed")
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct StubAggregateUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl datafusion_expr::AggregateUDFImpl for StubAggregateUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        unreachable!("Stub aggregate should not be executed")
    }
}

// --- Type-preserving stubs ---

/// A scalar UDF that returns the type of its first non-Null argument.
///
/// Used for COALESCE, IFNULL, NULLIF where the output type should match the input.
#[derive(Debug, Hash, PartialEq, Eq)]
struct TypePreservingScalarUDF {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for TypePreservingScalarUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        // Return the first non-Null argument type, falling back to Utf8
        Ok(args
            .iter()
            .find(|t| !matches!(t, DataType::Null))
            .cloned()
            .unwrap_or(DataType::Utf8))
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        unreachable!("Stub UDF should not be executed")
    }
}

/// An aggregate UDF that returns the same type as its input.
///
/// Used for SUM, AVG, MIN, MAX where the output type should match the input.
#[derive(Debug, Hash, PartialEq, Eq)]
struct TypePreservingAggregateUDF {
    name: String,
    signature: Signature,
}

impl datafusion_expr::AggregateUDFImpl for TypePreservingAggregateUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        // Return the first argument's type, falling back to Utf8
        Ok(args.first().cloned().unwrap_or(DataType::Utf8))
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        unreachable!("Stub aggregate should not be executed")
    }
}
