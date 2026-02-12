# `ff analyze` Deep Dive: How Feather-Flow Uses DataFusion LogicalPlans for Static Type Analysis

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [The Two-Tier Analysis System](#the-two-tier-analysis-system)
4. [The Type System: `SqlType`](#the-type-system-sqltype)
5. [The Type Bridge: SqlType to Arrow and Back](#the-type-bridge-sqltype-to-arrow-and-back)
6. [The DataFusion Bridge](#the-datafusion-bridge)
   - [ContextProvider (`provider.rs`)](#contextprovider-providerrs)
   - [SQL-to-LogicalPlan Planner (`planner.rs`)](#sql-to-logicalplan-planner-plannerrs)
   - [DuckDB Function Stubs (`functions.rs`)](#duckdb-function-stubs-functionsrs)
   - [Schema Propagation (`propagation.rs`)](#schema-propagation-propagationrs)
   - [Column-Level Lineage (`lineage.rs`)](#column-level-lineage-lineagers)
7. [The Pass Infrastructure](#the-pass-infrastructure)
   - [IR-Based Passes (Legacy)](#ir-based-passes-legacy)
   - [LogicalPlan-Based Passes (DataFusion)](#logicalplan-based-passes-datafusion)
8. [End-to-End: How `ff analyze` Executes](#end-to-end-how-ff-analyze-executes)
9. [Schema Mismatch Detection: The Core Value Proposition](#schema-mismatch-detection-the-core-value-proposition)
10. [Diagnostic Codes Reference](#diagnostic-codes-reference)
11. [File Index](#file-index)

---

## Executive Summary

`ff analyze` is Feather-Flow's static analysis command. It catches type mismatches, schema inconsistencies, nullable column violations, join key problems, and unused columns **without ever connecting to a database**. It achieves this through two complementary analysis tiers:

1. **Custom IR passes**: SQL is parsed by `sqlparser-rs` v0.60, lowered to a custom relational algebra IR (`RelOp`/`TypedExpr`), and analyzed by composable passes.
2. **DataFusion LogicalPlan passes**: SQL is **re-parsed** through DataFusion's own SQL planner (which bundles `sqlparser` v0.59 internally), producing `LogicalPlan` nodes with full Arrow type information. Schemas propagate through the DAG in topological order, enabling cross-model type checking.

The key insight is that DataFusion's `SqlToRel` planner performs **full type inference** when converting SQL text into a `LogicalPlan`. Every column in the plan's output `DFSchema` carries a concrete `Arrow DataType` and a `nullable` flag. Feather-Flow harnesses this by feeding model schemas forward through the DAG — each model's inferred output becomes the input schema for downstream models — then cross-checking the inferred types against YAML declarations.

---

## Architecture Overview

```
                           ┌──────────────────────┐
                           │    featherflow.yml    │
                           │   + model YAML files  │
                           └──────────┬───────────┘
                                      │
                              ┌───────▼────────┐
                              │  ff-core        │
                              │  Project::load()│
                              └───────┬────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                 │                 │
            ┌───────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
            │  ff-jinja     │  │  ff-sql      │  │  ff-analysis │
            │  Render SQL   │  │  Parse AST   │  │  Two-tier    │
            │  templates    │  │  + lineage   │  │  analysis    │
            └───────┬──────┘  └──────┬──────┘  └──────┬──────┘
                    │                │                 │
                    │         ┌──────▼──────┐         │
                    │         │  sqlparser   │         │
                    │         │  v0.60       │         │
                    │         └─────────────┘         │
                    │                                  │
                    │    ┌────────────────────────────┘
                    │    │
                    ▼    ▼
            ┌───────────────────────────────────┐
            │       ff-analysis crate            │
            │                                    │
            │  Tier 1: Custom IR                 │
            │  ┌──────────────────────────────┐  │
            │  │ sqlparser AST → RelOp IR     │  │
            │  │ (lowering/)                  │  │
            │  │ Passes: TypeInference,       │  │
            │  │   Nullability, JoinKeys,     │  │
            │  │   UnusedColumns              │  │
            │  └──────────────────────────────┘  │
            │                                    │
            │  Tier 2: DataFusion LogicalPlan     │
            │  ┌──────────────────────────────┐  │
            │  │ SQL string → DataFusion      │  │
            │  │ re-parse (sqlparser v0.59)   │  │
            │  │ → SqlToRel → LogicalPlan     │  │
            │  │ → Schema propagation (DAG)   │  │
            │  │ → Cross-model consistency    │  │
            │  └──────────────────────────────┘  │
            │                                    │
            │  Output: Vec<Diagnostic>           │
            └───────────────────────────────────┘
```

### Crate Layout

| Crate | Role |
|-------|------|
| `ff-cli` | CLI argument parsing, command dispatch, output formatting |
| `ff-core` | Project loading, config, model discovery, DAG construction |
| `ff-jinja` | Jinja template rendering (Minijinja) |
| `ff-sql` | SQL parsing (sqlparser v0.60), dependency extraction, AST-based lineage |
| `ff-analysis` | Both analysis tiers, type system, DataFusion bridge |

---

## The Two-Tier Analysis System

### Why Two Tiers?

Feather-Flow started with a custom relational algebra IR (`RelOp`) for analysis. This works well for intra-model checks (type inference within a single model, nullability from JOINs, join key type mismatches). However, it cannot perform **cross-model** type checking because it doesn't propagate schemas between models.

DataFusion was added to solve this. DataFusion's `SqlToRel` planner requires a `ContextProvider` that resolves table names to Arrow schemas. By feeding each model's inferred output schema into the catalog for downstream models, Feather-Flow achieves DAG-wide type checking.

The two tiers coexist during a migration period. The plan is to port all IR-based passes to DataFusion LogicalPlans and then remove the custom IR entirely.

### Tier 1: Custom IR (RelOp)

**Pipeline**: `sqlparser AST` → `lowering/` → `RelOp` tree → per-model passes

The custom IR is defined in `crates/ff-analysis/src/ir/`:

```rust
// relop.rs — Relational operator nodes
pub enum RelOp {
    Scan { table_name, alias, schema: RelSchema },
    Project { input, columns: Vec<(String, TypedExpr)>, schema },
    Filter { input, predicate: TypedExpr, schema },
    Join { left, right, join_type: JoinType, condition: Option<TypedExpr>, schema },
    Aggregate { input, group_by, aggregates, schema },
    Sort { input, order_by, schema },
    Limit { input, limit, offset, schema },
    SetOp { left, right, op: SetOpKind, schema },
}
```

Every `RelOp` variant carries a `schema: RelSchema` — an ordered list of `TypedColumn`s describing the output at that point in the plan tree.

```rust
// expr.rs — Typed expression tree
pub enum TypedExpr {
    ColumnRef { table, column, resolved_type: SqlType, nullability },
    Literal { value: LiteralValue, resolved_type: SqlType },
    BinaryOp { left, op: BinOp, right, resolved_type, nullability },
    UnaryOp { op: UnOp, expr, resolved_type, nullability },
    FunctionCall { name, args, resolved_type, nullability },
    Cast { expr, target_type: SqlType, nullability },
    Case { operand, conditions, results, else_result, resolved_type, nullability },
    IsNull { expr, negated: bool },
    Subquery { resolved_type, nullability },
    Wildcard { table },
    Unsupported { description, resolved_type, nullability },
}
```

Every expression node carries its `resolved_type: SqlType` and `nullability: Nullability`. The lowering pass infers these during AST-to-IR conversion using the `SchemaCatalog` (a `HashMap<String, RelSchema>` mapping table names to known schemas).

**Passes that run on this IR:**

| Pass | Struct | Codes | What It Checks |
|------|--------|-------|----------------|
| `type_inference` | `TypeInference` | A001-A005 | Unknown types, UNION mismatches, SUM/AVG on strings, lossy casts |
| `nullability` | `NullabilityPropagation` | A010-A012 | Nullable columns from outer JOINs without guards, YAML NOT NULL conflicts |
| `join_keys` | `JoinKeyAnalysis` | A030, A032-A033 | Join key type mismatches, cross joins, non-equi joins |
| `unused_columns` | `UnusedColumnDetection` (DAG pass) | A020-A021 | Columns produced but never consumed downstream |

### Tier 2: DataFusion LogicalPlan

**Pipeline**: `SQL string` → DataFusion re-parse (sqlparser v0.59) → `SqlToRel` → `LogicalPlan` → DAG propagation → cross-model passes

This tier uses Apache DataFusion 52.x purely as a **planning engine** — no queries are ever executed. The key components live in `crates/ff-analysis/src/datafusion_bridge/`:

| Module | File | Purpose |
|--------|------|---------|
| Provider | `provider.rs` | Implements `ContextProvider` to resolve table names to Arrow schemas |
| Planner | `planner.rs` | Converts SQL strings to `LogicalPlan` via DataFusion's `SqlToRel` |
| Functions | `functions.rs` | Registers DuckDB-specific function stubs with correct type signatures |
| Types | `types.rs` | Bidirectional conversion between `SqlType` and Arrow `DataType` |
| Propagation | `propagation.rs` | Walks DAG in topological order, feeding schemas forward |
| Lineage | `lineage.rs` | Extracts column-level lineage (Copy/Transform/Inspect) from LogicalPlans |

**Pass that runs on LogicalPlans:**

| Pass | Struct | Codes | What It Checks |
|------|--------|-------|----------------|
| `cross_model_consistency` | `CrossModelConsistency` (DAG pass) | A040-A041 | YAML vs inferred schema mismatches across models |

---

## The Type System: `SqlType`

Defined in `crates/ff-analysis/src/ir/types.rs`, `SqlType` is Feather-Flow's internal representation of SQL data types. It normalizes the many SQL type aliases into a compact enum:

```rust
pub enum SqlType {
    Boolean,
    Integer { bits: IntBitWidth },      // I8 (TINYINT), I16 (SMALLINT), I32 (INT), I64 (BIGINT)
    HugeInt,                            // 128-bit (DuckDB HUGEINT)
    Float { bits: FloatBitWidth },      // F32 (FLOAT), F64 (DOUBLE)
    Decimal { precision: Option<u16>, scale: Option<u16> },
    String { max_length: Option<u32> },
    Date,
    Time,
    Timestamp,
    Interval,
    Binary,
    Json,
    Uuid,
    Array(Box<SqlType>),
    Struct(Vec<(String, SqlType)>),
    Map { key: Box<SqlType>, value: Box<SqlType> },
    Unknown(String),                    // Fallback with reason
}
```

### Type Compatibility

`SqlType::is_compatible_with()` defines compatibility rules for cross-checking:

- **All numeric types are mutually compatible** — `Integer`, `Float`, `Decimal`, and `HugeInt` are in the same family
- **String types** are compatible with `Json` and `Uuid` (both stored as strings)
- **Date and Timestamp** are mutually compatible
- **Unknown is compatible with everything** — avoids false positives when types can't be determined
- **Arrays, Structs, Maps** are compatible if their inner types are compatible

### Nullability

```rust
pub enum Nullability {
    NotNull,    // Guaranteed non-null
    Nullable,   // May contain nulls
    Unknown,    // Can't determine (treated as nullable for safety)
}
```

`Nullability::combine()` follows a conservative merge: if either side is `Nullable`, the result is `Nullable`. This is used in binary operations and JOIN outputs.

### Parsing SQL Type Strings

`parse_sql_type(s: &str) -> SqlType` handles the full spectrum of DuckDB type names:

- Simple types: `BOOL`, `INT`, `BIGINT`, `VARCHAR`, `DATE`, `TIMESTAMP`, etc.
- Aliases: `INT4` → I32, `INT8` → I64, `FLOAT8` → F64, `TEXT` → String
- Parameterized: `VARCHAR(255)`, `DECIMAL(10,2)`, `INTEGER(32)`
- Complex: `INTEGER[]` (array), `STRUCT(name VARCHAR, age INT)`, `MAP(VARCHAR, INTEGER)`
- Nested: `STRUCT(items INTEGER[], meta MAP(VARCHAR, VARCHAR))`

The parser is recursive — array, struct, and map types parse their inner types via `parse_sql_type()` calls. Top-level delimiter splitting respects parenthesis nesting via `split_top_level()`.

---

## The Type Bridge: SqlType to Arrow and Back

The bridge in `crates/ff-analysis/src/datafusion_bridge/types.rs` converts between Feather-Flow's `SqlType` and Apache Arrow's `DataType`. This is critical because DataFusion operates entirely in Arrow types.

### `sql_type_to_arrow(sql_type: &SqlType) -> ArrowDataType`

This function converts Feather-Flow types **into** Arrow for feeding schemas to DataFusion:

| SqlType | Arrow DataType |
|---------|---------------|
| `Boolean` | `Boolean` |
| `Integer { I8 }` | `Int8` |
| `Integer { I16 }` | `Int16` |
| `Integer { I32 }` | `Int32` |
| `Integer { I64 }` | `Int64` |
| `HugeInt` | `Decimal128(38, 0)` |
| `Float { F32 }` | `Float32` |
| `Float { F64 }` | `Float64` |
| `Decimal { p, s }` | `Decimal128(p.min(38), s.min(127))` |
| `String { .. }` | `Utf8` |
| `Date` | `Date32` |
| `Time` | `Time64(Microsecond)` |
| `Timestamp` | `Timestamp(Microsecond, None)` |
| `Interval` | `Interval(DayTime)` |
| `Binary` | `Binary` |
| `Json` | `Utf8` (JSON stored as string) |
| `Uuid` | `Utf8` (UUID stored as string) |
| `Array(inner)` | `List(Field::new("item", inner, true))` |
| `Struct(fields)` | `Struct(Fields)` |
| `Map { key, value }` | `Map(entries_field, false)` |
| `Unknown(_)` | `Utf8` (fallback) |

### `arrow_to_sql_type(arrow_type: &ArrowDataType) -> SqlType`

This function converts Arrow types **back** to Feather-Flow types after DataFusion infers the output schema. Notable mappings:

| Arrow DataType | SqlType |
|---------------|---------|
| `UInt8` | `Integer { I16 }` (widened: max 255 doesn't fit in I8 max 127) |
| `UInt16` | `Integer { I32 }` (widened) |
| `UInt32` | `Integer { I64 }` (widened) |
| `UInt64` | `HugeInt` (widened) |
| `Float16` | `Float { F32 }` |
| `Decimal128(38, 0)` | `HugeInt` (special case: 128-bit integer) |
| `Decimal128(p, s)` / `Decimal256(p, s)` | `Decimal { precision, scale }` |
| `Utf8` / `LargeUtf8` / `Utf8View` | `String { max_length: None }` |
| `Date32` / `Date64` | `Date` |
| `Time32(_)` / `Time64(_)` | `Time` |
| `Duration(_)` | `Interval` |
| `FixedSizeBinary(_)` | `Binary` |
| `List(f)` / `LargeList(f)` / `FixedSizeList(f, _)` | `Array(inner)` |
| `Null` | `Unknown("NULL")` |

**Key design decision**: Unsigned integers are widened to the next larger signed integer type. This prevents information loss — Arrow `UInt8` (0..255) can't be represented by `SqlType::Integer { I8 }` (max 127), so it maps to I16.

---

## The DataFusion Bridge

### ContextProvider (`provider.rs`)

The `FeatherFlowProvider` implements DataFusion's `ContextProvider` trait, which is the interface that `SqlToRel` uses to resolve names during planning.

```rust
pub struct FeatherFlowProvider<'a> {
    catalog: &'a SchemaCatalog,         // Maps table names → RelSchema
    config: ConfigOptions,              // DataFusion config (defaults)
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
}
```

**Construction**: `FeatherFlowProvider::with_user_functions(catalog, user_functions)` populates the function maps from:
1. Built-in DuckDB function stubs (`duckdb_scalar_udfs()` and `duckdb_aggregate_udfs()`)
2. User-defined function stubs from `UserFunctionStub` (parsed from YAML function definitions)

All function names are stored **uppercased** for case-insensitive lookup.

**Table resolution** (`get_table_source`): When DataFusion's planner encounters a table reference like `stg_orders`, it calls `get_table_source("stg_orders")`. The provider:
1. Tries exact match in the `SchemaCatalog`
2. Falls back to case-insensitive match (lowercased comparison)
3. Returns `plan_err!("Table not found: ...")` if neither matches

When a match is found, the provider converts the `RelSchema` to an Arrow `SchemaRef` via `rel_schema_to_arrow()`:

```rust
fn rel_schema_to_arrow(schema: &RelSchema) -> SchemaRef {
    let fields: Vec<Field> = schema.columns.iter().map(|col| {
        let arrow_type = sql_type_to_arrow(&col.sql_type);
        let nullable = !matches!(col.nullability, Nullability::NotNull);
        Field::new(&col.name, arrow_type, nullable)
    }).collect();
    Arc::new(Schema::new(fields))
}
```

This is where **the SqlType-to-Arrow bridge is invoked for input schemas**. Each column's `SqlType` becomes an Arrow `DataType`, and the nullability flag is derived from the `Nullability` enum.

The returned `LogicalTableSource` wraps this Arrow schema. When `SqlToRel` processes a `SELECT` that references columns from this table, it knows each column's exact Arrow type and nullability.

### SQL-to-LogicalPlan Planner (`planner.rs`)

```rust
pub fn sql_to_plan(sql: &str, provider: &FeatherFlowProvider) -> AnalysisResult<LogicalPlan>
```

This is the critical function that converts a SQL string into a DataFusion `LogicalPlan`. Here's what happens:

1. **Re-parse** the SQL through DataFusion's own parser (which uses `sqlparser` v0.59, not our v0.60):
   ```rust
   let dialect = datafusion_expr::sqlparser::dialect::DuckDbDialect {};
   let mut parser = DFParserBuilder::new(sql)
       .with_dialect(&dialect)
       .build()?;
   let df_stmts = parser.parse_statements()?;
   ```

2. **Plan** the statement using `SqlToRel`:
   ```rust
   let planner = SqlToRel::new(provider);
   let plan = planner.statement_to_plan(first_stmt)?;
   ```

**Why re-parse?** Feather-Flow uses `sqlparser` v0.60 for its primary parsing, but DataFusion 52.x bundles `sqlparser` v0.59. These versions have incompatible Rust types (different `Statement` structs). Rather than maintaining cross-version AST conversion, the SQL string is simply re-parsed. The SQL text is the canonical interchange format.

**What `SqlToRel::statement_to_plan()` does internally:**

DataFusion's `SqlToRel` is a full SQL planner. For a `SELECT` statement, it:

1. Resolves `FROM` clause table references by calling `provider.get_table_source()` → gets Arrow schemas
2. Plans `JOIN` operations, computing output schemas by merging left and right schemas (with nullability adjustments for outer joins)
3. Resolves column references in `SELECT`, `WHERE`, `HAVING`, `GROUP BY` against the current scope's schema
4. Applies function calls by looking up `provider.get_function_meta()` → gets return type from stub
5. Handles `CAST`, `CASE`, subqueries, set operations (`UNION`), etc.
6. Produces a `LogicalPlan` tree where every node has a `DFSchema` describing its output

The output `LogicalPlan` node's `plan.schema()` returns a `DFSchemaRef` containing:
- Column names
- Arrow `DataType` for each column (fully resolved through expression type inference)
- Nullable flag for each column (propagated through joins, aggregations, etc.)

**This is how type inference works** — DataFusion does all the heavy lifting. The stubs just need to provide correct input schemas and function return types.

### DuckDB Function Stubs (`functions.rs`)

DataFusion doesn't know about DuckDB-specific functions. Without stubs, any query using `date_trunc()`, `regexp_matches()`, or `string_agg()` would fail to plan. The stub system registers these functions with their correct type signatures **so that planning succeeds and type inference is accurate**.

There are four categories of stubs:

#### 1. Fixed-Signature Scalar Stubs (`StubScalarUDF`)

```rust
struct StubScalarUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
}
```

These have exact argument types and a fixed return type. Examples:

| Function | Args | Returns |
|----------|------|---------|
| `date_trunc` | `(Utf8, Timestamp)` | `Timestamp` |
| `date_part` | `(Utf8, Timestamp)` | `Int64` |
| `date_diff` | `(Utf8, Timestamp, Timestamp)` | `Int64` |
| `regexp_matches` | `(Utf8, Utf8)` | `Boolean` |
| `hash` | `(Utf8)` | `Int64` |
| `md5` | `(Utf8)` | `Utf8` |

The `return_type()` method simply returns the fixed type regardless of arguments.

#### 2. Type-Preserving Scalar Stubs (`TypePreservingScalarUDF`)

Used for functions where the output type depends on the input type:

```rust
fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
    Ok(args.iter()
        .find(|t| !matches!(t, DataType::Null))
        .cloned()
        .unwrap_or(DataType::Utf8))
}
```

This returns the **first non-Null argument's type**. Registered for:
- `COALESCE(a, b, c)` → type of first non-null arg
- `IFNULL(a, b)` → type of first non-null arg
- `NULLIF(a, b)` → type of first non-null arg

#### 3. Fixed-Signature Aggregate Stubs (`StubAggregateUDF`)

Like scalar stubs but for aggregate functions:

| Function | Input | Returns |
|----------|-------|---------|
| `count` | `Utf8` (any) | `Int64` |
| `string_agg` | `Utf8` | `Utf8` |
| `bool_and` | `Boolean` | `Boolean` |
| `approx_count_distinct` | `Utf8` (any) | `Int64` |
| `median` | `Float64` | `Float64` |

#### 4. Type-Preserving Aggregate Stubs (`TypePreservingAggregateUDF`)

For aggregates where the result type matches the input:

```rust
fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
    Ok(args.first().cloned().unwrap_or(DataType::Utf8))
}
```

Registered for: `SUM`, `AVG`, `MIN`, `MAX`

So `SUM(price)` where `price` is `Decimal128(10,2)` returns `Decimal128(10,2)`, and `MAX(name)` where `name` is `Utf8` returns `Utf8`.

#### 5. User-Defined Function Stubs

User functions defined in YAML (`.yml` + `.sql` pairs in `function_paths`) are converted to stubs:

```rust
pub fn make_user_scalar_udf(name: &str, arg_types: &[String], return_type: &str) -> Arc<ScalarUDF> {
    let arrow_args: Vec<DataType> = arg_types.iter()
        .map(|t| sql_type_to_arrow(&parse_sql_type(t)))
        .collect();
    let arrow_ret = sql_type_to_arrow(&parse_sql_type(return_type));
    make_scalar(name, arrow_args, arrow_ret)
}
```

The SQL type strings from the YAML are parsed to `SqlType` via `parse_sql_type()`, then converted to Arrow `DataType` via `sql_type_to_arrow()`, and finally registered as a `StubScalarUDF` with exact signature.

**Important**: All stubs will panic if `invoke_with_args()` or `accumulator()` is called — they are **planning-only** stubs that should never be executed.

### Schema Propagation (`propagation.rs`)

This is the heart of cross-model type checking. The `propagate_schemas()` function walks the entire model DAG in topological order, building LogicalPlans for each model and feeding inferred schemas forward.

```rust
pub fn propagate_schemas(
    topo_order: &[String],          // Models sorted by dependency order
    sql_sources: &HashMap<String, String>,   // Model name → rendered SQL
    yaml_schemas: &HashMap<String, RelSchema>, // Model name → YAML-declared schema
    initial_catalog: &SchemaCatalog, // Starting schema catalog (seeds, sources, external)
    user_functions: &[UserFunctionStub],
) -> PropagationResult
```

**Algorithm:**

```
catalog = initial_catalog.clone()  // Start with seeds + sources + external tables

for model_name in topological_order:
    sql = sql_sources[model_name]

    // Build provider with CURRENT catalog state
    // (includes all upstream models' inferred schemas)
    provider = FeatherFlowProvider::with_user_functions(&catalog, user_functions)

    // Convert SQL to LogicalPlan
    // DataFusion resolves all table refs against the catalog,
    // infers all column types, propagates nullability
    plan = sql_to_plan(sql, &provider)

    // Extract the inferred output schema from the plan
    inferred_schema = extract_schema_from_plan(&plan)
    // ↑ This calls plan.schema(), iterates DFSchema fields,
    //   converts Arrow DataType → SqlType via arrow_to_sql_type(),
    //   and maps Arrow nullable flag → Nullability enum

    // Cross-check against YAML declaration
    mismatches = compare_schemas(yaml_schema, &inferred_schema)

    // CRITICAL: Register the inferred schema for downstream models
    catalog.insert(model_name, inferred_schema)
    // ↑ This is what enables cross-model type checking.
    //   When the next model references this one, the provider
    //   will return this inferred schema, not the YAML schema.
```

**Schema extraction** from a LogicalPlan:

```rust
fn extract_schema_from_plan(plan: &LogicalPlan) -> RelSchema {
    let df_schema = plan.schema();
    let columns: Vec<TypedColumn> = df_schema.fields().iter().map(|field| {
        let sql_type = arrow_to_sql_type(field.data_type());
        let nullability = if field.is_nullable() {
            Nullability::Nullable
        } else {
            Nullability::NotNull
        };
        TypedColumn {
            name: field.name().clone(),
            source_table: None,
            sql_type,
            nullability,
            provenance: vec![],
        }
    }).collect();
    RelSchema::new(columns)
}
```

Here, `arrow_to_sql_type()` converts **back** from Arrow to `SqlType`. This round-trip is where type information flows:

```
YAML → parse_sql_type() → SqlType → sql_type_to_arrow() → Arrow DataType
                                                              │
                                          DataFusion Planning │
                                          (type inference,    │
                                           join resolution,   │
                                           function dispatch) │
                                                              ▼
Arrow DataType (inferred output) → arrow_to_sql_type() → SqlType (inferred)
                                                              │
                                          compare_schemas()   │
                                                              ▼
                                          SchemaMismatch diagnostics
```

**Schema comparison** (`compare_schemas()`):

```rust
fn compare_schemas(yaml: &RelSchema, inferred: &RelSchema) -> Vec<SchemaMismatch> {
    // 1. Extra columns: in SQL output but not in YAML
    for inferred_col in &inferred.columns {
        if yaml.find_column(&inferred_col.name).is_none() {
            mismatches.push(ExtraInSql { column });
        }
    }

    // 2. Missing columns: in YAML but not in SQL output
    for yaml_col in &yaml.columns {
        match inferred.find_column(&yaml_col.name) {
            None => mismatches.push(MissingFromSql { column }),
            Some(inferred_col) => {
                // 3. Type compatibility check
                if !yaml_col.sql_type.is_compatible_with(&inferred_col.sql_type) {
                    mismatches.push(TypeMismatch { ... });
                }
                // 4. Nullability check (only flag if YAML says NOT NULL but SQL infers nullable)
                if !yaml_nullable && inferred_nullable {
                    mismatches.push(NullabilityMismatch { ... });
                }
            }
        }
    }
}
```

**Key nuance**: The nullability check is asymmetric. It only flags when YAML declares `NOT NULL` but the SQL infers `nullable`. The reverse (YAML says nullable, SQL infers NOT NULL) is not flagged — it's safe for a column to be "more constrained" than declared.

### Column-Level Lineage (`lineage.rs`)

The lineage module walks a `LogicalPlan` to classify how each output column relates to its sources:

```rust
pub enum LineageKind {
    Copy,       // Direct column reference: SELECT a FROM t
    Transform,  // Used in computation: SELECT a + b AS c
    Inspect,    // Read but not in output: WHERE status = 'active'
}
```

The walker handles:
- **Projection**: Classifies each output expression (Copy if bare column ref, Transform otherwise)
- **Filter**: All predicate column refs are `Inspect`
- **Join**: Join key columns are `Inspect`
- **Aggregate**: GROUP BY keys preserve their classification; aggregate expressions are `Transform`
- **SubqueryAlias, Sort, Limit**: Pass through
- **Union**: Recurse into all inputs
- **TableScan**: Leaf node

Expression classification (`classify_expr`):
```rust
fn classify_expr(expr: &Expr) -> LineageKind {
    match expr {
        Expr::Column(_) => LineageKind::Copy,
        Expr::Alias(alias) => classify_expr(&alias.expr),  // Unwrap aliases
        _ => LineageKind::Transform,                        // Everything else
    }
}
```

Column reference collection (`collect_column_refs`) handles: `Column`, `Alias`, `BinaryExpr`, `ScalarFunction`, `AggregateFunction`, `Case`, `Cast`, `TryCast`, `IsNull`, `IsNotNull`, `Not`, `Negative`, `Between`, `Like`, `InList`.

---

## The Pass Infrastructure

### IR-Based Passes (Legacy)

Managed by `PassManager` in `crates/ff-analysis/src/pass/mod.rs`:

```rust
pub struct PassManager {
    model_passes: Vec<Box<dyn AnalysisPass>>,
    dag_passes: Vec<Box<dyn DagPass>>,
}
```

**`AnalysisPass` trait** (per-model):
```rust
pub trait AnalysisPass: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn run_model(&self, model_name: &str, ir: &RelOp, ctx: &AnalysisContext) -> Vec<Diagnostic>;
}
```

**`DagPass` trait** (cross-model):
```rust
pub trait DagPass: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn run_project(&self, models: &HashMap<String, RelOp>, ctx: &AnalysisContext) -> Vec<Diagnostic>;
}
```

Execution: For each model in topological order, all `AnalysisPass`es run. Then all `DagPass`es run across all models. Optional `pass_filter` restricts which passes execute.

### LogicalPlan-Based Passes (DataFusion)

Managed by `PlanPassManager` in `crates/ff-analysis/src/pass/plan_pass.rs`:

```rust
pub struct PlanPassManager {
    model_passes: Vec<Box<dyn PlanPass>>,
    dag_passes: Vec<Box<dyn DagPlanPass>>,
}
```

**`PlanPass` trait** (per-model, not yet populated):
```rust
pub trait PlanPass: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn run_model(&self, model_name: &str, plan: &LogicalPlan, ctx: &AnalysisContext) -> Vec<Diagnostic>;
}
```

**`DagPlanPass` trait** (cross-model):
```rust
pub trait DagPlanPass: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn run_project(&self, models: &HashMap<String, ModelPlanResult>, ctx: &AnalysisContext) -> Vec<Diagnostic>;
}
```

Currently, `PlanPassManager::with_defaults()` registers only one pass:
```rust
dag_passes: vec![Box::new(CrossModelConsistency)],
```

The `CrossModelConsistency` pass iterates all `ModelPlanResult`s, reads the `mismatches` vector (populated by `propagate_schemas`), and converts each `SchemaMismatch` into a `Diagnostic`:

| SchemaMismatch variant | Diagnostic Code | Severity |
|----------------------|-----------------|----------|
| `ExtraInSql` | A040 | Warning |
| `MissingFromSql` | A040 | **Error** |
| `TypeMismatch` | A040 | Warning |
| `NullabilityMismatch` | A041 | Warning |

Note: `MissingFromSql` is the only **Error**-severity diagnostic from cross-model analysis. A column declared in YAML but absent from the SQL output is a definite bug. The others are warnings because they might be intentional (e.g., extra columns in SQL that haven't been documented yet).

### The Diagnostic Type

```rust
pub struct Diagnostic {
    pub code: DiagnosticCode,           // Enum: A001, A002, ..., A041
    pub severity: Severity,             // Info, Warning, Error
    pub message: String,                // Human-readable description
    pub model: String,                  // Which model produced this
    pub column: Option<String>,         // Specific column, if applicable
    pub hint: Option<String>,           // Suggested fix
    pub pass_name: Cow<'static, str>,   // Which pass emitted this
}
```

---

## End-to-End: How `ff analyze` Executes

The CLI command is implemented in `crates/ff-cli/src/commands/analyze.rs`. Here's the complete flow:

### Step 1: Load Project
```rust
let project = load_project(global)?;
```
Reads `featherflow.yml`, discovers all models (directory-per-model), loads YAML schemas.

### Step 2: Initialize Tools
```rust
let parser = SqlParser::from_dialect_name(&project.config.dialect.to_string())?;
let jinja = JinjaEnvironment::with_macros(&project.config.vars, &macro_paths);
```

### Step 3: Build Schema Catalog
```rust
let external_tables = build_external_tables_lookup(&project);
let (mut schema_catalog, yaml_schemas) = build_schema_catalog(&project, &external_tables);
```

`build_schema_catalog()` iterates all models' YAML schemas, converts each column's `data_type` string → `SqlType` via `parse_sql_type()`, sets nullability based on constraints, and creates a `RelSchema` per model. External tables get empty schemas.

### Step 4: Build DAG
```rust
let dep_map = /* model → internal dependencies */;
let dag = ModelDag::build(&dep_map)?;
let topo_order = dag.topological_order()?;
```

### Step 5: Build AST-Based Lineage
```rust
let mut project_lineage = ProjectLineage::new();
for (name, model) in &project.models {
    let rendered = jinja.render(&model.raw_sql)?;
    let stmts = parser.parse(&rendered)?;
    if let Some(lineage) = extract_column_lineage(&stmts[0], name) {
        project_lineage.add_model_lineage(lineage);
    }
}
project_lineage.resolve_edges(&known_models);
```

### Step 6: Lower Models to IR (Tier 1)
```rust
for name in &topo_order {
    let rendered = jinja.render(&model.raw_sql)?;
    let stmts = parser.parse(&rendered)?;
    let ir = lower_statement(&stmts[0], &schema_catalog)?;

    // Feed schema forward for downstream models
    schema_catalog.insert(name.clone(), ir.schema().clone());
    model_irs.insert(name.clone(), ir);
}
```

This is notable: even the IR-based tier propagates schemas through the catalog! Each model's IR output schema is registered for downstream models to reference during lowering.

### Step 7: Run IR Passes
```rust
let ctx = AnalysisContext::new(project, dag, yaml_schemas, project_lineage);
let pass_manager = PassManager::with_defaults();
let mut diagnostics = pass_manager.run(&order, &model_irs, &ctx, pass_filter.as_deref());
```

Runs: `type_inference`, `nullability`, `join_keys` (per-model), then `unused_columns` (DAG-level).

### Step 8: Run DataFusion LogicalPlan Passes (Tier 2)
```rust
// Collect rendered SQL for all models in order
let sql_sources: HashMap<String, String> = /* ... */;

// Rebuild catalog for DataFusion propagation
let mut plan_catalog: SchemaCatalog = yaml_string_map.clone();
for ext in &external_tables {
    plan_catalog.insert(ext.clone(), RelSchema::empty());
}

// Build user function stubs
let user_fn_stubs = build_user_function_stubs(ctx.project());

// Propagate schemas through DAG via DataFusion
let propagation = propagate_schemas(&order, &sql_sources, &yaml_string_map, &plan_catalog, &user_fn_stubs);

// Run LogicalPlan passes
let plan_pass_manager = PlanPassManager::with_defaults();
let plan_diagnostics = plan_pass_manager.run(&order, &propagation.model_plans, &ctx, pass_filter.as_deref());
diagnostics.extend(plan_diagnostics);
```

### Step 9: Filter and Output
```rust
let filtered: Vec<_> = diagnostics.into_iter()
    .filter(|d| d.severity >= min_severity)
    .collect();

match args.output {
    AnalyzeOutput::Json => print_json(&filtered)?,
    AnalyzeOutput::Table => print_table(&filtered),
}

// Exit code 1 if any Error-severity diagnostics
if filtered.iter().any(|d| d.severity == Severity::Error) {
    return Err(ExitCode(1).into());
}
```

---

## Schema Mismatch Detection: The Core Value Proposition

The most valuable output of `ff analyze` is catching schema drift between YAML declarations and actual SQL output. Here's a concrete example:

### Example: Type Mismatch

**YAML** (`stg_orders.yml`):
```yaml
columns:
  - name: order_id
    data_type: INTEGER
  - name: total_amount
    data_type: DECIMAL(10,2)
  - name: order_date
    data_type: DATE
```

**SQL** (`stg_orders.sql`):
```sql
SELECT
    order_id,
    CAST(amount AS DOUBLE) AS total_amount,
    created_at AS order_date
FROM raw_orders
```

**What happens during propagation:**

1. YAML says `total_amount` is `DECIMAL(10,2)` → Arrow `Decimal128(10, 2)`
2. DataFusion plans the `CAST(amount AS DOUBLE)` → Arrow `Float64`
3. `arrow_to_sql_type(Float64)` → `SqlType::Float { F64 }`
4. `compare_schemas()` checks: Is `Decimal(10,2)` compatible with `Float { F64 }`?
5. `SqlType::is_compatible_with()` → **yes** (both numeric)
6. No mismatch reported for this column

But if the SQL had `CAST(amount AS VARCHAR)`:
1. DataFusion infers `Utf8`
2. `arrow_to_sql_type(Utf8)` → `SqlType::String { max_length: None }`
3. Is `Decimal(10,2)` compatible with `String`? → **no**
4. `TypeMismatch { column: "total_amount", yaml_type: "DECIMAL(10,2)", inferred_type: "VARCHAR" }`
5. Diagnostic A040, Warning severity

### Example: Missing Column

If the SQL doesn't produce a column that the YAML declares:

```sql
SELECT order_id, total_amount  -- missing order_date!
FROM raw_orders
```

This produces `MissingFromSql { column: "order_date" }` → Diagnostic A040, **Error** severity. This blocks `ff run` (unless `--skip-static-analysis`).

### Example: Nullability Propagation Through JOINs

```sql
SELECT
    o.order_id,
    c.customer_name
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
```

DataFusion's `SqlToRel` planner automatically marks the right side of a LEFT JOIN as nullable. So `c.customer_name` will have `nullable=true` in the output schema, even if `stg_customers.customer_name` was declared NOT NULL.

If the YAML for this model declares `customer_name` as NOT NULL:
- Inferred: nullable=true (from LEFT JOIN)
- YAML: nullable=false (NOT NULL constraint)
- → `NullabilityMismatch` → Diagnostic A041, Warning

---

## Diagnostic Codes Reference

### IR-Based Pass Diagnostics

| Code | Pass | Severity | Description |
|------|------|----------|-------------|
| A001 | `type_inference` | Info | Column type could not be determined (Unknown type from YAML) |
| A002 | `type_inference` | Warning | UNION columns have incompatible types |
| A003 | `type_inference` | Error | UNION operands have different column counts |
| A004 | `type_inference` | Warning | SUM() or AVG() applied to a string column |
| A005 | `type_inference` | Info | Potentially lossy cast (e.g., FLOAT→INT, STRING→INT) |
| A010 | `nullability` | Warning | Column nullable after JOIN but used without COALESCE/IS NOT NULL guard |
| A011 | `nullability` | Warning | YAML declares NOT NULL but column is nullable after JOIN |
| A012 | `nullability` | Info | IS NULL or IS NOT NULL check on an always-NOT-NULL column (redundant) |
| A020 | `unused_columns` | Info | Column produced but never referenced by any downstream model |
| A021 | `unused_columns` | Info | Model uses SELECT * — cannot detect unused columns |
| A030 | `join_keys` | Warning | Join key columns have incompatible types |
| A032 | `join_keys` | Info | Cross join (Cartesian product) detected |
| A033 | `join_keys` | Info | Non-equi join condition detected |

### LogicalPlan-Based Pass Diagnostics

| Code | Pass | Severity | Description |
|------|------|----------|-------------|
| A040 | `cross_model_consistency` | Warning | Column in SQL but not in YAML (ExtraInSql) |
| A040 | `cross_model_consistency` | **Error** | Column in YAML but missing from SQL (MissingFromSql) |
| A040 | `cross_model_consistency` | Warning | YAML type differs from DataFusion-inferred type (TypeMismatch) |
| A041 | `cross_model_consistency` | Warning | YAML nullability conflicts with DataFusion-inferred nullability |

### Analysis Engine Error Codes (Internal)

| Code | Description |
|------|-------------|
| AE001 | Failed to lower SQL statement to IR |
| AE002 | Unsupported SQL construct during lowering |
| AE003 | Unknown table referenced (not in catalog) |
| AE004 | Column resolution failed |
| AE005 | SQL parse error during analysis |
| AE006 | Core error propagation |
| AE007 | SQL crate error propagation |
| AE008 | DataFusion planning error |

---

## File Index

| File | Purpose |
|------|---------|
| `crates/ff-analysis/src/lib.rs` | Public API, re-exports |
| `crates/ff-analysis/src/ir/types.rs` | `SqlType`, `Nullability`, `TypedColumn`, `parse_sql_type()` |
| `crates/ff-analysis/src/ir/schema.rs` | `RelSchema` — ordered list of typed columns |
| `crates/ff-analysis/src/ir/expr.rs` | `TypedExpr` — typed expression tree for IR |
| `crates/ff-analysis/src/ir/relop.rs` | `RelOp` — relational operator IR tree |
| `crates/ff-analysis/src/lowering/mod.rs` | AST-to-IR lowering entry point |
| `crates/ff-analysis/src/lowering/query.rs` | Query lowering (SELECT statements) |
| `crates/ff-analysis/src/lowering/select.rs` | SELECT clause lowering |
| `crates/ff-analysis/src/lowering/expr.rs` | Expression lowering |
| `crates/ff-analysis/src/lowering/join.rs` | JOIN lowering |
| `crates/ff-analysis/src/context.rs` | `AnalysisContext` — project metadata for passes |
| `crates/ff-analysis/src/error.rs` | `AnalysisError` (AE001-AE008) |
| `crates/ff-analysis/src/pass/mod.rs` | Pass infrastructure, `PassManager`, `DiagnosticCode`, `Severity` |
| `crates/ff-analysis/src/pass/type_inference.rs` | A001-A005: Type checking on IR |
| `crates/ff-analysis/src/pass/nullability.rs` | A010-A012: Nullability propagation on IR |
| `crates/ff-analysis/src/pass/join_keys.rs` | A030-A033: Join key analysis on IR |
| `crates/ff-analysis/src/pass/unused_columns.rs` | A020-A021: Unused column detection (DAG) |
| `crates/ff-analysis/src/pass/plan_pass.rs` | `PlanPassManager`, `PlanPass`, `DagPlanPass` traits |
| `crates/ff-analysis/src/pass/plan_cross_model.rs` | A040-A041: Cross-model consistency on LogicalPlan |
| `crates/ff-analysis/src/datafusion_bridge/mod.rs` | Bridge module declaration |
| `crates/ff-analysis/src/datafusion_bridge/types.rs` | `sql_type_to_arrow()`, `arrow_to_sql_type()` |
| `crates/ff-analysis/src/datafusion_bridge/provider.rs` | `FeatherFlowProvider` (ContextProvider impl) |
| `crates/ff-analysis/src/datafusion_bridge/planner.rs` | `sql_to_plan()` — SQL string → LogicalPlan |
| `crates/ff-analysis/src/datafusion_bridge/functions.rs` | DuckDB function stubs for planning |
| `crates/ff-analysis/src/datafusion_bridge/propagation.rs` | `propagate_schemas()` — DAG-wide type checking |
| `crates/ff-analysis/src/datafusion_bridge/lineage.rs` | Column lineage from LogicalPlans |
| `crates/ff-cli/src/commands/analyze.rs` | CLI command implementation |
| `crates/ff-cli/src/commands/common.rs` | Shared utilities (`build_schema_catalog`, etc.) |
| `crates/ff-cli/src/cli.rs` | `AnalyzeArgs`, `AnalyzeOutput`, `AnalyzeSeverity` |
