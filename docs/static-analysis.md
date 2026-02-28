# Static Analysis Pipeline

Feather-Flow includes a compile-time static analysis engine built on [Apache DataFusion](https://datafusion.apache.org/). It validates SQL models against their YAML schema declarations, detects type errors, nullable-column hazards, unused columns, and cross-model inconsistencies — all without executing any SQL against a database.

Static analysis runs automatically during `ff dt compile` and as a gate before `ff run` (unless `--skip-static-analysis` is passed).

## Pipeline Overview

The analysis pipeline has four stages:

```
YAML + SQL sources
      |
      v
1. Schema Catalog   -- build_schema_catalog()
      |
      v
2. SQL -> LogicalPlan  -- DataFusion SqlToRel via sql_to_plan()
      |
      v
3. Schema Propagation  -- propagate_schemas() walks DAG in topo order
      |
      v
4. Diagnostic Passes   -- PlanPassManager runs per-model + DAG passes
      |
      v
   Diagnostics (A0xx, SA0x, AE0xx)
```

### Stage 1: Schema Catalog

`build_schema_catalog()` (in `ff-cli/src/commands/common.rs`) builds a `SchemaCatalog` — a `HashMap<String, Arc<RelSchema>>` mapping table names to their column definitions.

Sources of schema information, in priority order:

1. **Model YAML columns** — each column's `type:` field is parsed via `parse_sql_type()` into a `SqlType`. Columns with a `not_null` test get `Nullability::NotNull`; all others get `Nullability::Unknown`.
2. **Source YAML columns** — source table definitions from `kind: source` YAML files. Same type/nullability parsing.
3. **External tables** — tables not defined in the project (referenced but external). Added with empty schemas so planning doesn't fail on unknown references.

The catalog also includes seed tables (CSVs) alongside models.

### Stage 2: SQL to LogicalPlan

Each model's compiled SQL is converted to a DataFusion `LogicalPlan` via `sql_to_plan()` (in `ff-analysis/src/datafusion_bridge/planner.rs`).

Because DataFusion 52.x bundles sqlparser 0.59 while Feather-Flow uses sqlparser 0.60, the SQL string is re-parsed through DataFusion's own parser to avoid cross-version type mismatches. The flow:

1. Parse SQL string with `DFParserBuilder` using `DuckDbDialect`
2. Plan the first statement via `SqlToRel` with a `FeatherFlowProvider`

The `FeatherFlowProvider` (in `ff-analysis/src/datafusion_bridge/provider.rs`) implements DataFusion's `ContextProvider` trait, providing:

- **Table resolution** — looks up tables in the schema catalog
- **Scalar UDF resolution** — registered DuckDB built-in stubs plus user-defined function stubs
- **Aggregate/window function resolution** — standard SQL aggregates

### Stage 3: Schema Propagation

`propagate_schemas()` (in `ff-analysis/src/datafusion_bridge/propagation.rs`) walks models in topological order:

1. For each model in topo order:
   a. Build a `FeatherFlowProvider` with the current schema catalog state
   b. Call `sql_to_plan()` to get the `LogicalPlan`
   c. Extract the inferred output schema from the plan's Arrow schema
   d. Cross-check against the YAML-declared schema, producing `SchemaMismatch` diagnostics
   e. **Feed forward**: insert the inferred schema into the catalog so downstream models see it

This feed-forward design means upstream type changes propagate correctly through the DAG.

The cross-check produces two classes of mismatches:
- **SA01 (MissingFromSql)** — a YAML-declared column doesn't appear in the SQL output. This is a hard error that blocks `ff run`.
- **SA02 (ExtraInSql / TypeMismatch / NullabilityMismatch)** — warnings that don't block execution.

### Stage 4: User Function Stubs

`build_user_function_stubs()` (in `ff-analysis/src/datafusion_bridge/provider.rs`) scans the project for `kind: function` nodes and creates `UserFunctionStub` entries so DataFusion's planner can resolve user-defined macros.

Each stub specifies the function name, argument types, and return type. The stubs are registered as DataFusion `ScalarUDF` instances during planning.

## Diagnostic Passes

After propagation, the `PlanPassManager` runs composable analysis passes. There are two categories:

### Per-Model Passes (`PlanPass` trait)

These run on each model's `LogicalPlan` independently:

| Pass | Codes | Description |
|------|-------|-------------|
| `plan_type_inference` | A002-A005 | Type mismatches in UNIONs, lossy casts, aggregate type issues |
| `plan_nullability` | A010-A012 | Nullable columns from JOINs used without null guards |
| `plan_join_keys` | A030, A032-A033 | Join key type mismatches, cross joins, non-equi joins |

### DAG-Level Passes (`DagPlanPass` trait)

These run across all models after per-model passes complete:

| Pass | Codes | Description |
|------|-------|-------------|
| `plan_unused_columns` | A020 | Columns produced but never consumed downstream |
| `cross_model_consistency` | A040-A041 | YAML vs inferred schema mismatches across models |
| `description_drift` | A050-A052 | Documentation drift across column lineage edges |

## Diagnostic Code Reference

### Type Inference (A00x)

| Code | Severity | Description |
|------|----------|-------------|
| A001 | — | *(Retired)* Unknown type — DataFusion resolves all types |
| A002 | Warning | UNION column type mismatch between branches |
| A003 | Warning | UNION branch column count mismatch |
| A004 | Warning | SUM/AVG applied to a string column |
| A005 | Warning | Lossy implicit cast (e.g., DOUBLE to INTEGER) |

### Nullability (A01x)

| Code | Severity | Description |
|------|----------|-------------|
| A010 | Warning | Column becomes nullable after LEFT/RIGHT/FULL JOIN without null guard |
| A011 | Warning | YAML declares NOT NULL but JOIN makes column nullable |
| A012 | Info | Redundant IS NULL check on a column that cannot be NULL |

### Unused Columns (A02x)

| Code | Severity | Description |
|------|----------|-------------|
| A020 | Info | Column produced by model but never referenced downstream |

### Join Keys (A03x)

| Code | Severity | Description |
|------|----------|-------------|
| A030 | Warning | Join key type mismatch (e.g., INTEGER = VARCHAR) |
| A032 | Warning | Cross join (Cartesian product) detected |
| A033 | Info | Non-equi join condition (e.g., `a.x > b.y`) |

### Cross-Model (A04x)

| Code | Severity | Description |
|------|----------|-------------|
| A040 | Warning/Error | Schema mismatch: extra column in SQL, missing column in SQL, or type mismatch |
| A041 | Warning | Nullability mismatch between YAML and inferred schema |

### Description Drift (A05x)

| Code | Severity | Description |
|------|----------|-------------|
| A050 | Info | Copy/Rename column has no description — consider inheriting from upstream |
| A051 | Info | Copy/Rename column description differs from upstream — potential drift |
| A052 | Info | Transform column has no description — needs new documentation |

### Schema Mismatches (SAxx)

| Code | Severity | Description |
|------|----------|-------------|
| SA01 | Error | Column declared in YAML but missing from SQL output — blocks `ff run` |
| SA02 | Warning | Extra column in SQL, type mismatch, or nullability mismatch vs YAML |

### Analysis Errors (AExx)

These are infrastructure errors, not model-quality diagnostics:

| Code | Description |
|------|-------------|
| AE003 | Unknown table referenced — schema catalog lookup failed |
| AE004 | Cannot resolve column reference |
| AE005 | SQL parse error during analysis |
| AE006 | Core library error |
| AE007 | SQL library error |
| AE008 | DataFusion planning error |

## Severity Overrides

Diagnostic severity can be overridden in `featherflow.yml`:

```yaml
analysis:
  severity_overrides:
    A020: "off"      # Suppress unused column warnings
    A010: "error"    # Promote nullable-from-join to error
    SA02: "off"      # Suppress schema mismatch warnings
```

Valid override values: `info`, `warning`, `error`, `off`.

## Reading Diagnostic Output

During `ff dt compile`, diagnostics appear as:

```
  [warn] stg_orders: Column 'amount' declared in YAML but missing from SQL output
  [error] fct_orders: Column 'total' declared in YAML but missing from SQL output
Static analysis: 5 models planned, 0 failures
```

Each line contains:
- **Severity** in brackets: `[info]`, `[warn]`, `[error]`
- **Model name** that produced the diagnostic
- **Message** describing the issue

For JSON output (`--output json`), diagnostics are included in the compile results object.

## Architecture

Key source files:

| File | Purpose |
|------|---------|
| `ff-analysis/src/datafusion_bridge/provider.rs` | `FeatherFlowProvider` — DataFusion ContextProvider |
| `ff-analysis/src/datafusion_bridge/planner.rs` | `sql_to_plan()` — SQL string to LogicalPlan |
| `ff-analysis/src/datafusion_bridge/propagation.rs` | `propagate_schemas()` — DAG-wide schema propagation |
| `ff-analysis/src/datafusion_bridge/functions.rs` | DuckDB UDF stubs for DataFusion |
| `ff-analysis/src/pass/plan_pass.rs` | `PlanPass` / `DagPlanPass` traits, `PlanPassManager` |
| `ff-analysis/src/pass/plan_type_inference.rs` | Type inference pass (A002-A005) |
| `ff-analysis/src/pass/plan_nullability.rs` | Nullability pass (A010-A012) |
| `ff-analysis/src/pass/plan_join_keys.rs` | Join key pass (A030-A033) |
| `ff-analysis/src/pass/plan_unused_columns.rs` | Unused columns pass (A020) |
| `ff-analysis/src/pass/plan_cross_model.rs` | Cross-model consistency (A040-A041) |
| `ff-analysis/src/pass/plan_description_drift.rs` | Description drift (A050-A052) |
| `ff-cli/src/commands/common.rs` | `build_schema_catalog()`, `run_static_analysis_pipeline()` |
