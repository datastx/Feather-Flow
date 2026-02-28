# Model Resolution

How Feather-Flow resolves model dependencies from SQL source files through to
execution ordering.

## Overview

Feather-Flow extracts dependencies directly from the SQL AST using sqlparser's
`visit_relations` visitor. There is no `ref()` or `source()` function -- models
write plain SQL with bare table names, and the compiler figures out what depends
on what.

## Dependency Extraction

The core extraction lives in `crates/ff-sql/src/extractor.rs`.

### `extract_dependencies(statements: &[Statement]) -> HashSet<String>`

Walks every parsed statement with `visit_relations`, collecting all table
references from FROM clauses, JOINs, and subqueries. CTE names defined in WITH
clauses are automatically filtered out so they don't appear as external
dependencies.

```
visit_relations(stmt, |relation| {
    deps.insert(object_name_to_string(relation));
    ControlFlow::Continue(())
});
```

Multi-part references like `schema.table` are preserved as-is. The last
component is compared against CTE names for filtering.

### `categorize_dependencies_with_unknown(deps, known_models, external_tables)`

After extraction, raw table names are classified into three buckets:

| Bucket | Description |
|--------|-------------|
| `model_deps` | Matches a known model or seed (case-insensitive) |
| `external_deps` | Matches a declared external table or source |
| `unknown_deps` | Not found in either set |

Model matching normalizes to the last dot-separated component
(`normalize_table_name`) so that `schema.table` still matches a model named
`table`.

A dialect-aware variant (`extract_dependencies_resolved` /
`categorize_dependencies_resolved`) preserves quoting and case-sensitivity
metadata for dialects like Snowflake where `"MyTable"` requires exact-case
matching.

## Name Resolution

When the compiler encounters a bare table name in SQL, it resolves against three
sources in order:

1. **Models and seeds** -- nodes defined in `nodes/` with `kind: sql` or
   `kind: seed`. Both populate the `known_models` set. Seeds are included so
   that seed-backed tables don't trigger false-positive unknown-dependency
   errors.

2. **External tables and sources** -- tables declared in `featherflow.yml`
   `external_tables` plus source tables from `kind: source` YAML files.
   Built by `build_external_tables_lookup()` in `common.rs`.

3. **User-defined table functions** -- if an unknown dependency matches a
   `kind: function` node with `function_type: Table`, the function's SQL body
   is parsed recursively to discover transitive model dependencies. Handled by
   `resolve_function_dependencies()` in `common.rs`.

4. **Hard error** -- any remaining unknown dependency after function resolution
   triggers a bail:

   ```
   Unknown dependencies in model 'X': [table_a, table_b].
   Each table must be defined as a model, seed, source, or function.
   ```

## 3-Part Qualification

After compilation, bare table references are rewritten to fully-qualified
3-part names (`database.schema.table`) using AST mutation.

### `build_qualification_map()` (in `common.rs`)

Builds a `HashMap<String, QualifiedRef>` mapping lowercase bare names to their
qualified form. All entry types produce 3-part names:

| Entry type | Database (catalog) | Schema | Table |
|------------|-------------------|--------|-------|
| Models | From DuckDB file path | Model's `config.schema` or project default | Model name |
| Seeds | From DuckDB file path | Seed's `target_schema()` or project default | Seed name |
| Sources | Source YAML `database:` field, or project default | Source `schema:` field | `identifier` or `name` |

### Catalog name derivation

The database/catalog name comes from `DuckDbBackend::catalog_name_for_path()`
(`crates/ff-db/src/duckdb.rs`):

| DuckDB path | Catalog name |
|-------------|-------------|
| `dev.duckdb` | `dev` |
| `/path/to/analytics.duckdb` | `analytics` |
| `:memory:` | `memory` |

DuckDB names its catalogs after the file stem, so Feather-Flow mirrors that
convention.

### `qualify_table_references()` / `qualify_statements()` (in `crates/ff-sql/src/qualify.rs`)

Uses `visit_relations_mut` to rewrite single-part (bare) names in the AST.
Already-qualified references (2+ parts) are left unchanged. The `QualifiedRef`
struct:

```rust
pub struct QualifiedRef {
    pub database: Option<String>,  // always Some for FF-produced entries
    pub schema: String,
    pub table: String,
}
```

## DAG Construction

Once every model's dependencies are extracted, the DAG is built.

### `ModelDag::build(dependencies)` (in `crates/ff-core/src/dag.rs`)

Takes `HashMap<String, Vec<String>>` (model name to list of dependency names)
and builds a `petgraph::DiGraph<ModelName, ()>`:

1. Add all models as nodes.
2. For each dependency edge, add a directed edge from the dependency to the
   dependent (`to_idx -> from_idx`), so topological sort yields dependencies
   first.
3. **Self-references are silently skipped** -- incremental models may reference
   their own table (e.g., `LEFT JOIN self_table`), which would create a cycle.
4. Only edges between known models are added; external tables are excluded.
5. **Cycle detection** via `petgraph::algo::toposort`. If a cycle exists,
   `CoreError::CircularDependency` is returned with the cycle path.

### `topological_order() -> Vec<String>`

Returns all models in dependency-first order, used to determine compilation
and execution sequence.

## Compile Pipeline

The full flow in `compile.rs`:

```
1. Load project         Project::load() reads nodes/, parses YAML
                         (model.depends_on is EMPTY at this stage)

2. Jinja render          render_with_config_and_model() expands templates
                         and captures config() values

3. SQL parse             SqlParser::parse() via sqlparser-rs

4. Validation            validate_no_complex_queries() rejects CTEs (S005)
                         and derived tables (S006)

5. Dependency extract    extract_dependencies() via visit_relations

6. Categorize            categorize_dependencies_with_unknown() splits into
                         model/external/unknown buckets

7. Function resolution   resolve_function_dependencies() handles table
                         function transitive deps

8. Unknown check         Hard error if any deps remain unknown

9. DAG build             ModelDag::build() + topological_order()

10. Static analysis      DataFusion schema propagation (SA01/SA02)

11. Qualification        build_qualification_map() + qualify_statements()
                         rewrites bare names to 3-part qualified names

12. Ephemeral inlining   collect_ephemeral_dependencies() + inline_ephemeral_ctes()
                         inlines ephemeral models as CTEs into consumers

13. Write output         Compiled SQL written to target/compiled/
```

Steps 2-8 happen per-model in `compile_model_phase1()`. Steps 9-13 happen
after all models complete phase 1.

## Error Handling

### CTE and Derived Table Bans

Every transform must be its own model. The validator in
`crates/ff-sql/src/validator.rs` enforces:

- **S005 `CteNotAllowed`** -- WITH clauses are rejected. Use separate models
  instead of CTEs.
- **S006 `DerivedTableNotAllowed`** -- Subqueries in FROM clauses are rejected.
  Scalar subqueries in SELECT/WHERE/HAVING remain allowed.

### Unknown Dependencies

After function resolution, any table reference that isn't a model, seed, source,
function, or declared external table causes a hard error in
`compile_model_phase1()`. This catches typos and missing declarations early.

### Circular Dependencies

`ModelDag::build()` calls `validate()` which runs `petgraph::algo::toposort`.
Cycles produce `CoreError::CircularDependency` with the cycle path
(e.g., `a -> b -> c -> a`).

## Key Source Files

| File | Role |
|------|------|
| `crates/ff-sql/src/extractor.rs` | `extract_dependencies()`, `categorize_dependencies*()` |
| `crates/ff-sql/src/validator.rs` | CTE/derived table validation |
| `crates/ff-sql/src/qualify.rs` | 3-part qualification via AST rewrite |
| `crates/ff-sql/src/parser.rs` | sqlparser-rs wrapper |
| `crates/ff-core/src/dag.rs` | `ModelDag` -- graph construction, cycle detection, topo sort |
| `crates/ff-core/src/model/mod.rs` | `Model` struct (depends_on, external_deps fields) |
| `crates/ff-cli/src/commands/compile.rs` | `compile_model_phase1()`, full compile pipeline |
| `crates/ff-cli/src/commands/common.rs` | `build_qualification_map()`, `build_project_dag()`, `resolve_function_dependencies()` |
| `crates/ff-db/src/duckdb.rs` | `DuckDbBackend::catalog_name_for_path()` |
