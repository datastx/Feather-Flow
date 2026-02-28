# Feather-Flow Roadmap

## 1. VS Code Extension Overhaul

The extension (`vscode-featherflow/`) has the scaffolding (tree view, go-to-definition, lineage webview) but the UX is rough. Needs:
- Separate **table-level lineage** view (DAG overview) vs **column-level lineage** drilldown (trace a single column upstream/downstream)
- Better sidebar tree — group by kind (sql, seed, source, function), show materialization type, surface schema test status
- Improve the lineage webview rendering (currently just pipes CLI output into a panel)

## 2. Advanced Materializations - Not ready todo yet

Current materializations (view, table, incremental, ephemeral) work but are basic. Improvements:
- **Snapshots** — type-2 SCD materialization that tracks historical changes with `valid_from`/`valid_to` columns
- **Custom materialization plugins** — let users define their own materialization strategy (e.g., partitioned table, external table) instead of hardcoding the four options in `execute.rs`
- **Smarter ephemeral inlining** — currently CTE-inlined at compile time; consider caching compiled ephemeral subqueries across dependent models to avoid recompilation

## 3. Query Comments & Runtime Telemetry

Query comments (`ff_core::query_comment`) already inject `/* ff_metadata: {...} */` into executed SQL. Enhancements:
- **Runtime telemetry emission** — emit structured execution metrics (row counts, duration, bytes scanned) as JSON events during `ff run`, consumable by observability tools
- **Configurable comment fields** — allow users to inject arbitrary key-value pairs beyond `custom_vars` (e.g., CI job ID, git SHA)
- Low priority for MVP — current implementation is functional

## 4. Multi-Dialect Support with Apache Iceberg - Not ready todo yet

Currently DuckDB-only. The goal is to be the first schema validation framework that natively supports multiple SQL dialects (DuckDB, Snowflake, BigQuery, Spark) backed by Apache Iceberg as the universal storage layer. This means:
- Abstract the `ff-db` crate into a trait-based backend system with dialect-specific implementations
- Use Iceberg as the table format so data written by one dialect is readable by another
- Leverage `sqlparser-rs` dialect support (already partially there) to validate SQL against the target dialect at compile time

## 5. Data Agent - Not ready todo yet

With schema validation, static analysis, column lineage, and the metadata database already in place, build an AI-powered data agent that can:
- Answer questions about the data pipeline ("what models feed into `fct_revenue`?", "which columns are nullable?")
- Generate new models from natural language descriptions, pre-wired with correct schemas and dependencies
- Diagnose pipeline failures using execution metadata from `ff_meta`

## 6. Enforce Schema Descriptions on All Nodes

Every node's YAML schema should require a `description` field on each column. This is already partially enforced — extend it so that:
- Missing column descriptions produce a compile error (not just a warning)
- Description propagation via lineage is surfaced (inherited vs. modified vs. missing — `DescriptionStatus` already exists in `ff-sql`)
- This strengthens cross-node documentation since schemas define the contract between nodes

## 7. Replace `is_incremental` with `is_exists` and Dual-Path Compilation

Currently `is_incremental()` in Jinja conflates "the model is incremental" with "the table exists." Refactor to:
- Rename the Jinja function to `is_exists()` — it returns true when the target table already exists in the database, regardless of materialization type
- **Compile two SQL paths** for incremental models: one for the initial full load, one for the incremental append/merge. Store both in `target/compiled/`
- This separates concerns — the full-refresh path can use `CREATE TABLE AS`, the incremental path can use `INSERT INTO`/`MERGE` without `{% if %}` branching in user SQL
- Enables better static analysis since each path is a complete, standalone SQL statement

## 8. Remove Contracts

Contracts (`ff-core/src/contract.rs`, `--contracts` flag, runtime `validate_model_contract()`) duplicate what schema validation and static analysis already enforce. Remove:
- Delete `contract.rs`, the `contract:` YAML key from `ModelSchema`, and the `--contracts` CLI flag
- Remove runtime contract checks from `hooks.rs`
- Schema validation (SA01/SA02) and DataFusion type checking already catch missing columns, type mismatches, and nullability violations at compile time

## 9. Document Model Resolution

Write a doc explaining how Feather-Flow resolves model dependencies:
- AST-based extraction via `visit_relations` (no `ref()` or `source()`)
- How bare table names in SQL are matched to nodes in `nodes/` vs external sources in config
- How 3-part table qualification (`database.schema.table`) works in `qualify.rs`
- How the DAG is constructed and topologically sorted for execution

## 10. Format All Test Fixtures + CI Gate

Run `ff dt fmt` across all 42 fixture projects in `crates/ff-cli/tests/fixtures/`. Then:
- Add a CI test that runs `ff dt fmt --check` on all fixtures and fails if any are unformatted
- Ensures fixture SQL stays consistent and doesn't drift from the formatter's output

## 11. Enforce Descriptions in Test Fixtures

All fixture YAML files that define columns should include `description:` fields where applicable. This:
- Serves as living documentation for the test suite
- Validates that description propagation and `DescriptionStatus` logic works correctly in integration tests
- Aligns fixtures with the standard enforced in production projects (see item 6)

## 12. Compile Hooks & Tests to Target + Static Analysis Doc

Pre-hooks, post-hooks (`ModelConfig.pre_hook`/`post_hook`), and schema tests are currently lazily evaluated at runtime — they're raw SQL strings that never go through the compile pipeline. Change this:
- **Compile hooks and tests to `target/`** alongside model SQL so they're visible, diffable, and analyzable
- Run static analysis on hook/test SQL the same way it runs on model SQL (type checking, dependency extraction)
- Write a "How Static Analysis Works" doc covering the full compile pipeline: Jinja render → sqlparser AST → DataFusion LogicalPlan → schema propagation → diagnostic passes

## 13. Run Groups

Named, reusable node selections defined in `featherflow.yml`. Mirrors dbt's selector concept but built around FF's existing `--nodes`/`-n` selector syntax and `ff run` modes.

### Config

```yaml
run_groups:
  daily-finance:
    description: "Daily finance pipeline"
    nodes: "tag:finance,tag:daily"
    mode: build                     # optional, overrides project default (build/models/test)
    full_refresh: false             # optional, default false
    fail_fast: true                 # optional, default false

  staging-refresh:
    description: "Full staging layer rebuild"
    nodes: "+stg_orders+,+stg_customers+"
    mode: models

  nightly-tests:
    description: "Run all tests without re-executing models"
    nodes: "tag:finance,tag:staging"
    mode: test
```

### CLI Usage

A run group is invoked via the `run-group:` prefix in the `-n` selector, reusing the same `apply_selectors()` pipeline:

```bash
# Run using a named run group
ff run -n run-group:daily-finance

# Run group inherits its mode/flags from config, but CLI flags override:
ff run -n run-group:daily-finance --full-refresh    # overrides full_refresh: false
ff run -n run-group:daily-finance --mode test        # overrides mode: build

# Combine run groups with inline selectors (union):
ff run -n run-group:daily-finance,stg_payments+

# List available run groups:
ff dt ls --resource-type run-group
```

### How It Works

- `Selector::parse()` gets a new `SelectorType::RunGroup { name }` variant
- When `apply_selectors()` encounters `run-group:<name>`, it looks up the group in `Config.run_groups`, expands `nodes` into its constituent selectors, and unions them into the result set like any other comma-separated selector
- `mode`, `full_refresh`, `fail_fast` from the run group config are passed up as defaults to `ff run`, but explicit CLI flags always win
- Run groups compose — a run group's `nodes` field can reference `tag:`, `path:`, `owner:`, `state:`, graph traversal, or even other run groups via `run-group:<name>`
- Validation: `ff dt compile` checks that all run groups resolve to at least one node and that referenced tags/paths exist

## 14. Scheduled Run Groups - Not ready todo yet

Build on run groups (item 13) to add cron-style scheduling metadata:

```yaml
schedules:
  daily-finance:
    run_group: daily-finance
    cron: "0 6 * * *"
```

FF itself won't run a scheduler daemon — this generates artifacts (Airflow DAGs, GitHub Actions workflows, cron entries) that external orchestrators consume. The value is keeping schedule definitions co-located with the pipeline definition.

## 15. Meta Database-Powered Testing & Memory Benchmarks

The `ff_meta` DuckDB database already stores execution state, schemas, and model metadata. Use it to:
- Build integration tests that query `ff_meta` tables to assert on compilation results, execution history, and schema propagation
- Add memory benchmarks that track peak RSS and allocation counts across fixture compilations, stored in `ff_meta` for trend analysis
- This replaces ad-hoc test assertions with data-driven validation

## 16. Move Config Exclusively to YAML

Currently `{{ config(materialized='table', schema='staging') }}` lives inside the companion file (`.sql`, `.py`). This mixes metadata with logic. Move all config to the `.yml` file:

```yaml
kind: sql
materialized: table
schema: staging
```

- Remove the `config()` Jinja function entirely — the `.yml` is already required and is the single source of truth for node metadata
- Eliminates the need to parse/render Jinja just to extract config, which simplifies the compile pipeline
- Config precedence becomes: YAML node config → project defaults in `featherflow.yml` (no middle layer)
- Applies uniformly to all node kinds — SQL, Python, Docker — since they all already have a `.yml` file
- **YAML compilation step** — the compile pipeline must validate YAML structure (required fields, valid enum values for `materialized`/`schema`/etc.) and report errors with the same diagnostic quality as SQL compilation. Today YAML is only deserialized; it needs to be actively compiled and cross-validated against the companion file (e.g., confirm the SQL output columns match the YAML `columns:` list). Compiled YAML output should be written to `target/compiled/` alongside the compiled SQL — this makes the full resolved config inspectable, diffable, and available for downstream tools
- **Functions and table functions must compile to target too** — currently UDFs (`kind: function`) are deployed directly without going through the compile pipeline. They should produce compiled output in `target/compiled/` just like SQL models, so their `CREATE MACRO` statements are visible, versioned, and run through static analysis

## 17. Formalize the Compile Pipeline as Discrete Stages

The compile pipeline should be a well-defined sequence of stages with clear inputs/outputs:

1. **Jinja render** — render all `.sql`/`.py` templates + compile all `.yml` files. Write everything to `target/compiled/` (SQL, YAML, functions, hooks, tests — nothing skips this step)
2. **Target validation** — run structural checks over `target/compiled/`: YAML schema validation, cross-reference YAML columns against SQL output, verify required fields, detect config conflicts
3. **AST construction** — parse all compiled SQL via `sqlparser`, extract dependencies via `visit_relations`, run static analysis passes (DataFusion LogicalPlan)
4. **DAG build** — construct the dependency graph from extracted relations, validate for cycles, resolve node ordering
5. **Artifact persistence** — write the fully resolved DAG, schemas, lineage, and compilation metadata to the `ff_meta` DuckDB database as the single build artifact

Today these stages are interleaved and some nodes (functions, hooks, tests) bypass steps entirely. Making each stage explicit and sequential means every node kind goes through the same pipeline, the intermediate state at each stage is inspectable in `target/`, and `ff run` consumes the DuckDB artifact rather than re-deriving state.

## 18. Comprehensive Test Harness Expansion

Once the above features land, do a test coverage pass:
- Add end-to-end tests for every new feature (multi-dialect, run groups, dual-path incremental, etc.)
- Increase fixture coverage — currently 42 fixture projects, target full coverage of edge cases (circular deps, schema mismatches, large DAGs)
- Profile and optimize: use the meta database benchmarks (item 15) to identify regressions
- Code cleanup: remove dead code paths, consolidate duplicated logic, ensure all public APIs have tests
