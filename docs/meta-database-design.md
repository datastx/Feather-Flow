# Meta Database Design & Implementation Plan

Replace `manifest.json` and `state.json` with a DuckDB meta database (`target/meta.duckdb`). Enable a SQL-based rules engine where users query the meta database to enforce custom linting, governance, and CI checks.

---

## Why

1. **manifest.json is a flat dump** — no relationships, no lineage traversal, not queryable
2. **Custom linting requires Rust code** — every new check needs a new analysis pass, diagnostic code, and release
3. **Column lineage is trapped in Rust structs** — users can't explore lineage interactively
4. **State tracking is disconnected** — `state.json` and `manifest.json` are separate files with duplicated data
5. **CI integration is rigid** — teams want project-specific rules but can only use built-in diagnostic codes

## What this unlocks

- **SQL-powered rules engine** — write a SELECT that returns violations, zero Rust needed
- **Interactive lineage** — `SELECT * FROM v_lineage WHERE target_column = 'email'`
- **Push complexity from Rust to SQL** — incremental detection, impact analysis, docs generation
- **Single source of truth** — one `meta.duckdb` replaces manifest.json + state.json + analysis results
- **DuckDB CLI access** — users open `target/meta.duckdb` directly for ad-hoc queries

---

## Implementation Phases

### Phase 1: `ff-meta` crate + DDL + population (no callers yet)

Build the new crate with schema creation, population functions, and tests. Nothing calls it yet — existing manifest/state code is untouched.

### Phase 2: Wire meta database into existing commands

`ff compile`, `ff validate`, `ff run`, `ff analyze` populate `meta.duckdb` alongside existing JSON files. Both outputs are written — no breaking changes.

### Phase 3: Rules engine

Add `rules:` config key, `ff rules` command, rule file discovery, execution, and violation reporting.

### Phase 4: Migrate readers from JSON to meta.duckdb

Commands that currently read `manifest.json` or `state.json` switch to querying `meta.duckdb`. JSON files still written for backwards compatibility.

### Phase 5: Remove JSON artifacts

Delete `Manifest`, `StateFile`, `ManifestModel`, `ManifestSource`, `ManifestSourceColumn` types. `meta.duckdb` is the single source of truth. Add `ff meta export --format json` for ecosystem compatibility.

---

## Task Breakdown

### Task 1: Create `ff-meta` crate with DDL

**What**: New crate `crates/ff-meta/` that owns the meta database schema. Contains DDL as embedded SQL, a function to create/migrate the schema, and a `MetaDb` connection wrapper.

**Files to create**:
- `crates/ff-meta/Cargo.toml` — depends on `duckdb` (bundled), `thiserror`
- `crates/ff-meta/src/lib.rs` — public API re-exports
- `crates/ff-meta/src/error.rs` — `MetaError` enum with thiserror
- `crates/ff-meta/src/connection.rs` — `MetaDb` struct wrapping `duckdb::Connection`, `open(path)`, `open_memory()`, `create_schema()`
- `crates/ff-meta/src/ddl.rs` — embedded DDL strings, `create_tables()`, `schema_version()` check
- `crates/ff-meta/src/migration.rs` — version table, migration runner

**Files to modify**:
- `Cargo.toml` (workspace) — add `ff-meta` to members

**Schema version table**:
```sql
CREATE TABLE IF NOT EXISTS meta.schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT now()
);
```

Each DDL version is a numbered migration. `create_schema()` runs all unapplied migrations.

**Tests**:
- `create_schema` on fresh database creates all tables
- `create_schema` is idempotent (running twice doesn't error)
- Schema version is recorded after migration
- `open_memory()` works for unit tests
- All tables exist with correct columns (query `information_schema.columns`)
- All foreign keys are valid

**Definition of done**: `make ci` passes. `MetaDb::open_memory()` creates a fully migrated meta database. All 24+ tables exist. Schema version is tracked.

---

### Task 2: Population functions — project load (Phase 1)

**What**: Functions that take `&Project` and populate the meta database with everything known after `Project::load()` — models (declared metadata only), sources, functions, seeds, tests, tags, columns.

**Files to create**:
- `crates/ff-meta/src/populate.rs` — main population module
- `crates/ff-meta/src/populate/project.rs` — `populate_project(&MetaDb, &Project)`
- `crates/ff-meta/src/populate/models.rs` — `populate_models(&MetaDb, project_id, &[Model])`
- `crates/ff-meta/src/populate/sources.rs` — `populate_sources(&MetaDb, project_id, &[SourceFile])`
- `crates/ff-meta/src/populate/functions.rs` — `populate_functions(&MetaDb, project_id, &[FunctionDef])`
- `crates/ff-meta/src/populate/seeds.rs` — `populate_seeds(&MetaDb, project_id, &[Seed])`
- `crates/ff-meta/src/populate/tests.rs` — `populate_tests(&MetaDb, project_id, ...)`

**Files to modify**:
- `crates/ff-meta/Cargo.toml` — add `ff-core` dependency

**Data flow**: `Project` → insert into `projects`, `models`, `model_columns` (from YAML), `model_column_constraints`, `model_column_references`, `model_config`, `model_hooks`, `model_tags`, `model_meta`, `sources`, `source_tags`, `source_tables`, `source_columns`, `functions`, `function_args`, `function_return_columns`, `seeds`, `seed_column_types`, `tests`, `singular_tests`

**Tests**:
- Populate from sample project fixture → verify row counts in each table
- Model columns match YAML definitions
- Model tags are normalized (one row per tag)
- Source columns have correct types
- Function args are ordered correctly
- Tests reference correct model/column
- Running populate twice (idempotent) — clears and repopulates
- Empty project produces zero rows (no errors)

**Definition of done**: Given a loaded `Project`, all Phase 1 tables are correctly populated. Round-trip verified by querying back and comparing to source structs.

---

### Task 3: Population functions — compilation (Phase 2)

**What**: Functions that update the meta database after compilation — compiled SQL, checksums, dependencies.

**Files to create**:
- `crates/ff-meta/src/populate/compilation.rs` — `update_model_compiled()`, `populate_dependencies()`

**Data flow**: For each compiled model: update `models.compiled_sql`, `models.compiled_path`, `models.sql_checksum`. Insert `model_dependencies` (model→model) and `model_external_dependencies` (model→external table).

**Tests**:
- After compilation update, `models.compiled_sql` is non-NULL
- `model_dependencies` matches `model.depends_on`
- `model_external_dependencies` matches `model.external_deps`
- Dependency self-reference is rejected (CHECK constraint)
- Recompilation clears and repopulates dependencies

**Definition of done**: After compilation, dependency and compiled SQL data is correct in meta.duckdb.

---

### Task 4: Population functions — analysis (Phase 3)

**What**: Functions that populate analysis results — inferred schemas, column lineage, diagnostics, schema mismatches.

**Files to create**:
- `crates/ff-meta/src/populate/analysis.rs` — `populate_inferred_schemas()`, `populate_column_lineage()`, `populate_diagnostics()`, `populate_schema_mismatches()`

**Files to modify**:
- `crates/ff-meta/Cargo.toml` — add `ff-analysis` dependency

**Data flow**:
- `PropagationResult.model_plans` → update `model_columns.inferred_type`, `model_columns.nullability_inferred`
- `ModelColumnLineage` → insert into `column_lineage`
- `Vec<Diagnostic>` → insert into `diagnostics` (with `compilation_runs` FK)
- `Vec<SchemaMismatch>` → insert into `schema_mismatches`
- Create `compilation_runs` row with run metadata

**Tests**:
- Inferred types update existing YAML-declared columns
- Column lineage edges match DataFusion lineage output
- Diagnostics are recorded with correct codes and severities
- Schema mismatches have correct declared vs inferred values
- Compilation run is recorded with correct timestamps

**Definition of done**: After analysis, all inferred metadata and diagnostics are queryable in meta.duckdb.

---

### Task 5: Population functions — execution (Phase 4)

**What**: Functions that record execution results — row counts, durations, status.

**Files to create**:
- `crates/ff-meta/src/populate/execution.rs` — `record_model_run()`, `complete_run()`

**Data flow**: After each model executes: insert `model_run_state` row. After all models: update `compilation_runs.completed_at` and `compilation_runs.status`.

**Tests**:
- Model run state records row count and duration
- Multiple runs accumulate (historical, not replaced)
- `model_latest_state` view returns most recent successful run
- Failed model runs are recorded with error status

**Definition of done**: `model_run_state` accurately reflects execution results. `model_latest_state` view works for incremental logic.

---

### Task 6: Convenience views

**What**: Create SQL views that flatten common query patterns.

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — add views to DDL

**Views**:
- `v_models` — models + config + tags + dependency counts
- `v_columns` — columns + constraints + tests + model name
- `v_lineage` — lineage with resolved model names and classifications
- `v_diagnostics` — diagnostics with model names and run metadata
- `v_source_columns` — sources + tables + columns flattened

**Tests**:
- Each view returns correct data after full population
- `v_lineage` resolves both model-to-model and source-to-model lineage
- `v_models` correctly aggregates tags and counts

**Definition of done**: All 5 views are created by `create_schema()` and return correct data.

---

### Task 7: Wire meta population into CLI commands

**What**: Call population functions from existing CLI commands alongside current JSON output.

**Files to modify**:
- `crates/ff-cli/Cargo.toml` — add `ff-meta` dependency
- `crates/ff-cli/src/commands/compile.rs` — after manifest save, populate meta.duckdb (phases 1-2)
- `crates/ff-cli/src/commands/validate.rs` — populate phases 1-3
- `crates/ff-cli/src/commands/run/mod.rs` — populate phases 1-4
- `crates/ff-cli/src/commands/run/compile.rs` — populate compilation data
- `crates/ff-cli/src/commands/run/state.rs` — populate execution data
- `crates/ff-cli/src/commands/analyze.rs` — populate phases 1-3
- `crates/ff-cli/src/commands/common.rs` — shared `open_meta_db()` helper

**Pattern**: Each command opens `MetaDb::open(target_dir.join("meta.duckdb"))`, calls the appropriate populate functions, and the meta database is written to disk when the connection closes. JSON files continue to be written as before.

**Tests**:
- Integration test: `ff compile` produces both `manifest.json` and `meta.duckdb`
- Integration test: `ff validate` populates diagnostics in meta.duckdb
- Integration test: `ff run` populates execution state in meta.duckdb
- Meta database file exists after each command
- Querying meta.duckdb returns correct model count

**Definition of done**: All 4 commands produce `meta.duckdb` alongside existing JSON. No existing behavior changes. `make ci` passes.

---

### Task 8: Rules engine — config and discovery

**What**: Add `rules:` configuration to `featherflow.yml`, discover rule SQL files, parse rule metadata from header comments.

**Files to modify**:
- `crates/ff-core/src/config.rs` — add `rules: Option<RulesConfig>` field (before `deny_unknown_fields` blocks)

**Files to create**:
- `crates/ff-core/src/rules.rs` — `RulesConfig`, `RuleFile`, `RuleSeverity`, `discover_rules()`

**Config shape**:
```yaml
rules:
  paths:
    - rules/
  severity: error        # default severity
  on_failure: fail       # fail | warn
```

**Rule file header parsing** (SQL comments at top of file):
```sql
-- rule: descriptive_rule_name
-- severity: error
-- description: Human-readable explanation.
SELECT ... FROM meta.models WHERE ...
```

**Tests**:
- Config with `rules:` key parses correctly
- Config without `rules:` key still parses (optional)
- Rule files are discovered from configured paths
- Header comments are parsed (rule name, severity, description)
- Default severity applies when header omits it
- Non-SQL files in rule directories are ignored

**Definition of done**: `RulesConfig` parses from YAML. `discover_rules()` finds and parses rule SQL files.

---

### Task 9: Rules engine — execution and reporting

**What**: Execute rule SQL against meta.duckdb, collect violations, report results.

**Files to create**:
- `crates/ff-meta/src/rules.rs` — `execute_rule()`, `execute_all_rules()`, `RuleViolation`, `RuleResult`

**Files to modify**:
- `crates/ff-meta/src/populate/mod.rs` — add `populate_rule_violations()`

**Behavior**:
1. Open `meta.duckdb` (already populated by compile/validate)
2. For each rule file: execute SQL, each returned row is a violation
3. Record violations in `rule_violations` table
4. Return `Vec<RuleResult>` with pass/fail per rule
5. Exit code: non-zero if any `error`-severity rule has violations (when `on_failure: fail`)

**Tests**:
- Rule that matches zero rows → pass
- Rule that matches rows → fail with correct violation count
- Rule severity is respected (warn doesn't cause failure)
- Rule violations are recorded in `rule_violations` table
- Invalid SQL in rule file → graceful error with file path
- Rule results are queryable: `SELECT * FROM meta.rule_violations`

**Definition of done**: Rules execute against meta.duckdb, violations are reported and stored.

---

### Task 10: `ff rules` CLI command

**What**: New CLI command that runs the rules engine.

**Files to create**:
- `crates/ff-cli/src/commands/rules.rs` — `execute_rules()`

**Files to modify**:
- `crates/ff-cli/src/cli.rs` — add `Rules` variant to command enum
- `crates/ff-cli/src/commands/mod.rs` — add `pub mod rules`
- `crates/ff-cli/src/main.rs` — wire `Rules` command

**Behavior**:
1. Load project, compile, populate meta.duckdb (phases 1-2)
2. Discover rule files from `rules.paths` config
3. Execute each rule against meta.duckdb
4. Print results table: rule name, severity, status, violation count
5. For failures: print first N violation rows
6. Exit non-zero if any error-severity rule fails

**Also integrate into `ff validate`**: After built-in validation, run rules if configured.

**Tests**:
- Integration test: `ff rules` with passing rules → exit 0
- Integration test: `ff rules` with failing rules → exit non-zero, violations printed
- Integration test: `ff validate` runs rules when `rules:` is configured
- Integration test: `ff rules` with no `rules:` config → message and exit 0

**Definition of done**: `ff rules` works end-to-end. `ff validate` includes rules when configured.

---

### Task 11: `ff meta query` command

**What**: Ad-hoc SQL query against meta.duckdb from the CLI.

**Files to create**:
- `crates/ff-cli/src/commands/meta.rs` — `execute_meta_query()`

**Files to modify**:
- `crates/ff-cli/src/cli.rs` — add `Meta` variant with `query` subcommand
- `crates/ff-cli/src/commands/mod.rs` — add `pub mod meta`
- `crates/ff-cli/src/main.rs` — wire `Meta` command

**Usage**:
```bash
ff meta query "SELECT name, materialization FROM meta.models"
ff meta query "SELECT * FROM meta.v_lineage WHERE target_model = 'fct_orders'"
```

**Output**: Formatted table (like DuckDB CLI). Support `--output json` for machine consumption.

**Tests**:
- `ff meta query "SELECT 1"` → outputs result
- `ff meta query` against populated project → returns model data
- `--output json` produces valid JSON
- Query against non-existent meta.duckdb → helpful error message

**Definition of done**: Users can query meta.duckdb from CLI with formatted output.

---

### Task 12: Migrate state readers to meta.duckdb

**What**: Replace `StateFile::load()` reads with queries against `meta.model_latest_state`.

**Files to modify**:
- `crates/ff-cli/src/commands/run/state.rs` — `is_model_stale()` queries meta.duckdb instead of state.json
- `crates/ff-cli/src/commands/run/mod.rs` — load state from meta.duckdb

**State.json still written** for this phase (backwards compatibility).

**Tests**:
- Incremental build detects changed models via meta.duckdb
- Incremental build skips unchanged models via meta.duckdb
- Behavior matches existing state.json-based detection

**Definition of done**: `ff run` uses meta.duckdb for incremental detection. Behavior identical to state.json.

---

### Task 13: Migrate manifest readers to meta.duckdb

**What**: Replace `Manifest::load()` reads with meta.duckdb queries.

**Files to modify**:
- `crates/ff-cli/src/commands/run/compile.rs` — cache validation and deferred manifest load from meta.duckdb
- `crates/ff-cli/src/commands/validate.rs` — reference manifest from meta.duckdb
- `crates/ff-cli/src/commands/docs/generate.rs` — lineage data from meta.duckdb

**Manifest.json still written** for this phase.

**Tests**:
- Deferred compilation reads from meta.duckdb
- Docs generation gets lineage from meta.duckdb
- Validate reads reference data from meta.duckdb

**Definition of done**: All manifest readers use meta.duckdb. JSON still written for compatibility.

---

### Task 14: Remove JSON artifacts

**What**: Delete `Manifest`, `StateFile`, and all JSON serialization code. Add `ff meta export --format json` for ecosystem compatibility.

**Files to delete/gut**:
- `crates/ff-core/src/manifest.rs` — remove `Manifest`, `ManifestModel`, `ManifestSource`, `ManifestSourceColumn`
- `crates/ff-core/src/manifest_test.rs` — remove or rewrite
- `crates/ff-core/src/state.rs` — remove `StateFile`, `ModelState`, `ModelStateConfig` (keep `compute_checksum`)

**Files to modify**:
- `crates/ff-cli/src/commands/compile.rs` — remove manifest.json write
- `crates/ff-cli/src/commands/run/mod.rs` — remove state.json write
- `crates/ff-cli/src/commands/meta.rs` — add `export` subcommand
- `crates/ff-core/src/project/mod.rs` — remove `manifest_path()`

**Tests**:
- `ff compile` no longer produces manifest.json
- `ff run` no longer produces state.json
- `ff meta export --format json` produces JSON from meta.duckdb
- All existing integration tests pass with meta.duckdb

**Definition of done**: JSON artifacts removed. `meta.duckdb` is the single source of truth. Export command available.

---

### Task 15: Fixture and sample project updates

**What**: Add rule files to both fixture directories. Update sample projects.

**Files to create**:
- `tests/fixtures/sample_project/rules/` — sample rule SQL files
- `crates/ff-cli/tests/fixtures/sample_project/rules/` — same rules
- `tests/fixtures/sample_project/featherflow.yml` — add `rules:` config
- `crates/ff-cli/tests/fixtures/sample_project/featherflow.yml` — add `rules:` config

**Tests**: Covered by integration tests in other tasks.

**Definition of done**: Both fixture directories have sample rules. `make ci` passes.

---

### Task 16: Add `model_run_input_checksums` table for incremental builds

**What**: Add a table that tracks SHA256 checksums of each upstream model's compiled SQL at execution time. This is critical — the current `ModelState` stores `input_checksums: HashMap<String, String>` (upstream model name → checksum) and `is_model_or_inputs_modified()` in `state.rs` checks every upstream checksum. Without this table, meta.duckdb cannot replace `state.json` for incremental builds.

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — add `model_run_input_checksums` table DDL
- `crates/ff-meta/src/populate/execution.rs` — record upstream checksums alongside `model_run_state`

**Data flow**: After each model executes, for each entry in the model's `depends_on` set, record the upstream model's current compiled SQL checksum. This creates a snapshot of "what my inputs looked like when I last ran."

**Tests**:
- Upstream checksums are recorded after model execution
- Changed upstream checksum detected as modified (compare current vs latest run)
- New upstream dependency (not in prior run) detected as modified
- Removed upstream dependency (in prior run but not current) detected as modified
- Query against `model_latest_state` + `model_run_input_checksums` matches behavior of `StateFile::is_model_or_inputs_modified()` for a 3-model chain (A → B → C)

**Definition of done**: `model_run_input_checksums` is populated during `ff run`. Smart incremental builds can be fully determined from meta.duckdb without `state.json`.

---

### Task 17: Add `model_run_config` table for config change detection

**What**: Snapshot model configuration at execution time. The current `ModelStateConfig` stores materialization, schema, unique_key, incremental_strategy, and on_schema_change — if any change between runs, the model must be re-materialized. Without this table, config drift detection is impossible.

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — add `model_run_config` table DDL
- `crates/ff-meta/src/populate/execution.rs` — record config snapshot alongside `model_run_state`

**Data flow**: After each model executes, insert a row with the model's effective config values at that point in time. The FK references `model_run_state(model_id, run_id)` so it's always associated with a specific execution.

**Tests**:
- Config snapshot recorded after successful model execution
- Config change between runs detected (e.g., materialization `view` → `table`)
- Schema change between runs detected (e.g., `NULL` → `staging`)
- Identical config between runs not flagged as modified
- All `ModelStateConfig` fields round-trip correctly (materialized, schema, unique_key, incremental_strategy, on_schema_change)

**Definition of done**: Config drift detection works via meta.duckdb. Comparing current model config to `model_run_config` from the latest successful run identifies changes.

---

### Task 18: Add `ON DELETE CASCADE` to all child-table FK references

**What**: Every REFERENCES clause in child tables must include `ON DELETE CASCADE`. The "clear-and-repopulate" strategy for phases 1-3 requires deleting all model/source/function data and re-inserting. Without CASCADE, this requires manual deletion in exact reverse-dependency order across 18+ tables — fragile and error-prone.

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — add `ON DELETE CASCADE` to all child-table REFERENCES clauses

**FK references to modify** (23 references across 18 tables):
- `model_config` → `models`
- `model_hooks` → `models`
- `model_tags` → `models`
- `model_meta` → `models`
- `model_columns` → `models`
- `model_column_constraints` → `model_columns`
- `model_column_references` → `model_columns`
- `model_dependencies` → `models` (both FKs)
- `model_external_dependencies` → `models`
- `source_tags` → `sources`
- `source_tables` → `sources`
- `source_columns` → `source_tables`
- `function_args` → `functions`
- `function_return_columns` → `functions`
- `seed_column_types` → `seeds`
- `tests` → `models`, `source_tables`
- `column_lineage` → `models` (both FKs)
- `diagnostics` → `compilation_runs`, `models`
- `schema_mismatches` → `compilation_runs`, `models`
- `rule_violations` → `compilation_runs`
- `model_run_state` → `models`, `compilation_runs`

**Tests**:
- `DELETE FROM meta.models WHERE project_id = ?` cascades to model_columns, model_tags, model_config, model_hooks, model_meta, model_dependencies, model_external_dependencies
- `DELETE FROM meta.model_columns WHERE model_id = ?` cascades to model_column_constraints, model_column_references
- `DELETE FROM meta.sources WHERE project_id = ?` cascades to source_tags, source_tables → source_columns
- `DELETE FROM meta.compilation_runs WHERE run_id = ?` cascades to diagnostics, schema_mismatches, rule_violations, model_run_state
- After cascade delete, query all child tables to verify zero orphan rows
- Full clear-and-repopulate cycle: delete project, re-insert project + all children — verify counts match

**Definition of done**: A single `DELETE FROM meta.models WHERE project_id = ?` cleanly removes all dependent data. No orphan rows after cascade. `make ci` passes.

---

### Task 19: Add CHECK constraints for enum-like VARCHAR columns

**What**: VARCHAR columns with enumerated valid values should have CHECK constraints. This catches typos at insert time (e.g., `"incremntal"` instead of `"incremental"`) and documents valid values directly in the schema.

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — add CHECK constraints to affected columns

**Columns to constrain**:

| Table | Column | Valid values | Nullable? |
|-------|--------|-------------|-----------|
| `models` | `materialization` | `view`, `table`, `incremental` | No |
| `model_hooks` | `hook_type` | `pre_hook`, `post_hook` | No |
| `model_columns` | `classification` | `pii`, `sensitive`, `internal`, `public` | Yes |
| `model_columns` | `nullability_declared` | `not_null`, `nullable` | Yes |
| `model_columns` | `nullability_inferred` | `not_null`, `nullable` | Yes |
| `model_column_constraints` | `constraint_type` | `not_null`, `primary_key`, `unique` | No |
| `functions` | `function_type` | `scalar`, `table` | No |
| `tests` | `test_type` | `not_null`, `unique`, `accepted_values`, `relationships` | No |
| `tests` | `severity` | `error`, `warn` | No |
| `column_lineage` | `lineage_kind` | `copy`, `transform`, `inspect` | No |
| `compilation_runs` | `run_type` | `compile`, `validate`, `run`, `analyze`, `rules` | No |
| `compilation_runs` | `status` | `running`, `success`, `error` | No |
| `model_run_state` | `status` | `success`, `error`, `skipped` | No |
| `schema_mismatches` | `mismatch_type` | `extra_in_sql`, `type_mismatch`, `nullability_mismatch` | No |
| `rule_violations` | `severity` | `error`, `warn` | No |

**Tests**:
- For each constrained column: insert valid value succeeds
- For each constrained column: insert invalid value returns constraint violation error
- NULL accepted for nullable enum columns (classification, nullability_declared, nullability_inferred)
- NULL rejected for non-nullable enum columns

**Definition of done**: All enum-like VARCHAR columns have CHECK constraints. Invalid values are rejected at the database level. `make ci` passes.

---

### Task 20: Expand `meta.projects` with missing config fields

**What**: The current `meta.projects` table omits several `Config` fields that rules need for project-level governance. Add `materialization` (project default), `wap_schema`, and normalized tables for project-level hooks (`on_run_start`/`on_run_end`) and template variables (`vars`).

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — add columns to `projects`, add `project_hooks` and `project_vars` tables
- `crates/ff-meta/src/populate/project.rs` — populate new fields from `Config`

**Missing fields to add to `meta.projects`**:
- `materialization VARCHAR NOT NULL DEFAULT 'view'` — project default materialization
- `wap_schema VARCHAR` — Write-Audit-Publish schema

**New tables**:
- `project_hooks` — normalized `on_run_start`/`on_run_end` SQL statements with ordering
- `project_vars` — normalized template variables with type tracking

**Tests**:
- Project default materialization stored correctly (`view`, `table`, or `incremental`)
- `wap_schema` populated when set in config, NULL when absent
- `on_run_start` hooks stored with correct `ordinal_position` ordering
- `on_run_end` hooks stored with correct `ordinal_position` ordering
- Template `vars` round-trip: string, number, boolean values stored correctly
- Empty `on_run_start`/`on_run_end` → zero rows in `project_hooks` (no error)
- Empty `vars` → zero rows in `project_vars` (no error)
- Rule query works: `SELECT * FROM meta.project_vars WHERE key = 'env'`

**Definition of done**: Rules can query project-level config for governance (e.g., "all staging models must use project default schema"). `make ci` passes.

---

### Task 21: Define rule file column contract

**What**: Document and implement the convention for how the rules engine extracts violation messages and entity names from rule SQL result columns. Without this, rule authors don't know what columns to return.

**Files to modify**:
- `crates/ff-meta/src/rules.rs` — implement column extraction logic in `execute_rule()`
- `docs/meta-database-design.md` — document the contract in the Rules Engine Examples section

**Column contract**:
1. Each returned row = one violation
2. Column named `violation` or `message` → `rule_violations.message`
3. Column named `model_name` or `entity_name` → `rule_violations.entity_name`
4. If no `violation`/`message` column → first VARCHAR column used as message
5. If no VARCHAR column → `"Rule violation (no message column)"` as default
6. All remaining columns → JSON-serialized into `rule_violations.context_json`

**Tests**:
- Rule returning `violation` column → message extracted correctly
- Rule returning `message` column → message extracted correctly
- Rule returning neither → first text column used as message
- Rule returning `model_name` column → entity_name set in violation row
- Rule returning `entity_name` column → entity_name set in violation row
- Rule returning extra columns (e.g., `source_path`, `count`) → stored in `context_json` as valid JSON
- Rule returning zero text columns (`SELECT 1 WHERE false`) → zero violations, no crash
- Rule returning only numeric columns → default message used

**Definition of done**: Column contract is documented. `execute_rule()` implements it. All rule examples in the doc conform to the contract. `make ci` passes.

---

### Task 22: Fix `model_latest_state` view to use `run_id`

**What**: The current view joins on `MAX(started_at)` which can return duplicate rows if two runs share the same timestamp (possible in fast CI or tests). Replace with `MAX(run_id)` which is guaranteed unique by the sequence.

**Files to modify**:
- `crates/ff-meta/src/ddl.rs` — replace `model_latest_state` view definition

**Bug**: Current view uses `cr2.started_at = latest.max_started` — if two `compilation_runs` have the same `started_at` timestamp, the join produces multiple rows per model.

**Fix**: Use a correlated subquery with `ORDER BY run_id DESC LIMIT 1` or `MAX(run_id)`.

**Tests**:
- Insert two `model_run_state` rows for the same model with identical `started_at` timestamps but different `run_id` values → view returns exactly one row (the higher `run_id`)
- View returns exactly one row per model that has successful runs
- View returns zero rows for models that have never run successfully
- View excludes failed runs (`status != 'success'`)
- View excludes non-`run` run types (compile, validate, analyze)

**Definition of done**: `model_latest_state` is deterministic regardless of timestamp collisions. Unit test with same-timestamp runs proves exactly one row per model.

---

### Task 23: Define `compilation_runs` lifecycle and `run_type` semantics

**What**: Document and implement which CLI commands create `compilation_runs` rows, what `run_type` values exist, and the exact sequence for "clear-and-repopulate" (phases 1-3) vs "append" (phase 4). This is the orchestration layer that ties all population functions together.

**Files to modify**:
- `crates/ff-meta/src/populate/mod.rs` — `begin_population(run_type)` and `complete_population(run_id, status)` functions
- `crates/ff-meta/src/ddl.rs` — CHECK constraint on `run_type` and `status`

**Lifecycle per command**:

| Command | `run_type` | Clears phase 1-3? | Appends phase 4? |
|---------|------------|---------------------|-------------------|
| `ff compile` | `compile` | Yes (models, columns, deps) | No |
| `ff validate` | `validate` | Yes (models, columns, deps, diagnostics) | No |
| `ff run` | `run` | Yes (models, columns, deps) | Yes (`model_run_state`) |
| `ff analyze` | `analyze` | Yes (models, columns, deps, diagnostics, lineage) | No |
| `ff rules` | `rules` | No (reads existing data) | No (appends `rule_violations` only) |

**`begin_population(run_type)` sequence**:
1. `BEGIN TRANSACTION`
2. `INSERT INTO meta.compilation_runs` → get `run_id`
3. `DELETE FROM meta.models WHERE project_id = ?` (CASCADE handles children)
4. Return `run_id`

**`complete_population(run_id, status)` sequence**:
1. `UPDATE meta.compilation_runs SET status = ?, completed_at = now() WHERE run_id = ?`
2. `COMMIT`

**Tests**:
- `ff compile` creates `run_type='compile'` row with status transition `running` → `success`
- `ff validate` creates `run_type='validate'` row
- `ff run` creates `run_type='run'` row
- `ff analyze` creates `run_type='analyze'` row
- Clear removes all phase 1-3 data (models, columns, deps, diagnostics, lineage)
- Phase 4 data (`model_run_state`) survives clear (belongs to prior `compilation_runs` rows)
- `compilation_runs` rows themselves accumulate (never deleted — historical log)
- Invalid `run_type` rejected by CHECK constraint
- Failed population sets `status='error'` and `completed_at`

**Definition of done**: Population lifecycle is deterministic and documented. Every command creates exactly one `compilation_runs` row. Clear-and-repopulate is transaction-wrapped. `make ci` passes.

---

### Task 24: Handle `model_meta` serialization and dual-source tag population

**What**: Two issues in the population logic for model metadata:

1. **Meta value serialization**: `ModelSchema.meta` is `HashMap<String, serde_yaml::Value>` — values can be strings, numbers, booleans, lists, or nested maps. The `model_meta` table stores `(key, value VARCHAR)`. Non-string values must be serialized without data loss.

2. **Dual-source tags**: Tags come from `ModelSchema.tags` (YAML file) AND `ModelConfig.tags` (SQL `{{ config(tags=['tag']) }}`). Both must populate `model_tags` with deduplication.

**Files to modify**:
- `crates/ff-meta/src/populate/models.rs` — serialization logic and tag merging

**Meta serialization rules**:
- `serde_yaml::Value::String(s)` → store `s` directly
- `serde_yaml::Value::Number(n)` → store `n.to_string()`
- `serde_yaml::Value::Bool(b)` → store `"true"` or `"false"`
- `serde_yaml::Value::Sequence(_)` → `serde_json::to_string()`
- `serde_yaml::Value::Mapping(_)` → `serde_json::to_string()`
- `serde_yaml::Value::Null` → store `"null"`

**Tests**:
- String meta `{"owner": "data-team"}` → stored as `"data-team"`
- Number meta `{"priority": 42}` → stored as `"42"`
- Boolean meta `{"active": true}` → stored as `"true"`
- List meta `{"reviewers": ["alice", "bob"]}` → stored as `'["alice","bob"]'`
- Nested map meta `{"sla": {"p50": 100}}` → stored as `'{"p50":100}'`
- Null meta `{"deprecated_date": null}` → stored as `"null"`
- YAML tags `["finance", "core"]` appear in `model_tags`
- SQL config tags `["daily"]` appear in `model_tags`
- Duplicate tag `"core"` from both YAML and SQL config → one row in `model_tags`
- Model with no tags from either source → zero rows in `model_tags`

**Definition of done**: All `serde_yaml::Value` variants stored in `model_meta` without data loss. Tags from both sources merged and deduplicated. `make ci` passes.

---

### Task 25: Wire `ff clean` to delete `meta.duckdb`

**What**: `ff clean` should delete `target/meta.duckdb` alongside other target artifacts. The `target/` directory is typically in `clean_targets`, but `meta.duckdb` should be explicitly handled to ensure it's always cleaned regardless of custom `clean_targets` config.

**Files to modify**:
- `crates/ff-cli/src/commands/clean.rs` — explicitly delete `target/meta.duckdb` in addition to configured `clean_targets`

**Tests**:
- `ff clean` removes `target/meta.duckdb` when it exists
- `ff clean` succeeds when `target/meta.duckdb` doesn't exist (no error on missing file)
- `ff clean` still removes all other target artifacts (manifest.json, state.json, compiled SQL)
- `ff compile && ff clean && ls target/` → no `meta.duckdb` file

**Definition of done**: `meta.duckdb` is cleaned up by `ff clean`. No orphan meta database after clean. `make ci` passes.

---

### Task 26: Meta population error handling and transaction boundaries

**What**: Wrap each population phase in a DuckDB transaction. During Phase 2 (dual-write, where JSON and meta.duckdb are both written), meta population failures must be logged as warnings — not fatal errors. JSON files remain the primary output until Phase 4 migrates readers. This prevents a meta.duckdb bug from breaking existing `ff compile`/`ff run` workflows.

**Files to modify**:
- `crates/ff-meta/src/connection.rs` — add `transaction()` / `commit()` / `rollback()` methods
- `crates/ff-meta/src/populate/mod.rs` — wrap each population phase in a transaction
- `crates/ff-cli/src/commands/compile.rs` — catch and warn on meta population errors
- `crates/ff-cli/src/commands/validate.rs` — catch and warn on meta population errors
- `crates/ff-cli/src/commands/run/mod.rs` — catch and warn on meta population errors
- `crates/ff-cli/src/commands/analyze.rs` — catch and warn on meta population errors

**Transaction boundaries**:
- One transaction per population phase (project load, compilation, analysis, execution)
- `compilation_runs` row committed in its own transaction first (must exist for FK references)
- If any INSERT within a phase fails, the entire phase rolls back
- Rolled-back phase does not affect previously committed phases

**Error handling during dual-write** (Phases 2-3):
```rust
match meta_db.populate_project(&project) {
    Ok(()) => {}
    Err(e) => {
        log::warn!("Meta database population failed: {e}. JSON output is unaffected.");
    }
}
```

**Tests**:
- Transaction rollback on INSERT failure leaves database in clean prior state
- Partial population failure (e.g., analysis phase fails) → project/compilation data still committed
- CLI command succeeds even when meta population fails (during dual-write)
- Warning message printed to stderr when meta population fails
- Successful population commits all data across all phases
- After Phase 4 migration: meta population failure becomes a fatal error (no JSON fallback)

**Definition of done**: No meta.duckdb bug can break `ff compile`/`ff run` during dual-write phase. Failed phases roll back cleanly. Successful phases are committed independently. `make ci` passes.

---

### Task dependency graph

```
Phase 1 (foundation):
  Task 1 (DDL) ← no deps
  Task 18 (CASCADE) ← Task 1
  Task 19 (CHECK constraints) ← Task 1
  Task 22 (fix view) ← Task 1
  Task 26 (transactions) ← Task 1

Phase 1 (population):
  Task 2 (project load) ← Task 1, 18, 19, 20, 24
  Task 3 (compilation) ← Task 2
  Task 4 (analysis) ← Task 3
  Task 5 (execution) ← Task 3, 16, 17
  Task 6 (views) ← Task 1, 22
  Task 20 (projects expansion) ← Task 1

Phase 2 (CLI wiring):
  Task 7 (wire into commands) ← Tasks 2-6, 26
  Task 23 (run lifecycle) ← Task 7
  Task 25 (ff clean) ← Task 7

Phase 3 (rules):
  Task 8 (config/discovery) ← no deps (ff-core only)
  Task 9 (execution) ← Tasks 7, 8, 21
  Task 10 (CLI command) ← Task 9
  Task 15 (fixtures) ← Task 10
  Task 21 (column contract) ← Task 9

Phase 4-5 (migration):
  Task 12 (state migration) ← Task 7, 16, 17
  Task 13 (manifest migration) ← Task 7
  Task 11 (meta query CLI) ← Task 7
  Task 14 (remove JSON) ← Tasks 12, 13
```

---

## Data Model (DDL)

All tables live in a `meta` schema inside `target/meta.duckdb`.

**DDL conventions** (applied to all tables below):
- All child-table `REFERENCES` clauses include `ON DELETE CASCADE` (Task 18)
- All VARCHAR columns with enumerated values include `CHECK` constraints (Task 19)
- DuckDB sequences persist across connections — IDs keep incrementing after clear-and-repopulate (e.g., `model_id` 47 after the 5th recompilation is expected, not a bug)
- `ON DELETE CASCADE` shown explicitly only on new tables below; existing tables require the same treatment per Task 18

```sql
-- ============================================================
-- Schema Version Tracking
-- ============================================================

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.schema_version (
    version     INTEGER NOT NULL,
    applied_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- Core: Projects
-- ============================================================

CREATE SEQUENCE meta.seq_project START 1;

CREATE TABLE meta.projects (
    project_id      INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_project'),
    name            VARCHAR NOT NULL UNIQUE,
    version         VARCHAR NOT NULL DEFAULT '1.0.0',
    root_path       VARCHAR NOT NULL,
    schema_name     VARCHAR,
    materialization VARCHAR NOT NULL DEFAULT 'view'
        CHECK (materialization IN ('view', 'table', 'incremental')),
    wap_schema      VARCHAR,
    dialect         VARCHAR NOT NULL DEFAULT 'duckdb',
    db_path         VARCHAR NOT NULL,
    db_name         VARCHAR NOT NULL DEFAULT 'main',
    target_path     VARCHAR NOT NULL DEFAULT 'target',
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- Core: Models
-- ============================================================

CREATE SEQUENCE meta.seq_model START 1;

CREATE TABLE meta.models (
    model_id              INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_model'),
    project_id            INTEGER NOT NULL REFERENCES meta.projects(project_id),
    name                  VARCHAR NOT NULL,
    source_path           VARCHAR NOT NULL,
    compiled_path         VARCHAR,
    materialization       VARCHAR NOT NULL DEFAULT 'view',
    schema_name           VARCHAR,
    description           VARCHAR,
    owner                 VARCHAR,
    deprecated            BOOLEAN NOT NULL DEFAULT false,
    deprecation_message   VARCHAR,
    base_name             VARCHAR,
    version_number        INTEGER,
    contract_enforced     BOOLEAN NOT NULL DEFAULT false,
    raw_sql               VARCHAR NOT NULL,
    compiled_sql          VARCHAR,
    sql_checksum          VARCHAR,
    created_at            TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE TABLE meta.model_config (
    model_id              INTEGER PRIMARY KEY REFERENCES meta.models(model_id),
    unique_key            VARCHAR,
    incremental_strategy  VARCHAR,
    on_schema_change      VARCHAR,
    wap_enabled           BOOLEAN DEFAULT false
);

CREATE SEQUENCE meta.seq_hook START 1;

CREATE TABLE meta.model_hooks (
    hook_id          INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_hook'),
    model_id         INTEGER NOT NULL REFERENCES meta.models(model_id),
    hook_type        VARCHAR NOT NULL,
    sql_text         VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (model_id, hook_type, ordinal_position)
);

CREATE TABLE meta.model_tags (
    model_id    INTEGER NOT NULL REFERENCES meta.models(model_id),
    tag         VARCHAR NOT NULL,
    PRIMARY KEY (model_id, tag)
);

CREATE TABLE meta.model_meta (
    model_id    INTEGER NOT NULL REFERENCES meta.models(model_id),
    key         VARCHAR NOT NULL,
    value       VARCHAR NOT NULL,
    PRIMARY KEY (model_id, key)
);

-- ============================================================
-- Core: Model Columns
-- ============================================================

CREATE SEQUENCE meta.seq_col START 1;

CREATE TABLE meta.model_columns (
    column_id            INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_col'),
    model_id             INTEGER NOT NULL REFERENCES meta.models(model_id),
    name                 VARCHAR NOT NULL,
    declared_type        VARCHAR,
    inferred_type        VARCHAR,
    nullability_declared VARCHAR,
    nullability_inferred VARCHAR,
    description          VARCHAR,
    is_primary_key       BOOLEAN NOT NULL DEFAULT false,
    classification       VARCHAR,
    ordinal_position     INTEGER NOT NULL,
    UNIQUE (model_id, name)
);

CREATE SEQUENCE meta.seq_constraint START 1;

CREATE TABLE meta.model_column_constraints (
    constraint_id   INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_constraint'),
    column_id       INTEGER NOT NULL REFERENCES meta.model_columns(column_id),
    constraint_type VARCHAR NOT NULL,
    UNIQUE (column_id, constraint_type)
);

CREATE TABLE meta.model_column_references (
    column_id              INTEGER PRIMARY KEY REFERENCES meta.model_columns(column_id),
    referenced_model_name  VARCHAR NOT NULL,
    referenced_column_name VARCHAR NOT NULL
);

-- ============================================================
-- Core: Dependencies
-- ============================================================

CREATE TABLE meta.model_dependencies (
    model_id            INTEGER NOT NULL REFERENCES meta.models(model_id),
    depends_on_model_id INTEGER NOT NULL REFERENCES meta.models(model_id),
    PRIMARY KEY (model_id, depends_on_model_id),
    CHECK (model_id != depends_on_model_id)
);

CREATE TABLE meta.model_external_dependencies (
    model_id    INTEGER NOT NULL REFERENCES meta.models(model_id),
    table_name  VARCHAR NOT NULL,
    PRIMARY KEY (model_id, table_name)
);

-- ============================================================
-- Core: Sources
-- ============================================================

CREATE SEQUENCE meta.seq_source START 1;

CREATE TABLE meta.sources (
    source_id     INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_source'),
    project_id    INTEGER NOT NULL REFERENCES meta.projects(project_id),
    name          VARCHAR NOT NULL,
    description   VARCHAR,
    database_name VARCHAR,
    schema_name   VARCHAR NOT NULL,
    owner         VARCHAR,
    UNIQUE (project_id, name)
);

CREATE TABLE meta.source_tags (
    source_id   INTEGER NOT NULL REFERENCES meta.sources(source_id),
    tag         VARCHAR NOT NULL,
    PRIMARY KEY (source_id, tag)
);

CREATE SEQUENCE meta.seq_src_table START 1;

CREATE TABLE meta.source_tables (
    source_table_id INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_src_table'),
    source_id       INTEGER NOT NULL REFERENCES meta.sources(source_id),
    name            VARCHAR NOT NULL,
    identifier      VARCHAR,
    description     VARCHAR,
    UNIQUE (source_id, name)
);

CREATE SEQUENCE meta.seq_src_col START 1;

CREATE TABLE meta.source_columns (
    source_column_id INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_src_col'),
    source_table_id  INTEGER NOT NULL REFERENCES meta.source_tables(source_table_id),
    name             VARCHAR NOT NULL,
    data_type        VARCHAR NOT NULL,
    description      VARCHAR,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (source_table_id, name)
);

-- ============================================================
-- Core: Functions (UDFs)
-- ============================================================

CREATE SEQUENCE meta.seq_func START 1;

CREATE TABLE meta.functions (
    function_id    INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_func'),
    project_id     INTEGER NOT NULL REFERENCES meta.projects(project_id),
    name           VARCHAR NOT NULL,
    function_type  VARCHAR NOT NULL,
    description    VARCHAR,
    sql_body       VARCHAR NOT NULL,
    sql_path       VARCHAR NOT NULL,
    yaml_path      VARCHAR NOT NULL,
    schema_name    VARCHAR,
    deterministic  BOOLEAN NOT NULL DEFAULT true,
    return_type    VARCHAR,
    UNIQUE (project_id, name)
);

CREATE SEQUENCE meta.seq_func_arg START 1;

CREATE TABLE meta.function_args (
    arg_id           INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_func_arg'),
    function_id      INTEGER NOT NULL REFERENCES meta.functions(function_id),
    name             VARCHAR NOT NULL,
    data_type        VARCHAR NOT NULL,
    default_value    VARCHAR,
    description      VARCHAR,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (function_id, name)
);

CREATE SEQUENCE meta.seq_func_ret START 1;

CREATE TABLE meta.function_return_columns (
    return_column_id INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_func_ret'),
    function_id      INTEGER NOT NULL REFERENCES meta.functions(function_id),
    name             VARCHAR NOT NULL,
    data_type        VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (function_id, name)
);

-- ============================================================
-- Core: Seeds
-- ============================================================

CREATE SEQUENCE meta.seq_seed START 1;

CREATE TABLE meta.seeds (
    seed_id       INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_seed'),
    project_id    INTEGER NOT NULL REFERENCES meta.projects(project_id),
    name          VARCHAR NOT NULL,
    path          VARCHAR NOT NULL,
    description   VARCHAR,
    schema_name   VARCHAR,
    delimiter     VARCHAR NOT NULL DEFAULT ',',
    enabled       BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (project_id, name)
);

CREATE TABLE meta.seed_column_types (
    seed_id     INTEGER NOT NULL REFERENCES meta.seeds(seed_id),
    column_name VARCHAR NOT NULL,
    data_type   VARCHAR NOT NULL,
    PRIMARY KEY (seed_id, column_name)
);

-- ============================================================
-- Core: Tests
-- ============================================================

CREATE SEQUENCE meta.seq_test START 1;

CREATE TABLE meta.tests (
    test_id         INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_test'),
    project_id      INTEGER NOT NULL REFERENCES meta.projects(project_id),
    test_type       VARCHAR NOT NULL,
    model_id        INTEGER REFERENCES meta.models(model_id),
    column_name     VARCHAR,
    source_table_id INTEGER REFERENCES meta.source_tables(source_table_id),
    severity        VARCHAR NOT NULL DEFAULT 'error',
    where_clause    VARCHAR,
    config_json     VARCHAR
);

CREATE SEQUENCE meta.seq_singular START 1;

CREATE TABLE meta.singular_tests (
    singular_test_id INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_singular'),
    project_id       INTEGER NOT NULL REFERENCES meta.projects(project_id),
    name             VARCHAR NOT NULL,
    path             VARCHAR NOT NULL,
    sql_text         VARCHAR NOT NULL,
    UNIQUE (project_id, name)
);

-- ============================================================
-- Analysis: Column Lineage
-- ============================================================

CREATE SEQUENCE meta.seq_lineage START 1;

CREATE TABLE meta.column_lineage (
    lineage_id       INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_lineage'),
    target_model_id  INTEGER NOT NULL REFERENCES meta.models(model_id),
    target_column    VARCHAR NOT NULL,
    source_model_id  INTEGER REFERENCES meta.models(model_id),
    source_table     VARCHAR,
    source_column    VARCHAR NOT NULL,
    lineage_kind     VARCHAR NOT NULL,
    is_direct        BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX idx_lineage_target ON meta.column_lineage (target_model_id, target_column);
CREATE INDEX idx_lineage_source ON meta.column_lineage (source_model_id, source_column);

-- ============================================================
-- Analysis: Diagnostics
-- ============================================================

CREATE SEQUENCE meta.seq_run START 1;

CREATE TABLE meta.compilation_runs (
    run_id        INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_run'),
    project_id    INTEGER NOT NULL REFERENCES meta.projects(project_id),
    run_type      VARCHAR NOT NULL,
    started_at    TIMESTAMP NOT NULL DEFAULT now(),
    completed_at  TIMESTAMP,
    status        VARCHAR NOT NULL DEFAULT 'running',
    node_selector VARCHAR
);

CREATE SEQUENCE meta.seq_diag START 1;

CREATE TABLE meta.diagnostics (
    diagnostic_id INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_diag'),
    run_id        INTEGER NOT NULL REFERENCES meta.compilation_runs(run_id),
    code          VARCHAR NOT NULL,
    severity      VARCHAR NOT NULL,
    message       VARCHAR NOT NULL,
    model_id      INTEGER REFERENCES meta.models(model_id),
    column_name   VARCHAR,
    hint          VARCHAR,
    pass_name     VARCHAR NOT NULL
);

CREATE SEQUENCE meta.seq_mismatch START 1;

CREATE TABLE meta.schema_mismatches (
    mismatch_id    INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_mismatch'),
    run_id         INTEGER NOT NULL REFERENCES meta.compilation_runs(run_id),
    model_id       INTEGER NOT NULL REFERENCES meta.models(model_id),
    column_name    VARCHAR NOT NULL,
    mismatch_type  VARCHAR NOT NULL,
    declared_value VARCHAR,
    inferred_value VARCHAR
);

-- ============================================================
-- Analysis: Rule Violations
-- ============================================================

CREATE SEQUENCE meta.seq_violation START 1;

CREATE TABLE meta.rule_violations (
    violation_id  INTEGER PRIMARY KEY DEFAULT nextval('meta.seq_violation'),
    run_id        INTEGER NOT NULL REFERENCES meta.compilation_runs(run_id),
    rule_name     VARCHAR NOT NULL,
    rule_path     VARCHAR NOT NULL,
    severity      VARCHAR NOT NULL,
    entity_name   VARCHAR,
    message       VARCHAR NOT NULL,
    context_json  VARCHAR
);

-- ============================================================
-- Project: Hooks and Variables (Task 20)
-- ============================================================

CREATE TABLE meta.project_hooks (
    project_id       INTEGER NOT NULL REFERENCES meta.projects(project_id) ON DELETE CASCADE,
    hook_type        VARCHAR NOT NULL CHECK (hook_type IN ('on_run_start', 'on_run_end')),
    sql_text         VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    PRIMARY KEY (project_id, hook_type, ordinal_position)
);

CREATE TABLE meta.project_vars (
    project_id INTEGER NOT NULL REFERENCES meta.projects(project_id) ON DELETE CASCADE,
    key        VARCHAR NOT NULL,
    value      VARCHAR NOT NULL,
    value_type VARCHAR NOT NULL DEFAULT 'string',
    PRIMARY KEY (project_id, key)
);

-- ============================================================
-- State: Model Run Tracking
-- ============================================================

CREATE TABLE meta.model_run_state (
    model_id         INTEGER NOT NULL REFERENCES meta.models(model_id),
    run_id           INTEGER NOT NULL REFERENCES meta.compilation_runs(run_id),
    status           VARCHAR NOT NULL CHECK (status IN ('success', 'error', 'skipped')),
    row_count        BIGINT,
    sql_checksum     VARCHAR,
    schema_checksum  VARCHAR,
    duration_ms      BIGINT,
    started_at       TIMESTAMP NOT NULL,
    completed_at     TIMESTAMP,
    PRIMARY KEY (model_id, run_id)
);

-- ============================================================
-- State: Input Checksums for Incremental Builds (Task 16)
-- ============================================================

CREATE TABLE meta.model_run_input_checksums (
    model_id          INTEGER NOT NULL REFERENCES meta.models(model_id) ON DELETE CASCADE,
    run_id            INTEGER NOT NULL REFERENCES meta.compilation_runs(run_id) ON DELETE CASCADE,
    upstream_model_id INTEGER NOT NULL REFERENCES meta.models(model_id) ON DELETE CASCADE,
    checksum          VARCHAR NOT NULL,
    PRIMARY KEY (model_id, run_id, upstream_model_id)
);

-- ============================================================
-- State: Config Snapshot for Drift Detection (Task 17)
-- ============================================================

CREATE TABLE meta.model_run_config (
    model_id              INTEGER NOT NULL,
    run_id                INTEGER NOT NULL,
    materialization       VARCHAR NOT NULL
        CHECK (materialization IN ('view', 'table', 'incremental')),
    schema_name           VARCHAR,
    unique_key            VARCHAR,
    incremental_strategy  VARCHAR,
    on_schema_change      VARCHAR,
    PRIMARY KEY (model_id, run_id),
    FOREIGN KEY (model_id, run_id) REFERENCES meta.model_run_state(model_id, run_id) ON DELETE CASCADE
);

-- ============================================================
-- Views
-- ============================================================

CREATE VIEW meta.model_latest_state AS
SELECT mrs.*
FROM meta.model_run_state mrs
WHERE mrs.run_id = (
    SELECT mrs2.run_id
    FROM meta.model_run_state mrs2
    JOIN meta.compilation_runs cr ON mrs2.run_id = cr.run_id
    WHERE mrs2.model_id = mrs.model_id
      AND mrs2.status = 'success'
      AND cr.run_type = 'run'
    ORDER BY mrs2.run_id DESC
    LIMIT 1
);

CREATE VIEW meta.v_models AS
SELECT
    m.model_id,
    m.name,
    m.materialization,
    m.schema_name,
    m.description,
    m.owner,
    m.deprecated,
    m.contract_enforced,
    m.source_path,
    m.sql_checksum,
    mc.unique_key,
    mc.incremental_strategy,
    mc.on_schema_change,
    mc.wap_enabled,
    list(DISTINCT mt.tag ORDER BY mt.tag) FILTER (WHERE mt.tag IS NOT NULL) AS tags,
    (SELECT COUNT(*) FROM meta.model_dependencies d WHERE d.model_id = m.model_id) AS dependency_count,
    (SELECT COUNT(*) FROM meta.model_dependencies d WHERE d.depends_on_model_id = m.model_id) AS dependent_count
FROM meta.models m
LEFT JOIN meta.model_config mc ON m.model_id = mc.model_id
LEFT JOIN meta.model_tags mt ON m.model_id = mt.model_id
GROUP BY m.model_id, m.name, m.materialization, m.schema_name, m.description,
         m.owner, m.deprecated, m.contract_enforced, m.source_path, m.sql_checksum,
         mc.unique_key, mc.incremental_strategy, mc.on_schema_change, mc.wap_enabled;

CREATE VIEW meta.v_columns AS
SELECT
    mc.column_id,
    m.name AS model_name,
    mc.name AS column_name,
    mc.declared_type,
    mc.inferred_type,
    mc.nullability_declared,
    mc.nullability_inferred,
    mc.description,
    mc.is_primary_key,
    mc.classification,
    mc.ordinal_position
FROM meta.model_columns mc
JOIN meta.models m ON mc.model_id = m.model_id;

CREATE VIEW meta.v_lineage AS
SELECT
    cl.lineage_id,
    tgt.name AS target_model,
    cl.target_column,
    COALESCE(src.name, cl.source_table) AS source_model,
    cl.source_column,
    cl.lineage_kind,
    cl.is_direct,
    tgt_col.classification AS target_classification,
    src_col.classification AS source_classification
FROM meta.column_lineage cl
JOIN meta.models tgt ON cl.target_model_id = tgt.model_id
LEFT JOIN meta.models src ON cl.source_model_id = src.model_id
LEFT JOIN meta.model_columns tgt_col
    ON tgt_col.model_id = cl.target_model_id AND tgt_col.name = cl.target_column
LEFT JOIN meta.model_columns src_col
    ON src_col.model_id = cl.source_model_id AND src_col.name = cl.source_column;

CREATE VIEW meta.v_diagnostics AS
SELECT
    d.code,
    d.severity,
    d.message,
    m.name AS model_name,
    d.column_name,
    d.hint,
    d.pass_name,
    cr.run_type,
    cr.started_at AS run_started_at
FROM meta.diagnostics d
JOIN meta.compilation_runs cr ON d.run_id = cr.run_id
LEFT JOIN meta.models m ON d.model_id = m.model_id;

CREATE VIEW meta.v_source_columns AS
SELECT
    s.name AS source_name,
    s.database_name,
    s.schema_name,
    st.name AS table_name,
    st.identifier AS actual_table_name,
    sc.name AS column_name,
    sc.data_type,
    sc.description
FROM meta.source_columns sc
JOIN meta.source_tables st ON sc.source_table_id = st.source_table_id
JOIN meta.sources s ON st.source_id = s.source_id;
```

---

## Rules Engine Examples

### Rule: all models must have descriptions

```sql
-- rule: models_must_have_descriptions
-- severity: error
-- description: Every model must have a description in its YAML file.

SELECT
    m.name AS model_name,
    m.source_path,
    'Model is missing a description' AS violation
FROM meta.models m
WHERE m.description IS NULL
ORDER BY m.name
```

### Rule: PII columns must have tests

```sql
-- rule: pii_columns_must_be_tested
-- severity: error
-- description: All PII-classified columns must have at least one test.

SELECT
    m.name AS model_name,
    mc.name AS column_name,
    'PII column has no tests defined' AS violation
FROM meta.model_columns mc
JOIN meta.models m ON mc.model_id = m.model_id
LEFT JOIN meta.tests t ON t.model_id = m.model_id AND t.column_name = mc.name
WHERE mc.classification = 'pii'
  AND t.test_id IS NULL
```

### Rule: max fan-in of 5

```sql
-- rule: max_fan_in
-- severity: warn
-- description: Models with more than 5 direct dependencies are hard to maintain.

SELECT
    m.name AS model_name,
    COUNT(*) AS dependency_count,
    'Model has too many direct dependencies (limit: 5)' AS violation
FROM meta.model_dependencies d
JOIN meta.models m ON d.model_id = m.model_id
GROUP BY m.name
HAVING COUNT(*) > 5
```

### Rule: no PII flowing to public columns

```sql
-- rule: no_pii_to_public
-- severity: error
-- description: PII data must not flow into public-classified columns.

SELECT
    source_model,
    source_column,
    target_model,
    target_column,
    'PII data flows into a public column' AS violation
FROM meta.v_lineage
WHERE source_classification = 'pii'
  AND target_classification = 'public'
```

### Recursive upstream lineage query

```sql
WITH RECURSIVE upstream AS (
    SELECT target_model, target_column, source_model, source_column, lineage_kind, 1 AS depth
    FROM meta.v_lineage
    WHERE target_model = 'fct_orders' AND target_column = 'customer_email'

    UNION ALL

    SELECT l.target_model, l.target_column, l.source_model, l.source_column, l.lineage_kind, u.depth + 1
    FROM meta.v_lineage l
    JOIN upstream u ON l.target_model = u.source_model AND l.target_column = u.source_column
    WHERE u.depth < 20
)
SELECT DISTINCT * FROM upstream ORDER BY depth;
```

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Separate file vs same DuckDB | Separate `meta.duckdb` | Clean separation. `ff clean` just deletes it. No risk of meta tables polluting user queries. |
| Historical diagnostics | Append-only with `run_id` FK | Enables trend analysis. Bounded by compilation_runs. |
| Rule file format | Plain SQL with comment headers | Testable directly in DuckDB CLI. No YAML wrapper needed. |
| Schema location | `meta` schema within meta.duckdb | Namespace isolation. All meta tables prefixed `meta.` in queries. |
| Population strategy | Clear-and-repopulate phases 1-3, append-only phase 4 | Current state always reflects current project. Historical execution preserved. |
| Concurrency | Single-writer, batch after parallel execution | DuckDB constraint. Accumulate `Vec<RunResult>` in memory, batch-write after. |
| Schema migration | Version table + numbered migrations | Required for upgrades. Each ff-meta release can add migrations. |
| JSON export | `ff meta export --format json` (Phase 5) | Ecosystem compatibility without constraining primary storage. |
| FK cascades | `ON DELETE CASCADE` on all child tables | Clear-and-repopulate requires single DELETE at parent level. Manual ordering across 18+ tables is fragile. |
| CHECK constraints | All enum-like VARCHAR columns | Catches typos at INSERT time. Documents valid values in schema. DuckDB enforces cheaply. |
| Transaction boundaries | One transaction per population phase | Failed analysis phase doesn't corrupt project/compilation data. Independent commit per phase. |
| Dual-write error handling | Warn, don't fail (Phases 2-3) | Meta.duckdb bugs must never break existing `ff compile`/`ff run` workflows during migration period. |
| model_latest_state | Correlated subquery on `run_id` (not timestamp) | `MAX(started_at)` can produce duplicates on same-timestamp runs. `run_id` is guaranteed unique by sequence. |
| Rule column contract | Convention-based (`violation`, `model_name`) | No schema enforcement on rule SQL output — just conventions. Keeps rule authoring simple. |
| Meta value serialization | JSON for non-string YAML values | `model_meta.value` is VARCHAR. Nested maps/lists serialized as JSON strings. Preserves data without requiring DuckDB JSON type. |
| Tag deduplication | Merge YAML + SQL config tags | Tags come from two sources. Single `model_tags` table with PK dedup. No distinction between YAML and config tags. |
| Sequence IDs after repopulate | IDs keep incrementing (not reset) | DuckDB sequences persist. `model_id` 47 after 5th recompile is expected. Avoids ID reuse which could confuse caches/logs. |
