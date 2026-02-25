# CLI Redesign Implementation Plan

## Overview

Reduce 18 commands to 12. The work is ordered to minimize risk: delete first, then rename, then merge, then build new.

**Guiding principle**: Every phase must leave the project in a compilable, test-passing state. No phase depends on a future phase.

---

## Phase 0: Snapshot Baseline

### Task 0.1: Record current test counts and CI state

**What**: Run `make test` and `make ci-e2e`, record pass counts. This is the regression baseline.

**Definition of Done**:
- [ ] `make test` passes, count recorded (currently ~1048+)
- [ ] `make ci-e2e` passes
- [ ] Commit message: "chore: record baseline test counts before CLI redesign"

---

## Phase 1: Low-Risk Deletions & Renames (No Logic Changes)

These changes only touch `cli.rs`, `main.rs`, `commands/mod.rs`, and test files. No business logic moves.

### Task 1.1: Remove `parse` command

**What**: Delete `parse.rs` and all references. `compile --parse-only` already provides identical functionality.

**Files to change**:
- Delete `crates/ff-cli/src/commands/parse.rs`
- `crates/ff-cli/src/cli.rs`: Remove `Parse(ParseArgs)` variant, `ParseArgs` struct, `ParseOutput` enum
- `crates/ff-cli/src/main.rs`: Remove `parse` import and match arm
- `crates/ff-cli/src/commands/mod.rs`: Remove `pub(crate) mod parse`
- Update any integration tests that invoke `ff parse`

**Tests**:
- [ ] `make test` passes (same count minus any parse-specific tests)
- [ ] `ff compile --parse-only -n <model>` produces equivalent output to what `ff parse` did
- [ ] `ff parse` returns "unknown command" error
- [ ] Clap verification test in `cli_test.rs` still passes

**Definition of Done**: `parse` command gone, `compile --parse-only` is the documented replacement, all tests pass.

---

### Task 1.2: Rename `run-operation` to `run-macro`

**What**: Rename the command and its args struct. The underlying `run_operation.rs` file can be renamed to `run_macro.rs`. No logic changes.

**Files to change**:
- Rename `crates/ff-cli/src/commands/run_operation.rs` -> `run_macro.rs`
- `crates/ff-cli/src/cli.rs`: Rename `RunOperation(RunOperationArgs)` -> `RunMacro(RunMacroArgs)`, rename struct
- `crates/ff-cli/src/main.rs`: Update import and match arm
- `crates/ff-cli/src/commands/mod.rs`: Update module name
- Update integration tests and ci-e2e Makefile targets that reference `run-operation`

**Tests**:
- [ ] `ff run-macro my_macro --args '{}'` works exactly as `ff run-operation` did
- [ ] `ff run-operation` returns "unknown command" error
- [ ] All existing tests pass

**Definition of Done**: Command renamed, all tests pass, no logic changes.

---

### Task 1.3: Remove `rules` command, absorb into `analyze`

**What**: Add `--rules` and `--rules-list` flags to `analyze`. Move rule execution logic into `analyze.rs`. Delete `rules.rs`.

**Files to change**:
- `crates/ff-cli/src/cli.rs`: Remove `Rules(RulesArgs)` variant and `RulesArgs` struct. Add `--rules` (bool) and `--rules-list` (bool) flags to `AnalyzeArgs`
- `crates/ff-cli/src/commands/analyze.rs`: Import and call `ff_meta::rules::execute_rules()` when `--rules` is set. Copy reporting logic from `rules.rs`
- Delete `crates/ff-cli/src/commands/rules.rs`
- `crates/ff-cli/src/main.rs`: Remove `rules` arm
- `crates/ff-cli/src/commands/mod.rs`: Remove `pub(crate) mod rules`
- Update Makefile ci-e2e if it invokes `ff rules`

**Tests**:
- [ ] `ff analyze --rules` produces same output as old `ff rules`
- [ ] `ff analyze --rules --list` produces same output as old `ff rules --list`
- [ ] `ff analyze` (without `--rules`) behaves exactly as before
- [ ] `ff rules` returns "unknown command"
- [ ] All existing tests pass

**Definition of Done**: Rules logic lives in analyze, `rules` command deleted, all tests pass.

---

### Phase 1 Checkpoint

**Run**: `make test && make ci-e2e`
**Expected**: All pass. 3 commands removed/renamed. 15 commands remain.

---

## Phase 2: Merge `validate` into `compile`

This is medium complexity. `validate.rs` has unique logic (duplicate detection, source validation, macro validation, contracts, governance) that compile does not currently perform.

### Task 2.1: Audit validate-only checks and create shared validation module

**What**: Extract the 4 validate-only checks into reusable functions in a new `commands/validation.rs` module (NOT a CLI command, just shared logic).

**Current validate-only checks** (not in compile or run):
1. `validate_duplicates()` — checks for duplicate model names
2. `validate_sources()` — checks source definitions are valid
3. `validate_macros()` — checks custom test macros are valid
4. Contract validation (`--contracts` flag)
5. Governance checks (`--governance` flag)

**Files to change**:
- Create `crates/ff-cli/src/commands/validation.rs` (shared module, not a CLI command)
- Extract the 5 validation functions from `validate.rs` into `validation.rs`
- `validate.rs` calls these extracted functions (no behavior change yet)
- `crates/ff-cli/src/commands/mod.rs`: Add `pub(crate) mod validation`

**Tests**:
- [ ] `ff validate` behaves identically to before
- [ ] `make test` passes with identical counts
- [ ] New `validation.rs` functions have the same signatures as their originals

**Definition of Done**: Validate logic is extracted into a shared module. No behavior changes. All tests pass.

---

### Task 2.2: Add validate checks to `compile` command

**What**: `compile` now runs all validation checks that `validate` does. Add `--strict`, `--contracts`, `--governance` flags to `CompileArgs`.

**Files to change**:
- `crates/ff-cli/src/cli.rs`: Add to `CompileArgs`:
  - `--strict` (bool) — warnings become errors
  - `--contracts` (bool) — validate schema contracts
  - `--state` (Option<String>) — reference manifest for contracts
  - `--governance` (bool) — governance checks
- `crates/ff-cli/src/commands/compile.rs`: After existing compilation, call:
  - `validation::validate_duplicates()`
  - `validation::validate_sources()`
  - `validation::validate_macros()`
  - If `--contracts`: `validation::validate_contracts()`
  - If `--governance`: `validation::validate_governance()`
  - If `--strict`: promote warnings to errors

**Tests**:
- [ ] `ff compile` now catches duplicate model names (previously only `validate` did)
- [ ] `ff compile` now catches invalid source definitions
- [ ] `ff compile` now catches invalid macros
- [ ] `ff compile --contracts --state manifest.json` works like `ff validate --contracts --state manifest.json`
- [ ] `ff compile --governance` works like `ff validate --governance`
- [ ] `ff compile --strict` promotes warnings to errors
- [ ] Port all `validate_test.rs` unit tests to also run against compile
- [ ] Integration tests that ran `ff validate` now pass with `ff compile`

**Definition of Done**: `compile` performs all checks that `validate` does. Both commands still exist (validate removed in next task).

---

### Task 2.3: Remove `validate` command

**What**: Delete `validate.rs` and the CLI command. `compile` is now the single validation entry point.

**Files to change**:
- Delete `crates/ff-cli/src/commands/validate.rs`
- Delete `crates/ff-cli/src/commands/validate_test.rs` (tests already ported to compile in 2.2)
- `crates/ff-cli/src/cli.rs`: Remove `Validate(ValidateArgs)` variant and `ValidateArgs` struct
- `crates/ff-cli/src/main.rs`: Remove `validate` arm
- `crates/ff-cli/src/commands/mod.rs`: Remove `pub(crate) mod validate`
- Update ci-e2e and any integration tests that invoke `ff validate`

**Tests**:
- [ ] `ff validate` returns "unknown command"
- [ ] `ff compile --strict` is the replacement for `ff validate --strict`
- [ ] All tests pass
- [ ] ci-e2e passes (update any `ff validate` calls to `ff compile`)

**Definition of Done**: `validate` command deleted. `compile` is the single "check everything" command. All tests pass.

---

### Phase 2 Checkpoint

**Run**: `make test && make ci-e2e`
**Expected**: All pass. 14 commands remain. `compile` now does everything `validate` did.

---

## Phase 3: Create `deploy` Command (absorbs `seed` + `function`)

### Task 3.1: Create `deploy` command with subcommands

**What**: New `deploy` command with `seeds` and `functions` subcommands. Initially just delegates to existing `seed::execute()` and `function::execute()`.

**Files to change**:
- Create `crates/ff-cli/src/commands/deploy.rs`
- `crates/ff-cli/src/cli.rs`: Add `Deploy(DeployArgs)` with subcommands:
  ```
  ff deploy              # Deploy all (seeds + functions)
  ff deploy seeds        # Delegates to seed logic
  ff deploy functions    # Delegates to function deploy logic
  ff deploy functions --drop [name]
  ff deploy functions --validate
  ff deploy functions --show <name>
  ```
- `crates/ff-cli/src/main.rs`: Add `deploy` arm
- `crates/ff-cli/src/commands/mod.rs`: Add `pub(crate) mod deploy`

**Design decisions**:
- `deploy.rs` imports and calls `seed::execute_inner()` and `function::deploy_inner()` — refactor seed/function to expose non-CLI logic
- `ff deploy` (no subcommand) runs seeds first, then functions (matching build phase ordering)
- `ff deploy functions` inherits all `function deploy` flags
- `ff deploy functions --show <name>` absorbs `function show`
- `ff deploy functions --validate` absorbs `function validate`

**Tests**:
- [ ] `ff deploy seeds` produces identical output to `ff seed`
- [ ] `ff deploy seeds --full-refresh` works
- [ ] `ff deploy seeds --show-columns` works
- [ ] `ff deploy functions` produces identical output to `ff function deploy`
- [ ] `ff deploy functions my_func` works
- [ ] `ff deploy functions --drop` works
- [ ] `ff deploy functions --validate` works
- [ ] `ff deploy functions --show my_func` works
- [ ] `ff deploy` (no args) runs seeds then functions
- [ ] New integration tests for `deploy` command
- [ ] All existing tests pass (old commands still exist temporarily)

**Definition of Done**: `deploy` command works as wrapper. Old `seed` and `function` commands still exist. All tests pass.

---

### Task 3.2: Remove `seed` and `function` commands

**What**: Delete standalone `seed` and `function` commands. `deploy` is the only entry point.

**Files to change**:
- Delete `crates/ff-cli/src/commands/seed.rs` (keep the inner logic as a module or move to deploy)
- Delete `crates/ff-cli/src/commands/function.rs` (keep inner logic)
- Actually: refactor `seed.rs` and `function.rs` to be non-CLI modules (no `execute(args, global)` signature), import from `deploy.rs`
- `crates/ff-cli/src/cli.rs`: Remove `Seed(SeedArgs)`, `Function(FunctionArgs)` variants and their arg structs
- `crates/ff-cli/src/main.rs`: Remove `seed` and `function` match arms
- `crates/ff-cli/src/commands/mod.rs`: Keep modules as internal, remove from public command list
- Update ci-e2e: `ff seed` -> `ff deploy seeds`, `ff function deploy` -> `ff deploy functions`
- Update `function_tests.rs` integration tests
- Update `integration_tests.rs` seed-related tests

**Tests**:
- [ ] `ff seed` returns "unknown command"
- [ ] `ff function` returns "unknown command"
- [ ] All ci-e2e steps pass with new syntax
- [ ] All integration tests pass
- [ ] `function_tests.rs` ported to use `ff deploy functions` syntax

**Definition of Done**: `seed` and `function` commands deleted. `deploy` is the entry point. All tests pass.

---

### Phase 3 Checkpoint

**Run**: `make test && make ci-e2e`
**Expected**: All pass. 12 commands remain: `init`, `compile`, `run`, `deploy`, `ls`, `docs`, `lineage`, `analyze`, `fmt`, `clean`, `run-macro`, `meta`.

---

## Phase 4: Unify `run` (absorbs `build` + `test`)

This is the highest-complexity phase. The run module is ~2,710 lines across 5 submodules.

### Task 4.1: Add `RunConfig` to `featherflow.yml`

**What**: Add a `run` section to the config that defines default run behavior.

**Files to change**:
- `crates/ff-core/src/config.rs`: Add `RunConfig` struct:
  ```rust
  #[derive(Deserialize, Debug, Clone)]
  pub struct RunConfig {
      /// Default run mode: "build" | "models" | "test"
      #[serde(default = "default_run_mode")]
      pub default_mode: RunMode,
  }

  #[derive(Deserialize, Debug, Clone, Default)]
  pub enum RunMode {
      /// Just execute models, no tests
      Models,
      /// Just run tests against existing tables
      Test,
      /// Run model then test in DAG order (default, safest)
      #[default]
      Build,
  }
  ```
- Add `run: Option<RunConfig>` to `Config` struct
- Update fixture `featherflow.yml` files (add `run:` section or use defaults)

**Tests**:
- [ ] Config deserialization works with `run: { default_mode: build }`
- [ ] Config deserialization works with `run: { default_mode: models }`
- [ ] Config deserialization works with `run: { default_mode: test }`
- [ ] Config deserialization works without `run:` section (defaults to `build`)
- [ ] Existing config tests pass (no `deny_unknown_fields` breakage)

**Definition of Done**: `RunConfig` exists in ff-core, deserializes correctly, defaults to `build`. All tests pass.

---

### Task 4.2: Add `--mode` flag and test-related flags to `RunArgs`

**What**: Extend `RunArgs` with mode selection and test-specific flags absorbed from `TestArgs` and `BuildArgs`.

**Files to change**:
- `crates/ff-cli/src/cli.rs`: Add to `RunArgs`:
  ```rust
  /// Run mode: models, test, or build (default from config)
  #[arg(long, value_enum)]
  pub mode: Option<RunMode>,

  /// Store test failure rows to target/test_failures/
  #[arg(long)]
  pub store_failures: bool,

  /// Treat test failures as warnings (exit 0)
  #[arg(long)]
  pub warn_only: bool,
  ```
- Add `RunMode` as a clap `ValueEnum`

**Tests**:
- [ ] `ff run --mode models` parses correctly
- [ ] `ff run --mode test` parses correctly
- [ ] `ff run --mode build` parses correctly
- [ ] `ff run` (no mode) uses config default
- [ ] `ff run --store-failures` parses correctly
- [ ] `ff run --warn-only` parses correctly
- [ ] Clap verification test passes

**Definition of Done**: New flags parse correctly. No behavior changes yet (flags are parsed but not used). All tests pass.

---

### Task 4.3: Implement `--mode models` (current `ff run` behavior)

**What**: When `--mode models` is selected (or resolved from config), `ff run` behaves exactly as it does today. This is a refactor to make the mode explicit in the code path.

**Files to change**:
- `crates/ff-cli/src/commands/run/mod.rs`: Add mode resolution logic at top of `execute()`:
  ```rust
  let mode = args.mode
      .or(project.config.run.as_ref().map(|r| r.default_mode.clone()))
      .unwrap_or(RunMode::Build);
  ```
- Branch on mode:
  - `RunMode::Models` -> existing `execute()` logic unchanged
  - `RunMode::Test` -> to be implemented (Task 4.4)
  - `RunMode::Build` -> to be implemented (Task 4.5)

**Tests**:
- [ ] `ff run --mode models` produces identical output to current `ff run`
- [ ] All existing `run` integration tests pass unchanged
- [ ] `ff run --mode models --full-refresh` works
- [ ] `ff run --mode models --smart` works
- [ ] `ff run --mode models --resume` works

**Definition of Done**: `--mode models` is the explicit path for current run behavior. All existing tests pass without modification.

---

### Task 4.4: Implement `--mode test` (absorbs `ff test`)

**What**: When `--mode test` is selected, `ff run` executes schema tests against existing tables. This reuses logic from `test.rs`.

**Files to change**:
- `crates/ff-cli/src/commands/run/mod.rs`: In the `RunMode::Test` branch, call test execution logic
- Refactor `test.rs` to expose inner logic as `test::execute_tests(project, db, nodes, options)` that doesn't depend on `TestArgs`
- Create `crates/ff-cli/src/commands/run/testing.rs` or reuse `test.rs` internals directly

**Key behavior to preserve**:
- Node selector (`-n`) filters which models' tests run
- `--fail-fast` stops on first test failure
- `--store-failures` writes failing rows
- `--warn-only` treats failures as warnings
- `--threads` controls parallelism
- JSON output format

**Tests**:
- [ ] `ff run --mode test` produces identical output to `ff test`
- [ ] `ff run --mode test -n dim_customers` works (node selection)
- [ ] `ff run --mode test --fail-fast` stops on first failure
- [ ] `ff run --mode test --store-failures` writes failure files
- [ ] `ff run --mode test --warn-only` exits 0 on failures
- [ ] `ff run --mode test --threads 4` runs tests in parallel
- [ ] `ff run --mode test -o json` produces JSON output
- [ ] Port all test-specific integration tests to use `ff run --mode test`

**Definition of Done**: `ff run --mode test` is functionally identical to `ff test`. All test-related integration tests pass with new syntax.

---

### Task 4.5: Implement `--mode build` (absorbs `ff build`)

**What**: When `--mode build` is selected (the default), `ff run` executes the 3-phase build pipeline: deploy seeds/functions -> run each model -> test each model, interleaved in DAG order.

This is the most complex task. The current `build.rs` orchestrates seed, run, and test as separate phases. The new `--mode build` should interleave run+test per model in DAG order for safety (upstream test failure skips downstream models).

**Files to change**:
- `crates/ff-cli/src/commands/run/mod.rs`: In the `RunMode::Build` branch:
  1. Call `deploy` logic (seeds + functions) as pre-step
  2. For each model in DAG order:
     a. Execute the model (existing `run_single_model()`)
     b. Run that model's tests (from `test.rs` logic)
     c. If test fails and `--fail-fast`: skip downstream
  3. Report combined results
- Refactor `build.rs` inner logic into reusable functions or inline into run
- Add `--store-failures` handling in the build path

**Key behavior differences from current `build`**:
- Current `build` runs ALL seeds, then ALL models, then ALL tests (3 serial phases)
- New `--mode build` should interleave: for each model in topo order, run then test
- This is the "pit of success" - bad data doesn't propagate

**Tests**:
- [ ] `ff run` (default, no mode) runs seeds, then interleaved model+test in DAG order
- [ ] `ff run --mode build` explicitly does the same
- [ ] `ff run --full-refresh` drops and recreates in build mode
- [ ] `ff run --fail-fast` skips downstream models when an upstream test fails
- [ ] `ff run --store-failures` stores test failure rows
- [ ] `ff run --threads 4` parallelizes where safe (models with no dependency on each other)
- [ ] `ff run --smart` skips unchanged models (and their tests)
- [ ] Seeds are deployed before any models execute
- [ ] Functions are deployed before any models execute
- [ ] Test failures in build mode are reported alongside model execution results
- [ ] JSON output includes both model results and test results
- [ ] `ff run -n stg_customers --mode build` runs only selected models + their tests
- [ ] Port all `build` integration tests to use `ff run` / `ff run --mode build`
- [ ] ci-e2e updated: `ff build` -> `ff run`

**Definition of Done**: `ff run` defaults to build mode with interleaved model+test execution. All build integration tests pass with new syntax. ci-e2e passes.

---

### Task 4.6: Remove `test` and `build` commands

**What**: Delete standalone `test` and `build` CLI commands. `run` is the single execution entry point.

**Files to change**:
- Refactor `crates/ff-cli/src/commands/test.rs` to be an internal module (not a CLI command). Keep the test execution logic, remove the `execute(args, global)` entry point that depends on `TestArgs`
- Delete `crates/ff-cli/src/commands/build.rs`
- `crates/ff-cli/src/cli.rs`: Remove `Test(TestArgs)`, `Build(BuildArgs)` variants and their arg structs
- `crates/ff-cli/src/main.rs`: Remove `test` and `build` match arms
- `crates/ff-cli/src/commands/mod.rs`: Keep `test` as internal module, remove `build`
- Update all integration tests and ci-e2e

**Tests**:
- [ ] `ff test` returns "unknown command"
- [ ] `ff build` returns "unknown command"
- [ ] All integration tests pass using `ff run --mode <X>` syntax
- [ ] ci-e2e passes entirely

**Definition of Done**: `test` and `build` commands deleted. `run` with modes is the single execution command. All tests pass.

---

### Phase 4 Checkpoint

**Run**: `make test && make ci-e2e`
**Expected**: All pass. **12 commands remain** - this is the target CLI surface.

---

## Phase 5: Enhance `fmt` Selectors

### Task 5.1: Add path/glob/file positional arguments to `fmt`

**What**: Allow `ff fmt` to accept file paths, directory paths, and glob patterns in addition to the existing `-n` node selector.

**Files to change**:
- `crates/ff-cli/src/cli.rs`: Add to `FmtArgs`:
  ```rust
  /// Files or directories to format (positional args)
  #[arg()]
  pub paths: Vec<PathBuf>,

  /// Glob pattern to match SQL files
  #[arg(long)]
  pub glob: Option<String>,
  ```
- `crates/ff-cli/src/commands/fmt.rs`: Add path resolution logic:
  - If `paths` is non-empty: resolve each path (file -> format it, directory -> find .sql files in it)
  - If `--glob` is set: use glob pattern to find matching .sql files
  - If neither: fall back to existing behavior (all project SQL files, optionally filtered by `-n`)

**Tests**:
- [ ] `ff fmt models/staging/stg_customers/stg_customers.sql` formats a single file
- [ ] `ff fmt models/staging/` formats all .sql files in directory
- [ ] `ff fmt --glob "models/**/*.sql"` formats matching files
- [ ] `ff fmt` (no args) formats all project SQL files (existing behavior)
- [ ] `ff fmt -n stg_customers` still works (existing behavior)
- [ ] `ff fmt --check models/staging/` works for CI
- [ ] `ff fmt --diff models/staging/stg_customers/stg_customers.sql` shows diff for single file
- [ ] Nonexistent path produces clear error
- [ ] Path outside project produces clear error

**Definition of Done**: `fmt` supports file paths, directory paths, and glob patterns as input. All existing `fmt` behavior unchanged. New integration tests pass.

---

## Phase 6: Update Documentation and Fixtures

### Task 6.1: Update ci-e2e pipeline

**What**: Ensure the full end-to-end pipeline uses new command syntax.

**Current ci-e2e flow** (from Makefile):
```
ff compile ...
ff seed --full-refresh                    -> ff deploy seeds --full-refresh
ff function deploy --functions ...        -> ff deploy functions --functions ...
ff run --full-refresh                     -> ff run --mode models --full-refresh
ff function deploy --functions ...        -> ff deploy functions --functions ...
ff run --full-refresh                     -> ff run --mode models --full-refresh
ff test -n ...                            -> ff run --mode test -n ...
ff build --full-refresh                   -> ff run --full-refresh
```

**Tests**:
- [ ] `make ci-e2e` passes with new syntax
- [ ] All pipeline steps complete successfully

**Definition of Done**: ci-e2e uses only new command syntax. Full pipeline passes.

---

### Task 6.2: Update CLAUDE.md and project docs

**What**: Update CLAUDE.md commands list, any README references, and inline help text.

**Files to change**:
- `CLAUDE.md`: Update "Commands Implemented" section
- `docs/cli-redesign-analysis.md`: Mark as "implemented"
- Any other docs referencing old commands

**Tests**:
- [ ] `ff --help` shows exactly 12 commands
- [ ] Each command's `--help` is accurate
- [ ] CLAUDE.md reflects current state

**Definition of Done**: All documentation reflects the new 12-command CLI surface.

---

### Task 6.3: Update memory files

**What**: Update auto-memory to reflect the new CLI surface.

**Definition of Done**: Memory files reflect new command structure.

---

## Phase 7: Final Validation

### Task 7.1: Full regression suite

**What**: Run every test target and verify counts.

**Tests**:
- [ ] `make test` passes
- [ ] `make ci-e2e` passes
- [ ] `make test-sa` passes
- [ ] `make test-sa-all` passes
- [ ] Test count is within expected range (some tests removed with deleted commands, new tests added for new functionality)

**Definition of Done**: All test suites green. No regressions.

---

## Summary: Task Dependency Graph

```
Phase 0: Baseline
  └─> 0.1 Record test counts

Phase 1: Low-Risk (parallel-safe)
  ├─> 1.1 Remove parse
  ├─> 1.2 Rename run-operation -> run-macro
  └─> 1.3 Absorb rules into analyze

Phase 2: Merge validate -> compile (sequential)
  └─> 2.1 Extract validation module
      └─> 2.2 Add validate checks to compile
          └─> 2.3 Remove validate command

Phase 3: Create deploy (sequential)
  └─> 3.1 Create deploy command
      └─> 3.2 Remove seed + function commands

Phase 4: Unify run (sequential, high complexity)
  └─> 4.1 Add RunConfig to featherflow.yml
      └─> 4.2 Add mode + test flags to RunArgs
          └─> 4.3 Implement --mode models
              └─> 4.4 Implement --mode test
                  └─> 4.5 Implement --mode build (default)
                      └─> 4.6 Remove test + build commands

Phase 5: Enhance fmt
  └─> 5.1 Add path/glob selectors

Phase 6: Documentation
  ├─> 6.1 Update ci-e2e
  ├─> 6.2 Update docs
  └─> 6.3 Update memory

Phase 7: Final validation
  └─> 7.1 Full regression
```

**Key**: Phases 1-3 can be developed on separate branches and merged independently. Phase 4 depends on Phase 3 (deploy must exist before run can call it in build mode). Phase 5 is independent. Phase 6 depends on all prior phases.

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Breaking CI for users | Pre-1.0 tool; clean break is acceptable |
| `run --mode build` interleaving is complex | Start with current 3-phase build behavior, iterate to true interleaving later |
| `validate` checks missed in `compile` | Task 2.2 explicitly ports each check with dedicated tests |
| `function show`/`list` UX degradation | `ff ls --resource-type function` covers listing; `ff deploy functions --show` covers details |
| Test count decrease | Expected — parse/validate/build/test/seed/function/rules tests are ported, not lost |
| `deny_unknown_fields` config breakage | Task 4.1 adds `run:` as `Option<RunConfig>` so existing configs without it still work |

---

## Estimated Task Sizes

| Task | Size | Notes |
|------|------|-------|
| 0.1 | XS | Script run + record |
| 1.1 | S | Delete + update refs |
| 1.2 | S | Rename + update refs |
| 1.3 | S-M | Move logic + add flags |
| 2.1 | M | Extract + refactor |
| 2.2 | M | Wire up + test porting |
| 2.3 | S | Delete + update refs |
| 3.1 | M | New command + delegate |
| 3.2 | M | Delete + update tests |
| 4.1 | S | Config struct + deser |
| 4.2 | S | Add flags to CLI |
| 4.3 | S | Refactor existing path |
| 4.4 | M | Port test logic |
| 4.5 | L | Build orchestration |
| 4.6 | M | Delete + update tests |
| 5.1 | S-M | Path resolution logic |
| 6.1-6.3 | S | Documentation updates |
| 7.1 | S | Run tests |

**Total**: ~19 tasks, largest being Task 4.5 (build mode orchestration).
