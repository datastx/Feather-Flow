# Feather-Flow CLI Redesign Analysis

## Current State: 18 Commands

| # | Command | What It Does |
|---|---------|-------------|
| 1 | `init` | Scaffold new project |
| 2 | `parse` | Parse SQL, output AST/deps |
| 3 | `compile` | Render Jinja, qualify refs, static analysis, write .sql |
| 4 | `run` | Compile internally + execute models in DAG order |
| 5 | `build` | Seed + Run + Test (3-phase atomic pipeline) |
| 6 | `ls` | List models, seeds, sources, functions |
| 7 | `test` | Run schema tests |
| 8 | `seed` | Load CSV seed files |
| 9 | `validate` | Exhaustive pre-flight check (NOT called by run) |
| 10 | `analyze` | DataFusion static analysis passes |
| 11 | `lineage` | Column-level lineage (DataFusion + AST) |
| 12 | `function` | UDF lifecycle (list/deploy/show/validate/drop) |
| 13 | `run-operation` | Execute standalone macro |
| 14 | `docs` | Generate/serve documentation |
| 15 | `clean` | Remove build artifacts |
| 16 | `rules` | SQL rules against meta DB |
| 17 | `meta` | Query/export meta DB |
| 18 | `fmt` | Format SQL with sqlfmt |

---

## Your Proposed Changes (Annotated)

| Your Point | Current | Proposed | Status |
|-----------|---------|----------|--------|
| 1 | `init` | Keep as-is | OK |
| 2 | `parse` | Remove | OK - `compile --parse-only` already covers this |
| 3 | `compile` | Keep as "does your project work?" | OK |
| 4 | `run` | Unify as the execution command; config-driven modes | Needs design (see below) |
| 5 | `ls` | Keep as-is | OK |
| 6a | `test` | Fold into `run --test` | OK - but needs careful design |
| 6b | `seed` | Move to new `deploy` subcommand | Questionable (see below) |
| 7 | `validate` | Roll into `run` (run can't run unless validate passes) | **Partially wrong** (see below) |
| 8 | `docs` | Keep as-is | OK |
| 9 | `clean` | Keep as-is | OK |
| 10 | `lineage` | Keep as-is | OK |
| 11 | `analyze` | Keep but absorb `meta` + `rules` via flags | OK with caveats |
| 12 | `function` | Roll into `deploy` | OK - natural fit |
| 13 | `build` | Roll into `run` | OK - `run` becomes the unified command |
| 14 | `rules` | Roll into `analyze` | OK |
| 15 | `meta` | Roll into `analyze` | Questionable (see below) |
| 16 | `fmt` | Keep, add glob/path/directory selectors | OK |
| 17 | `run-operation` | Rename to `run-macro` | OK |

**You asked if you missed anything.** You covered all 18 commands. Nothing is missing.

---

## Deep Analysis of Each Proposal

### 1. Remove `parse` -- AGREE

`compile --parse-only` already exists and does the same thing but better (it also renders Jinja). The standalone `parse` command is a developer debugging tool that nobody uses in production. Removing it reduces cognitive load with zero capability loss.

**Risk**: None. `compile --parse-only` is a direct replacement.

### 2. `compile` stays as the "dry-run validation" -- AGREE

This is the right call. `compile` should be the "does my project compile?" command - the equivalent of `cargo check` or `tsc --noEmit`. It already does: Jinja rendering, SQL parsing, dependency extraction, DAG building, table qualification, and DataFusion static analysis.

**One thing to consider**: Right now `compile` writes `.sql` files to `target/compiled/`. You may want to add a `--dry-run` mode (or make dry-run the default) that only validates without writing. This aligns with the "compile = check" mental model.

### 3. Unify `run` as the primary execution command -- AGREE WITH CAVEATS

This is the biggest change and the most important to get right. Here's my analysis of your proposed run modes:

#### Proposed Run Modes

You described:
- **(a)** Just run the nodes (current `ff run`)
- **(b)** Just test the nodes (current `ff test`)
- **(c)** Run a node, then test it, in DAG order (current `ff build` behavior)

This maps cleanly to what the industry has converged on. dbt's `build` command (mode c) has become the recommended production default because **it prevents bad data from propagating** - if an upstream model's tests fail, downstream models are skipped.

#### Recommended Implementation

```
ff run                          # Default mode from config (c recommended)
ff run --models-only            # Mode (a): just execute models, no tests
ff run --test-only              # Mode (b): just run tests against existing tables
ff run --test                   # Mode (c): explicit DAG-interleaved run+test
```

Or more concisely, using a mode enum:

```yaml
# featherflow.yml
run:
  default_mode: build    # "build" | "models" | "test"
```

```
ff run                          # Uses config default (build)
ff run --mode models            # Override: just models
ff run --mode test              # Override: just tests
ff run --mode build             # Override: run + test interleaved
```

**My recommendation**: Make `build` (mode c) the default. This is the "pit of success" - the safest behavior is the default behavior. Every major tool (dbt, SDF, SQLMesh) has converged on this pattern.

#### What happens to `test`-specific flags?

Current `test` flags that need to survive:
- `--store-failures` -> `ff run --store-failures` (already on `build`)
- `--warn-only` -> `ff run --warn-only` (new flag on run)
- `--fail-fast` -> already on `run`

Current `build` flags that fold naturally:
- `--store-failures` -> already a `run` concept
- All others are already shared

### 4. `validate` rolled into `run` -- PARTIALLY DISAGREE

**Here's the critical fact**: `validate` is NOT called by `run` today. This is intentional. They do different things:

| Check | `validate` | `run` (internal) |
|-------|-----------|-----------------|
| Jinja syntax | Yes | Yes (during compile) |
| SQL syntax | Yes | Yes (during compile) |
| DAG cycles | Yes | Yes (during compile) |
| Duplicate model names | Yes | No |
| Schema file presence | Yes | Yes (during load) |
| Source definitions valid | Yes | No |
| Custom macro validation | Yes | No |
| DataFusion static analysis | Yes | Yes (as gate) |
| Schema contracts (`--contracts`) | Yes (with flag) | No |
| Governance checks (`--governance`) | Yes (with flag) | No |

`run` already has a static analysis gate that blocks execution on errors. But `validate` does 4 additional checks that `run` doesn't: duplicate detection, source validation, macro validation, and contract/governance checks.

**My recommendation**: You're right that `validate` as a separate command is confusing - users wonder "if run already validates, why do I need validate?" But you should **absorb validate's extra checks into `compile`** rather than `run`:

- `compile` = "does my project compile AND is it valid?" (like `cargo check`)
- `run` = "compile + execute" (like `cargo run`)

This way:
- `ff compile` catches everything `validate` currently catches
- `ff run` doesn't need a separate validate step because compile already ran
- `--contracts` and `--governance` become `compile` flags
- `--strict` becomes a `compile` flag

**Why not just fold it into `run`?** Because `compile` should be the fast, safe "check everything" command. If validate logic is only in `run`, there's no way to dry-run validate without actually executing.

### 5. `seed` + `function deploy` -> new `deploy` command -- MIXED FEELINGS

You're right that seeds and functions have a "deploy these resources" lifecycle that's different from "run these transformations." But let me push back a little:

**The case for `deploy`:**
- Seeds and functions are "deployed" resources, not "transformations"
- Both need to exist before models run
- Both have a `--full-refresh` / drop-and-recreate pattern
- Grouping them makes conceptual sense

**The case against `deploy`:**
- dbt solved this by making `build` automatically handle seeds before models. Users don't think about "deploying seeds" - they think about "loading data."
- If `run` (in mode c / build mode) handles seeds automatically, why do you need a separate `deploy` command?
- The `function` lifecycle (list/show/validate/drop) has more operations than just "deploy"

**My recommendation**: Keep `deploy` but be clear about what it is:

```
ff deploy seeds                 # Load CSV seeds
ff deploy seeds --full-refresh  # Drop + reload
ff deploy functions             # Deploy UDFs
ff deploy functions --drop      # Drop UDFs
ff deploy functions my_func     # Deploy specific function
ff deploy                       # Deploy all (seeds + functions)
```

And have `ff run` (in build mode) call `deploy` internally before execution, so most users never need to think about it.

**What about `function list`, `function show`, `function validate`?** These are inspection commands. They should survive somewhere. Options:
- `ff ls --resource-type function` already handles listing
- `ff deploy functions --validate` for validation
- `ff deploy functions --show my_func` for showing details

Or keep `function` as a sub-namespace of `deploy`:
```
ff deploy function list
ff deploy function show my_func
ff deploy function validate
```

This gets verbose. A simpler approach: `ff ls` already supports `--resource-type function`. For `show`, you could add `ff ls --resource-type function --verbose` or just keep `ff function show` as a convenience alias.

### 6. `rules` + `meta` rolled into `analyze` -- AGREE FOR RULES, DISAGREE FOR META

**Rules -> Analyze**: This is a natural fit. Rules are a form of analysis - they analyze the meta database for governance violations. Implementation:

```
ff analyze                      # DataFusion static analysis (current behavior)
ff analyze --rules              # Also run SQL rules
ff analyze --rules --list       # List rules without executing
```

**Meta -> Analyze**: This is a poor fit. `meta` is a **query tool**, not an analysis tool. `ff meta query "SELECT * FROM ff_meta.runs"` is ad-hoc SQL against the meta database. Rolling it into `analyze` would be like putting `psql` inside `pytest`.

**My recommendation**: Keep `meta` as a separate command. It's a power-user inspection tool with a small surface:

```
ff meta query "SELECT ..."      # Ad-hoc SQL
ff meta export --output file.json
ff meta tables                  # List tables + row counts
```

If you truly want to reduce commands, rename it to something that signals "this is advanced/optional": `ff inspect` or keep it as `ff meta`.

### 7. `run-operation` -> `run-macro` -- AGREE

The rename makes the command self-documenting. `run-operation` is inherited from dbt terminology that most people don't understand. `run-macro` says exactly what it does.

```
ff run-macro my_macro --args '{"key": "value"}'
```

### 8. `fmt` with expanded selectors -- AGREE

Adding glob/path/directory selectors to `fmt` is a good idea. Currently `fmt` uses the node selector (`-n`), which requires knowing model names. For a formatting tool, file-based selection is more natural:

```
ff fmt                          # Format all SQL files
ff fmt --nodes stg_customers    # Format by model name (existing)
ff fmt --path models/staging/   # Format by directory (new)
ff fmt --glob "models/**/*.sql" # Format by glob (new)
ff fmt models/staging/stg_customers/stg_customers.sql  # Format specific file (new)
ff fmt --check                  # CI gate (existing)
```

---

## Proposed New CLI Surface

### Primary Commands (what you show in `ff --help`)

```
USAGE: ff [OPTIONS] <COMMAND>

Commands:
  init        Initialize a new Featherflow project
  compile     Compile and validate the project (dry-run)
  run         Execute models against the database
  deploy      Deploy seeds and functions
  ls          List project resources
  docs        Generate and serve documentation
  lineage     Show column-level lineage
  analyze     Static analysis, rules, and diagnostics
  fmt         Format SQL source files
  clean       Remove generated artifacts
  run-macro   Execute a standalone SQL macro
  meta        Query the meta database
```

**12 commands** (down from 18). More importantly, **3 commands cover 90% of usage**: `compile`, `run`, `deploy`.

### Command Details

#### `ff compile` (absorbs `validate` + `parse`)
```
ff compile                      # Full compile + validate
ff compile -n stg_customers     # Specific models
ff compile --parse-only         # Parse without writing (replaces ff parse)
ff compile --explain my_model   # Show DataFusion LogicalPlan
ff compile --contracts          # Validate schema contracts (from validate)
ff compile --governance         # Governance checks (from validate)
ff compile --strict             # Warnings become errors (from validate)
ff compile --skip-static-analysis
ff compile -o json              # CI output format
```

#### `ff run` (absorbs `build` + `test` + `validate` logic)
```
ff run                          # Default mode from config (build recommended)
ff run --mode models            # Just execute models, no tests
ff run --mode test              # Just run tests
ff run --mode build             # Run + test interleaved in DAG order (default)
ff run -n stg_customers         # Specific models
ff run --full-refresh           # Drop and recreate
ff run --fail-fast              # Stop on first failure
ff run --threads 4              # Parallel execution
ff run --smart                  # Skip unchanged models
ff run --resume                 # Resume from failed run
ff run --store-failures         # Save failing test rows
ff run --warn-only              # Test failures are warnings
ff run --skip-static-analysis   # Bypass DataFusion checks
ff run -o json -q               # CI mode
```

Config-driven default:
```yaml
# featherflow.yml
run:
  default_mode: build    # "build" | "models" | "test"
```

#### `ff deploy` (absorbs `seed` + `function deploy/drop`)
```
ff deploy                       # Deploy all seeds + functions
ff deploy seeds                 # Seeds only
ff deploy seeds --full-refresh  # Drop + reload seeds
ff deploy seeds --show-columns  # Preview schema
ff deploy functions             # Functions only
ff deploy functions my_func     # Specific function
ff deploy functions --drop      # Drop all functions
ff deploy functions --drop my_func  # Drop specific function
ff deploy functions --validate  # Validate without deploying
```

#### `ff analyze` (absorbs `rules`)
```
ff analyze                      # DataFusion static analysis
ff analyze -n stg_customers     # Specific models
ff analyze --pass type_inference,nullability  # Specific passes
ff analyze --rules              # Also run SQL rules
ff analyze --rules --list       # List rules without running
ff analyze --severity warning   # Filter by severity
ff analyze -o json              # JSON output
```

#### `ff meta` (stays independent)
```
ff meta query "SELECT ..."      # Ad-hoc SQL against meta DB
ff meta export -o results.json  # Export meta DB
ff meta tables                  # List tables + row counts
```

#### `ff fmt` (expanded selectors)
```
ff fmt                          # Format all
ff fmt -n stg_customers         # By model name
ff fmt --path models/staging/   # By directory
ff fmt path/to/file.sql         # Specific file
ff fmt --check                  # CI gate
ff fmt --diff                   # Show changes
ff fmt --line-length 120        # Override config
```

### Commands Removed (6)

| Removed | Absorbed Into | How |
|---------|--------------|-----|
| `parse` | `compile --parse-only` | Already exists |
| `validate` | `compile` | Compile gains `--contracts`, `--governance`, `--strict` |
| `test` | `run --mode test` | Test is now a run mode |
| `build` | `run --mode build` (default) | Build behavior is default run |
| `seed` | `deploy seeds` | New deploy command |
| `function` | `deploy functions` + `ls --resource-type function` | Deploy + list |
| `rules` | `analyze --rules` | Rules are analysis |

---

## Does This Make Sense? Critical Evaluation

### What's Good About This Redesign

1. **"Pit of success" default**: `ff run` defaults to build mode (run+test interleaved). Users fall into the safest behavior without knowing it. This is what dbt, SDF, and SQLMesh all converged on.

2. **Three commands cover 90% of use**: `compile` (check), `run` (execute), `deploy` (setup). This matches Terraform's 5-command core and SQLMesh's 2-command core.

3. **Compile = check**: Making `compile` absorb `validate` creates a single "is my project valid?" command. This is the `cargo check` / `tsc --noEmit` mental model that developers already understand.

4. **Config-driven defaults**: The `run.default_mode` config means users set it once and forget. Fewer flags to remember.

5. **Progressive disclosure**: 12 commands with clear grouping. Power users can discover `meta`, `analyze`, `run-macro`. New users learn `compile` -> `run`.

### What's Risky

1. **`run --mode` complexity**: Having modes adds a concept users need to learn. An alternative is to just make `run` always do build mode and add `--skip-tests` for the rare case someone doesn't want tests. Simpler mental model: `run` = run everything, `--skip-tests` if you're iterating.

2. **`deploy` as a new concept**: dbt users expect `seed` as a top-level command. Introducing `deploy` is a new abstraction they need to learn. However, since Feather-Flow isn't trying to be dbt-compatible, this is fine as long as it's well-documented.

3. **Losing `function show`/`function list`**: These are handy for debugging UDFs. Make sure `ff ls --resource-type function` is a full replacement for `function list`, and consider `ff deploy functions --show my_func` or similar for the show case.

4. **`validate` disappearing**: Some teams have CI pipelines that run `ff validate` as a PR check without running models. Make sure `ff compile --strict` covers this use case completely. Since compile already does everything validate does (or will, after this change), this should be fine.

5. **Meta staying separate**: If the goal is to minimize commands, `meta` could become subcommands of `analyze`. But I'd recommend keeping it separate - it's a genuinely different operation (ad-hoc querying vs. automated analysis).

### Alternative: Even More Aggressive Reduction

If you wanted to go to the absolute minimum (a la SQLMesh's 2-command core), you could:

```
ff check                        # compile + validate + analyze + rules
ff run                          # deploy + run + test (all-in-one)
ff fmt                          # format
```

Plus utilities: `init`, `ls`, `docs`, `lineage`, `clean`, `meta`, `run-macro`.

**9 total commands.** But this may be too aggressive - losing `compile` as separate from `run` removes the ability to dry-run check without a database connection.

---

## Impact Assessment

### Breaking Changes

This is a **major breaking change** to the CLI surface. Every user who has:
- CI scripts calling `ff test`, `ff validate`, `ff build`, `ff seed`, `ff function deploy`, `ff parse`, `ff rules`
- Documentation referencing these commands
- Muscle memory for these commands

...will need to update.

### Migration Path

Recommend a deprecation period:
1. **Phase 1**: Add new commands alongside old ones. Old commands print deprecation warnings.
2. **Phase 2**: Remove old commands after N releases.

Or, since this is pre-1.0, just rip the band-aid off with a clear migration guide.

### Implementation Complexity

| Change | Complexity | Notes |
|--------|-----------|-------|
| Remove `parse` | Low | Just delete the command, already covered by compile |
| Absorb `validate` into `compile` | Medium | Move validate checks into compile.rs |
| Absorb `build` + `test` into `run` | High | Refactor run to support modes, move test/build logic |
| Create `deploy` | Medium | New command that wraps seed + function deploy |
| Absorb `rules` into `analyze` | Low | Add flag, call rules logic from analyze |
| Rename `run-operation` to `run-macro` | Low | Rename only |
| Add selectors to `fmt` | Low | Add path/glob args |

**Total estimated effort**: Medium-high. The `run` unification is the hardest part because `build` has complex 3-phase orchestration with interleaved test execution.

---

## Recommendation

**Do it.** The redesign is well-reasoned and aligns with industry trends. Specific recommendations:

1. Make `ff run` default to build mode (run+test interleaved). Use `--skip-tests` instead of `--mode models` for simplicity.
2. Absorb `validate` into `compile`, not `run`. `compile` = "check everything", `run` = "execute everything".
3. Keep `meta` separate from `analyze`. They serve different purposes.
4. The `deploy` command is a good idea. Seeds and functions are "deployed resources" with a different lifecycle than transformation models.
5. Rename `run-operation` to `run-macro`. Self-documenting is always better.
6. Since this is pre-1.0, just make the breaking changes. Don't waste time on deprecation shims.

### Final Command Count: 12

```
ff init         # Setup
ff compile      # Check (absorbs validate, parse)
ff run          # Execute (absorbs build, test)
ff deploy       # Deploy resources (absorbs seed, function)
ff ls           # List
ff docs         # Documentation
ff lineage      # Lineage
ff analyze      # Analysis (absorbs rules)
ff fmt          # Format
ff clean        # Cleanup
ff run-macro    # Ad-hoc macros (renamed from run-operation)
ff meta         # Meta DB queries
```

Down from 18 to 12. More importantly, the **learning curve drops from "which of these 18 commands do I need?" to "compile, run, done."**
