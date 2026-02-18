# Plan: Consolidate `parse`, `validate`, `analyze` into `compile`

## Summary

Remove the `parse`, `validate`, and `analyze` commands as standalone subcommands.
Fold their behavior into `compile` via flags:

| Old command | New equivalent |
|---|---|
| `ff parse` | `ff compile --dry-run` (no file writes, just parse + deps) |
| `ff validate` | `ff compile --validate` (all validation checks) |
| `ff analyze --pass X --severity Y` | `ff compile --analyze [--pass X] [--severity Y]` |
| `ff compile` (unchanged) | `ff compile` (render + static analysis + write files) |
| `ff compile --parse-only` (existing) | `ff compile --dry-run` (renamed for clarity) |

The existing `--parse-only` flag gets renamed to `--dry-run` for consistency with other CLI tools.

## New `CompileArgs` flags

```
--dry-run           Parse and validate SQL only, don't write output files (replaces --parse-only and the old `parse` command)
--validate          Run full project validation (schemas, DAG cycles, duplicates, macros, governance, contracts, rules)
--strict            With --validate: treat warnings as errors
--contracts         With --validate: validate schema contracts
--state <FILE>      With --validate --contracts: reference manifest for contract comparison
--governance        With --validate: enable data governance checks
--analyze           Run deep static analysis passes (replaces `analyze` command)
--pass <PASSES>     With --analyze: comma-separated pass names to run
--severity <LEVEL>  With --analyze: minimum severity to display (info/warning/error)
```

Flags compose naturally:
- `ff compile --validate` → validate + write compiled files
- `ff compile --validate --dry-run` → validate only, no writes
- `ff compile --analyze --dry-run` → analyze only, no writes
- `ff compile --validate --analyze` → full validation + deep analysis + write files

## Implementation Steps

### 1. Update `cli.rs` — Modify `CompileArgs`, remove old args structs

- Add `--dry-run`, `--validate`, `--strict`, `--contracts`, `--state`, `--governance`, `--analyze`, `--pass`, `--severity` to `CompileArgs`
- Keep `--parse-only` as a hidden alias for `--dry-run` (backward compat)
- Remove `ParseArgs`, `ParseOutput`, `ValidateArgs`, `AnalyzeArgs`, `AnalyzeOutput`, `AnalyzeSeverity`
- Remove `Commands::Parse`, `Commands::Validate`, `Commands::Analyze` variants

### 2. Update `compile.rs` — Integrate validate + analyze logic

- When `--validate` is set: run the validation checks from `validate.rs` (SQL syntax, jinja vars, DAG cycles, duplicates, schemas, sources, macros, contracts, governance, rules)
- When `--analyze` is set: run the deep analysis passes from `analyze.rs`
- When `--dry-run` is set: skip file writes (current `--parse-only` behavior)
- Preserve existing compile behavior as the default

### 3. Remove old command files

- Delete `commands/parse.rs`
- Delete `commands/analyze.rs`
- Move validation logic from `commands/validate.rs` into `compile.rs` (or keep as a private helper module)
- Keep `validate_test.rs` — move tests into compile or a shared test file

### 4. Update `main.rs` and `commands/mod.rs`

- Remove parse, validate, analyze from dispatch and module declarations

### 5. Update `build.rs`

- Check if `build` command references any of the removed commands (it doesn't — it calls `run` and `test` directly)

### 6. Tests

- Run `make test` to ensure nothing breaks
- Run `make lint` for clippy/fmt
- The `cli_test.rs` test (`verify_cli_args`) will catch any clap definition issues
