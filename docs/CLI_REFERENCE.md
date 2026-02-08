# CLI Reference

## Global Options

All commands accept these global flags:

```
ff [OPTIONS] <COMMAND>

Options:
  --project-dir <PATH>    Project root directory (default: current directory)
  --target <TARGET>       Target environment (default: "dev")
  --verbose               Enable verbose output
```

## Commands

### `ff run`

Execute models in dependency order against DuckDB.

```
ff run [OPTIONS]

Options:
  --select <MODELS>       Comma-separated list of models to run
  --exclude <MODELS>      Models to exclude
  --full-refresh          Drop and recreate incremental models
  --fail-fast             Stop on first model failure
  --defer                 Defer unmodified models to production state
  --state <PATH>          Path to production manifest for defer
  --retry-failed          Only retry models that failed in previous run
```

The run command:
1. Loads the project and renders all Jinja templates
2. Parses SQL and extracts dependencies
3. Builds the DAG and topologically sorts
4. Validates all models (no CTEs, no derived tables)
5. Executes in order: `CREATE SCHEMA IF NOT EXISTS` then `CREATE TABLE/VIEW AS`
6. Tracks state in `target/run_state.json` for resume-on-failure

### `ff compile`

Render Jinja templates and validate SQL without executing.

```
ff compile [OPTIONS]

Options:
  --select <MODELS>       Models to compile
  --output <FORMAT>       Output format: text, json (default: text)
```

Outputs the compiled SQL for each model. Validates no CTEs and no derived tables.

### `ff validate`

Full project validation.

```
ff validate [OPTIONS]

Options:
  --select <MODELS>       Models to validate
```

Checks:
- All models parse successfully
- No CTEs (S005) or derived tables (S006)
- All dependencies resolve to known models or external tables
- No circular dependencies
- All `.yml` files present and valid

### `ff test`

Run schema tests defined in `.yml` files.

```
ff test [OPTIONS]

Options:
  --select <MODELS>       Only test these models
  --test-type <TYPE>      Filter by test type (unique, not_null, etc.)
```

### `ff seed`

Load CSV files from the seeds directory into DuckDB tables.

```
ff seed [OPTIONS]

Options:
  --select <SEEDS>        Specific seeds to load
  --full-refresh          Drop and recreate seed tables
```

### `ff clean`

Remove the `target/` directory and all compiled artifacts.

```
ff clean
```

### `ff docs`

Generate JSON documentation for all models.

```
ff docs [OPTIONS]

Options:
  --output <PATH>         Output file path (default: stdout)
```

Produces a JSON document with model metadata, columns, dependencies, and test definitions.

### `ff ls`

List models with optional filtering.

```
ff ls [OPTIONS]

Options:
  --select <MODELS>       dbt-style selector to filter models
  --exclude <MODELS>      Exclude models matching pattern
  --resource-type <TYPE>  Filter by type: model, source, seed, test
  --owner <OWNER>         Filter by owner
  --output <FORMAT>       Output format: table, json, tree, path
```

### `ff lineage`

Column-level lineage analysis.

```
ff lineage [OPTIONS]

Options:
  --model <MODEL>         Target model (required)
  --column <COLUMN>       Specific column to trace
  --direction <DIR>       upstream, downstream, or both (default: both)
  --output <FORMAT>       table, json, dot (default: table)
```

The `dot` output format generates Graphviz DOT for visualization:

```
ff lineage --model fct_orders --output dot | dot -Tpng -o lineage.png
```

### `ff parse`

Parse SQL and show extracted table dependencies.

```
ff parse [OPTIONS]

Options:
  --models <MODELS>       Models to parse
  --dialect <DIALECT>     SQL dialect override
  --output <FORMAT>       json, pretty, deps
```

### `ff analyze`

Run static analysis passes.

```
ff analyze [OPTIONS]

Options:
  --models <MODELS>       Models to analyze (comma-separated)
  --pass <PASSES>         Specific passes to run (comma-separated)
  --severity <LEVEL>      Minimum severity: info, warning, error
  --output <FORMAT>       table, json
```

Available passes: `type_inference`, `nullability`, `join_keys`, `unused_columns`

### `ff diff`

Compare model output between two databases.

```
ff diff [OPTIONS]

Options:
  --model <MODEL>         Model to diff (required)
  --compare <PATH>        Path to comparison database
  --key-columns <COLS>    Columns to use as row key
  --sample-size <N>       Number of sample rows
  --output <FORMAT>       text, json
```

### `ff init`

Scaffold a new Featherflow project.

```
ff init [PATH]
```

Creates the standard directory structure with `featherflow.yml`, `models/`, `seeds/`, and `macros/`.

### `ff snapshot`

Capture table state for slowly-changing dimension tracking.

```
ff snapshot [OPTIONS]

Options:
  --select <SNAPSHOTS>    Specific snapshots to run
```

### `ff source`

Manage external source definitions.

```
ff source <SUBCOMMAND>
```

### `ff run-operation`

Execute a standalone macro that returns SQL.

```
ff run-operation <MACRO_NAME> [OPTIONS]

Options:
  --args <JSON>           Arguments to pass to the macro as JSON
```

### `ff metric`

Work with semantic layer metrics.

```
ff metric [NAME] [OPTIONS]

Options:
  --list                  List all metrics
  --execute               Execute the metric query against the database
  --output <FORMAT>       Output format: text, json
```

### `ff freshness`

Check data freshness for models.

```
ff freshness [OPTIONS]

Options:
  --select <SOURCES>      Sources to check
  --output <FORMAT>       text, json
```
