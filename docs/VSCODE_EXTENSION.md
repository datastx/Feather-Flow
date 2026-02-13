# Feather-Flow VS Code Extension — Design Specification

## Overview

A VS Code extension that provides IDE-level intelligence for Feather-Flow SQL projects. The extension wraps the `ff` CLI binary (downloaded from [GitHub Releases](https://github.com/datastx/Feather-Flow/releases)) and uses its structured JSON outputs to power navigation, diagnostics, lineage visualization, and project exploration — without reimplementing any parsing or analysis logic in TypeScript.

### Design Principles

1. **CLI-first** — The `ff` binary is the single source of truth. The extension never parses SQL or YAML itself; it delegates to `ff` commands and consumes their JSON output.
2. **Offline-capable** — No API keys, no cloud services. Everything runs locally.
3. **Incremental** — Start with high-value features (Go to Definition, lineage panel, diagnostics), then expand.
4. **Convention-aware** — The extension understands Feather-Flow's directory-per-model layout (`models/<name>/<name>.sql + .yml`) and uses it for fast file resolution without needing to shell out to `ff` for every navigation.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    VS Code Extension                  │
│                                                       │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │  Providers   │  │  Webview     │  │  Tree Views  │ │
│  │  (Definition │  │  Panels      │  │  (Sidebar)   │ │
│  │   Hover      │  │  (Lineage    │  │              │ │
│  │   Diagnostics│  │   Docs)      │  │              │ │
│  │   Completion)│  │              │  │              │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ │
│         │                 │                 │         │
│  ┌──────▼─────────────────▼─────────────────▼───────┐ │
│  │              Project Index (Cache)                │ │
│  │  - Model map: name → file path                   │ │
│  │  - Source map: name → source YAML                 │ │
│  │  - Dependency graph (upstream/downstream)         │ │
│  │  - Column schemas per model                       │ │
│  │  - Diagnostic results                             │ │
│  └──────────────────────┬───────────────────────────┘ │
│                         │                             │
│  ┌──────────────────────▼───────────────────────────┐ │
│  │              CLI Runner                           │ │
│  │  - Spawns `ff` binary with JSON output flags      │ │
│  │  - Parses stdout, routes stderr to output channel │ │
│  │  - Debounced refresh on file save                 │ │
│  └──────────────────────┬───────────────────────────┘ │
│                         │                             │
└─────────────────────────┼─────────────────────────────┘
                          │
                    ┌─────▼─────┐
                    │  ff binary │  (from GitHub Releases)
                    └───────────┘
```

### Key Components

| Component | Responsibility |
|-----------|---------------|
| **CLI Runner** | Spawns `ff` commands, captures JSON stdout, routes stderr to VS Code output channel. Handles binary discovery, version checking, and auto-download. |
| **Project Index** | In-memory cache populated from `ff ls --output json`, `ff docs --format json`, and `ff analyze --output json`. Refreshed on file save (debounced 500ms). |
| **Definition Provider** | Resolves table names in SQL to model file paths. Uses Project Index for name→path mapping. |
| **Hover Provider** | Shows model metadata, column schemas, and lineage on hover. |
| **Diagnostics Provider** | Maps `ff validate` and `ff analyze` output to VS Code Problems panel. |
| **Lineage Panel** | Webview showing ±N upstream/downstream models for the active file. |
| **Sidebar Tree Views** | Project explorer, upstream/downstream models, column schemas, tests. |

---

## Binary Management

### Discovery Order

1. `featherflow.binaryPath` setting (user override)
2. `./node_modules/.bin/ff` (local project install)
3. `$PATH` lookup for `ff`
4. `~/.featherflow/bin/ff` (extension-managed install)

### Auto-Download

On first activation (or when binary is missing):

1. Detect platform: `darwin-arm64`, `darwin-x86_64`, `linux-x86_64`
2. Fetch latest release from `https://api.github.com/repos/datastx/Feather-Flow/releases/latest`
3. Download the appropriate binary to `~/.featherflow/bin/ff`
4. Verify SHA256 checksum against release assets
5. Set executable permission (`chmod +x`)
6. Show notification: "Feather-Flow v0.1.1 installed"

### Version Checking

On activation and daily thereafter:

1. Run `ff --version` to get current version
2. Check GitHub API for latest release
3. If newer version available, show notification with "Update" action
4. Update replaces binary in `~/.featherflow/bin/ff`

---

## Feature Specifications

### Feature 1: Go to Definition

**The core feature.** Click on a table name in a SQL model's `FROM` or `JOIN` clause and navigate directly to the referenced model's `.sql` file.

#### How It Works

Feather-Flow models reference each other with plain SQL table names (no `ref()`):

```sql
-- In models/dim_customers/dim_customers.sql
SELECT
    m.customer_id,
    m.customer_name,
    c.email
FROM int_customer_metrics m          -- ← Ctrl+Click navigates to
INNER JOIN stg_customers c ON ...    --   models/int_customer_metrics/int_customer_metrics.sql
```

#### Resolution Strategy

The extension uses a **two-tier resolution** approach for speed:

**Tier 1: Fast Path (no CLI call)**

Since Feather-Flow enforces directory-per-model layout, most references can be resolved by simple filesystem convention:

```
Table name: "int_customer_metrics"
  → Look for: <project_root>/models/int_customer_metrics/int_customer_metrics.sql
  → If exists: return as definition location
```

This handles ~90% of cases instantly without spawning the CLI.

**Tier 2: Index Lookup (from cached `ff ls` output)**

For cases where:
- Multiple `model_paths` are configured in `featherflow.yml`
- Schema-qualified references (`staging.stg_orders`)
- Source table references (`raw_customers` → navigate to source YAML)

The extension falls back to the Project Index, which maps every model name and source table to its file path.

#### Implementation

```typescript
// DefinitionProvider registration
vscode.languages.registerDefinitionProvider(
  { language: 'sql', scheme: 'file' },
  new FeatherFlowDefinitionProvider(projectIndex)
);
```

**Token detection**: When the user Ctrl+Clicks or invokes "Go to Definition":

1. Get the word under cursor (VS Code's `getWordRangeAtPosition`)
2. Determine if cursor is inside a `FROM` or `JOIN` clause by scanning backward from cursor position for SQL keywords
3. Strip any schema prefix (e.g., `staging.stg_orders` → `stg_orders`)
4. Strip any alias (the word after the table name, e.g., `stg_orders o` → `stg_orders`)
5. Look up in Project Index

**SQL context detection** (simplified AST-free approach):

```
Patterns that trigger definition lookup:
  FROM <table_name>
  JOIN <table_name>
  INNER JOIN <table_name>
  LEFT JOIN <table_name>
  RIGHT JOIN <table_name>
  FULL JOIN <table_name>
  CROSS JOIN <table_name>
  LEFT OUTER JOIN <table_name>
  RIGHT OUTER JOIN <table_name>
  FULL OUTER JOIN <table_name>
```

Regex for detecting table reference context:
```
/(FROM|JOIN)\s+(\w+\.)?(\w+)/gi
```

**Navigation targets**:

| Reference Type | Target |
|---------------|--------|
| Model name (`stg_orders`) | `models/stg_orders/stg_orders.sql:1:1` |
| Source table (`raw_customers`) | `sources/raw_ecommerce.yml` at the line defining that table |
| Seed (`country_codes`) | `seeds/country_codes.csv:1:1` |
| Function (`safe_divide`) | `functions/safe_divide/safe_divide.sql:1:1` |

#### Edge Cases

| Case | Behavior |
|------|----------|
| Table name matches both model and source | Prefer model, show QuickPick if ambiguous |
| Schema-qualified name (`analytics.dim_customers`) | Strip schema, resolve model name |
| Aliased table (`stg_orders o`) | Resolve `stg_orders`, ignore alias `o` |
| External table (not in project) | No definition found — no action |
| Cursor on alias (`o.customer_id`) | Resolve alias `o` → `stg_orders` → navigate |
| CTE name (shouldn't exist, but just in case) | No navigation (CTEs are banned, but be defensive) |

---

### Feature 2: Lineage Panel

An interactive webview panel that shows the dependency graph centered on the currently active model. Appears in the bottom panel area alongside Terminal and Problems.

#### Default View: ±1 Neighbors

When a `.sql` model file is active, the lineage panel shows:

```
┌─────────────┐     ┌──────────────────────┐     ┌──────────────┐
│ stg_orders  │────▶│ int_orders_enriched   │────▶│  fct_orders  │
└─────────────┘     └──────────────────────┘     └──────────────┘
┌──────────────┐           ▲
│ stg_payments │───────────┘
└──────────────┘

                    [Current Model]
```

- **Center node**: The model in the active editor (highlighted)
- **Left nodes**: Direct upstream dependencies (models this one reads FROM/JOINs)
- **Right nodes**: Direct downstream dependents (models that read from this one)
- **Depth control**: Buttons to expand to ±2, ±3, or full lineage
- **Click to navigate**: Click any node to open that model's `.sql` file

#### Data Source

```bash
# Get full project dependency info
ff ls --output json -p <project_root>
```

The `ff ls --output json` output provides `model_deps` and `external_deps` for every model. The extension builds a bidirectional adjacency list from this and slices it to ±N depth around the active model.

For column-level lineage (future enhancement):
```bash
ff lineage --model <name> --output json -p <project_root>
```

#### Panel Layout

```
┌──────────────────────────────────────────────────────────┐
│ Lineage: dim_customers                    [±1] [±2] [All]│
│                                           [Fit] [Zoom]   │
├──────────────────────────────────────────────────────────┤
│                                                          │
│   ┌───────────────────┐    ┌─────────────────┐           │
│   │ int_customer_     │───▶│ dim_customers   │           │
│   │ metrics           │    │ (table)         │           │
│   │ view · 5 cols     │    │ 8 cols          │           │
│   └───────────────────┘    └─────────────────┘           │
│   ┌───────────────────┐           │                      │
│   │ stg_customers     │───────────┘                      │
│   │ view · 5 cols     │                                  │
│   └───────────────────┘                                  │
│                                                          │
│──────────────────────────────────────────────────────────│
│ Model Details                                            │
│ Name: dim_customers                                      │
│ Materialization: table    Schema: analytics              │
│ Owner: analytics-team     Tags: daily, mart              │
│ Upstream: int_customer_metrics, stg_customers            │
│ Downstream: (none)                                       │
└──────────────────────────────────────────────────────────┘
```

#### Node Display

Each node shows:
- Model name (bold)
- Materialization type badge: `view` `table` `incremental` `ephemeral`
- Column count
- Color coding by layer: staging=blue, intermediate=yellow, mart=green, source=gray

#### Interactions

| Action | Result |
|--------|--------|
| Click node | Open model's `.sql` file in editor |
| Hover node | Show tooltip with description, columns, owner |
| Click ±1/±2/All buttons | Expand/collapse depth |
| Active editor changes to different `.sql` file | Panel re-centers on new model |
| Right-click node | Context menu: Open SQL, Open YAML, Run Model, Compile Model |

#### Rendering Technology

Use **SVG rendered in a VS Code Webview panel** with a lightweight layout library. Options:

| Library | Size | Pros | Cons |
|---------|------|------|------|
| **dagre + d3** | ~150KB | Battle-tested DAG layout, full control | Heavier |
| **elkjs** | ~300KB | Best automatic layout for DAGs | Largest |
| **Custom SVG** | ~5KB | Minimal, fast, no dependencies | Manual layout math |

**Recommendation**: Start with **custom SVG** for the ±1 default view (simple left-to-right layout with max ~20 nodes), upgrade to dagre if users request larger views.

---

### Feature 3: Diagnostics Integration

Map `ff validate` and `ff analyze` output to VS Code's Problems panel for real-time error/warning display.

#### Trigger Points

| Event | Action |
|-------|--------|
| File save (`.sql` or `.yml`) | Run `ff validate --output json` (debounced 1s) |
| Extension activation | Full `ff validate` + `ff analyze` |
| Manual command | "Feather-Flow: Validate Project" |
| Pre-compile | Automatic before compile/run commands |

#### Error Code Mapping

```typescript
const severityMap: Record<string, vscode.DiagnosticSeverity> = {
  // Hard errors — block execution
  'E010': Error,    // Missing schema file
  'E011': Error,    // Invalid model directory (loose .sql)
  'E012': Error,    // Model directory mismatch
  'S005': Error,    // CTE not allowed
  'S006': Error,    // Derived table not allowed
  'SA01': Error,    // Schema mismatch (missing from SQL)
  'E006': Error,    // SQL parse error
  'E007': Error,    // Circular dependency

  // Warnings — should fix but won't block
  'SA02': Warning,  // Extra in SQL / type mismatch / nullability
  'A001': Warning,  // Unknown type
  'A002': Warning,  // UNION type mismatch
  'A004': Warning,  // SUM/AVG on string
  'A010': Warning,  // Nullable from JOIN
  'A030': Warning,  // Join key type mismatch
  'A040': Warning,  // Cross-model inconsistency

  // Info — style/best practice
  'A012': Info,     // Redundant IS NULL
  'A020': Info,     // Unused column
};
```

#### Diagnostic Display

Problems panel entries include:
- **Source**: "Feather-Flow"
- **Code**: Clickable error code (e.g., `S005`) that links to documentation
- **File + Line**: Points to the exact location in the `.sql` or `.yml` file
- **Message**: Human-readable description from `ff` output

#### Inline Decorations

For key errors, show inline hints in the editor:

```sql
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id
--        ^^^^^^^^^^^ A030: Join key type mismatch (INTEGER vs BIGINT)
```

---

### Feature 4: Hover Information

Show rich information when hovering over table names and column references.

#### Table Name Hover

When hovering over a table name in `FROM`/`JOIN`:

```
┌─────────────────────────────────────────────┐
│ int_customer_metrics                        │
│ ─────────────────────────────────────────── │
│ Intermediate model aggregating customer     │
│ order metrics                               │
│                                             │
│ Materialization: view                       │
│ Schema: intermediate                        │
│ Owner: analytics-team                       │
│                                             │
│ Columns:                                    │
│   customer_id    INTEGER   (PK, not null)   │
│   customer_name  VARCHAR                    │
│   total_orders   INTEGER                    │
│   lifetime_value DECIMAL                    │
│   last_order_date DATE                      │
│                                             │
│ Upstream: stg_customers, stg_orders         │
│ Downstream: dim_customers                   │
└─────────────────────────────────────────────┘
```

#### Source Table Hover

When hovering over a source table name:

```
┌─────────────────────────────────────────────┐
│ raw_customers (source: raw_ecommerce)       │
│ ─────────────────────────────────────────── │
│ Raw customer data from production database  │
│                                             │
│ Schema: main                                │
│ Freshness: warn after 2h, error after 6h   │
│                                             │
│ Columns:                                    │
│   customer_id    INTEGER   (PK)             │
│   customer_name  VARCHAR                    │
│   email          VARCHAR                    │
└─────────────────────────────────────────────┘
```

#### Data Source

Hover data comes from the cached `ff docs --format json` output, which includes descriptions, columns, types, tests, and lineage for every model.

---

### Feature 5: Autocomplete

Provide contextual completions for table names and column names.

#### Table Name Completion

Triggered when typing after `FROM` or `JOIN` keywords:

```sql
SELECT ... FROM stg_|
                     ├── stg_customers    (view · staging)
                     ├── stg_orders       (view · staging)
                     ├── stg_payments     (view · staging)
                     └── stg_products     (view · staging)
```

Completion items include:
- Model name as insert text
- Materialization and schema as detail
- Description as documentation
- Icon: model=table, source=database, seed=file

#### Column Name Completion

Triggered when typing after a table alias dot (`o.`):

```sql
SELECT o.|
         ├── order_id       INTEGER   (PK)
         ├── customer_id    INTEGER
         ├── order_date     DATE
         ├── amount         DECIMAL
         └── status         VARCHAR
```

This requires resolving the alias to a model, then looking up columns from the cached schema.

**Alias Resolution**: Scan the current statement for `FROM <table> <alias>` and `JOIN <table> <alias>` patterns. Build an alias→model map for the current query.

---

### Feature 6: Sidebar — Project Explorer

A tree view in the Activity Bar showing the full project structure.

```
FEATHER-FLOW EXPLORER
├── Models (9)
│   ├── Staging
│   │   ├── stg_customers      view   ✓
│   │   ├── stg_orders         view   ✓
│   │   ├── stg_payments       view   ✓
│   │   └── stg_products       view   ✓
│   ├── Intermediate
│   │   ├── int_customer_metrics   view   ✓
│   │   └── int_orders_enriched    view   ✓
│   └── Marts
│       ├── dim_customers      table  ✓
│       ├── dim_products       table  ✓
│       └── fct_orders         table  ✓
├── Sources (1)
│   └── raw_ecommerce
│       ├── raw_orders
│       ├── raw_customers
│       ├── raw_products
│       └── raw_payments
├── Seeds (0)
├── Functions (2)
│   ├── cents_to_dollars   scalar
│   └── safe_divide        scalar
└── Tests
    ├── Schema Tests (24)
    └── Singular Tests (0)
```

**Grouping**: Models grouped by naming convention prefix (`stg_` → Staging, `int_` → Intermediate, `dim_`/`fct_` → Marts). Configurable via setting.

**Status indicators**: Checkmark for valid, warning icon for diagnostics, error icon for failures.

**Click actions**: Click model → open `.sql` file. Click source → open source YAML.

---

### Feature 7: Sidebar — Active Model Context

When a `.sql` model is open in the editor, show contextual information:

```
ACTIVE MODEL: dim_customers
├── Upstream (2)
│   ├── int_customer_metrics   → click to open
│   └── stg_customers          → click to open
├── Downstream (0)
│   └── (no dependents)
├── Columns (8)
│   ├── customer_id      INTEGER    PK, not_null, unique
│   ├── customer_name    VARCHAR    not_null
│   ├── email            VARCHAR    PII
│   ├── signup_date      DATE
│   ├── total_orders     INTEGER    positive
│   ├── lifetime_value   DECIMAL    non_negative
│   ├── last_order_date  DATE
│   └── computed_tier    VARCHAR    accepted_values
├── Tests (12)
│   ├── ✓ unique(customer_id)
│   ├── ✓ not_null(customer_id)
│   ├── ✓ not_null(customer_name)
│   └── ...
└── Config
    ├── Materialization: table
    ├── Schema: analytics
    └── WAP: enabled
```

---

### Feature 8: Command Palette Actions

Register commands accessible via `Ctrl+Shift+P`:

| Command | Description | Shortcut |
|---------|-------------|----------|
| `Feather-Flow: Run Model` | Run the active model | `Ctrl+Shift+R` |
| `Feather-Flow: Run Model + Upstream` | Run with `+model` selection | |
| `Feather-Flow: Run Model + Downstream` | Run with `model+` selection | |
| `Feather-Flow: Compile Model` | Compile the active model | `Ctrl+Shift+C` |
| `Feather-Flow: Validate Project` | Run full validation | |
| `Feather-Flow: Analyze Project` | Run static analysis | |
| `Feather-Flow: Show Lineage` | Open lineage panel for active model | `Ctrl+Shift+L` |
| `Feather-Flow: Run Tests` | Run tests for active model | |
| `Feather-Flow: Open Schema` | Open the `.yml` file for active model | `Ctrl+Shift+Y` |
| `Feather-Flow: Generate Docs` | Run `ff docs` | |
| `Feather-Flow: Seed` | Load seed data | |
| `Feather-Flow: Clean` | Remove target artifacts | |
| `Feather-Flow: Show Compiled SQL` | Preview compiled SQL for active model | |

---

### Feature 9: Code Lenses

Inline actions displayed above model SQL:

```sql
  [Run] [Compile] [Test] [Lineage] [Open Schema]
  {{ config(materialized='table', schema='analytics', wap='true') }}
  SELECT
      m.customer_id,
      ...
```

Code lenses appear at the top of every `.sql` file inside a `models/` directory.

---

### Feature 10: Status Bar

A persistent status bar item showing project state:

```
┌─────────────────────────────────────────────────────────────┐
│  $(database) Feather-Flow v0.1.1 │ 9 models │ 0 errors    │
└─────────────────────────────────────────────────────────────┘
```

Click to show: version info, model count, last validation time, quick actions.

---

## Project Index — Cache Design

The Project Index is the central data store. It is populated from CLI output and refreshed on file changes.

### Refresh Strategy

| Event | Commands Run | Debounce |
|-------|-------------|----------|
| Extension activation | `ff ls --output json` + `ff docs --format json` | None |
| `.sql` file saved | `ff validate` (targeted) + `ff ls --output json` | 1000ms |
| `.yml` file saved | `ff docs --format json` (targeted) + `ff validate` | 1000ms |
| `featherflow.yml` saved | Full refresh: all commands | 2000ms |
| Manual refresh command | Full refresh: all commands | None |

### Cached Data Structures

```typescript
interface ProjectIndex {
  // From ff ls --output json
  models: Map<string, ModelInfo>;       // name → metadata
  sources: Map<string, SourceInfo>;     // name → metadata
  functions: Map<string, FunctionInfo>; // name → metadata

  // Computed from model dependencies
  upstreamMap: Map<string, string[]>;   // name → upstream model names
  downstreamMap: Map<string, string[]>; // name → downstream model names

  // From ff docs --format json
  schemas: Map<string, ColumnInfo[]>;   // name → column definitions

  // From ff validate + ff analyze
  diagnostics: Map<string, Diagnostic[]>; // file path → diagnostics
}

interface ModelInfo {
  name: string;
  path: string;           // Absolute path to .sql file
  yamlPath: string;       // Absolute path to .yml file
  materialization: string;
  schema: string;
  modelDeps: string[];    // Upstream model names
  externalDeps: string[]; // External table names
  tags: string[];
  owner: string;
  description: string;
}

interface ColumnInfo {
  name: string;
  dataType: string;
  description: string;
  isPrimaryKey: boolean;
  tests: string[];
  classification?: string;
  references?: { model: string; column: string };
}
```

---

## Extension Settings

```jsonc
{
  // Path to ff binary (auto-detected if not set)
  "featherflow.binaryPath": "",

  // Project root (auto-detected from featherflow.yml)
  "featherflow.projectRoot": "",

  // Auto-refresh diagnostics on save
  "featherflow.diagnostics.onSave": true,

  // Diagnostic severity for analysis codes
  "featherflow.diagnostics.analysisSeverity": "warning",

  // Default lineage depth
  "featherflow.lineage.defaultDepth": 1,

  // Auto-open lineage panel when switching SQL files
  "featherflow.lineage.autoOpen": false,

  // Model grouping in sidebar (prefix-based or flat)
  "featherflow.explorer.groupBy": "prefix",

  // Custom prefix-to-group mapping
  "featherflow.explorer.prefixGroups": {
    "stg_": "Staging",
    "int_": "Intermediate",
    "dim_": "Marts",
    "fct_": "Marts"
  },

  // Target for run/compile commands
  "featherflow.defaultTarget": "",

  // Additional args for ff run
  "featherflow.run.additionalArgs": ""
}
```

---

## Activation

The extension activates when:
- Workspace contains `**/featherflow.yml` or `**/featherflow.yaml`
- A `.sql` file is opened in a directory containing `featherflow.yml` (ancestor search)

```jsonc
// package.json
{
  "activationEvents": [
    "workspaceContains:**/featherflow.yml",
    "workspaceContains:**/featherflow.yaml"
  ]
}
```

---

## File Associations

Register `.sql` files within Feather-Flow projects for enhanced treatment:

```jsonc
{
  "contributes": {
    "languages": [
      {
        "id": "featherflow-sql",
        "aliases": ["Feather-Flow SQL"],
        "extensions": [".sql"],
        "configuration": "./language-configuration.json"
      }
    ],
    "grammars": [
      {
        "language": "featherflow-sql",
        "scopeName": "source.sql.featherflow",
        "path": "./syntaxes/featherflow-sql.tmLanguage.json",
        "embeddedLanguages": {
          "meta.embedded.block.jinja": "jinja"
        }
      }
    ]
  }
}
```

The grammar extends standard SQL with Jinja template syntax highlighting for `{{ config() }}`, `{{ var() }}`, `{% if %}`, etc.

---

## CLI Runner — Implementation Detail

```typescript
interface CliResult<T> {
  data: T;
  stderr: string;
  exitCode: number;
  durationMs: number;
}

class CliRunner {
  private binaryPath: string;
  private projectRoot: string;
  private outputChannel: vscode.OutputChannel;

  /**
   * Execute an ff command and parse JSON output.
   * Routes stderr to the "Feather-Flow" output channel.
   * Rejects if exit code is non-zero (except for expected codes like 2 for test failures).
   */
  async run<T>(args: string[], options?: { timeout?: number }): Promise<CliResult<T>> {
    const proc = spawn(this.binaryPath, [
      '-p', this.projectRoot,
      ...args
    ]);
    // ... collect stdout, parse as JSON, pipe stderr to output channel
  }

  // Convenience methods
  async ls(): Promise<ModelInfo[]> {
    return this.run<ModelInfo[]>(['ls', '--output', 'json']);
  }

  async docs(): Promise<DocsOutput> {
    return this.run<DocsOutput>(['docs', '--format', 'json']);
  }

  async validate(): Promise<ValidationResult> {
    return this.run<ValidationResult>(['validate']);
  }

  async analyze(models?: string[]): Promise<AnalysisDiagnostic[]> {
    const args = ['analyze', '--output', 'json'];
    if (models) args.push('--models', models.join(','));
    return this.run<AnalysisDiagnostic[]>(args);
  }

  async lineage(model: string, depth?: number): Promise<LineageEdge[]> {
    return this.run<LineageEdge[]>([
      'lineage', '--model', model, '--output', 'json'
    ]);
  }

  async compile(model?: string): Promise<CompileResult> {
    const args = ['compile', '--output', 'json'];
    if (model) args.push('--models', model);
    return this.run<CompileResult>(args);
  }

  async runModel(model: string, options?: RunOptions): Promise<RunResult> {
    const args = ['run', '--output', 'json', '--select', model];
    if (options?.fullRefresh) args.push('--full-refresh');
    if (options?.upstream) args[args.indexOf(model)] = `+${model}`;
    return this.run<RunResult>(args);
  }
}
```

---

## Phase Plan

### Phase 1: Foundation (MVP)

**Goal**: Core navigation and project awareness.

| Feature | Priority | Effort |
|---------|----------|--------|
| Binary management (download, version check) | P0 | M |
| Project detection and activation | P0 | S |
| Project Index from `ff ls --output json` | P0 | M |
| **Go to Definition** for model references | P0 | M |
| Open Schema command (`.sql` → `.yml` toggle) | P0 | S |
| Status bar with project info | P1 | S |
| Basic sidebar: model list | P1 | M |

**Deliverable**: Install extension, open a Feather-Flow project, Ctrl+Click on table names to navigate between models.

### Phase 2: Intelligence

**Goal**: Real-time feedback and rich information.

| Feature | Priority | Effort |
|---------|----------|--------|
| Diagnostics from `ff validate` on save | P0 | M |
| Diagnostics from `ff analyze` | P1 | M |
| **Hover information** for table names | P0 | M |
| Schema cache from `ff docs --format json` | P0 | M |
| Active Model Context sidebar | P1 | M |
| Code lenses (Run, Compile, Test) | P1 | M |

**Deliverable**: See errors in Problems panel, hover for column schemas, run models from editor.

### Phase 3: Lineage

**Goal**: Visual dependency exploration.

| Feature | Priority | Effort |
|---------|----------|--------|
| **Lineage panel** (webview, ±1 default) | P0 | L |
| Depth control (±1, ±2, All) | P0 | M |
| Click-to-navigate from lineage nodes | P0 | S |
| Node tooltips with model metadata | P1 | M |
| Column-level lineage (from `ff lineage`) | P2 | L |

**Deliverable**: Open lineage panel, see upstream/downstream graph, click to navigate.

### Phase 4: Productivity

**Goal**: Full editing workflow.

| Feature | Priority | Effort |
|---------|----------|--------|
| Table name autocomplete | P1 | M |
| Column name autocomplete (after alias dot) | P2 | L |
| Command palette actions (Run, Compile, Test, etc.) | P1 | M |
| Compiled SQL preview | P1 | M |
| Jinja + SQL syntax highlighting | P1 | M |
| Keyboard shortcuts | P2 | S |

---

## Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Extension framework | VS Code Extension API (TypeScript) | Standard |
| Webview (lineage) | Vanilla SVG + TypeScript | Minimal bundle, fast render |
| Build | esbuild | Fast TypeScript bundling |
| Test | Vitest | Fast, TypeScript-native |
| Package manager | pnpm | Fast, disk-efficient |
| CI | GitHub Actions | Same as main repo |

### Project Structure

```
vscode-featherflow/
├── package.json           # Extension manifest
├── tsconfig.json
├── esbuild.config.ts
├── src/
│   ├── extension.ts       # Activation, registration
│   ├── cli/
│   │   ├── runner.ts      # CLI spawning and JSON parsing
│   │   └── binary.ts      # Binary discovery and auto-download
│   ├── index/
│   │   ├── projectIndex.ts    # Central cache
│   │   └── fileWatcher.ts     # File change → refresh
│   ├── providers/
│   │   ├── definition.ts      # Go to Definition
│   │   ├── hover.ts           # Hover tooltips
│   │   ├── completion.ts      # Autocomplete
│   │   ├── diagnostics.ts     # Problems panel
│   │   └── codeLens.ts        # Inline actions
│   ├── views/
│   │   ├── explorer.ts        # Project Explorer tree
│   │   ├── activeModel.ts     # Active Model Context tree
│   │   └── statusBar.ts       # Status bar item
│   ├── panels/
│   │   ├── lineage.ts         # Lineage webview panel
│   │   └── compiledSql.ts     # Compiled SQL preview
│   ├── commands/
│   │   ├── run.ts
│   │   ├── compile.ts
│   │   ├── test.ts
│   │   └── openSchema.ts
│   └── types.ts               # Shared type definitions
├── webview/
│   ├── lineage/
│   │   ├── index.html
│   │   ├── lineage.ts         # SVG rendering
│   │   └── lineage.css
│   └── compiledSql/
│       └── index.html
├── syntaxes/
│   └── featherflow-sql.tmLanguage.json
├── test/
│   ├── unit/
│   └── integration/
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Comparison: dbt Power User vs. Feather-Flow Extension

| Feature | dbt Power User | FF Extension (Planned) |
|---------|---------------|----------------------|
| Go to Definition | Via `ref('model')` regex | Via SQL table names in FROM/JOIN |
| Lineage | Full DAG + column-level (API key) | ±N depth panel, column-level via `ff lineage` |
| Autocomplete | Inside `ref()`, `source()`, `{{ }}` | After FROM/JOIN keywords, after alias dots |
| Diagnostics | Limited (Python bridge) | Full: `ff validate` + `ff analyze` (15+ codes) |
| Hover | Model/macro/source metadata | Model/source metadata + column schemas |
| Run/Compile | Click-to-run with ±model selection | Same, via `ff` CLI |
| Schema tests | Tree view | Tree view + inline decorations |
| Static analysis | None built-in | Full type inference, nullability, join analysis |
| AI features | Yes (API key required) | No (not planned) |
| Compiled SQL preview | Yes | Yes, via `ff compile` |
| Requires Python | Yes (dbt Core) | No — standalone Rust binary |
| API key needed | For advanced features | Never |

### Key Advantage

Feather-Flow's extension can offer **richer diagnostics** than dbt Power User because the `ff` CLI includes a built-in static analysis engine with type inference, nullability propagation, and cross-model schema checking. dbt has no equivalent — its extension relies on an external Python bridge for basic validation.

The tradeoff is that Go to Definition is harder in Feather-Flow (plain SQL table names vs. explicit `ref()` calls), but the directory-per-model convention makes filesystem-based resolution reliable.

---

## Open Questions

1. **Multi-root workspaces**: Should we support multiple Feather-Flow projects in one VS Code workspace? (dbt Power User does.)
2. **Remote development**: Should the binary management work over SSH / Dev Containers?
3. **Separate repo or monorepo?**: Should the extension live in `vscode-featherflow/` at the repo root, or in a separate repository?
4. **Extension naming**: `featherflow` (short) vs. `feather-flow` (matches repo) vs. `ff-vscode`?
5. **Marketplace publisher**: `datastx` org account on VS Code Marketplace?
