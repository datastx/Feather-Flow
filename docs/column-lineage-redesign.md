# Column-Level Lineage Redesign

## Industry Research

### dbt Fusion (SDF Labs Engine)

dbt Fusion's column-level lineage is powered by an acquisition of SDF Labs — a Rust-based SQL compiler that builds an internal representation (IR) / logical plan for every query. Key characteristics:

- **AOT (Ahead-of-Time) rendering**: compiles and validates ALL models before any warehouse execution, so lineage is available for the entire project before anything runs
- **No database connection required**: static analysis works locally
- **Schema propagation across DAG**: the engine knows what columns exist in every model, understands function type signatures, and propagates types in topological order

**Lineage classification (3 types):**

| Type          | Meaning                                                                         |
| ------------- | ------------------------------------------------------------------------------- |
| **Copy**      | Column passed through without modification                                      |
| **Transform** | Column modified via operations (aggregations, expressions)                      |
| **Inspect**   | Column referenced/examined but lineage is ambiguous (JSON unpacking, complex ops)|

**Column evolution (secondary classification):**

| Type            | Meaning                                                         |
| --------------- | --------------------------------------------------------------- |
| **Passthrough** | Reused without change — auto-inherits upstream descriptions     |
| **Rename**      | Reused with new name — still inherits descriptions              |
| **Transformed** | Modified — breaks inheritance chain, needs new docs             |

**Limitations**: only tracks SELECT statements (not WHERE/JOIN usage), can't parse Python models, introspective models (`run_query()`) fall back to JIT. Enterprise-only.

### SQLMesh / sqlglot (Tobiko Data)

Uses sqlglot, an open-source Python SQL parser/transpiler (20+ dialects) with a built-in optimizer that powers lineage.

**Core API**: `lineage(column, sql, schema, sources, dialect)` — returns a tree of `Node` objects representing the lineage chain. The optimizer qualifies columns (resolves which table each unqualified column belongs to) before tracing.

**Critical requirement**: schema metadata MUST be externally provided for all root tables. Without schemas, unqualified column references (`SELECT id FROM t`) cannot be resolved correctly.

**Handles complex SQL**: UNION (positional column matching), PIVOTs, UDTFs, `SELECT *` expansion. (CTE/recursive CTE support is irrelevant to Feather-Flow since we forbid CTEs and derived tables via S005/S006.)

**No built-in classification** — tools built on top (Recce, DataHub) add their own: Passthrough / Rename / Derived / Source / Unknown.

**Limitations**: requires pre-existing schemas, no dynamic SQL, Python-based (slower than Rust), classification must be layered on top.

### Key Takeaway From Both

Both approaches require **schema awareness** as a prerequisite for correct column resolution. The critical step is **column qualification** — resolving which table each unqualified column reference belongs to. Without this, `SELECT id FROM raw_customers` loses the connection between `id` and `raw_customers`.

---

## Current Feather-Flow Implementation

### Two Independent Systems

1. **AST-based (`ff-sql/src/lineage.rs`)** — used by `ff lineage` CLI command. Parses SQL with sqlparser-rs and walks the AST.
2. **DataFusion-based (`ff-analysis/src/datafusion_bridge/lineage.rs`)** — used only within the analysis engine. Per-model only, no cross-model resolution.

### The Specific Bug: `int_customer_ranking.customer_id` Lineage

**Expected full chain:**

```text
raw_customers.id → stg_customers.customer_id → int_customer_ranking.customer_id
```

**Actual output (only 1 hop):**

```text
stg_customers.customer_id → int_customer_ranking.customer_id
```

### Root Cause Analysis

**Two critical bugs in `resolve_single_edge()` at `ff-sql/src/lineage.rs:395`:**

#### Bug 1: Unqualified column references resolve to empty string

```rust
// lineage.rs:402
let source_table = source_ref.table.as_deref().unwrap_or("");
```

When `stg_customers.sql` says `SELECT id as customer_id FROM raw_customers` (no table prefix on `id`), the extracted `ColumnRef` has `table: None`. This becomes `""`, which matches no model. **Edge silently dropped.**

#### Bug 2: Seeds and sources excluded from `known_models`

```rust
// lineage.rs (CLI lineage.rs:18)
let known_models: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();
```

Even if `raw_customers` were resolved as the source table, it's a **seed** (lives in `project.seeds`), not a model. Seeds and source tables are invisible to edge resolution.

### Walk-through of the Failure

1. **stg_customers extraction**: `id as customer_id` → `ColumnRef { table: None, column: "id" }`, `source_tables: { "raw_customers" }`
2. **Edge resolution**: `source_ref.table` is `None` → resolves to `""` → no match in `known_models` → **no edge created**
3. **int_customer_ranking extraction**: `c.customer_id` → alias `c` resolves to `stg_customers` → `ColumnRef { table: "stg_customers", column: "customer_id" }`
4. **Edge resolution**: `stg_customers` IS in `known_models` → edge created: `stg_customers.customer_id → int_customer_ranking.customer_id`
5. **Recursive trace**: BFS from `int_customer_ranking.customer_id` → finds hop to `stg_customers.customer_id` → looks for edges targeting `stg_customers.customer_id` → **none exist** → stops

---

## Feather-Flow's Unfair Advantage

Unlike dbt or SQLMesh, Feather-Flow **already has** the key prerequisites that make column lineage hard:

1. **Mandatory 1:1 YAML schemas** — every node MUST have a `.yml` with column definitions. We don't need to introspect a database or guess schemas.
2. **Source YAML with full column metadata** — `raw_ecommerce.yml` already declares every column with name + type for every source table. All sources are required to have schema definitions.
3. **Static analysis validates schemas match SQL** — SA01/SA02 diagnostics ensure YAML columns match what the SQL actually produces. This means our schema metadata is **guaranteed correct** at runtime.
4. **DataFusion schema propagation** — `propagate_schemas()` already walks the DAG in topo order, building full schema context for every model.
5. **Seed schemas** — seed nodes have YAML with column definitions.
6. **No CTEs or derived tables** — S005/S006 enforce this. Every SELECT operates directly on named tables, making lineage tracing structurally simpler than what dbt/SQLMesh must handle. No recursive scope descent needed.

Most tools struggle to get schema information. We have it by design, it's validated, and our SQL constraints make tracing deterministic.

---

## Proposed Design: DataFusion-First Column Lineage

### Core Principles

1. **DataFusion is the lineage engine** — use DataFusion `LogicalPlan` for per-model column extraction (more accurate, already integrated with schema propagation). Retire AST-based lineage in `ff-sql` as the primary system.
2. **Leverage validated YAML schemas** — build a `SchemaRegistry` from our guaranteed-correct YAML metadata for column qualification and description tracking.
3. **No CTEs/derived tables simplifies everything** — since S005/S006 forbid these, every FROM/JOIN target is a named table. Column qualification is unambiguous given our schema metadata.

### Classification System

All four kinds answer the question "how is this column **used** relative to the SELECT output?"

| Kind          | In SELECT? | Definition                                                    | Example                                          |
| ------------- | ---------- | ------------------------------------------------------------- | ------------------------------------------------ |
| **Copy**      | Yes        | Column passed through with same name, no transformation       | `SELECT customer_id FROM stg_customers`          |
| **Rename**    | Yes        | Column passed through (direct ref) but aliased to a new name | `SELECT id AS customer_id FROM raw_customers`    |
| **Transform** | Yes        | Column derived from expression, aggregation, function, etc.   | `SELECT coalesce(val, 0) AS val FROM ...`        |
| **Inspect**   | No         | Column referenced in WHERE, JOIN ON, GROUP BY, HAVING only    | `WHERE status = 'active'`, `ON a.id = b.id`      |

### Description Tracking

Each lineage edge carries a `description_status` indicating whether the column's YAML description has changed across the hop:

| Status        | Meaning                                                        |
| ------------- | -------------------------------------------------------------- |
| **Inherited** | Description text is identical to the upstream column's         |
| **Modified**  | Description text differs from the upstream column's            |
| **Missing**   | No description defined on this node's YAML                     |

This enables:

- **Lint rules**: warn when a Copy/Rename column has a modified description (likely stale or inconsistent)
- **Auto-suggest**: for Copy/Rename columns missing descriptions, suggest inheriting from upstream
- **Documentation drift detection**: surface models where descriptions diverge from their source of truth

### Phase 1: Fix the Two Bugs (Minimal, High-Impact)

Fix the two root causes without architectural changes:

**1a. Resolve unqualified columns using `source_tables`**

In `resolve_single_edge()`, when `source_ref.table` is `None`:

- If the model has exactly 1 source table, use it (unambiguous)
- If multiple source tables, match the column name against YAML schemas for each source table to disambiguate
- Since we forbid CTEs/derived tables, every source table is a named table with known schema — this is always resolvable

**1b. Expand `known_models` to include seeds and source tables**

```rust
let mut known_nodes: HashSet<&str> = project.models.keys().map(|k| k.as_str()).collect();
for seed in &project.seeds {
    known_nodes.insert(seed.name.as_str());
}
for source_file in &project.sources {
    for table in &source_file.tables {
        known_nodes.insert(&table.name);
    }
}
```

**Result after Phase 1**: The full chain `raw_customers.id → stg_customers.customer_id → int_customer_ranking.customer_id` works.

### Phase 2: Schema-Powered Column Qualification + Description Tracking

Build a proper column qualification pass using validated YAML metadata.

**2a. Build a `SchemaRegistry`**

A lookup structure built from project metadata:

```rust
struct ColumnMeta {
    name: String,
    data_type: Option<String>,
    description: Option<String>,
    classification: Option<String>,
}

struct SchemaRegistry {
    /// model_name → { column_name → ColumnMeta }
    models: HashMap<String, HashMap<String, ColumnMeta>>,
    /// source_table_name → { column_name → ColumnMeta }
    sources: HashMap<String, HashMap<String, ColumnMeta>>,
    /// seed_name → { column_name → ColumnMeta }
    seeds: HashMap<String, HashMap<String, ColumnMeta>>,
}
```

Populated from:

- Model YAML `columns:` sections
- Source YAML `tables[].columns:` sections
- Seed YAML `columns:` sections

Since all of these are mandatory and SA-validated, the registry is always complete.

**2b. Column qualification pass**

Before edge resolution, qualify unresolved column references:

- For every `ColumnRef { table: None, column }`, look up which source table(s) in the model's FROM clause own that column via the `SchemaRegistry`
- For `SELECT *`, expand to all columns from the source table(s) using schemas
- For ambiguous columns (same name in multiple JOINed tables), flag as ambiguous (this is already a SQL error that DataFusion would catch)

Since CTEs and derived tables are forbidden, every table reference resolves to a known node with a known schema. No recursive scope descent needed.

**2c. Richer `LineageEdge`**

Extend `LineageEdge` with:

```rust
struct LineageEdge {
    source_model: String,
    source_column: String,
    target_model: String,
    target_column: String,
    kind: LineageKind,           // Copy, Rename, Transform, Inspect
    classification: Option<String>,
    description_status: DescriptionStatus,  // Inherited, Modified, Missing
}
```

Classification rules:

- **Copy**: `ExprType::Column` AND `source_column == target_column`
- **Rename**: `ExprType::Column` AND `source_column != target_column`
- **Transform**: `ExprType::Function | Expression | Cast | Case | Subquery`
- **Inspect**: column appears in WHERE/JOIN/GROUP BY but not in SELECT

### Phase 3: DataFusion-First Lineage Engine

Unify the two lineage systems with DataFusion as the primary engine.

**Why DataFusion over AST**:

- DataFusion already runs during `propagate_schemas()` in topo order across the DAG
- DataFusion's `LogicalPlan` resolves column references more accurately than raw AST walking (it understands join semantics, projection pushdown, etc.)
- DataFusion already classifies Copy/Transform/Inspect from the plan
- We already build `LogicalPlan` for every model during static analysis — lineage extraction is a natural extension

**Architecture**:

1. During `propagate_schemas()`, after building each model's `LogicalPlan`, extract per-model column lineage from the plan (already done in `lineage.rs` DataFusion bridge)
2. Feed extracted per-model lineage into `ProjectLineage` for cross-model resolution (reuse the BFS traversal logic from `ff-sql`)
3. Use `SchemaRegistry` for column qualification of unresolved references
4. The `ff lineage` CLI command triggers the unified pipeline instead of doing its own AST extraction

**What stays in `ff-sql`**: `ProjectLineage`, `LineageEdge`, cross-model resolution (`resolve_edges`), recursive traversal (`trace_column_recursive`, `column_consumers_recursive`), DOT output. These are graph operations, not SQL parsing.

**What moves to DataFusion**: per-model column extraction (replaces `extract_column_lineage` AST walker).

### Phase 4: Description Inheritance + Lint Rules

Since we now track `kind` and `description_status`:

- **Copy/Rename columns with `Missing` description**: suggest inheriting from upstream
- **Copy/Rename columns with `Modified` description**: warn about potential documentation drift (new diagnostic code, e.g., A050)
- **Transform columns with `Missing` description**: warn that transformed columns need documentation
- `ff validate` surfaces these as warnings
- `ff lineage --column X` shows description status in the chain view

### Phase 5: Enhanced CLI Output

**Chain view** (default when `--column` is specified):

```text
$ ff lineage -n int_customer_ranking --column customer_id --direction upstream

CHAIN: int_customer_ranking.customer_id
════════════════════════════════════════════════════════════════

  raw_customers.id                    (source, INTEGER)
  "Unique order identifier"
       │ rename → description: modified
  stg_customers.customer_id          (sql, INTEGER, pii)
  "Unique identifier for the customer"
       │ copy → description: modified
  int_customer_ranking.customer_id   (sql, INTEGER, pii)
  "Unique customer identifier"

3 nodes in lineage chain.
```

**Table view** (default when no `--column`):

```text
$ ff lineage -n int_customer_ranking

SOURCE MODEL              SOURCE COLUMN    TARGET MODEL              TARGET COLUMN    KIND       CLASS    DESC STATUS
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
stg_customers             customer_id      int_customer_ranking      customer_id      copy       pii      modified
stg_customers             customer_name    int_customer_ranking      customer_name    copy       pii      inherited
int_customer_metrics      lifetime_value   int_customer_ranking      lifetime_value   copy       -        inherited
int_customer_metrics      lifetime_value   int_customer_ranking      value_or_zero    transform  -        modified
int_customer_metrics      total_orders     int_customer_ranking      nonzero_orders   transform  -        modified
int_customer_metrics      customer_id      int_customer_ranking      customer_id      inspect    pii      -
```

**JSON output** (`--output json`): full edge metadata including kind, classification, description_status, source/target types.

**DOT output** (`--output dot`): column-level DAG with edge labels for kind, suitable for VS Code extension.

---

## Implementation Priority

| Phase   | Effort | Impact     | Description                                                        |
| ------- | ------ | ---------- | ------------------------------------------------------------------ |
| **1**   | Small  | **High**   | Fix two bugs — unqualified columns + seeds/sources in known_nodes  |
| **2**   | Medium | **High**   | SchemaRegistry + column qualification + description tracking       |
| **3**   | Medium | **High**   | DataFusion-first engine, retire AST extraction as primary          |
| **4**   | Small  | Medium     | Description inheritance lint rules (A050+)                         |
| **5**   | Small  | Medium     | Enhanced CLI output (chain view, description status)               |

Phase 1 alone fixes the reported bug. Phases 2-3 build the real engine. Phases 4-5 are the polish that makes this best-in-class.

---

## Comparison: Where Feather-Flow Lands After This

| Capability                   | dbt Fusion          | SQLMesh              | Feather-Flow (after)                      |
| ---------------------------- | ------------------- | -------------------- | ----------------------------------------- |
| Schema source                | Inferred from DAG   | Must be provided     | Mandatory YAML + SA validation            |
| Database required            | No                  | No                   | No                                        |
| Classification               | Copy/Transform/Inspect | N/A (add-on)      | Copy/Rename/Transform/Inspect             |
| Description inheritance      | Enterprise only     | No                   | Built-in                                  |
| Description drift detection  | No                  | No                   | Built-in (Inherited/Modified/Missing)     |
| Column qualification         | Built into compiler | sqlglot optimizer    | SchemaRegistry from validated YAML        |
| Cross-model resolution       | Built-in            | Per-query only       | Built-in (recursive BFS)                  |
| Seeds/sources in lineage     | Yes                 | Depends on config    | Yes                                       |
| Validates schemas match SQL  | Yes (SA)            | No                   | Yes (SA01/SA02)                           |
| CTE/derived table handling   | Full support needed | Full support needed  | Not needed (S005/S006 forbid them)        |
| Lineage engine               | Custom Rust IR      | Python sqlglot       | DataFusion LogicalPlan (Rust)             |
| Language                     | Rust                | Python               | Rust                                      |
| Open source                  | No (Enterprise)     | Yes                  | Yes                                       |

---

## Test Harness: Complete Column Lineage Map

This section documents the expected lineage for **every column in the sample project**, traced from raw sources through every model. This is the ground truth for the test harness.

### DAG Structure

```text
                    RAW SOURCES (seeds + source YAML)
    ┌──────────────┬──────────────┬──────────────┐
    │              │              │              │
raw_customers  raw_orders   raw_products  raw_payments
    │              │              │          │       │
    ▼              ▼              ▼          ▼       ▼
stg_customers  stg_orders   stg_products  stg_payments  stg_payments_star
    │    │         │    │        │    │        │
    │    │         │    │        │    │        │
    ▼    │    ┌────▼────┤        ▼    ▼        ▼
int_customer  │  int_orders   dim_products  │
  _metrics    │  _enriched    dim_products  │
    │    │    │    │    │      _extended     │
    │    ▼    │    │    │                    │
    │  int_customer  │  │                   │
    │  _ranking      │  │                   │
    │         │      │  │                   │
    ▼─────────│──────▼  ▼───────────────────┘
  dim_customers    fct_orders     int_all_orders
                       │          int_high_value_orders
                       │
                   rpt_customer_orders
                   rpt_order_volume (via table function)
```

### Classification Rules (for reference)

- **Copy**: column in SELECT, passed through with same name, no transformation
- **Rename**: column in SELECT, direct reference but aliased to a different name
- **Transform**: column in SELECT, derived from expression/function/aggregation/cast
- **Inspect**: column used in WHERE, JOIN ON, GROUP BY, or HAVING **only** (not in SELECT)

When a column appears in both SELECT and WHERE/JOIN/GROUP BY, the SELECT classification takes precedence — no separate Inspect edge.

### Description Status Rules

- **inherited**: both source and target YAML have descriptions, and they are identical
- **modified**: both source and target YAML have descriptions, and they differ
- **missing**: either source or target (or both) lack a description in YAML

### Source Column Inventory

All columns defined in raw source/seed nodes. Most have no YAML descriptions (seeds have no `columns:` section; most source columns in `raw_ecommerce.yml` omit `description:`).

**raw_customers** (seed + source):

| Column     | Type    | Source YAML Description |
| ---------- | ------- | ----------------------- |
| id         | INTEGER | (none)                  |
| name       | VARCHAR | (none)                  |
| email      | VARCHAR | (none)                  |
| created_at | DATE    | (none)                  |
| tier       | VARCHAR | (none)                  |

**raw_orders** (seed + source):

| Column     | Type          | Source YAML Description    |
| ---------- | ------------- | -------------------------- |
| id         | INTEGER       | "Unique order identifier"  |
| user_id    | INTEGER       | (none)                     |
| created_at | DATE          | (none)                     |
| amount     | DECIMAL(10,2) | (none)                     |
| status     | VARCHAR       | (none)                     |

**raw_products** (seed + source):

| Column   | Type          | Source YAML Description |
| -------- | ------------- | ----------------------- |
| id       | INTEGER       | (none)                  |
| name     | VARCHAR       | (none)                  |
| category | VARCHAR       | (none)                  |
| price    | DECIMAL(10,2) | (none)                  |
| active   | BOOLEAN       | (none)                  |

**raw_payments** (seed + source):

| Column         | Type          | Source YAML Description |
| -------------- | ------------- | ----------------------- |
| id             | INTEGER       | (none)                  |
| order_id       | INTEGER       | (none)                  |
| payment_method | VARCHAR       | (none)                  |
| amount         | DECIMAL(10,2) | (none)                  |
| created_at     | DATE          | (none)                  |

---

### Per-Model Edge Matrix

Every output column for every SQL model, with its immediate upstream source(s). This is the core test data — each row is one expected `LineageEdge`.

#### stg_customers

Source: `raw_customers` (single table, no alias, all column refs unqualified)

| # | target_column | source_model  | source_column | kind      | desc_status |
| - | ------------- | ------------- | ------------- | --------- | ----------- |
| 1 | customer_id   | raw_customers | id            | rename    | missing     |
| 2 | customer_name | raw_customers | name          | rename    | missing     |
| 3 | email         | raw_customers | email         | copy      | missing     |
| 4 | signup_date   | raw_customers | created_at    | rename    | missing     |
| 5 | customer_tier | raw_customers | tier          | rename    | missing     |

Inspect edges: none

#### stg_orders

Source: `raw_orders` (single table, no alias, WHERE `created_at >= ...`)

| # | target_column | source_model | source_column | kind      | desc_status |
| - | ------------- | ------------ | ------------- | --------- | ----------- |
| 1 | order_id      | raw_orders   | id            | rename    | modified    |
| 2 | customer_id   | raw_orders   | user_id       | rename    | missing     |
| 3 | order_date    | raw_orders   | created_at    | rename    | missing     |
| 4 | amount        | raw_orders   | amount        | copy      | missing     |
| 5 | status        | raw_orders   | status        | copy      | missing     |

Inspect edges: none (`created_at` is in both SELECT and WHERE — SELECT wins as rename)

Note: edge #1 is the only raw→staging edge where both source and target have descriptions. Source: "Unique order identifier", Target: "Unique identifier for the order" → **modified**.

#### stg_payments

Source: `raw_payments` (single table, no alias). After Jinja rendering: `{{ cents_to_dollars("amount") }}` → `amount / 100.0`

| # | target_column | source_model | source_column | kind      | desc_status |
| - | ------------- | ------------ | ------------- | --------- | ----------- |
| 1 | payment_id    | raw_payments | id            | rename    | missing     |
| 2 | order_id      | raw_payments | order_id      | copy      | missing     |
| 3 | amount        | raw_payments | amount        | transform | missing     |

Inspect edges: none

Note: `amount / 100.0` is an arithmetic expression → transform (not copy, even though output name matches).

#### stg_payments_star

Source: `raw_payments` (single table, `SELECT *`)

| # | target_column  | source_model | source_column  | kind | desc_status |
| - | -------------- | ------------ | -------------- | ---- | ----------- |
| 1 | id             | raw_payments | id             | copy | missing     |
| 2 | order_id       | raw_payments | order_id       | copy | missing     |
| 3 | payment_method | raw_payments | payment_method | copy | missing     |
| 4 | amount         | raw_payments | amount         | copy | missing     |
| 5 | created_at     | raw_payments | created_at     | copy | missing     |

Inspect edges: none

Note: `SELECT *` requires schema-aware expansion. Each column must be resolved using the `SchemaRegistry` for `raw_payments`. This is a key test case for Phase 2.

#### stg_products

Source: `raw_products` (single table, no alias)

| # | target_column | source_model | source_column | kind      | desc_status |
| - | ------------- | ------------ | ------------- | --------- | ----------- |
| 1 | product_id    | raw_products | id            | rename    | missing     |
| 2 | product_name  | raw_products | name          | rename    | missing     |
| 3 | category      | raw_products | category      | copy      | missing     |
| 4 | price         | raw_products | price         | transform | missing     |
| 5 | active        | raw_products | active        | copy      | missing     |

Inspect edges: none

Note: `cast(price as decimal(10, 2)) as price` is a CAST → transform, even though the output name matches the input name.

#### int_customer_metrics

Sources: `stg_customers c` JOIN `stg_orders o` ON `c.customer_id = o.customer_id`, GROUP BY `c.customer_id, c.customer_name`

| # | target_column  | source_model  | source_column | kind      | desc_status |
| - | -------------- | ------------- | ------------- | --------- | ----------- |
| 1 | customer_id    | stg_customers | customer_id   | copy      | modified    |
| 2 | customer_name  | stg_customers | customer_name | copy      | modified    |
| 3 | total_orders   | stg_orders    | order_id      | transform | n/a         |
| 4 | lifetime_value | stg_orders    | amount        | transform | n/a         |
| 5 | last_order_date| stg_orders    | order_date    | transform | n/a         |

Inspect edges:

| # | source_model | source_column | used_in |
| - | ------------ | ------------- | ------- |
| 1 | stg_orders   | customer_id   | JOIN ON |

Description details for select edges:
- #1: "Unique identifier for the customer" → "Unique customer identifier" = **modified**
- #2: "Full name of the customer" → "Customer full name" = **modified**
- #3-5: transform columns — description comparison is n/a (new semantics)

#### int_customer_ranking

Sources: `stg_customers c` JOIN `int_customer_metrics m` ON `c.customer_id = m.customer_id`

| # | target_column  | source_model         | source_column  | kind      | desc_status |
| - | -------------- | -------------------- | -------------- | --------- | ----------- |
| 1 | customer_id    | stg_customers        | customer_id    | copy      | modified    |
| 2 | customer_name  | stg_customers        | customer_name  | copy      | modified    |
| 3 | lifetime_value | int_customer_metrics | lifetime_value | copy      | inherited   |
| 4 | value_or_zero  | int_customer_metrics | lifetime_value | transform | n/a         |
| 5 | nonzero_orders | int_customer_metrics | total_orders   | transform | n/a         |

Inspect edges:

| # | source_model         | source_column | used_in |
| - | -------------------- | ------------- | ------- |
| 1 | int_customer_metrics | customer_id   | JOIN ON |

Description details:
- #1: "Unique identifier for the customer" → "Unique customer identifier" = **modified**
- #2: "Full name of the customer" → "Customer full name" = **modified**
- #3: "Total amount spent across all orders" → "Total amount spent across all orders" = **inherited**

#### int_orders_enriched

Sources: `stg_orders o` JOIN `stg_payments p` ON `o.order_id = p.order_id`, GROUP BY `o.order_id, o.customer_id, o.order_date, o.amount, o.status`

| # | target_column | source_model | source_column | kind      | desc_status |
| - | ------------- | ------------ | ------------- | --------- | ----------- |
| 1 | order_id      | stg_orders   | order_id      | copy      | modified    |
| 2 | customer_id   | stg_orders   | customer_id   | copy      | modified    |
| 3 | order_date    | stg_orders   | order_date    | copy      | inherited   |
| 4 | order_amount  | stg_orders   | amount        | rename    | n/a         |
| 5 | status        | stg_orders   | status        | copy      | inherited   |
| 6 | payment_total | stg_payments | amount        | transform | n/a         |
| 7 | payment_count | stg_payments | payment_id    | transform | n/a         |

Inspect edges:

| # | source_model | source_column | used_in |
| - | ------------ | ------------- | ------- |
| 1 | stg_payments | order_id      | JOIN ON |

Description details:
- #1: "Unique identifier for the order" → "Unique order identifier" = **modified**
- #2: "Foreign key to stg_customers" → "Reference to the customer" = **modified**
- #3: "Date the order was placed" → "Date the order was placed" = **inherited**
- #5: "Order status" → "Order status" = **inherited**

#### int_high_value_orders

Source: `stg_orders o`, GROUP BY `o.customer_id`, HAVING `sum(o.amount) > 100`

| # | target_column | source_model | source_column | kind      | desc_status |
| - | ------------- | ------------ | ------------- | --------- | ----------- |
| 1 | customer_id   | stg_orders   | customer_id   | copy      | modified    |
| 2 | order_count   | stg_orders   | order_id      | transform | n/a         |
| 3 | total_amount  | stg_orders   | amount        | transform | n/a         |
| 4 | min_order     | stg_orders   | amount        | transform | n/a         |
| 5 | max_order     | stg_orders   | amount        | transform | n/a         |
| 6 | avg_order     | stg_orders   | amount        | transform | n/a         |

Inspect edges: none (customer_id in GROUP BY is also in SELECT; amount in HAVING is also in SELECT transforms)

Description details:
- #1: "Foreign key to stg_customers" → "Customer identifier" = **modified**

#### int_all_orders (UNION ALL)

Two branches. Each output column has edges from **both** branches.

Branch 1: `int_orders_enriched` WHERE `status = 'completed'`
Branch 2: `stg_orders` WHERE `status = 'pending'`

| #  | target_column | source_model (B1)   | source_column (B1) | kind (B1) | source_model (B2) | source_column (B2) | kind (B2) |
| -- | ------------- | ------------------- | ------------------ | --------- | ----------------- | ------------------ | --------- |
| 1  | order_id      | int_orders_enriched | order_id           | copy      | stg_orders        | order_id           | copy      |
| 2  | customer_id   | int_orders_enriched | customer_id        | copy      | stg_orders        | customer_id        | copy      |
| 3  | order_date    | int_orders_enriched | order_date         | copy      | stg_orders        | order_date         | copy      |
| 4  | order_amount  | int_orders_enriched | order_amount       | copy      | stg_orders        | amount             | rename    |
| 5  | status        | int_orders_enriched | status             | copy      | stg_orders        | status             | copy      |
| 6  | source        | (literal)           | —                  | transform | (literal)         | —                  | transform |

Inspect edges: none (status in WHERE is also in SELECT)

Description status (B1 edges):
- #1: "Unique order identifier" → "Unique order identifier" = **inherited**
- #2: "Reference to the customer" → "Customer reference" = **modified**
- #3: "Date the order was placed" → "Date the order was placed" = **inherited**
- #4: "Original order amount" → "Order amount" = **modified**
- #5: "Order status" → "Order status" = **inherited**

Description status (B2 edges):
- #1: "Unique identifier for the order" → "Unique order identifier" = **modified**
- #2: "Foreign key to stg_customers" → "Customer reference" = **modified**
- #3: "Date the order was placed" → "Date the order was placed" = **inherited**
- #4: "Order total in USD" → "Order amount" = **modified**
- #5: "Order status" → "Order status" = **inherited**

Note: `source` column is a string literal with no upstream column dependency — transform with zero source columns.

#### dim_customers

Sources: `int_customer_metrics m` JOIN `stg_customers c` ON `m.customer_id = c.customer_id`

| # | target_column  | source_model         | source_column  | kind      | desc_status |
| - | -------------- | -------------------- | -------------- | --------- | ----------- |
| 1 | customer_id    | int_customer_metrics | customer_id    | copy      | inherited   |
| 2 | customer_name  | stg_customers        | customer_name  | copy      | modified    |
| 3 | email          | stg_customers        | email          | copy      | inherited   |
| 4 | signup_date    | stg_customers        | signup_date    | copy      | inherited   |
| 5 | total_orders   | int_customer_metrics | total_orders   | copy      | inherited   |
| 6 | lifetime_value | int_customer_metrics | lifetime_value | copy      | inherited   |
| 7 | last_order_date| int_customer_metrics | last_order_date| copy      | inherited   |
| 8 | computed_tier  | int_customer_metrics | lifetime_value | transform | n/a         |

Inspect edges:

| # | source_model  | source_column | used_in |
| - | ------------- | ------------- | ------- |
| 1 | stg_customers | customer_id   | JOIN ON |

Description details:
- #1: "Unique customer identifier" → "Unique customer identifier" = **inherited**
- #2: "Full name of the customer" → "Customer full name" = **modified**
- #3: "Customer email address" → "Customer email address" = **inherited**
- #4: "Date the customer signed up" → "Date the customer signed up" = **inherited**
- #5: "Total number of orders placed" → "Total number of orders placed" = **inherited**
- #6: "Total amount spent across all orders" → "Total amount spent across all orders" = **inherited**
- #7: "Date of the most recent order" → "Date of the most recent order" = **inherited**

#### dim_products

Source: `stg_products` (single table, no alias, WHERE `active = true`)

| # | target_column  | source_model | source_column | kind      | desc_status |
| - | -------------- | ------------ | ------------- | --------- | ----------- |
| 1 | product_id     | stg_products | product_id    | copy      | inherited   |
| 2 | product_name   | stg_products | product_name  | copy      | inherited   |
| 3 | category       | stg_products | category      | copy      | modified    |
| 4 | price          | stg_products | price         | copy      | inherited   |
| 5 | category_group | stg_products | category      | transform | n/a         |
| 6 | price_tier     | stg_products | price         | transform | n/a         |

Inspect edges:

| # | source_model | source_column | used_in |
| - | ------------ | ------------- | ------- |
| 1 | stg_products | active        | WHERE   |

Description details:
- #1: "Unique product identifier" → "Unique product identifier" = **inherited**
- #2: "Product display name" → "Product display name" = **inherited**
- #3: "Product category" → "Original product category" = **modified**
- #4: "Product price in dollars" → "Product price in dollars" = **inherited**

#### dim_products_extended

Source: `stg_products` (single table, no alias, SELECT DISTINCT)

| # | target_column     | source_model | source_column | kind      | desc_status |
| - | ----------------- | ------------ | ------------- | --------- | ----------- |
| 1 | product_id        | stg_products | product_id    | copy      | inherited   |
| 2 | product_name      | stg_products | product_name  | copy      | inherited   |
| 3 | category          | stg_products | category      | copy      | modified    |
| 4 | price             | stg_products | price         | copy      | inherited   |
| 5 | id_scaled         | stg_products | product_id    | transform | n/a         |
| 6 | detailed_category | stg_products | category      | transform | n/a         |
| 7 | detailed_category | stg_products | price         | transform | n/a         |

Inspect edges: none

Note: `detailed_category` has **two** source columns — both `category` and `price` feed into the nested CASE expression. This produces two edges for the same target column.

Description details:
- #1: "Unique product identifier" → "Unique product identifier" = **inherited**
- #2: "Product display name" → "Product display name" = **inherited**
- #3: "Product category" → "Original product category" = **modified**
- #4: "Product price in dollars" → "Product price in dollars" = **inherited**

#### fct_orders

Sources: `int_orders_enriched e` JOIN `stg_customers c` ON `e.customer_id = c.customer_id`

| #  | target_column | source_model        | source_column | kind      | desc_status |
| -- | ------------- | ------------------- | ------------- | --------- | ----------- |
| 1  | order_id      | int_orders_enriched | order_id      | copy      | modified    |
| 2  | customer_id   | int_orders_enriched | customer_id   | copy      | inherited   |
| 3  | customer_name | stg_customers       | customer_name | copy      | modified    |
| 4  | customer_tier | stg_customers       | customer_tier | copy      | modified    |
| 5  | order_date    | int_orders_enriched | order_date    | copy      | inherited   |
| 6  | amount        | int_orders_enriched | order_amount  | rename    | n/a         |
| 7  | status        | int_orders_enriched | status        | copy      | inherited   |
| 8  | payment_total | int_orders_enriched | payment_total | copy      | modified    |
| 9  | payment_count | int_orders_enriched | payment_count | copy      | modified    |
| 10 | balance_due   | int_orders_enriched | order_amount  | transform | n/a         |
| 11 | balance_due   | int_orders_enriched | payment_total | transform | n/a         |
| 12 | payment_ratio | int_orders_enriched | payment_total | transform | n/a         |
| 13 | payment_ratio | int_orders_enriched | order_amount  | transform | n/a         |

Inspect edges:

| # | source_model  | source_column | used_in |
| - | ------------- | ------------- | ------- |
| 1 | stg_customers | customer_id   | JOIN ON |

Description details:
- #1: "Unique order identifier" → "Unique identifier for the order" = **modified**
- #2: "Reference to the customer" → "Reference to the customer" = **inherited**
- #3: "Full name of the customer" → "Customer full name" = **modified**
- #4: "Customer tier (gold, silver, bronze)" → "Customer tier at time of order" = **modified**
- #5: "Date the order was placed" → "Date the order was placed" = **inherited**
- #7: "Order status" → "Order status" = **inherited**
- #8: "Total payments received for this order" → "Total payments received" = **modified**
- #9: "Number of payments made for this order" → "Number of payments made" = **modified**

#### rpt_customer_orders

Sources: `stg_customers c` JOIN `int_orders_enriched e` ON `c.customer_id = e.customer_id` JOIN `stg_orders o` ON `e.order_id = o.order_id`, WHERE `e.order_amount BETWEEN o.amount AND o.amount`

| # | target_column   | source_model        | source_column | kind      | desc_status |
| - | --------------- | ------------------- | ------------- | --------- | ----------- |
| 1 | customer_id     | stg_customers       | customer_id   | copy      | modified    |
| 2 | customer_name   | stg_customers       | customer_name | copy      | modified    |
| 3 | email           | stg_customers       | email         | copy      | inherited   |
| 4 | order_id        | int_orders_enriched | order_id      | copy      | modified    |
| 5 | order_amount    | int_orders_enriched | order_amount  | copy      | inherited   |
| 6 | payment_total   | int_orders_enriched | payment_total | copy      | modified    |
| 7 | balance_with_fee| int_orders_enriched | order_amount  | transform | n/a         |
| 8 | balance_with_fee| int_orders_enriched | payment_total | transform | n/a         |
| 9 | combined_metric | int_orders_enriched | order_amount  | transform | n/a         |
| 10| combined_metric | int_orders_enriched | payment_total | transform | n/a         |
| 11| combined_metric | int_orders_enriched | payment_count | transform | n/a         |

Inspect edges:

| # | source_model        | source_column | used_in |
| - | ------------------- | ------------- | ------- |
| 1 | int_orders_enriched | customer_id   | JOIN ON |
| 2 | stg_orders          | order_id      | JOIN ON |
| 3 | stg_orders          | amount        | WHERE   |

Description details:
- #1: "Unique identifier for the customer" → "Unique customer identifier" = **modified**
- #2: "Full name of the customer" → "Customer full name" = **modified**
- #3: "Customer email address" → "Customer email address" = **inherited**
- #4: "Unique order identifier" → "Order identifier" = **modified**
- #5: "Original order amount" → "Original order amount" = **inherited**
- #6: "Total payments received for this order" → "Total payments for order" = **modified**

#### rpt_order_volume

Source: `order_volume_by_status({{ var("min_order_count") }})` — table function call

| # | target_column  | source_model             | source_column | kind      | desc_status |
| - | -------------- | ------------------------ | ------------- | --------- | ----------- |
| 1 | status         | order_volume_by_status   | status        | copy      | n/a         |
| 2 | order_count    | order_volume_by_status   | order_count   | copy      | n/a         |
| 3 | pct_of_hundred | order_volume_by_status   | order_count   | transform | n/a         |

**Special case**: lineage stops at the table function boundary. The function body internally reads from `fct_orders`, but tracing through function definitions is a future enhancement. Test assertions should verify edges exist from the function output; cross-function lineage is out of scope for initial implementation.

---

### Full Upstream Chains: Every Mart/Report Column to Raw Sources

These chains trace every terminal column all the way back to raw_ sources. Each line is one hop. The test harness should verify that `trace_column_recursive(model, column)` returns exactly these edges.

#### dim_customers

**dim_customers.customer_id**:

```text
raw_customers.id ──rename──▶ stg_customers.customer_id
stg_customers.customer_id ──copy──▶ int_customer_metrics.customer_id
int_customer_metrics.customer_id ──copy──▶ dim_customers.customer_id
```

**dim_customers.customer_name**:

```text
raw_customers.name ──rename──▶ stg_customers.customer_name
stg_customers.customer_name ──copy──▶ dim_customers.customer_name
```

**dim_customers.email**:

```text
raw_customers.email ──copy──▶ stg_customers.email
stg_customers.email ──copy──▶ dim_customers.email
```

**dim_customers.signup_date**:

```text
raw_customers.created_at ──rename──▶ stg_customers.signup_date
stg_customers.signup_date ──copy──▶ dim_customers.signup_date
```

**dim_customers.total_orders**:

```text
raw_orders.id ──rename──▶ stg_orders.order_id
stg_orders.order_id ──transform(count)──▶ int_customer_metrics.total_orders
int_customer_metrics.total_orders ──copy──▶ dim_customers.total_orders
```

**dim_customers.lifetime_value**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──transform(coalesce+sum)──▶ int_customer_metrics.lifetime_value
int_customer_metrics.lifetime_value ──copy──▶ dim_customers.lifetime_value
```

**dim_customers.last_order_date**:

```text
raw_orders.created_at ──rename──▶ stg_orders.order_date
stg_orders.order_date ──transform(max)──▶ int_customer_metrics.last_order_date
int_customer_metrics.last_order_date ──copy──▶ dim_customers.last_order_date
```

**dim_customers.computed_tier**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──transform(coalesce+sum)──▶ int_customer_metrics.lifetime_value
int_customer_metrics.lifetime_value ──transform(case)──▶ dim_customers.computed_tier
```

#### fct_orders

**fct_orders.order_id**:

```text
raw_orders.id ──rename──▶ stg_orders.order_id
stg_orders.order_id ──copy──▶ int_orders_enriched.order_id
int_orders_enriched.order_id ──copy──▶ fct_orders.order_id
```

**fct_orders.customer_id**:

```text
raw_orders.user_id ──rename──▶ stg_orders.customer_id
stg_orders.customer_id ──copy──▶ int_orders_enriched.customer_id
int_orders_enriched.customer_id ──copy──▶ fct_orders.customer_id
```

**fct_orders.customer_name**:

```text
raw_customers.name ──rename──▶ stg_customers.customer_name
stg_customers.customer_name ──copy──▶ fct_orders.customer_name
```

**fct_orders.customer_tier**:

```text
raw_customers.tier ──rename──▶ stg_customers.customer_tier
stg_customers.customer_tier ──copy──▶ fct_orders.customer_tier
```

**fct_orders.order_date**:

```text
raw_orders.created_at ──rename──▶ stg_orders.order_date
stg_orders.order_date ──copy──▶ int_orders_enriched.order_date
int_orders_enriched.order_date ──copy──▶ fct_orders.order_date
```

**fct_orders.amount**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──rename──▶ int_orders_enriched.order_amount
int_orders_enriched.order_amount ──rename──▶ fct_orders.amount
```

**fct_orders.status**:

```text
raw_orders.status ──copy──▶ stg_orders.status
stg_orders.status ──copy──▶ int_orders_enriched.status
int_orders_enriched.status ──copy──▶ fct_orders.status
```

**fct_orders.payment_total**:

```text
raw_payments.amount ──transform(cents_to_dollars)──▶ stg_payments.amount
stg_payments.amount ──transform(coalesce+sum)──▶ int_orders_enriched.payment_total
int_orders_enriched.payment_total ──copy──▶ fct_orders.payment_total
```

**fct_orders.payment_count**:

```text
raw_payments.id ──rename──▶ stg_payments.payment_id
stg_payments.payment_id ──transform(count)──▶ int_orders_enriched.payment_count
int_orders_enriched.payment_count ──copy──▶ fct_orders.payment_count
```

**fct_orders.balance_due** (multi-source transform):

```text
Path A:
  raw_orders.amount ──copy──▶ stg_orders.amount
  stg_orders.amount ──rename──▶ int_orders_enriched.order_amount
  int_orders_enriched.order_amount ──transform(subtraction)──▶ fct_orders.balance_due

Path B:
  raw_payments.amount ──transform(cents_to_dollars)──▶ stg_payments.amount
  stg_payments.amount ──transform(coalesce+sum)──▶ int_orders_enriched.payment_total
  int_orders_enriched.payment_total ──transform(subtraction)──▶ fct_orders.balance_due
```

**fct_orders.payment_ratio** (multi-source transform):

```text
Path A:
  raw_payments.amount ──transform(cents_to_dollars)──▶ stg_payments.amount
  stg_payments.amount ──transform(coalesce+sum)──▶ int_orders_enriched.payment_total
  int_orders_enriched.payment_total ──transform(safe_divide)──▶ fct_orders.payment_ratio

Path B:
  raw_orders.amount ──copy──▶ stg_orders.amount
  stg_orders.amount ──rename──▶ int_orders_enriched.order_amount
  int_orders_enriched.order_amount ──transform(safe_divide)──▶ fct_orders.payment_ratio
```

#### dim_products

**dim_products.product_id**:

```text
raw_products.id ──rename──▶ stg_products.product_id
stg_products.product_id ──copy──▶ dim_products.product_id
```

**dim_products.product_name**:

```text
raw_products.name ──rename──▶ stg_products.product_name
stg_products.product_name ──copy──▶ dim_products.product_name
```

**dim_products.category**:

```text
raw_products.category ──copy──▶ stg_products.category
stg_products.category ──copy──▶ dim_products.category
```

**dim_products.price**:

```text
raw_products.price ──transform(cast)──▶ stg_products.price
stg_products.price ──copy──▶ dim_products.price
```

**dim_products.category_group**:

```text
raw_products.category ──copy──▶ stg_products.category
stg_products.category ──transform(case)──▶ dim_products.category_group
```

**dim_products.price_tier**:

```text
raw_products.price ──transform(cast)──▶ stg_products.price
stg_products.price ──transform(case)──▶ dim_products.price_tier
```

#### dim_products_extended

**dim_products_extended.product_id**:

```text
raw_products.id ──rename──▶ stg_products.product_id
stg_products.product_id ──copy──▶ dim_products_extended.product_id
```

**dim_products_extended.product_name**:

```text
raw_products.name ──rename──▶ stg_products.product_name
stg_products.product_name ──copy──▶ dim_products_extended.product_name
```

**dim_products_extended.category**:

```text
raw_products.category ──copy──▶ stg_products.category
stg_products.category ──copy──▶ dim_products_extended.category
```

**dim_products_extended.price**:

```text
raw_products.price ──transform(cast)──▶ stg_products.price
stg_products.price ──copy──▶ dim_products_extended.price
```

**dim_products_extended.id_scaled**:

```text
raw_products.id ──rename──▶ stg_products.product_id
stg_products.product_id ──transform(cast+multiply)──▶ dim_products_extended.id_scaled
```

**dim_products_extended.detailed_category** (multi-source transform):

```text
Path A:
  raw_products.category ──copy──▶ stg_products.category
  stg_products.category ──transform(nested case)──▶ dim_products_extended.detailed_category

Path B:
  raw_products.price ──transform(cast)──▶ stg_products.price
  stg_products.price ──transform(nested case)──▶ dim_products_extended.detailed_category
```

#### rpt_customer_orders

**rpt_customer_orders.customer_id**:

```text
raw_customers.id ──rename──▶ stg_customers.customer_id
stg_customers.customer_id ──copy──▶ rpt_customer_orders.customer_id
```

**rpt_customer_orders.customer_name**:

```text
raw_customers.name ──rename──▶ stg_customers.customer_name
stg_customers.customer_name ──copy──▶ rpt_customer_orders.customer_name
```

**rpt_customer_orders.email**:

```text
raw_customers.email ──copy──▶ stg_customers.email
stg_customers.email ──copy──▶ rpt_customer_orders.email
```

**rpt_customer_orders.order_id**:

```text
raw_orders.id ──rename──▶ stg_orders.order_id
stg_orders.order_id ──copy──▶ int_orders_enriched.order_id
int_orders_enriched.order_id ──copy──▶ rpt_customer_orders.order_id
```

**rpt_customer_orders.order_amount**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──rename──▶ int_orders_enriched.order_amount
int_orders_enriched.order_amount ──copy──▶ rpt_customer_orders.order_amount
```

**rpt_customer_orders.payment_total**:

```text
raw_payments.amount ──transform(cents_to_dollars)──▶ stg_payments.amount
stg_payments.amount ──transform(coalesce+sum)──▶ int_orders_enriched.payment_total
int_orders_enriched.payment_total ──copy──▶ rpt_customer_orders.payment_total
```

**rpt_customer_orders.balance_with_fee** (multi-source transform):

```text
Path A:
  raw_orders.amount ──copy──▶ stg_orders.amount
  stg_orders.amount ──rename──▶ int_orders_enriched.order_amount
  int_orders_enriched.order_amount ──transform(expression)──▶ rpt_customer_orders.balance_with_fee

Path B:
  raw_payments.amount ──transform(cents_to_dollars)──▶ stg_payments.amount
  stg_payments.amount ──transform(coalesce+sum)──▶ int_orders_enriched.payment_total
  int_orders_enriched.payment_total ──transform(expression)──▶ rpt_customer_orders.balance_with_fee
```

**rpt_customer_orders.combined_metric** (multi-source transform):

```text
Path A: int_orders_enriched.order_amount (same chain as balance_with_fee Path A)
Path B: int_orders_enriched.payment_total (same chain as balance_with_fee Path B)
Path C:
  raw_payments.id ──rename──▶ stg_payments.payment_id
  stg_payments.payment_id ──transform(count)──▶ int_orders_enriched.payment_count
  int_orders_enriched.payment_count ──transform(expression)──▶ rpt_customer_orders.combined_metric
```

#### int_customer_ranking (the original bug report)

**int_customer_ranking.customer_id**:

```text
raw_customers.id ──rename──▶ stg_customers.customer_id
stg_customers.customer_id ──copy──▶ int_customer_ranking.customer_id
```

**int_customer_ranking.customer_name**:

```text
raw_customers.name ──rename──▶ stg_customers.customer_name
stg_customers.customer_name ──copy──▶ int_customer_ranking.customer_name
```

**int_customer_ranking.lifetime_value**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──transform(coalesce+sum)──▶ int_customer_metrics.lifetime_value
int_customer_metrics.lifetime_value ──copy──▶ int_customer_ranking.lifetime_value
```

**int_customer_ranking.value_or_zero**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──transform(coalesce+sum)──▶ int_customer_metrics.lifetime_value
int_customer_metrics.lifetime_value ──transform(coalesce)──▶ int_customer_ranking.value_or_zero
```

**int_customer_ranking.nonzero_orders**:

```text
raw_orders.id ──rename──▶ stg_orders.order_id
stg_orders.order_id ──transform(count)──▶ int_customer_metrics.total_orders
int_customer_metrics.total_orders ──transform(nullif)──▶ int_customer_ranking.nonzero_orders
```

---

### Downstream Trace Test Cases

The test harness should also verify `column_consumers_recursive`. Key downstream traces:

**raw_customers.id downstream**:

```text
raw_customers.id ──rename──▶ stg_customers.customer_id
stg_customers.customer_id ──copy──▶ int_customer_metrics.customer_id
stg_customers.customer_id ──copy──▶ int_customer_ranking.customer_id
stg_customers.customer_id ──copy──▶ rpt_customer_orders.customer_id
stg_customers.customer_id ──inspect──▶ dim_customers (JOIN ON)
stg_customers.customer_id ──inspect──▶ fct_orders (JOIN ON)
int_customer_metrics.customer_id ──copy──▶ dim_customers.customer_id
int_customer_metrics.customer_id ──inspect──▶ int_customer_ranking (JOIN ON)
```

**raw_orders.amount downstream**:

```text
raw_orders.amount ──copy──▶ stg_orders.amount
stg_orders.amount ──rename──▶ int_orders_enriched.order_amount
stg_orders.amount ──transform──▶ int_customer_metrics.lifetime_value
stg_orders.amount ──transform──▶ int_high_value_orders.total_amount
stg_orders.amount ──transform──▶ int_high_value_orders.min_order
stg_orders.amount ──transform──▶ int_high_value_orders.max_order
stg_orders.amount ──transform──▶ int_high_value_orders.avg_order
stg_orders.amount ──rename──▶ int_all_orders.order_amount (B2)
stg_orders.amount ──inspect──▶ rpt_customer_orders (WHERE)
int_orders_enriched.order_amount ──rename──▶ fct_orders.amount
int_orders_enriched.order_amount ──transform──▶ fct_orders.balance_due
int_orders_enriched.order_amount ──transform──▶ fct_orders.payment_ratio
int_orders_enriched.order_amount ──copy──▶ int_all_orders.order_amount (B1)
int_orders_enriched.order_amount ──copy──▶ rpt_customer_orders.order_amount
int_orders_enriched.order_amount ──transform──▶ rpt_customer_orders.balance_with_fee
int_orders_enriched.order_amount ──transform──▶ rpt_customer_orders.combined_metric
int_customer_metrics.lifetime_value ──copy──▶ dim_customers.lifetime_value
int_customer_metrics.lifetime_value ──transform──▶ dim_customers.computed_tier
int_customer_metrics.lifetime_value ──copy──▶ int_customer_ranking.lifetime_value
int_customer_metrics.lifetime_value ──transform──▶ int_customer_ranking.value_or_zero
```

---

### Edge Count Summary

Expected total SELECT edges per model (for test assertions):

| Model                  | SELECT Edges | Inspect Edges | Notes                          |
| ---------------------- | ------------ | ------------- | ------------------------------ |
| stg_customers          | 5            | 0             |                                |
| stg_orders             | 5            | 0             |                                |
| stg_payments           | 3            | 0             |                                |
| stg_payments_star      | 5            | 0             | SELECT * expansion             |
| stg_products           | 5            | 0             |                                |
| int_customer_metrics   | 5            | 1             |                                |
| int_customer_ranking   | 5            | 1             |                                |
| int_orders_enriched    | 7            | 1             |                                |
| int_high_value_orders  | 6            | 0             |                                |
| int_all_orders         | 10           | 0             | 5 cols x 2 branches, +2 literals |
| dim_customers          | 8            | 1             |                                |
| dim_products           | 6            | 1             |                                |
| dim_products_extended  | 7            | 0             | detailed_category has 2 sources|
| fct_orders             | 13           | 1             | balance_due, payment_ratio multi-source |
| rpt_customer_orders    | 11           | 3             | balance_with_fee, combined_metric multi-source |
| rpt_order_volume       | 3            | 0             | Table function boundary        |
| **TOTAL**              | **104**      | **8**         |                                |

Note: int_all_orders `source` column (literal) has no upstream model edge — it's 2 literal transforms. The 10 count includes 5 columns x 2 branches for the non-literal columns, but `source` has 0 model edges (just literals). Adjust to **10 edges from models** (5 per branch) + 0 for literals = 10 model edges. The 2 literal entries would be transform edges with no source_model.

---

### Special Cases for Test Coverage

#### 1. Unqualified Column References (Phase 1 fix)

Models where SQL uses bare column names without table prefix. The column qualification pass must resolve these using `source_tables`:

- **stg_customers**: all 5 columns are unqualified (`id`, `name`, `email`, `created_at`, `tier`) — single source table `raw_customers`
- **stg_orders**: all 5 columns are unqualified — single source table `raw_orders`
- **stg_payments**: all 3 columns are unqualified — single source table `raw_payments`
- **stg_products**: all 5 columns are unqualified — single source table `raw_products`
- **stg_payments_star**: `SELECT *` — requires schema expansion
- **dim_products**: `product_id`, `product_name`, `category`, `price`, `active` are unqualified — single source table `stg_products`
- **dim_products_extended**: same as dim_products — unqualified, single source
- **int_all_orders** branch 1: `order_id`, `customer_id`, `order_date`, `order_amount`, `status` are unqualified — single source table `int_orders_enriched`

#### 2. Seeds and Sources in known_nodes (Phase 1 fix)

These models reference seeds/sources that must be in `known_nodes`:

- **stg_customers** → `raw_customers` (seed)
- **stg_orders** → `raw_orders` (seed)
- **stg_payments** → `raw_payments` (seed)
- **stg_payments_star** → `raw_payments` (seed)
- **stg_products** → `raw_products` (seed)

#### 3. SELECT * Expansion (Phase 2)

- **stg_payments_star**: `SELECT * FROM raw_payments` must expand to 5 columns using `SchemaRegistry` for `raw_payments`: `id`, `order_id`, `payment_method`, `amount`, `created_at`

#### 4. UNION ALL (positional matching)

- **int_all_orders**: columns matched positionally across branches. Branch 2 `amount` maps to output `order_amount` (rename). Test must verify both branches produce edges.

#### 5. Multi-Source Transform Columns

Columns with edges from 2+ source columns:

- **fct_orders.balance_due**: `order_amount - payment_total`
- **fct_orders.payment_ratio**: `safe_divide(payment_total, order_amount)`
- **rpt_customer_orders.balance_with_fee**: `(order_amount - payment_total) * 1.1`
- **rpt_customer_orders.combined_metric**: `order_amount + payment_total + payment_count`
- **dim_products_extended.detailed_category**: `CASE WHEN category ... WHEN price ...`

#### 6. Table Function Boundary

- **rpt_order_volume**: lineage stops at `order_volume_by_status` function output. Does not trace through to `fct_orders` (future enhancement).

#### 7. Jinja-Rendered Function Call

- **stg_payments**: `{{ cents_to_dollars("amount") }}` renders to `amount / 100.0` — the lineage must be extracted from the **rendered** SQL, not the Jinja template.

#### 8. Inspect-Only Columns

Columns used in WHERE/JOIN/GROUP BY but NOT in SELECT:

| Model               | Inspect Column                     | Source Model        | Used In |
| -------------------- | ---------------------------------- | ------------------- | ------- |
| int_customer_metrics | stg_orders.customer_id             | stg_orders          | JOIN ON |
| int_customer_ranking | int_customer_metrics.customer_id   | int_customer_metrics| JOIN ON |
| int_orders_enriched  | stg_payments.order_id              | stg_payments        | JOIN ON |
| dim_customers        | stg_customers.customer_id          | stg_customers       | JOIN ON |
| dim_products         | stg_products.active                | stg_products        | WHERE   |
| fct_orders           | stg_customers.customer_id          | stg_customers       | JOIN ON |
| rpt_customer_orders  | int_orders_enriched.customer_id    | int_orders_enriched | JOIN ON |
| rpt_customer_orders  | stg_orders.order_id                | stg_orders          | JOIN ON |
| rpt_customer_orders  | stg_orders.amount                  | stg_orders          | WHERE   |

---

### Test Assertion Format

Each test should assert against the per-model edge matrix above. Suggested Rust test structure:

```rust
#[test]
fn test_stg_customers_lineage_edges() {
    let project_lineage = build_sample_project_lineage();

    // SELECT edges
    assert_edge(&project_lineage, "raw_customers", "id", "stg_customers", "customer_id", LineageKind::Rename);
    assert_edge(&project_lineage, "raw_customers", "name", "stg_customers", "customer_name", LineageKind::Rename);
    assert_edge(&project_lineage, "raw_customers", "email", "stg_customers", "email", LineageKind::Copy);
    assert_edge(&project_lineage, "raw_customers", "created_at", "stg_customers", "signup_date", LineageKind::Rename);
    assert_edge(&project_lineage, "raw_customers", "tier", "stg_customers", "customer_tier", LineageKind::Rename);

    // No inspect edges
    assert_no_inspect_edges(&project_lineage, "stg_customers");

    // Edge count
    assert_select_edge_count(&project_lineage, "stg_customers", 5);
}

#[test]
fn test_full_upstream_chain_dim_customers_customer_id() {
    let project_lineage = build_sample_project_lineage();
    let chain = project_lineage.trace_column_recursive("dim_customers", "customer_id");

    assert_eq!(chain.len(), 3);
    assert_chain_contains(&chain, "raw_customers", "id", "stg_customers", "customer_id", LineageKind::Rename);
    assert_chain_contains(&chain, "stg_customers", "customer_id", "int_customer_metrics", "customer_id", LineageKind::Copy);
    assert_chain_contains(&chain, "int_customer_metrics", "customer_id", "dim_customers", "customer_id", LineageKind::Copy);
}

#[test]
fn test_description_status_tracking() {
    let project_lineage = build_sample_project_lineage();

    // Inherited: exact match
    assert_desc_status(&project_lineage, "int_customer_metrics", "lifetime_value", "dim_customers", "lifetime_value", DescriptionStatus::Inherited);

    // Modified: both exist, differ
    assert_desc_status(&project_lineage, "stg_customers", "customer_name", "dim_customers", "customer_name", DescriptionStatus::Modified);

    // Missing: source has no description
    assert_desc_status(&project_lineage, "raw_customers", "id", "stg_customers", "customer_id", DescriptionStatus::Missing);
}
```
