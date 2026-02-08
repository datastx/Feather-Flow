# Interactive Documentation Site

## Overview

`ff docs serve` launches a local interactive documentation site that visualizes the entire project: model DAG, column-level lineage, schema documentation, test coverage, and compiled SQL. It reuses the same project loading and compilation pipeline as `ff compile`.

The site ships embedded in the `ff` binary. No Node.js, no npm, no external dependencies. One command, instant documentation.

```bash
# Generate and serve docs on localhost:4040
ff docs serve

# Compile specific models, then serve
ff docs serve --select fct_orders,dim_customers

# Custom port
ff docs serve --port 8080

# Generate static site to disk (no server)
ff docs serve --static-export ./site
```

## Why dbt Docs Is Slow (And What We Do Differently)

dbt's documentation site has well-documented performance problems that get worse as projects grow. Understanding these failures is critical because they inform every design decision below.

### dbt's Architecture

```
dbt docs generate
  → queries INFORMATION_SCHEMA (minutes on large warehouses)
  → writes manifest.json (58 MB for GitLab's project)
  → writes catalog.json (warehouse metadata)
  → copies Angular.js 1.x SPA as index.html

dbt docs serve
  → starts HTTP server
  → browser downloads ALL of manifest.json + catalog.json
  → Angular.js parses everything into memory (~350 MB RAM per tab)
  → dagre computes layout for entire graph
  → Cytoscape.js renders every node (SVG)
```

### The Problems

| Problem | Cause | Impact |
|---------|-------|--------|
| 48-second initial load | 58 MB manifest.json parsed client-side | Users give up, stop using docs |
| 350 MB RAM per tab | Entire project in browser memory | Browser crashes on 1000+ models |
| Graph freezes on selection | Full subgraph re-rendered without culling | 2-3 second freezes, unresponsive UI |
| No column-level lineage | Graph only shows model edges | Users cannot trace data flow |
| Search race condition | Client-side index built asynchronously | Queries fail during load |
| Stale framework | Angular.js 1.x (EOL) | No modern web features, no active maintenance |

### Our Approach

| dbt | Feather-Flow |
|-----|-------------|
| 58 MB monolithic JSON | Split manifest: ~5 KB index + per-model JSON on demand |
| Angular.js 1.x SPA | Vanilla JS + Preact (3 KB) for interactive components |
| dagre layout (main thread) | ELK.js layout in Web Worker |
| Cytoscape.js SVG (all nodes) | Canvas rendering with viewport culling |
| No column lineage in graph | Column-level lineage is the default view |
| Warehouse queries for catalog | DuckDB is local -- schema introspection is instant |
| Separate generate + serve | Single command, data generated on startup |

## Architecture

### System Diagram

```
ff docs serve
  │
  ├─ 1. Load Project (same as ff compile)
  │     Config → Models → Jinja render → SQL parse → DAG build
  │
  ├─ 2. Generate Documentation Data
  │     For each model (topological order):
  │       - Schema from .yml (columns, tests, descriptions)
  │       - Compiled SQL
  │       - Column lineage from AST
  │       - Test suggestions
  │       - Static analysis diagnostics (optional)
  │     Build ProjectLineage with cross-model edges
  │
  ├─ 3. Build API Payloads
  │     /api/index.json      → model names, edges, basic metadata (~5 KB)
  │     /api/models/{name}   → full model detail (columns, SQL, lineage)
  │     /api/lineage.json    → full ProjectLineage with edges
  │     /api/search-index    → precomputed search index
  │
  └─ 4. Serve
        Embedded HTTP server (axum)
        Static assets compiled into binary (rust-embed)
        API endpoints serve generated JSON
        Browser loads index, fetches details on demand
```

### What Ships in the Binary

The frontend is a set of static files (HTML, JS, CSS) compiled into the Rust binary via `rust-embed` or `include_dir`. No filesystem extraction needed. The embedded HTTP server serves these alongside generated API responses.

```
static/
  index.html          # Shell page, loads app.js
  app.js              # Main application (~50 KB gzipped target)
  graph-worker.js     # ELK.js layout in Web Worker
  style.css           # Styles
```

### Rust Dependencies

| Crate | Purpose |
|-------|---------|
| `axum` | Embedded HTTP server (already in tokio ecosystem) |
| `rust-embed` | Compile static assets into binary |
| `tower-http` | Compression middleware (gzip responses) |

These are dev/optional dependencies gated behind a `docs-serve` feature flag so they do not bloat the binary for users who never use `ff docs serve`.

## Data Model

### Index Payload (`/api/index.json`)

Loaded on startup. Contains everything needed to render the DAG overview and search. Target size: under 50 KB for a 500-model project.

```json
{
  "project": "my_project",
  "generated_at": "2025-01-15T10:30:00Z",
  "models": [
    {
      "name": "fct_orders",
      "description": "Fact table for completed orders",
      "materialization": "table",
      "schema": "analytics",
      "tags": ["finance", "core"],
      "owner": "data-eng",
      "column_count": 12,
      "test_count": 8,
      "has_diagnostics": true,
      "depends_on": ["stg_orders", "stg_payments"],
      "dependents": ["rpt_revenue"]
    }
  ],
  "sources": [
    {
      "name": "raw_orders",
      "schema": "raw",
      "source_name": "raw",
      "description": "Raw order events from ETL"
    }
  ],
  "edges": [
    { "from": "stg_orders", "to": "fct_orders" },
    { "from": "stg_payments", "to": "fct_orders" }
  ],
  "stats": {
    "model_count": 47,
    "source_count": 5,
    "edge_count": 83,
    "test_count": 156
  }
}
```

### Model Detail Payload (`/api/models/{name}.json`)

Loaded on demand when a user clicks a model. Contains full schema, SQL, lineage, and diagnostics.

```json
{
  "name": "fct_orders",
  "description": "Fact table for completed orders with payment aggregation",
  "materialization": "table",
  "schema": "analytics",
  "owner": "data-eng",
  "tags": ["finance", "core"],
  "sql": {
    "raw": "SELECT\n  o.order_id,\n  ...",
    "compiled": "SELECT\n  o.order_id,\n  ..."
  },
  "columns": [
    {
      "name": "order_id",
      "data_type": "INTEGER",
      "description": "Unique order identifier",
      "is_nullable": false,
      "tests": ["unique", "not_null"],
      "classification": null,
      "lineage": {
        "sources": [
          { "model": "stg_orders", "column": "order_id" }
        ],
        "is_direct": true,
        "expr_type": "column"
      }
    },
    {
      "name": "total_amount",
      "data_type": "DECIMAL(10,2)",
      "description": "Sum of all payment amounts",
      "is_nullable": true,
      "tests": ["not_null", "positive"],
      "classification": null,
      "lineage": {
        "sources": [
          { "model": "stg_payments", "column": "amount" }
        ],
        "is_direct": false,
        "expr_type": "function"
      }
    }
  ],
  "depends_on": ["stg_orders", "stg_payments"],
  "dependents": ["rpt_revenue"],
  "external_deps": [],
  "diagnostics": [
    {
      "code": "A010",
      "severity": "warning",
      "message": "Column 'discount' from LEFT JOIN is nullable but used without COALESCE",
      "line": 14,
      "column": 8
    }
  ],
  "test_suggestions": [
    {
      "column": "customer_id",
      "test": "relationship",
      "reason": "References dim_customers.customer_id"
    }
  ]
}
```

### Column Lineage Payload (`/api/lineage.json`)

Full cross-model column lineage for the graph visualization. Loaded when the user opens the lineage view.

```json
{
  "edges": [
    {
      "source_model": "stg_orders",
      "source_column": "order_id",
      "target_model": "fct_orders",
      "target_column": "order_id",
      "is_direct": true,
      "expr_type": "column",
      "classification": null
    }
  ]
}
```

## Frontend Design

### Technology Choices

| Choice | Rationale |
|--------|-----------|
| **Vanilla JS + Preact** | 3 KB framework. No build step needed for development. Fast hydration. |
| **Canvas 2D** for graph rendering | 10x faster than SVG for 100+ nodes. Viewport culling is trivial. |
| **ELK.js** for DAG layout | Runs in Web Worker. Better layouts than dagre for complex graphs. Supports layered/hierarchical positioning with configurable spacing. |
| **CSS Grid + custom properties** | Modern layout, dark/light theme via CSS variables, no CSS framework. |
| **No build toolchain** | The JS is written as ES modules. Bundled with a simple `esbuild` invocation (single command, no config file). If we want zero external tools, we can ship unbundled ES modules since this is localhost-only. |

### Page Structure

```
┌─────────────────────────────────────────────────────────┐
│  [ff] Project Name          [Search...]    [Theme] [?]  │
├────────┬────────────────────────────────────────────────┤
│        │                                                │
│ Models │              Graph / Detail View                │
│  ├ stg │                                                │
│  ├ dim │   ┌──────┐     ┌──────────┐     ┌─────────┐   │
│  ├ fct │   │ stg_ │────→│ fct_     │────→│ rpt_    │   │
│  └ rpt │   │orders│     │ orders   │     │ revenue │   │
│        │   └──────┘     └──────────┘     └─────────┘   │
│Sources │                                                │
│  └ raw │                                                │
│        │                                                │
│Tags    │                                                │
│  ├ core│                                                │
│  └ fin │                                                │
│        │                                                │
├────────┴────────────────────────────────────────────────┤
│  47 models · 5 sources · 156 tests · 0 errors           │
└─────────────────────────────────────────────────────────┘
```

### Three Main Views

**1. DAG View (default)**

The full model dependency graph. Nodes are colored by type (model, source) and shaped by materialization (table = rectangle, view = rounded rectangle, incremental = diamond, ephemeral = dashed). Edges show dependency direction.

Interactions:
- **Click node**: Select it, highlight upstream/downstream path, show summary panel
- **Double-click node**: Navigate to detail view
- **Hover node**: Tooltip with name, materialization, column count, test count
- **Scroll to zoom**, drag to pan
- **Selector bar**: Type dbt-style selectors (`+fct_orders+`, `tag:finance`, `stg_*`)
- **Filter toggles**: By materialization, tag, schema, owner
- **Minimap**: Small overview in corner showing current viewport position

Performance targets:
- 500 models: instant layout, 60 FPS interaction
- 2000 models: <2 second layout (Web Worker), 60 FPS interaction with culling
- 10000 models: <5 second layout, cluster/collapse groups automatically

**2. Column Lineage View**

When a model is selected, switch to column-level lineage. Each model node expands to show its columns. Edges connect specific columns across models.

```
┌─────────────┐          ┌──────────────────┐          ┌────────────┐
│ stg_orders  │          │   fct_orders     │          │rpt_revenue │
│─────────────│          │──────────────────│          │────────────│
│ order_id   ─┼─────────→│ order_id         │          │            │
│ customer_id─┼─────────→│ customer_id      │          │            │
│ amount     ─┼────┐     │ total_amount    ─┼─────────→│ revenue    │
└─────────────┘    │     │ discount         │          └────────────┘
                   │     └──────────────────┘
┌─────────────┐    │
│stg_payments │    │
│─────────────│    │
│ amount     ─┼────┘
└─────────────┘
```

This is the killer feature. dbt does not offer this outside of dbt Cloud Enterprise. Feather-Flow already extracts this data from the SQL AST via `extract_column_lineage()`.

Edge styling:
- **Solid line**: Direct pass-through (`is_direct: true`)
- **Dashed line**: Transformation (function, cast, expression)
- **Color coding**: Blue for direct, orange for transformed, red for PII-classified columns

**3. Model Detail View**

Full documentation for a single model, shown as a slide-over panel or full page.

Sections:
- **Header**: Name, materialization badge, schema, owner, tags
- **Description**: Rendered markdown from `.yml`
- **Columns table**: Name, type, nullable, description, tests (as badges), classification
- **SQL**: Raw and compiled SQL with syntax highlighting (highlight.js or Prism, ~15 KB for SQL grammar)
- **Lineage**: Mini column lineage graph showing just this model's inputs/outputs
- **Diagnostics**: Static analysis warnings/errors from `ff analyze` (if enabled)
- **Test suggestions**: Suggested tests from SQL pattern analysis
- **Dependencies**: Clickable links to upstream and downstream models

### Search

Client-side full-text search over model names, descriptions, column names, and tags. The search index is precomputed server-side and served as a compact JSON payload.

Implementation: Use a simple trigram index built in Rust. For a 500-model project with 5000 columns, the index is ~100 KB. Search results appear as-you-type with <10ms latency.

Alternative: Use Fuse.js (~7 KB) for fuzzy matching if we want typo tolerance.

### Theming

Two themes: light and dark. Controlled by CSS custom properties. Respects `prefers-color-scheme` by default with a manual toggle.

```css
:root {
  --bg-primary: #ffffff;
  --bg-secondary: #f8f9fa;
  --text-primary: #1a1a2e;
  --accent: #0094b3;
  --node-model: #0094b3;
  --node-source: #5fb825;
  --edge-direct: #0094b3;
  --edge-transform: #ff9800;
  --edge-pii: #e53935;
}

[data-theme="dark"] {
  --bg-primary: #1a1a2e;
  --bg-secondary: #16213e;
  --text-primary: #e8e8e8;
}
```

## Graph Rendering: Technical Details

### Canvas Renderer

The graph is rendered on an HTML `<canvas>` element. This is fundamentally different from dbt's SVG approach and is what enables smooth interaction at scale.

```
Frame loop (requestAnimationFrame):
  1. Clear canvas
  2. Apply camera transform (pan + zoom)
  3. Compute visible bounds (viewport culling)
  4. For each edge where BOTH endpoints are visible:
     - Draw bezier curve
     - Draw arrowhead (if zoomed in enough)
  5. For each node in visible bounds:
     - Draw shape (rectangle, rounded rect, diamond)
     - Draw label (if zoom level > threshold)
     - Draw column count badge (if zoom level > threshold)
  6. Draw selection highlight
  7. Draw minimap (scaled-down overview)
```

**Viewport culling**: Only nodes whose bounding box intersects the visible viewport are drawn. For a 2000-node graph where the user is zoomed in to see ~50 nodes, we draw 50 nodes instead of 2000. This alone makes the difference between 15 FPS and 60 FPS.

**Level of detail**:
- Zoom < 0.3: Nodes are colored dots, no labels, no edges
- Zoom 0.3-0.7: Nodes are shapes with names, edges are simple lines
- Zoom > 0.7: Full detail -- column counts, test badges, styled edges with arrows

**Hit testing**: Use a quadtree spatial index to find which node the user clicked in O(log n) instead of O(n).

### ELK.js Layout

ELK (Eclipse Layout Kernel) is compiled to WebAssembly and runs in a Web Worker. It produces significantly better layouts than dagre for complex DAGs because it supports:

- Layer assignment with proper crossing minimization
- Configurable node/edge spacing
- Port-based edge routing (needed for column-level lineage)
- Incremental layout (only reposition changed nodes)

```javascript
// graph-worker.js
importScripts('elk.bundled.js');

const elk = new ELK();

onmessage = async (e) => {
  const { nodes, edges, options } = e.data;

  const graph = {
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'RIGHT',
      'elk.layered.spacing.nodeNodeBetweenLayers': 150,
      'elk.spacing.nodeNode': 40,
      'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
    },
    children: nodes,
    edges: edges,
  };

  const layout = await elk.layout(graph);
  postMessage(layout);
};
```

Layout runs off the main thread. The UI stays responsive even for 2000+ node graphs.

## Performance Budget

| Metric | Target | dbt Equivalent |
|--------|--------|----------------|
| Initial page load (500 models) | < 500ms | ~48 seconds |
| Initial page load (2000 models) | < 2 seconds | Browser crash |
| Memory usage (500 models) | < 30 MB | ~350 MB |
| Graph interaction latency | < 16ms (60 FPS) | 2-3 seconds |
| Model detail load | < 100ms | N/A (already loaded) |
| Search result latency | < 10ms | Race condition / seconds |
| Binary size increase | < 500 KB (compressed assets) | N/A |

### How We Hit These Targets

1. **Split payloads**: Index is ~5 KB. Model details loaded on demand. Never load everything at once.
2. **Canvas rendering**: 10x faster than SVG for graphs. GPU-composited by the browser.
3. **Web Worker layout**: ELK runs off main thread. UI never blocks.
4. **Viewport culling**: Only draw visible nodes. O(visible) not O(total).
5. **Precomputed data**: Search index and lineage edges computed in Rust at startup, not in the browser.
6. **No framework overhead**: Preact is 3 KB. No virtual DOM diffing for the graph (direct canvas rendering).
7. **Gzip compression**: All API responses gzipped by tower-http middleware.

## CLI Interface

### New Subcommand

`ff docs serve` becomes a subcommand of the existing `ff docs` command:

```
ff docs serve [OPTIONS]

Options:
  --port <PORT>             Port to serve on (default: 4040)
  --host <HOST>             Host to bind to (default: 127.0.0.1)
  --select <MODELS>         Compile and document specific models
  --exclude <MODELS>        Exclude models
  --no-browser              Do not open browser automatically
  --static-export <PATH>    Export static site to directory (no server)
  --with-analysis           Include static analysis diagnostics
```

The existing `ff docs` command (generates markdown/json/html files) is unchanged. `ff docs serve` is additive.

### Static Export

`--static-export` writes a self-contained static site to disk. All API responses are written as JSON files. The site can be hosted on any static file server (S3, GitHub Pages, nginx) without the `ff` binary.

```
site/
  index.html
  app.js
  graph-worker.js
  style.css
  api/
    index.json
    lineage.json
    search-index.json
    models/
      stg_orders.json
      fct_orders.json
      ...
```

## Feature Comparison

| Feature | dbt docs | Feather-Flow docs serve |
|---------|----------|------------------------|
| Model DAG visualization | Yes (dagre + Cytoscape, SVG) | Yes (ELK + Canvas, WebGL fallback) |
| Column-level lineage graph | No (Cloud Enterprise only) | Yes (from SQL AST, always available) |
| Model detail pages | Yes | Yes |
| Column documentation | Yes | Yes |
| Test coverage display | Yes | Yes + test suggestions |
| Static analysis diagnostics | No | Yes (A001-A033 codes) |
| SQL syntax highlighting | Yes | Yes |
| Search | Slow, client-side race conditions | Precomputed trigram index, instant |
| Dark mode | No | Yes |
| Performance (500 models) | Seconds to load | Sub-second |
| Performance (2000 models) | Browser crash likely | 2-second layout, 60 FPS |
| Column-level lineage with PII tracking | No | Yes (classification propagation) |
| Static export | Partial (needs server for search) | Full (works as static files) |
| Binary size impact | N/A (Python + npm) | ~500 KB compressed |
| External dependencies | Node.js, Angular.js | None (embedded in binary) |

## Implementation Phases

### Phase 1: Foundation

- Add `docs serve` subcommand to CLI
- Add `axum` + `rust-embed` dependencies (feature-gated)
- Build API layer that generates index, model detail, and lineage JSON from existing project data
- Minimal HTML shell with hardcoded CSS
- Basic model list and detail view (no graph yet)

### Phase 2: DAG Graph

- Implement Canvas renderer with pan/zoom/click
- Integrate ELK.js via Web Worker for layout
- Viewport culling and level-of-detail rendering
- Node selection with upstream/downstream highlighting
- Minimap
- Selector bar with dbt-style syntax

### Phase 3: Column Lineage

- Column-level lineage view with expanded model nodes
- Edge styling (direct vs transformed, PII classification)
- Click-through from column lineage to model detail
- Bidirectional tracing (upstream and downstream from any column)

### Phase 4: Polish

- Full-text search with trigram index
- Dark/light theme
- SQL syntax highlighting
- Static export
- Keyboard navigation (arrow keys to traverse graph, `/` to focus search)
- URL routing (deep links to specific models)

## Open Questions

1. **Bundle strategy**: Do we vendor ELK.js (WASM, ~300 KB gzipped) into the binary, or download it on first use? Vendoring is simpler and matches the "no external dependencies" philosophy.

2. **Hot reload**: Should `ff docs serve` watch for file changes and rebuild? This would require a filesystem watcher (`notify` crate) and WebSocket push to the browser. Nice to have but not essential for phase 1.

3. **Static analysis integration**: Should `ff docs serve --with-analysis` run the full analysis pipeline (IR lowering + passes) or just display precomputed results? Running analysis adds latency to startup but guarantees freshness.

4. **Graph rendering library**: The design above assumes a custom Canvas renderer for maximum control and minimal dependencies. An alternative is to use a lightweight graph library like `d3-dag` for layout + custom Canvas rendering. The custom approach is more work but avoids JS dependency management entirely.
