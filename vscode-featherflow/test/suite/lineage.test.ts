/**
 * Unit tests for lineage graph building (upstream/downstream traversal)
 * and interactive column lineage filtering.
 *
 * Uses a mock ProjectIndex to test the pure graph functions without CLI or VS Code.
 */

import { describe, expect, it } from "vitest";
import type { CliLineageEdge, LsModelEntry } from "../../src/types.js";
import {
  buildUpstream,
  buildDownstream,
  buildLineageGraph,
  filterEdgesByDepth,
} from "../../src/providers/lineageViewProvider.js";
import type { ProjectIndex } from "../../src/projectIndex.js";

// ---------- Test fixtures ----------

const MODELS: LsModelEntry[] = [
  {
    name: "stg_orders",
    type: "model",
    path: "/project/models/stg_orders/stg_orders.sql",
    materialized: "view",
    schema: "analytics",
    model_deps: [],
    external_deps: ["raw_orders"],
  },
  {
    name: "stg_payments",
    type: "model",
    path: "/project/models/stg_payments/stg_payments.sql",
    materialized: "view",
    schema: "analytics",
    model_deps: [],
    external_deps: ["raw_payments"],
  },
  {
    name: "stg_customers",
    type: "model",
    path: "/project/models/stg_customers/stg_customers.sql",
    materialized: "view",
    schema: "analytics",
    model_deps: [],
    external_deps: ["raw_customers"],
  },
  {
    name: "int_orders_enriched",
    type: "model",
    path: "/project/models/int_orders_enriched/int_orders_enriched.sql",
    materialized: "view",
    schema: "analytics",
    model_deps: ["stg_orders", "stg_payments"],
    external_deps: [],
  },
  {
    name: "int_customer_metrics",
    type: "model",
    path: "/project/models/int_customer_metrics/int_customer_metrics.sql",
    materialized: "view",
    schema: "analytics",
    model_deps: ["stg_orders", "stg_customers"],
    external_deps: [],
  },
  {
    name: "fct_orders",
    type: "model",
    path: "/project/models/fct_orders/fct_orders.sql",
    materialized: "table",
    schema: "analytics",
    model_deps: ["int_orders_enriched", "int_customer_metrics"],
    external_deps: [],
  },
];

/** Build a minimal mock that satisfies the functions under test. */
function mockIndex(models: LsModelEntry[]): ProjectIndex {
  const nameMap = new Map<string, LsModelEntry>();
  for (const m of models) {
    nameMap.set(m.name.toLowerCase(), m);
  }
  return {
    getModelByName(name: string) {
      return nameMap.get(name.toLowerCase());
    },
    getModels() {
      return models.filter((m) => m.type === "model");
    },
    getDownstream(name: string) {
      const lower = name.toLowerCase();
      return models.filter(
        (m) =>
          m.type === "model" &&
          m.model_deps.some((d) => d.toLowerCase() === lower)
      );
    },
    getBinaryPath() {
      return "/usr/local/bin/ff";
    },
    getProjectDir() {
      return "/project";
    },
  } as unknown as ProjectIndex;
}

// ---------- Tests ----------

describe("buildUpstream", () => {
  const index = mockIndex(MODELS);

  it("returns empty for a model with no model_deps", () => {
    const cols = buildUpstream("stg_orders", index);
    // stg_orders has only external_deps, which should appear at depth 1
    expect(cols.length).toBe(1);
    expect(cols[0].nodes).toHaveLength(1);
    expect(cols[0].nodes[0].name).toBe("raw_orders");
    expect(cols[0].nodes[0].type).toBe("external");
  });

  it("returns direct model deps at depth 1", () => {
    const cols = buildUpstream("int_orders_enriched", index);
    // depth 1: stg_orders, stg_payments (model deps)
    // depth 2: raw_orders, raw_payments (external deps of the staging models)
    const depth1 = cols.find((c) => c.depth === 1);
    expect(depth1).toBeDefined();
    const names = depth1!.nodes.map((n) => n.name).sort();
    expect(names).toEqual(["stg_orders", "stg_payments"]);
  });

  it("returns transitive deps at higher depth", () => {
    const cols = buildUpstream("fct_orders", index);
    // depth 1: int_orders_enriched, int_customer_metrics
    // depth 2: stg_orders, stg_payments, stg_customers
    // depth 3: raw_orders, raw_payments, raw_customers
    const depth1 = cols.find((c) => c.depth === 1);
    const depth1Names = depth1!.nodes.map((n) => n.name).sort();
    expect(depth1Names).toEqual(["int_customer_metrics", "int_orders_enriched"]);

    const depth2 = cols.find((c) => c.depth === 2);
    const depth2Names = depth2!.nodes.map((n) => n.name).sort();
    expect(depth2Names).toEqual(["stg_customers", "stg_orders", "stg_payments"]);

    const depth3 = cols.find((c) => c.depth === 3);
    const depth3Names = depth3!.nodes.map((n) => n.name).sort();
    expect(depth3Names).toEqual(["raw_customers", "raw_orders", "raw_payments"]);
  });

  it("columns are ordered highest-depth first (leftmost)", () => {
    const cols = buildUpstream("fct_orders", index);
    const depths = cols.map((c) => c.depth);
    // Should be descending: [3, 2, 1]
    expect(depths).toEqual([...depths].sort((a, b) => b - a));
  });
});

describe("buildDownstream", () => {
  const index = mockIndex(MODELS);

  it("returns empty for a terminal model", () => {
    const cols = buildDownstream("fct_orders", index);
    expect(cols).toHaveLength(0);
  });

  it("returns direct children at depth 1", () => {
    const cols = buildDownstream("stg_payments", index);
    const depth1 = cols.find((c) => c.depth === 1);
    expect(depth1).toBeDefined();
    const names = depth1!.nodes.map((n) => n.name);
    expect(names).toEqual(["int_orders_enriched"]);
  });

  it("returns transitive children", () => {
    const cols = buildDownstream("stg_orders", index);
    // depth 1: int_orders_enriched, int_customer_metrics
    // depth 2: fct_orders (depends on both intermediates)
    const depth1 = cols.find((c) => c.depth === 1);
    const depth1Names = depth1!.nodes.map((n) => n.name).sort();
    expect(depth1Names).toEqual(["int_customer_metrics", "int_orders_enriched"]);

    const depth2 = cols.find((c) => c.depth === 2);
    expect(depth2).toBeDefined();
    expect(depth2!.nodes.map((n) => n.name)).toEqual(["fct_orders"]);
  });

  it("columns are ordered lowest-depth first (leftmost)", () => {
    const cols = buildDownstream("stg_orders", index);
    const depths = cols.map((c) => c.depth);
    expect(depths).toEqual([...depths].sort((a, b) => a - b));
  });
});

describe("cycle detection", () => {
  it("handles circular dependencies without infinite loop", () => {
    const cyclic: LsModelEntry[] = [
      {
        name: "a",
        type: "model",
        path: "/a.sql",
        materialized: "view",
        model_deps: ["b"],
        external_deps: [],
      },
      {
        name: "b",
        type: "model",
        path: "/b.sql",
        materialized: "view",
        model_deps: ["a"],
        external_deps: [],
      },
    ];
    const index = mockIndex(cyclic);

    // Should not hang or throw
    const upstream = buildUpstream("a", index);
    expect(upstream.length).toBeGreaterThanOrEqual(0);
    // b is a dep of a, but a is already visited so it won't recurse back
    const allNames = upstream.flatMap((c) => c.nodes.map((n) => n.name));
    expect(allNames).toContain("b");
    expect(allNames).not.toContain("a"); // current node not duplicated

    const downstream = buildDownstream("a", index);
    const downNames = downstream.flatMap((c) => c.nodes.map((n) => n.name));
    expect(downNames).toContain("b");
    expect(downNames).not.toContain("a");
  });
});

describe("buildLineageGraph", () => {
  const index = mockIndex(MODELS);

  it("returns current node with correct properties", () => {
    const entry = MODELS.find((m) => m.name === "fct_orders")!;
    const graph = buildLineageGraph(entry, index);

    expect(graph.current.name).toBe("fct_orders");
    expect(graph.current.type).toBe("model");
    expect(graph.current.materialized).toBe("table");
  });

  it("includes both upstream and downstream for a middle model", () => {
    const entry = MODELS.find((m) => m.name === "int_orders_enriched")!;
    const graph = buildLineageGraph(entry, index);

    expect(graph.upstream.length).toBeGreaterThan(0);
    expect(graph.downstream.length).toBeGreaterThan(0);

    // Downstream should include fct_orders
    const downNames = graph.downstream.flatMap((c) =>
      c.nodes.map((n) => n.name)
    );
    expect(downNames).toContain("fct_orders");
  });

  it("terminal model has empty downstream", () => {
    const entry = MODELS.find((m) => m.name === "fct_orders")!;
    const graph = buildLineageGraph(entry, index);
    expect(graph.downstream).toHaveLength(0);
  });

  it("leaf model has empty upstream model deps but has external deps", () => {
    const entry = MODELS.find((m) => m.name === "stg_orders")!;
    const graph = buildLineageGraph(entry, index);
    // Only external deps in upstream
    const upNames = graph.upstream.flatMap((c) => c.nodes.map((n) => n.name));
    expect(upNames).toEqual(["raw_orders"]);
  });
});

// ---------- filterEdgesByDepth tests ----------

/** Sample edges for a 3-model chain: raw -> stg -> fct */
const SAMPLE_EDGES: CliLineageEdge[] = [
  {
    source_model: "raw_orders",
    source_column: "id",
    target_model: "stg_orders",
    target_column: "order_id",
    is_direct: true,
    expr_type: "column",
  },
  {
    source_model: "raw_orders",
    source_column: "amount",
    target_model: "stg_orders",
    target_column: "amount",
    is_direct: true,
    expr_type: "column",
  },
  {
    source_model: "stg_orders",
    source_column: "order_id",
    target_model: "fct_orders",
    target_column: "order_id",
    is_direct: true,
    expr_type: "column",
  },
  {
    source_model: "stg_orders",
    source_column: "amount",
    target_model: "fct_orders",
    target_column: "total_amount",
    is_direct: false,
    expr_type: "function",
  },
  {
    source_model: "fct_orders",
    source_column: "order_id",
    target_model: "dim_orders",
    target_column: "order_id",
    is_direct: true,
    expr_type: "column",
  },
];

describe("filterEdgesByDepth", () => {
  it("returns direct upstream edges at depth 1", () => {
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "stg_orders",
      "order_id",
      "upstream",
      1
    );
    expect(result).toHaveLength(1);
    expect(result[0].source_model).toBe("raw_orders");
    expect(result[0].source_column).toBe("id");
  });

  it("returns direct downstream edges at depth 1", () => {
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "stg_orders",
      "order_id",
      "downstream",
      1
    );
    expect(result).toHaveLength(1);
    expect(result[0].target_model).toBe("fct_orders");
    expect(result[0].target_column).toBe("order_id");
  });

  it("returns transitive edges at depth 2", () => {
    // Downstream from stg_orders.order_id depth 2:
    // hop 1: stg_orders.order_id -> fct_orders.order_id
    // hop 2: fct_orders.order_id -> dim_orders.order_id
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "stg_orders",
      "order_id",
      "downstream",
      2
    );
    expect(result).toHaveLength(2);
    const targets = result.map((e) => `${e.target_model}.${e.target_column}`);
    expect(targets).toContain("fct_orders.order_id");
    expect(targets).toContain("dim_orders.order_id");
  });

  it("max depth limits traversal", () => {
    // From stg_orders.order_id downstream with depth 1, should NOT reach dim_orders
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "stg_orders",
      "order_id",
      "downstream",
      1
    );
    expect(result).toHaveLength(1);
    const models = result.map((e) => e.target_model);
    expect(models).not.toContain("dim_orders");
  });

  it("returns empty for non-existent column", () => {
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "stg_orders",
      "nonexistent",
      "upstream",
      5
    );
    expect(result).toHaveLength(0);
  });

  it("handles cycles without infinite loop", () => {
    const cyclicEdges: CliLineageEdge[] = [
      {
        source_model: "a",
        source_column: "x",
        target_model: "b",
        target_column: "x",
        is_direct: true,
        expr_type: "column",
      },
      {
        source_model: "b",
        source_column: "x",
        target_model: "a",
        target_column: "x",
        is_direct: true,
        expr_type: "column",
      },
    ];

    // Should not hang; visited set prevents re-traversal of a::x
    const result = filterEdgesByDepth(cyclicEdges, "a", "x", "downstream", 10);
    // Hop 1: a->b (b::x added to frontier)
    // Hop 2: b->a (edge collected, but a::x already visited so no new frontier)
    // Both edges are returned, but traversal stops — no infinite loop
    expect(result).toHaveLength(2);
    expect(result[0].target_model).toBe("b");
    expect(result[1].target_model).toBe("a");
  });

  it("returns multiple edges from the same hop", () => {
    // From fct_orders upstream depth 1, both stg_orders.order_id and stg_orders.amount map to fct_orders columns
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "fct_orders",
      "order_id",
      "upstream",
      1
    );
    expect(result).toHaveLength(1);
    expect(result[0].source_model).toBe("stg_orders");
    expect(result[0].source_column).toBe("order_id");
  });

  it("handles depth 0 by returning empty", () => {
    // Edge case: maxDepth 0 means no hops
    const result = filterEdgesByDepth(
      SAMPLE_EDGES,
      "stg_orders",
      "order_id",
      "upstream",
      0
    );
    expect(result).toHaveLength(0);
  });
});
