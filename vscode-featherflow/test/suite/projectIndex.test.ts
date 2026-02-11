/**
 * Unit tests for project index JSON parsing and lookup.
 *
 * Tests the data handling logic without the VS Code API or CLI calls.
 */

import { describe, expect, it } from "vitest";
import type { LsModelEntry } from "../../src/types.js";

/** Sample JSON that matches `ff ls --output json` output structure. */
const SAMPLE_JSON: LsModelEntry[] = [
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
    name: "fct_orders",
    type: "model",
    path: "/project/models/fct_orders/fct_orders.sql",
    materialized: "table",
    schema: "analytics",
    model_deps: ["stg_orders", "stg_payments"],
    external_deps: [],
  },
  {
    name: "raw_db.raw_orders",
    type: "source",
    schema: "raw_db",
    model_deps: [],
    external_deps: [],
  },
  {
    name: "my_func",
    type: "function (scalar)",
    path: "/project/functions/my_func/my_func.sql",
    model_deps: [],
    external_deps: [],
  },
];

/** Build a simple name→entry map the same way ProjectIndex does. */
function buildNameMap(entries: LsModelEntry[]): Map<string, LsModelEntry> {
  const map = new Map<string, LsModelEntry>();
  for (const entry of entries) {
    map.set(entry.name.toLowerCase(), entry);
  }
  return map;
}

describe("JSON parsing", () => {
  it("parses the standard ff ls JSON output", () => {
    // Simulate parsing JSON string → typed array
    const raw = JSON.stringify(SAMPLE_JSON);
    const parsed: LsModelEntry[] = JSON.parse(raw);

    expect(parsed).toHaveLength(4);
    expect(parsed[0].name).toBe("stg_orders");
    expect(parsed[0].type).toBe("model");
    expect(parsed[0].materialized).toBe("view");
  });

  it("handles entries without optional path field", () => {
    const source = SAMPLE_JSON[2];
    expect(source.path).toBeUndefined();
    expect(source.type).toBe("source");
  });

  it("handles entries without optional materialized field", () => {
    const func = SAMPLE_JSON[3];
    expect(func.materialized).toBeUndefined();
    expect(func.type).toBe("function (scalar)");
  });
});

describe("name lookup", () => {
  const nameMap = buildNameMap(SAMPLE_JSON);

  it("finds model by exact name", () => {
    const entry = nameMap.get("stg_orders");
    expect(entry).toBeDefined();
    expect(entry!.name).toBe("stg_orders");
    expect(entry!.path).toBe("/project/models/stg_orders/stg_orders.sql");
  });

  it("is case-insensitive", () => {
    const entry = nameMap.get("STG_ORDERS".toLowerCase());
    expect(entry).toBeDefined();
    expect(entry!.name).toBe("stg_orders");
  });

  it("returns undefined for unknown model", () => {
    expect(nameMap.get("nonexistent")).toBeUndefined();
  });

  it("finds source entries", () => {
    const entry = nameMap.get("raw_db.raw_orders");
    expect(entry).toBeDefined();
    expect(entry!.type).toBe("source");
  });

  it("finds function entries", () => {
    const entry = nameMap.get("my_func");
    expect(entry).toBeDefined();
    expect(entry!.type).toBe("function (scalar)");
  });
});

describe("model filtering", () => {
  it("filters to models only", () => {
    const models = SAMPLE_JSON.filter((e) => e.type === "model");
    expect(models).toHaveLength(2);
    expect(models.map((m) => m.name)).toEqual(["stg_orders", "fct_orders"]);
  });

  it("counts models correctly", () => {
    const count = SAMPLE_JSON.filter((e) => e.type === "model").length;
    expect(count).toBe(2);
  });
});
