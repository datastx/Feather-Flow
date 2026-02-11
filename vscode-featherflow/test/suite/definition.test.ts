/**
 * Unit tests for FROM/JOIN table reference detection.
 *
 * Tests the `isTableReference` function directly, without requiring
 * the VS Code API.
 */

import { describe, expect, it } from "vitest";
import { isTableReference } from "../../src/providers/definitionProvider.js";

/** Minimal document mock for testing. */
function makeDoc(lines: string[]) {
  return {
    getText(range?: {
      start: { line: number; character: number };
      end: { line: number; character: number };
    }): string {
      if (!range) return lines.join("\n");
      const result: string[] = [];
      for (let l = range.start.line; l <= range.end.line; l++) {
        const line = lines[l] ?? "";
        const start = l === range.start.line ? range.start.character : 0;
        const end = l === range.end.line ? range.end.character : line.length;
        result.push(line.slice(start, end));
      }
      return result.join("\n");
    },
    lineAt(line: number) {
      return { text: lines[line] ?? "" };
    },
  };
}

describe("isTableReference", () => {
  it("detects simple FROM reference", () => {
    const doc = makeDoc(["SELECT * FROM stg_orders"]);
    const range = { start: { line: 0, character: 14 } }; // "stg_orders" starts at 14
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects FROM on previous line", () => {
    const doc = makeDoc(["SELECT *", "FROM", "  stg_orders"]);
    const range = { start: { line: 2, character: 2 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects LEFT JOIN reference", () => {
    const doc = makeDoc(["SELECT * FROM a LEFT JOIN stg_payments"]);
    const range = { start: { line: 0, character: 26 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects LEFT OUTER JOIN reference", () => {
    const doc = makeDoc(["LEFT OUTER JOIN stg_payments"]);
    const range = { start: { line: 0, character: 16 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects INNER JOIN reference", () => {
    const doc = makeDoc(["INNER JOIN dim_customers"]);
    const range = { start: { line: 0, character: 11 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects RIGHT JOIN reference", () => {
    const doc = makeDoc(["RIGHT JOIN fct_orders"]);
    const range = { start: { line: 0, character: 11 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects FULL OUTER JOIN reference", () => {
    const doc = makeDoc(["FULL OUTER JOIN stg_products"]);
    const range = { start: { line: 0, character: 16 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects CROSS JOIN reference", () => {
    const doc = makeDoc(["CROSS JOIN dim_dates"]);
    const range = { start: { line: 0, character: 11 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("detects JOIN on multiline", () => {
    const doc = makeDoc(["SELECT *", "FROM orders o", "LEFT JOIN", "  stg_payments p"]);
    const range = { start: { line: 3, character: 2 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("rejects word in SELECT clause", () => {
    const doc = makeDoc(["SELECT stg_orders FROM something"]);
    const range = { start: { line: 0, character: 7 } }; // "stg_orders" in SELECT
    expect(isTableReference(doc, range)).toBe(false);
  });

  it("rejects word in WHERE clause", () => {
    const doc = makeDoc([
      "SELECT * FROM orders",
      "WHERE status = 'active'",
    ]);
    const range = { start: { line: 1, character: 6 } }; // "status"
    expect(isTableReference(doc, range)).toBe(false);
  });

  it("rejects alias after table name", () => {
    // "o" is an alias, not a table reference — preceding text is "FROM stg_orders "
    const doc = makeDoc(["SELECT * FROM stg_orders o"]);
    const range = { start: { line: 0, character: 25 } }; // "o"
    // The text before "o" is "SELECT * FROM stg_orders " which does NOT end with FROM/JOIN
    expect(isTableReference(doc, range)).toBe(false);
  });

  it("is case-insensitive for keywords", () => {
    const doc = makeDoc(["select * from stg_orders"]);
    const range = { start: { line: 0, character: 14 } };
    expect(isTableReference(doc, range)).toBe(true);
  });

  it("handles FROM with extra whitespace", () => {
    const doc = makeDoc(["FROM   stg_orders"]);
    const range = { start: { line: 0, character: 7 } };
    expect(isTableReference(doc, range)).toBe(true);
  });
});
