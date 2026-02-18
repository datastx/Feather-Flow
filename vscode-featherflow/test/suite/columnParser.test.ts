/**
 * Unit tests for the SQL column parser.
 */

import { describe, expect, it } from "vitest";
import { parseColumns } from "../../src/providers/columnParser.js";

describe("parseColumns", () => {
  it("extracts simple column names", () => {
    const sql = "SELECT id, name, email FROM users";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(3);
    expect(cols.map((c) => c.name)).toEqual(["id", "name", "email"]);
    expect(cols.every((c) => !c.isComputed)).toBe(true);
  });

  it("extracts table-qualified columns", () => {
    const sql = "SELECT u.id, u.name FROM users u";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(2);
    expect(cols[0].name).toBe("id");
    expect(cols[0].sourceTable).toBe("u");
    expect(cols[1].name).toBe("name");
    expect(cols[1].sourceTable).toBe("u");
  });

  it("handles AS aliases", () => {
    const sql = "SELECT u.id AS user_id, u.email AS contact FROM users u";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(2);
    expect(cols[0].name).toBe("user_id");
    expect(cols[0].sourceTable).toBe("u");
    expect(cols[1].name).toBe("contact");
  });

  it("identifies computed/aggregate columns", () => {
    const sql =
      "SELECT COUNT(*) AS total, SUM(amount) AS revenue FROM orders";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(2);
    expect(cols[0].name).toBe("total");
    expect(cols[0].isComputed).toBe(true);
    expect(cols[1].name).toBe("revenue");
    expect(cols[1].isComputed).toBe(true);
  });

  it("handles SELECT *", () => {
    const sql = "SELECT * FROM orders";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(1);
    expect(cols[0].name).toBe("*");
  });

  it("handles SELECT DISTINCT", () => {
    const sql = "SELECT DISTINCT id, name FROM users";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(2);
    expect(cols[0].name).toBe("id");
  });

  it("handles Jinja template syntax", () => {
    const sql = `
      SELECT
        id,
        {{ config.get('name_col') }} AS display_name,
        status
      FROM {% if dev %}dev_users{% else %}users{% endif %}
    `;
    const cols = parseColumns(sql);
    // Jinja blocks are stripped, so we get id, the jinja replacement, and status
    expect(cols.length).toBeGreaterThanOrEqual(2);
    expect(cols[0].name).toBe("id");
  });

  it("returns empty for non-SELECT SQL", () => {
    const sql = "INSERT INTO users (id, name) VALUES (1, 'test')";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(0);
  });

  it("handles nested parentheses in expressions", () => {
    const sql =
      "SELECT COALESCE(a.val, b.val, 0) AS final_val, id FROM t";
    const cols = parseColumns(sql);
    expect(cols).toHaveLength(2);
    expect(cols[0].name).toBe("final_val");
    expect(cols[0].isComputed).toBe(true);
    expect(cols[1].name).toBe("id");
    expect(cols[1].isComputed).toBe(false);
  });
});
