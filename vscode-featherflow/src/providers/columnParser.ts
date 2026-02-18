/**
 * Basic SQL column extraction for column-level lineage display.
 *
 * Parses SELECT statements to extract column names and their source tables.
 * This is a best-effort parser for MVP — it handles common SQL patterns but
 * does not cover every edge case.
 */

/** A column extracted from a SQL SELECT statement. */
export interface ParsedColumn {
  /** The output column name (alias or original name). */
  name: string;
  /** The source table reference, if determinable. */
  sourceTable?: string;
  /** The original column expression (e.g., "t.id" or "COUNT(*)"). */
  expression: string;
  /** Whether this is an aggregate or computed column. */
  isComputed: boolean;
}

/**
 * Extract column definitions from a SQL SELECT statement.
 *
 * Handles patterns like:
 *   - `SELECT a, b, c FROM ...`
 *   - `SELECT t.col AS alias FROM t`
 *   - `SELECT COUNT(*) AS total FROM ...`
 *   - `SELECT * FROM ...`
 */
export function parseColumns(sql: string): ParsedColumn[] {
  // Strip Jinja/template blocks so they don't interfere with parsing
  const cleaned = stripTemplates(sql);

  // Find the top-level SELECT ... FROM boundary
  const selectMatch = cleaned.match(
    /\bSELECT\b\s+(DISTINCT\s+)?(.+?)\bFROM\b/is
  );
  if (!selectMatch) return [];

  const selectBody = selectMatch[2].trim();
  if (selectBody === "*") {
    return [{ name: "*", expression: "*", isComputed: false }];
  }

  // Split on top-level commas (respecting parentheses depth)
  const parts = splitOnTopLevelCommas(selectBody);

  return parts.map((part) => parseColumnExpression(part.trim()));
}

/**
 * Parse a single column expression like `t.col AS alias` or `COUNT(*) AS total`.
 */
function parseColumnExpression(expr: string): ParsedColumn {
  // Check for alias: `... AS alias` or `... alias` (no keywords)
  const asMatch = expr.match(/^(.+?)\s+AS\s+(\w+)\s*$/i);
  const fullExpr = asMatch ? asMatch[1].trim() : expr.trim();
  const alias = asMatch ? asMatch[2] : undefined;

  // Check if it's a function call / computed expression
  const isComputed =
    /\(/.test(fullExpr) ||
    /[+\-*/]/.test(fullExpr) ||
    /\bCASE\b/i.test(fullExpr) ||
    /\bCOALESCE\b/i.test(fullExpr) ||
    /\bCAST\b/i.test(fullExpr);

  // Try to extract table.column reference
  const dotMatch = fullExpr.match(/^(\w+)\.(\w+)$/);
  if (dotMatch) {
    return {
      name: alias ?? dotMatch[2],
      sourceTable: dotMatch[1],
      expression: fullExpr,
      isComputed: false,
    };
  }

  // Simple column name (no table prefix)
  const simpleMatch = fullExpr.match(/^(\w+)$/);
  if (simpleMatch && !isComputed) {
    return {
      name: alias ?? simpleMatch[1],
      expression: fullExpr,
      isComputed: false,
    };
  }

  // Computed / aggregate expression
  const name = alias ?? abbreviateExpression(fullExpr);
  return {
    name,
    expression: fullExpr,
    isComputed: true,
    sourceTable: extractTableFromExpression(fullExpr),
  };
}

/** Split a string on commas that are not inside parentheses. */
function splitOnTopLevelCommas(str: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = "";

  for (const ch of str) {
    if (ch === "(") depth++;
    else if (ch === ")") depth--;

    if (ch === "," && depth === 0) {
      parts.push(current);
      current = "";
    } else {
      current += ch;
    }
  }

  if (current.trim()) parts.push(current);
  return parts;
}

/** Strip Jinja template tags {% ... %}, {{ ... }}, {# ... #}. */
function stripTemplates(sql: string): string {
  return sql
    .replace(/\{%.*?%\}/gs, "")
    .replace(/\{\{.*?\}\}/gs, "'_jinja_'")
    .replace(/\{#.*?#\}/gs, "");
}

/** Create a short name for a complex expression. */
function abbreviateExpression(expr: string): string {
  // Try to extract the function name for aggregates
  const funcMatch = expr.match(/^(\w+)\s*\(/);
  if (funcMatch) {
    return `${funcMatch[1].toLowerCase()}_expr`;
  }
  // Truncate long expressions
  if (expr.length > 30) {
    return `${expr.slice(0, 27)}...`;
  }
  return expr;
}

/** Try to find a table reference inside a complex expression. */
function extractTableFromExpression(expr: string): string | undefined {
  const match = expr.match(/(\w+)\.\w+/);
  return match ? match[1] : undefined;
}
