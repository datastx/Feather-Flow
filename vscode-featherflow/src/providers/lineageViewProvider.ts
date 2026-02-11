/**
 * Webview panel showing upstream and downstream model lineage.
 *
 * Renders a left-to-right DAG: upstream → current → downstream.
 * Listens for active editor changes to track the current `.sql` model.
 */

import * as vscode from "vscode";
import type { ProjectIndex } from "../projectIndex.js";
import type { LsModelEntry } from "../types.js";

/** A node in the lineage graph. */
export interface LineageNode {
  name: string;
  type: "model" | "external";
  materialized?: string;
  path?: string;
}

/** A column in the DAG layout (one per depth level). */
export interface LineageColumn {
  depth: number;
  nodes: LineageNode[];
}

/** Full lineage graph data sent to the webview. */
export interface LineageGraph {
  current: LineageNode;
  upstream: LineageColumn[];
  downstream: LineageColumn[];
}

/** Walk upstream dependencies recursively, returning columns by depth. */
export function buildUpstream(
  modelName: string,
  index: ProjectIndex,
  visited?: Set<string>
): LineageColumn[] {
  const seen = visited ?? new Set<string>();
  seen.add(modelName.toLowerCase());

  const entry = index.getModelByName(modelName);
  if (!entry) return [];

  const columns = new Map<number, LineageNode[]>();

  function walk(name: string, depth: number): void {
    const model = index.getModelByName(name);
    if (!model) return;

    // Model deps (internal)
    for (const dep of model.model_deps) {
      const key = dep.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);

      const depEntry = index.getModelByName(dep);
      const node: LineageNode = depEntry
        ? {
            name: depEntry.name,
            type: "model",
            materialized: depEntry.materialized,
            path: depEntry.path,
          }
        : { name: dep, type: "model" };

      if (!columns.has(depth)) columns.set(depth, []);
      columns.get(depth)!.push(node);

      walk(dep, depth + 1);
    }

    // External deps (leaf nodes, always at depth)
    for (const ext of model.external_deps) {
      const key = `ext:${ext.toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);

      if (!columns.has(depth)) columns.set(depth, []);
      columns.get(depth)!.push({ name: ext, type: "external" });
    }
  }

  walk(modelName, 1);

  // Convert map to sorted array (highest depth first = leftmost)
  const maxDepth = Math.max(0, ...columns.keys());
  const result: LineageColumn[] = [];
  for (let d = maxDepth; d >= 1; d--) {
    const nodes = columns.get(d);
    if (nodes && nodes.length > 0) {
      result.push({ depth: d, nodes });
    }
  }
  return result;
}

/** Walk downstream dependencies recursively, returning columns by depth. */
export function buildDownstream(
  modelName: string,
  index: ProjectIndex,
  visited?: Set<string>
): LineageColumn[] {
  const seen = visited ?? new Set<string>();
  seen.add(modelName.toLowerCase());

  const columns = new Map<number, LineageNode[]>();

  function walk(name: string, depth: number): void {
    const children = index.getDownstream(name);
    for (const child of children) {
      const key = child.name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);

      const node: LineageNode = {
        name: child.name,
        type: "model",
        materialized: child.materialized,
        path: child.path,
      };

      if (!columns.has(depth)) columns.set(depth, []);
      columns.get(depth)!.push(node);

      walk(child.name, depth + 1);
    }
  }

  walk(modelName, 1);

  const maxDepth = Math.max(0, ...columns.keys());
  const result: LineageColumn[] = [];
  for (let d = 1; d <= maxDepth; d++) {
    const nodes = columns.get(d);
    if (nodes && nodes.length > 0) {
      result.push({ depth: d, nodes });
    }
  }
  return result;
}

/** Build the full lineage graph for a model. */
export function buildLineageGraph(
  entry: LsModelEntry,
  index: ProjectIndex
): LineageGraph {
  const current: LineageNode = {
    name: entry.name,
    type: "model",
    materialized: entry.materialized,
    path: entry.path,
  };

  return {
    current,
    upstream: buildUpstream(entry.name, index),
    downstream: buildDownstream(entry.name, index),
  };
}

export class LineageViewProvider
  implements vscode.WebviewViewProvider, vscode.Disposable
{
  static readonly viewType = "featherflowLineage";

  private view?: vscode.WebviewView;
  private disposables: vscode.Disposable[] = [];

  constructor(
    private extensionUri: vscode.Uri,
    private index: ProjectIndex
  ) {
    // Re-render when active editor changes
    this.disposables.push(
      vscode.window.onDidChangeActiveTextEditor(() => this.updateView())
    );

    // Re-render when the index refreshes
    this.disposables.push(index.onDidChange(() => this.updateView()));
  }

  resolveWebviewView(webviewView: vscode.WebviewView): void {
    this.view = webviewView;
    webviewView.webview.options = { enableScripts: true };

    // Handle messages from the webview (click-to-open)
    this.disposables.push(
      webviewView.webview.onDidReceiveMessage((msg: { command: string; path?: string }) => {
        if (msg.command === "openModel" && msg.path) {
          vscode.commands.executeCommand("vscode.open", vscode.Uri.file(msg.path));
        }
      })
    );

    // Render when the view becomes visible
    this.disposables.push(
      webviewView.onDidChangeVisibility(() => {
        if (webviewView.visible) this.updateView();
      })
    );

    this.updateView();
  }

  private updateView(): void {
    if (!this.view || !this.view.visible) return;

    const editor = vscode.window.activeTextEditor;
    if (!editor || !editor.document.fileName.endsWith(".sql")) {
      this.view.webview.html = renderEmpty("Open a .sql model file to see its lineage.");
      return;
    }

    const entry = this.index.getModelByPath(editor.document.uri.fsPath);
    if (!entry) {
      this.view.webview.html = renderEmpty("This file is not a tracked Feather-Flow model.");
      return;
    }

    const graph = buildLineageGraph(entry, this.index);
    this.view.webview.html = renderLineageHtml(graph);
  }

  dispose(): void {
    for (const d of this.disposables) d.dispose();
    this.disposables = [];
  }
}

/** Render a placeholder message when no lineage is available. */
function renderEmpty(message: string): string {
  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body { font-family: var(--vscode-font-family, sans-serif); color: var(--vscode-foreground);
    background: var(--vscode-panel-background, transparent); display: flex;
    align-items: center; justify-content: center; height: 100vh; margin: 0;
    font-size: 13px; }
  .empty { opacity: 0.6; text-align: center; padding: 16px; }
</style>
</head><body><div class="empty">${escapeHtml(message)}</div></body></html>`;
}

/** Render the full lineage DAG as HTML. */
function renderLineageHtml(graph: LineageGraph): string {
  const upstreamHtml = graph.upstream
    .map((col) => renderColumn(col, "upstream"))
    .join(renderArrow());

  const downstreamHtml = graph.downstream
    .map((col) => renderColumn(col, "downstream"))
    .join(renderArrow());

  const currentHtml = renderNode(graph.current, true);

  // Build the full row: upstream → current → downstream
  const parts: string[] = [];
  if (upstreamHtml) parts.push(upstreamHtml, renderArrow());
  parts.push(`<div class="column current-col">${currentHtml}</div>`);
  if (downstreamHtml) parts.push(renderArrow(), downstreamHtml);

  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  * { box-sizing: border-box; }
  body {
    font-family: var(--vscode-font-family, sans-serif);
    color: var(--vscode-foreground);
    background: var(--vscode-panel-background, transparent);
    margin: 0; padding: 12px;
    font-size: 12px;
    overflow-x: auto;
  }
  .lineage-container {
    display: flex;
    align-items: center;
    gap: 4px;
    min-height: 60px;
    padding: 8px 0;
  }
  .column {
    display: flex;
    flex-direction: column;
    gap: 6px;
    align-items: center;
  }
  .node {
    padding: 6px 12px;
    border: 1px solid var(--vscode-panel-border, #444);
    border-radius: 4px;
    background: var(--vscode-editor-background, #1e1e1e);
    cursor: pointer;
    white-space: nowrap;
    transition: border-color 0.15s;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .node:hover {
    border-color: var(--vscode-focusBorder, #007acc);
  }
  .node.current {
    border-color: var(--vscode-focusBorder, #007acc);
    background: var(--vscode-editor-selectionBackground, #264f78);
    font-weight: 600;
  }
  .node.external {
    border-style: dashed;
    opacity: 0.75;
    cursor: default;
  }
  .node-icon {
    font-size: 14px;
    flex-shrink: 0;
  }
  .arrow {
    color: var(--vscode-panel-border, #555);
    font-size: 16px;
    padding: 0 2px;
    flex-shrink: 0;
  }
  .header {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    opacity: 0.5;
    margin-bottom: 2px;
  }
</style>
</head>
<body>
  <div class="lineage-container">
    ${parts.join("\n    ")}
  </div>
  <script>
    const vscode = acquireVsCodeApi();
    document.querySelectorAll('.node[data-path]').forEach(el => {
      el.addEventListener('click', () => {
        const path = el.getAttribute('data-path');
        if (path) vscode.postMessage({ command: 'openModel', path });
      });
    });
  </script>
</body></html>`;
}

/** Render a column of nodes at a given depth level. */
function renderColumn(col: LineageColumn, _side: "upstream" | "downstream"): string {
  const nodesHtml = col.nodes.map((n) => renderNode(n, false)).join("\n");
  return `<div class="column">${nodesHtml}</div>`;
}

/** Render a single node. */
function renderNode(node: LineageNode, isCurrent: boolean): string {
  const classes = ["node"];
  if (isCurrent) classes.push("current");
  if (node.type === "external") classes.push("external");

  const pathAttr = node.path ? ` data-path="${escapeHtml(node.path)}"` : "";
  const icon = getIcon(node);

  return `<div class="${classes.join(" ")}"${pathAttr}><span class="node-icon">${icon}</span>${escapeHtml(node.name)}</div>`;
}

/** Arrow connector between columns. */
function renderArrow(): string {
  return `<div class="arrow">\u2192</div>`;
}

/** Map materialization to an icon character. */
function getIcon(node: LineageNode): string {
  if (node.type === "external") return "\uD83D\uDDC3\uFE0F"; // file cabinet
  switch (node.materialized) {
    case "view":
      return "\uD83D\uDC41\uFE0F"; // eye
    case "table":
      return "\uD83D\uDCCB"; // clipboard
    case "incremental":
      return "\u2B06\uFE0F"; // up arrow
    case "ephemeral":
      return "\uD83D\uDC7B"; // ghost
    default:
      return "\uD83D\uDCC4"; // page
  }
}

/** Minimal HTML escaping. */
function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
