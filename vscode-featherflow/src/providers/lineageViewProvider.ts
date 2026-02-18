/**
 * Webview panel showing upstream and downstream model lineage.
 *
 * Supports two modes via a dropdown selector:
 * - **Table Lineage**: left-to-right DAG of model dependencies
 * - **Column Lineage**: column-level mapping between current model and its sources
 *
 * Listens for active editor changes to track the current `.sql` model.
 */

import * as fs from "node:fs";
import * as vscode from "vscode";
import type { ProjectIndex } from "../projectIndex.js";
import type { LsModelEntry } from "../types.js";
import { parseColumns, type ParsedColumn } from "./columnParser.js";

// ── Data types ──────────────────────────────────────────────────────

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

/** Column lineage data for the column view. */
export interface ColumnLineageData {
  modelName: string;
  materialized?: string;
  columns: ParsedColumn[];
  upstreamModels: { name: string; type: "model" | "external" }[];
}

/** Lineage display mode. */
export type LineageMode = "table" | "column";

// ── Graph building ──────────────────────────────────────────────────

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

/** Build column lineage data for a model. */
export function buildColumnLineage(
  entry: LsModelEntry,
  index: ProjectIndex
): ColumnLineageData {
  let columns: ParsedColumn[] = [];
  if (entry.path) {
    try {
      const sql = fs.readFileSync(entry.path, "utf-8");
      columns = parseColumns(sql);
    } catch {
      // File not readable — return empty columns
    }
  }

  const upstreamModels: { name: string; type: "model" | "external" }[] = [
    ...entry.model_deps.map((d) => ({ name: d, type: "model" as const })),
    ...entry.external_deps.map((d) => ({ name: d, type: "external" as const })),
  ];

  return {
    modelName: entry.name,
    materialized: entry.materialized,
    columns,
    upstreamModels,
  };
}

// ── View provider ───────────────────────────────────────────────────

export class LineageViewProvider
  implements vscode.WebviewViewProvider, vscode.Disposable
{
  static readonly viewType = "featherflowLineage";

  private view?: vscode.WebviewView;
  private mode: LineageMode = "table";
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

  /** Switch the lineage display mode. */
  setMode(mode: LineageMode): void {
    this.mode = mode;
    this.updateView();
  }

  getMode(): LineageMode {
    return this.mode;
  }

  resolveWebviewView(webviewView: vscode.WebviewView): void {
    this.view = webviewView;
    webviewView.webview.options = { enableScripts: true };

    // Handle messages from the webview
    this.disposables.push(
      webviewView.webview.onDidReceiveMessage(
        (msg: { command: string; path?: string; mode?: string }) => {
          if (msg.command === "openModel" && msg.path) {
            vscode.commands.executeCommand(
              "vscode.open",
              vscode.Uri.file(msg.path)
            );
          } else if (msg.command === "setMode" && msg.mode) {
            this.mode = msg.mode as LineageMode;
            this.updateView();
          }
        }
      )
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
      this.view.webview.html = renderEmpty(
        "Open a .sql model file to see its lineage."
      );
      return;
    }

    const entry = this.index.getModelByPath(editor.document.uri.fsPath);
    if (!entry) {
      this.view.webview.html = renderEmpty(
        "This file is not a tracked Feather-Flow model."
      );
      return;
    }

    if (this.mode === "column") {
      const data = buildColumnLineage(entry, this.index);
      this.view.webview.html = renderColumnLineageHtml(data, this.mode);
    } else {
      const graph = buildLineageGraph(entry, this.index);
      this.view.webview.html = renderTableLineageHtml(graph, this.mode);
    }
  }

  dispose(): void {
    for (const d of this.disposables) d.dispose();
    this.disposables = [];
  }
}

// ── Shared styles ───────────────────────────────────────────────────

function sharedStyles(): string {
  return `
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: var(--vscode-font-family, -apple-system, BlinkMacSystemFont, sans-serif);
    color: var(--vscode-foreground);
    background: var(--vscode-panel-background, transparent);
    font-size: 12px;
    line-height: 1.4;
    overflow-x: auto;
  }

  /* ── Toolbar ── */
  .toolbar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.2));
    background: var(--vscode-sideBar-background, transparent);
    position: sticky;
    top: 0;
    z-index: 10;
  }
  .toolbar-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    opacity: 0.7;
    white-space: nowrap;
  }
  .mode-select {
    background: var(--vscode-dropdown-background, #3c3c3c);
    color: var(--vscode-dropdown-foreground, inherit);
    border: 1px solid var(--vscode-dropdown-border, rgba(128,128,128,0.3));
    border-radius: 4px;
    padding: 3px 8px;
    font-size: 12px;
    font-family: inherit;
    cursor: pointer;
    outline: none;
    min-width: 140px;
  }
  .mode-select:focus {
    border-color: var(--vscode-focusBorder, #007acc);
  }
  .model-badge {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    opacity: 0.7;
  }
  .model-badge .badge-icon {
    font-size: 12px;
  }
  .model-badge .badge-type {
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    background: var(--vscode-badge-background, rgba(128,128,128,0.2));
    color: var(--vscode-badge-foreground, inherit);
  }
  `;
}

// ── Table lineage HTML ──────────────────────────────────────────────

function renderTableLineageHtml(graph: LineageGraph, mode: LineageMode): string {
  // Flatten all columns into a flat ordered array of { colIndex, nodes }
  // for easier SVG connection rendering
  const allColumns: { id: string; nodes: LineageNode[] }[] = [];

  for (let i = 0; i < graph.upstream.length; i++) {
    allColumns.push({ id: `up-${i}`, nodes: graph.upstream[i].nodes });
  }
  allColumns.push({ id: "current", nodes: [graph.current] });
  for (let i = 0; i < graph.downstream.length; i++) {
    allColumns.push({ id: `down-${i}`, nodes: graph.downstream[i].nodes });
  }

  const columnsHtml = allColumns
    .map((col, colIdx) => {
      const nodesHtml = col.nodes
        .map((n, nodeIdx) => {
          const isCurrent = col.id === "current";
          return renderTableNode(n, isCurrent, `node-${colIdx}-${nodeIdx}`);
        })
        .join("\n");

      const isCurrentCol = col.id === "current";
      const colClass = isCurrentCol ? "dag-column current-column" : "dag-column";
      return `<div class="${colClass}" data-col="${colIdx}">${nodesHtml}</div>`;
    })
    .join("");

  const matBadge = graph.current.materialized
    ? `<span class="badge-type">${escapeHtml(graph.current.materialized)}</span>`
    : "";

  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  ${sharedStyles()}

  /* ── DAG layout ── */
  .dag-wrapper {
    padding: 16px 12px;
    overflow-x: auto;
  }
  .dag-container {
    display: flex;
    align-items: stretch;
    gap: 0;
    min-height: 80px;
    position: relative;
  }
  .dag-column {
    display: flex;
    flex-direction: column;
    gap: 8px;
    justify-content: center;
    padding: 0 6px;
    position: relative;
  }
  .dag-column:not(:last-child)::after {
    content: '';
    position: absolute;
    right: -8px;
    top: 50%;
    width: 16px;
    height: 2px;
    background: var(--vscode-panel-border, rgba(128,128,128,0.35));
  }
  .dag-column:not(:last-child)::before {
    content: '';
    position: absolute;
    right: -9px;
    top: calc(50% - 4px);
    width: 0;
    height: 0;
    border-left: 6px solid var(--vscode-panel-border, rgba(128,128,128,0.35));
    border-top: 5px solid transparent;
    border-bottom: 5px solid transparent;
  }

  /* ── Nodes ── */
  .dag-node {
    padding: 8px 14px;
    border: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.25));
    border-radius: 6px;
    background: var(--vscode-editor-background, #1e1e1e);
    cursor: pointer;
    white-space: nowrap;
    transition: all 0.15s ease;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    position: relative;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  .dag-node:hover {
    border-color: var(--vscode-focusBorder, #007acc);
    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    transform: translateY(-1px);
  }
  .dag-node.current {
    border-color: var(--vscode-focusBorder, #007acc);
    border-width: 2px;
    background: var(--vscode-editor-selectionBackground, #264f78);
    font-weight: 600;
    box-shadow: 0 0 0 3px rgba(0,122,204,0.15), 0 2px 8px rgba(0,0,0,0.2);
  }
  .dag-node.external {
    border-style: dashed;
    opacity: 0.65;
    cursor: default;
    font-style: italic;
  }
  .dag-node.external:hover {
    transform: none;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  .node-icon {
    width: 20px;
    height: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    flex-shrink: 0;
    color: #fff;
  }
  .node-icon.mat-view { background: #2ea043; }
  .node-icon.mat-table { background: #1f6feb; }
  .node-icon.mat-incremental { background: #9b59b6; }
  .node-icon.mat-ephemeral { background: #6e7681; }
  .node-icon.mat-external { background: #d29922; }
  .node-icon.mat-default { background: #57606a; }

  .node-name {
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 180px;
  }

  /* ── Depth labels ── */
  .depth-label {
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    opacity: 0.4;
    text-align: center;
    margin-bottom: 6px;
    font-weight: 600;
  }

  /* ── Section labels ── */
  .section-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 12px 6px;
  }
  .section-label {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    opacity: 0.4;
    font-weight: 600;
  }
  .section-line {
    flex: 1;
    height: 1px;
    background: var(--vscode-panel-border, rgba(128,128,128,0.15));
  }

  /* ── Empty state ── */
  .empty-hint {
    padding: 24px 16px;
    text-align: center;
    opacity: 0.5;
    font-size: 12px;
  }
</style>
</head>
<body>
  <div class="toolbar">
    <span class="toolbar-label">Lineage</span>
    <select class="mode-select" id="modeSelect">
      <option value="table"${mode === "table" ? " selected" : ""}>Table Lineage</option>
      <option value="column"${mode === "column" ? " selected" : ""}>Column Lineage</option>
    </select>
    <div class="model-badge">
      ${matBadge}
    </div>
  </div>
  <div class="dag-wrapper">
    ${allColumns.length > 1 || graph.upstream.length > 0 || graph.downstream.length > 0
      ? `<div class="dag-container">${columnsHtml}</div>`
      : `<div class="empty-hint">No upstream or downstream dependencies found.</div>`
    }
  </div>
  <script>
    const vscode = acquireVsCodeApi();

    // Mode dropdown
    document.getElementById('modeSelect').addEventListener('change', (e) => {
      vscode.postMessage({ command: 'setMode', mode: e.target.value });
    });

    // Click-to-open model files
    document.querySelectorAll('.dag-node[data-path]').forEach(el => {
      el.addEventListener('click', () => {
        const path = el.getAttribute('data-path');
        if (path) vscode.postMessage({ command: 'openModel', path });
      });
    });
  </script>
</body></html>`;
}

function renderTableNode(
  node: LineageNode,
  isCurrent: boolean,
  id: string
): string {
  const classes = ["dag-node"];
  if (isCurrent) classes.push("current");
  if (node.type === "external") classes.push("external");

  const pathAttr = node.path ? ` data-path="${escapeHtml(node.path)}"` : "";
  const iconClass = getIconClass(node);
  const iconLetter = getIconLetter(node);

  return `<div class="${classes.join(" ")}" id="${id}"${pathAttr}>
    <span class="node-icon ${iconClass}">${iconLetter}</span>
    <span class="node-name">${escapeHtml(node.name)}</span>
  </div>`;
}

function getIconClass(node: LineageNode): string {
  if (node.type === "external") return "mat-external";
  switch (node.materialized) {
    case "view":
      return "mat-view";
    case "table":
      return "mat-table";
    case "incremental":
      return "mat-incremental";
    case "ephemeral":
      return "mat-ephemeral";
    default:
      return "mat-default";
  }
}

function getIconLetter(node: LineageNode): string {
  if (node.type === "external") return "E";
  switch (node.materialized) {
    case "view":
      return "V";
    case "table":
      return "T";
    case "incremental":
      return "I";
    case "ephemeral":
      return "~";
    default:
      return "M";
  }
}

// ── Column lineage HTML ─────────────────────────────────────────────

function renderColumnLineageHtml(
  data: ColumnLineageData,
  mode: LineageMode
): string {
  const matBadge = data.materialized
    ? `<span class="badge-type">${escapeHtml(data.materialized)}</span>`
    : "";

  // Build upstream sources panel
  const sourcesHtml = data.upstreamModels
    .map((src) => {
      const cls = src.type === "external" ? "source-item external" : "source-item";
      const iconClass = src.type === "external" ? "mat-external" : "mat-view";
      const letter = src.type === "external" ? "E" : "M";
      return `<div class="${cls}">
        <span class="node-icon ${iconClass}">${letter}</span>
        <span class="source-name">${escapeHtml(src.name)}</span>
      </div>`;
    })
    .join("\n");

  // Build columns panel
  const columnsHtml =
    data.columns.length > 0
      ? data.columns
          .map((col) => {
            const sourceTag = col.sourceTable
              ? `<span class="col-source" title="From: ${escapeHtml(col.sourceTable)}">${escapeHtml(col.sourceTable)}</span>`
              : "";
            const computedTag = col.isComputed
              ? `<span class="col-computed" title="${escapeHtml(col.expression)}">fx</span>`
              : "";
            return `<div class="col-item">
              <span class="col-name">${escapeHtml(col.name)}</span>
              <span class="col-tags">${computedTag}${sourceTag}</span>
            </div>`;
          })
          .join("\n")
      : `<div class="empty-hint">Could not parse columns from SQL.</div>`;

  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  ${sharedStyles()}

  /* ── Column lineage layout ── */
  .col-lineage-wrapper {
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .col-section {
    border: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.2));
    border-radius: 8px;
    overflow: hidden;
    background: var(--vscode-editor-background, #1e1e1e);
    box-shadow: 0 1px 4px rgba(0,0,0,0.1);
  }
  .col-section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    opacity: 0.7;
    border-bottom: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.15));
    background: var(--vscode-sideBar-background, transparent);
  }
  .col-section-header .count {
    margin-left: auto;
    font-weight: 400;
    opacity: 0.6;
    font-size: 10px;
  }

  /* ── Source items ── */
  .source-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 12px;
    border-bottom: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.08));
    font-size: 12px;
  }
  .source-item:last-child { border-bottom: none; }
  .source-item.external {
    opacity: 0.65;
    font-style: italic;
  }
  .source-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* ── Column items ── */
  .col-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 5px 12px;
    border-bottom: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.08));
    font-size: 12px;
  }
  .col-item:last-child { border-bottom: none; }
  .col-item:hover {
    background: var(--vscode-list-hoverBackground, rgba(128,128,128,0.08));
  }
  .col-name {
    font-family: var(--vscode-editor-font-family, 'Cascadia Code', 'Fira Code', monospace);
    font-size: 12px;
  }
  .col-tags {
    margin-left: auto;
    display: flex;
    gap: 4px;
    align-items: center;
    flex-shrink: 0;
  }
  .col-source {
    font-size: 10px;
    padding: 1px 6px;
    border-radius: 3px;
    background: rgba(30, 111, 235, 0.15);
    color: var(--vscode-textLink-foreground, #3794ff);
    white-space: nowrap;
  }
  .col-computed {
    font-size: 9px;
    font-weight: 700;
    padding: 1px 5px;
    border-radius: 3px;
    background: rgba(155, 89, 182, 0.15);
    color: #c084fc;
    cursor: help;
    white-space: nowrap;
  }

  .node-icon {
    width: 20px;
    height: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    flex-shrink: 0;
    color: #fff;
  }
  .node-icon.mat-view { background: #2ea043; }
  .node-icon.mat-table { background: #1f6feb; }
  .node-icon.mat-incremental { background: #9b59b6; }
  .node-icon.mat-ephemeral { background: #6e7681; }
  .node-icon.mat-external { background: #d29922; }
  .node-icon.mat-default { background: #57606a; }

  .empty-hint {
    padding: 16px;
    text-align: center;
    opacity: 0.5;
    font-size: 12px;
  }
</style>
</head>
<body>
  <div class="toolbar">
    <span class="toolbar-label">Lineage</span>
    <select class="mode-select" id="modeSelect">
      <option value="table"${mode === "table" ? " selected" : ""}>Table Lineage</option>
      <option value="column"${mode === "column" ? " selected" : ""}>Column Lineage</option>
    </select>
    <div class="model-badge">
      ${matBadge}
    </div>
  </div>
  <div class="col-lineage-wrapper">
    <div class="col-section">
      <div class="col-section-header">
        <span>Upstream Sources</span>
        <span class="count">${data.upstreamModels.length}</span>
      </div>
      ${data.upstreamModels.length > 0 ? sourcesHtml : '<div class="empty-hint">No upstream sources</div>'}
    </div>
    <div class="col-section">
      <div class="col-section-header">
        <span>Columns &mdash; ${escapeHtml(data.modelName)}</span>
        <span class="count">${data.columns.length}</span>
      </div>
      ${columnsHtml}
    </div>
  </div>
  <script>
    const vscode = acquireVsCodeApi();

    document.getElementById('modeSelect').addEventListener('change', (e) => {
      vscode.postMessage({ command: 'setMode', mode: e.target.value });
    });
  </script>
</body></html>`;
}

// ── Empty state ─────────────────────────────────────────────────────

/** Render a placeholder message when no lineage is available. */
function renderEmpty(message: string): string {
  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  ${sharedStyles()}
  .empty-container {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100vh;
    padding: 24px;
  }
  .empty-content {
    text-align: center;
    opacity: 0.5;
  }
  .empty-icon {
    font-size: 32px;
    margin-bottom: 12px;
    opacity: 0.4;
  }
  .empty-text {
    font-size: 13px;
    line-height: 1.5;
  }
</style>
</head>
<body>
  <div class="empty-container">
    <div class="empty-content">
      <div class="empty-icon">&#8693;</div>
      <div class="empty-text">${escapeHtml(message)}</div>
    </div>
  </div>
</body></html>`;
}

// ── Utilities ───────────────────────────────────────────────────────

/** Minimal HTML escaping. */
function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
