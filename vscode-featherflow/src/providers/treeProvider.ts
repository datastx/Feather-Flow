/**
 * Sidebar tree view showing models grouped by naming convention prefix.
 *
 * Groups: Staging (stg_), Intermediate (int_), Dimensions (dim_), Facts (fct_), Other.
 */

import * as vscode from "vscode";
import type { ProjectIndex } from "../projectIndex.js";
import type { LsModelEntry } from "../types.js";

/** Known prefix groups and their display labels. */
const PREFIX_GROUPS: [string, string][] = [
  ["stg_", "Staging (stg_)"],
  ["int_", "Intermediate (int_)"],
  ["dim_", "Dimensions (dim_)"],
  ["fct_", "Facts (fct_)"],
];

type TreeElement = GroupNode | ModelNode;

interface GroupNode {
  kind: "group";
  label: string;
  models: LsModelEntry[];
}

interface ModelNode {
  kind: "model";
  entry: LsModelEntry;
}

export class TreeProvider
  implements vscode.TreeDataProvider<TreeElement>
{
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private subscription: vscode.Disposable;

  constructor(private index: ProjectIndex) {
    this.subscription = index.onDidChange(() =>
      this._onDidChangeTreeData.fire()
    );
  }

  getTreeItem(element: TreeElement): vscode.TreeItem {
    if (element.kind === "group") {
      const item = new vscode.TreeItem(
        `${element.label} (${element.models.length})`,
        vscode.TreeItemCollapsibleState.Collapsed
      );
      item.contextValue = "group";
      return item;
    }

    const entry = element.entry;
    const item = new vscode.TreeItem(
      entry.name,
      vscode.TreeItemCollapsibleState.None
    );

    item.description = entry.materialized ?? "";
    item.contextValue = "model";

    // Icon based on materialization
    switch (entry.materialized) {
      case "view":
        item.iconPath = new vscode.ThemeIcon("eye");
        break;
      case "table":
        item.iconPath = new vscode.ThemeIcon("symbol-class");
        break;
      case "incremental":
        item.iconPath = new vscode.ThemeIcon("arrow-up");
        break;
      case "ephemeral":
        item.iconPath = new vscode.ThemeIcon("ghost");
        break;
      default:
        item.iconPath = new vscode.ThemeIcon("file");
    }

    // Click to open .sql file
    if (entry.path) {
      item.command = {
        command: "vscode.open",
        title: "Open Model",
        arguments: [vscode.Uri.file(entry.path)],
      };
    }

    // Tooltip with details
    const deps = [
      ...entry.model_deps,
      ...entry.external_deps.map((d) => `${d} (external)`),
    ];
    const depsText = deps.length > 0 ? deps.join(", ") : "none";
    item.tooltip = new vscode.MarkdownString(
      [
        `**${entry.name}**`,
        "",
        `- **Materialization:** ${entry.materialized ?? "default"}`,
        `- **Schema:** ${entry.schema ?? "default"}`,
        `- **Dependencies:** ${depsText}`,
      ].join("\n")
    );

    return item;
  }

  getChildren(element?: TreeElement): TreeElement[] {
    if (!element) {
      return this.buildGroups();
    }

    if (element.kind === "group") {
      return element.models
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((entry) => ({ kind: "model" as const, entry }));
    }

    return [];
  }

  private buildGroups(): GroupNode[] {
    const models = this.index.getModels();
    const groups = new Map<string, LsModelEntry[]>();

    // Initialize known groups
    for (const [, label] of PREFIX_GROUPS) {
      groups.set(label, []);
    }
    groups.set("Other", []);

    for (const model of models) {
      let placed = false;
      for (const [prefix, label] of PREFIX_GROUPS) {
        if (model.name.toLowerCase().startsWith(prefix)) {
          groups.get(label)!.push(model);
          placed = true;
          break;
        }
      }
      if (!placed) {
        groups.get("Other")!.push(model);
      }
    }

    // Return only non-empty groups
    const result: GroupNode[] = [];
    for (const [, label] of PREFIX_GROUPS) {
      const items = groups.get(label)!;
      if (items.length > 0) {
        result.push({ kind: "group", label, models: items });
      }
    }
    const other = groups.get("Other")!;
    if (other.length > 0) {
      result.push({ kind: "group", label: "Other", models: other });
    }

    return result;
  }

  dispose(): void {
    this.subscription.dispose();
    this._onDidChangeTreeData.dispose();
  }
}
