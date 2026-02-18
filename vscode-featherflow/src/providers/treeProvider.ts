/**
 * Sidebar tree view showing models grouped by naming convention prefix.
 *
 * Groups: Staging (stg_), Intermediate (int_), Dimensions (dim_), Facts (fct_), Other.
 * Each model shows materialization type, dependency counts, and rich tooltips.
 */

import * as vscode from "vscode";
import type { ProjectIndex } from "../projectIndex.js";
import type { LsModelEntry } from "../types.js";

/** Known prefix groups and their display labels. */
const PREFIX_GROUPS: [string, string, string][] = [
  ["stg_", "Staging", "symbol-event"],
  ["int_", "Intermediate", "symbol-interface"],
  ["dim_", "Dimensions", "symbol-enum"],
  ["fct_", "Facts", "symbol-class"],
];

type TreeElement = GroupNode | ModelNode;

interface GroupNode {
  kind: "group";
  label: string;
  icon: string;
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
        element.label,
        vscode.TreeItemCollapsibleState.Expanded
      );
      item.description = `${element.models.length}`;
      item.contextValue = "group";
      item.iconPath = new vscode.ThemeIcon(element.icon);
      return item;
    }

    const entry = element.entry;
    const item = new vscode.TreeItem(
      entry.name,
      vscode.TreeItemCollapsibleState.None
    );

    // Description: materialization + dependency summary
    const totalDeps = entry.model_deps.length + entry.external_deps.length;
    const downstream = this.index.getDownstream(entry.name).length;
    const parts: string[] = [];
    if (entry.materialized) parts.push(entry.materialized);
    if (totalDeps > 0 || downstream > 0) {
      parts.push(`${totalDeps} up / ${downstream} down`);
    }
    item.description = parts.join(" \u2022 ");

    item.contextValue = "model";

    // Icon based on materialization
    switch (entry.materialized) {
      case "view":
        item.iconPath = new vscode.ThemeIcon("eye");
        break;
      case "table":
        item.iconPath = new vscode.ThemeIcon("table");
        break;
      case "incremental":
        item.iconPath = new vscode.ThemeIcon("arrow-up");
        break;
      case "ephemeral":
        item.iconPath = new vscode.ThemeIcon("ghost");
        break;
      default:
        item.iconPath = new vscode.ThemeIcon("file-code");
    }

    // Click to open .sql file
    if (entry.path) {
      item.command = {
        command: "vscode.open",
        title: "Open Model",
        arguments: [vscode.Uri.file(entry.path)],
      };
    }

    // Rich tooltip with details
    const modelDeps = entry.model_deps.length > 0
      ? entry.model_deps.map((d) => `  - \`${d}\``).join("\n")
      : "  _none_";
    const extDeps = entry.external_deps.length > 0
      ? entry.external_deps.map((d) => `  - \`${d}\` _(external)_`).join("\n")
      : "  _none_";
    const downstreamModels = this.index.getDownstream(entry.name);
    const downText = downstreamModels.length > 0
      ? downstreamModels.map((d) => `  - \`${d.name}\``).join("\n")
      : "  _none_";

    item.tooltip = new vscode.MarkdownString(
      [
        `### ${entry.name}`,
        "",
        `| Property | Value |`,
        `|----------|-------|`,
        `| **Materialization** | ${entry.materialized ?? "default"} |`,
        `| **Schema** | ${entry.schema ?? "default"} |`,
        `| **Upstream** | ${totalDeps} |`,
        `| **Downstream** | ${downstreamModels.length} |`,
        "",
        `**Upstream Dependencies:**`,
        modelDeps,
        extDeps,
        "",
        `**Downstream Consumers:**`,
        downText,
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
    const groups = new Map<string, { icon: string; models: LsModelEntry[] }>();

    // Initialize known groups
    for (const [, label, icon] of PREFIX_GROUPS) {
      groups.set(label, { icon, models: [] });
    }
    groups.set("Other", { icon: "symbol-misc", models: [] });

    for (const model of models) {
      let placed = false;
      for (const [prefix, label] of PREFIX_GROUPS) {
        if (model.name.toLowerCase().startsWith(prefix)) {
          groups.get(label)!.models.push(model);
          placed = true;
          break;
        }
      }
      if (!placed) {
        groups.get("Other")!.models.push(model);
      }
    }

    // Return only non-empty groups
    const result: GroupNode[] = [];
    for (const [, label, icon] of PREFIX_GROUPS) {
      const group = groups.get(label)!;
      if (group.models.length > 0) {
        result.push({ kind: "group", label, icon, models: group.models });
      }
    }
    const other = groups.get("Other")!;
    if (other.models.length > 0) {
      result.push({
        kind: "group",
        label: "Other",
        icon: "symbol-misc",
        models: other.models,
      });
    }

    return result;
  }

  dispose(): void {
    this.subscription.dispose();
    this._onDidChangeTreeData.dispose();
  }
}
