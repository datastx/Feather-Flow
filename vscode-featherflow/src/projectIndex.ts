/**
 * Central project index: cached model/source/function metadata.
 *
 * Powered by `ff ls --output json`. Provides lookup maps and a file watcher
 * for automatic refresh on changes.
 */

import * as vscode from "vscode";
import { ffLs } from "./cli.js";
import type { LsModelEntry } from "./types.js";

export class ProjectIndex implements vscode.Disposable {
  private nameMap = new Map<string, LsModelEntry>();
  private pathMap = new Map<string, LsModelEntry>();
  private projectName = "";
  private binaryPath: string;
  private projectDir: string;

  private watcher: vscode.FileSystemWatcher | undefined;
  private refreshTimer: ReturnType<typeof setTimeout> | undefined;

  private readonly _onDidChange = new vscode.EventEmitter<void>();
  /** Fires after the index has been refreshed. */
  readonly onDidChange = this._onDidChange.event;

  constructor(binaryPath: string, projectDir: string) {
    this.binaryPath = binaryPath;
    this.projectDir = projectDir;
  }

  /** Load the index for the first time. */
  async initialize(): Promise<void> {
    await this.refresh();
    this.startWatching();
  }

  /** Re-run `ff ls` and rebuild all lookup maps. */
  async refresh(): Promise<void> {
    try {
      const entries = await ffLs(this.binaryPath, this.projectDir);
      this.rebuildMaps(entries);
      this._onDidChange.fire();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      vscode.window.showWarningMessage(
        `Feather-Flow: failed to refresh index — ${msg}`
      );
    }
  }

  private rebuildMaps(entries: LsModelEntry[]): void {
    this.nameMap.clear();
    this.pathMap.clear();

    for (const entry of entries) {
      // Case-insensitive key for name lookup
      this.nameMap.set(entry.name.toLowerCase(), entry);

      if (entry.path) {
        this.pathMap.set(entry.path, entry);
      }
    }
  }

  private startWatching(): void {
    const pattern = new vscode.RelativePattern(this.projectDir, "**/*.{sql,yml,yaml}");
    this.watcher = vscode.workspace.createFileSystemWatcher(pattern);

    const debouncedRefresh = () => {
      if (this.refreshTimer) {
        clearTimeout(this.refreshTimer);
      }
      this.refreshTimer = setTimeout(() => this.refresh(), 1000);
    };

    this.watcher.onDidChange(debouncedRefresh);
    this.watcher.onDidCreate(debouncedRefresh);
    this.watcher.onDidDelete(debouncedRefresh);
  }

  /** Look up a model by name (case-insensitive). */
  getModelByName(name: string): LsModelEntry | undefined {
    return this.nameMap.get(name.toLowerCase());
  }

  /** Look up a model by its absolute file path. */
  getModelByPath(filePath: string): LsModelEntry | undefined {
    return this.pathMap.get(filePath);
  }

  /** Return all entries. */
  getAllEntries(): LsModelEntry[] {
    return [...this.nameMap.values()];
  }

  /** Return only model-type entries (no sources or functions). */
  getModels(): LsModelEntry[] {
    return this.getAllEntries().filter((e) => e.type === "model");
  }

  /** Number of model-type entries. */
  getModelCount(): number {
    return this.getModels().length;
  }

  /** Return models whose `model_deps` include the given name. */
  getDownstream(name: string): LsModelEntry[] {
    const lower = name.toLowerCase();
    return this.getModels().filter((m) =>
      m.model_deps.some((d) => d.toLowerCase() === lower)
    );
  }

  /** Project name derived from `featherflow.yml`. */
  getProjectName(): string {
    return this.projectName;
  }

  /** Set the project name (called from extension activation). */
  setProjectName(name: string): void {
    this.projectName = name;
  }

  dispose(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
    }
    this.watcher?.dispose();
    this._onDidChange.dispose();
  }
}
