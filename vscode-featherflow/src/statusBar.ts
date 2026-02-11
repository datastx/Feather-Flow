/**
 * Status bar item showing project name and model count.
 */

import * as vscode from "vscode";
import type { ProjectIndex } from "./projectIndex.js";

export class StatusBar implements vscode.Disposable {
  private item: vscode.StatusBarItem;
  private subscription: vscode.Disposable;

  constructor(index: ProjectIndex) {
    this.item = vscode.window.createStatusBarItem(
      vscode.StatusBarAlignment.Left,
      50
    );
    this.item.command = "featherflow.refreshIndex";
    this.item.tooltip = "Click to refresh Feather-Flow project index";

    this.update(index);
    this.item.show();

    this.subscription = index.onDidChange(() => this.update(index));
  }

  private update(index: ProjectIndex): void {
    const name = index.getProjectName() || "featherflow";
    const count = index.getModelCount();
    this.item.text = `$(database) ${name}: ${count} models`;
  }

  dispose(): void {
    this.subscription.dispose();
    this.item.dispose();
  }
}
