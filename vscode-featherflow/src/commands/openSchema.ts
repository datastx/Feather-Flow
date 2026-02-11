/**
 * Toggle between .sql and .yml for the same model.
 *
 * Leverages the directory-per-model convention where each model lives in
 * `models/<name>/<name>.sql + <name>.yml`.
 */

import * as path from "node:path";
import * as vscode from "vscode";

export async function openSchema(): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  if (!editor) {
    return;
  }

  const currentPath = editor.document.uri.fsPath;
  const ext = path.extname(currentPath).toLowerCase();
  const dir = path.dirname(currentPath);
  const baseName = path.basename(currentPath, path.extname(currentPath));

  let targetPath: string | undefined;

  if (ext === ".sql") {
    // Look for .yml or .yaml
    const ymlPath = path.join(dir, `${baseName}.yml`);
    const yamlPath = path.join(dir, `${baseName}.yaml`);

    try {
      await vscode.workspace.fs.stat(vscode.Uri.file(ymlPath));
      targetPath = ymlPath;
    } catch {
      try {
        await vscode.workspace.fs.stat(vscode.Uri.file(yamlPath));
        targetPath = yamlPath;
      } catch {
        vscode.window.showWarningMessage(
          `No schema file found for ${baseName}.sql`
        );
        return;
      }
    }
  } else if (ext === ".yml" || ext === ".yaml") {
    // Look for .sql
    const sqlPath = path.join(dir, `${baseName}.sql`);
    try {
      await vscode.workspace.fs.stat(vscode.Uri.file(sqlPath));
      targetPath = sqlPath;
    } catch {
      vscode.window.showWarningMessage(
        `No SQL file found for ${baseName}${ext}`
      );
      return;
    }
  } else {
    return;
  }

  // Open in a side-by-side column
  await vscode.window.showTextDocument(vscode.Uri.file(targetPath), {
    viewColumn: vscode.ViewColumn.Beside,
    preserveFocus: false,
  });
}
