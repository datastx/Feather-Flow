/**
 * Feather-Flow VS Code extension entry point.
 *
 * Activates when a workspace contains `featherflow.yml` or `featherflow.yaml`.
 * Provides Go to Definition for model references, a sidebar tree, status bar,
 * and SQL/YAML schema toggling.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import * as vscode from "vscode";
import { disposeOutputChannel, getVersion, resolveBinaryPath } from "./cli.js";
import { openSchema } from "./commands/openSchema.js";
import { ProjectIndex } from "./projectIndex.js";
import { DefinitionProvider } from "./providers/definitionProvider.js";
import { LineageViewProvider } from "./providers/lineageViewProvider.js";
import { TreeProvider } from "./providers/treeProvider.js";
import { StatusBar } from "./statusBar.js";

const disposables: vscode.Disposable[] = [];

/** Dedicated output channel — created once, used for all logging. */
let log: vscode.OutputChannel;

function getLog(): vscode.OutputChannel {
  if (!log) {
    log = vscode.window.createOutputChannel("Feather-Flow");
  }
  return log;
}

export async function activate(
  context: vscode.ExtensionContext
): Promise<void> {
  const out = getLog();
  out.appendLine("Feather-Flow extension activating...");

  // Always register the openSchema command (works without CLI)
  context.subscriptions.push(
    vscode.commands.registerCommand("featherflow.openSchema", openSchema)
  );

  // 1. Find project directory
  out.appendLine("Searching for featherflow.yml...");
  const projectDir = await findProjectDir();
  if (!projectDir) {
    out.appendLine("ERROR: No featherflow project found in this workspace.");
    registerEmptyTree(context);
    context.subscriptions.push(
      vscode.commands.registerCommand("featherflow.refreshIndex", () =>
        vscode.window.showWarningMessage(
          "Feather-Flow: no project found in this workspace."
        )
      )
    );
    return;
  }
  out.appendLine(`Found project dir: ${projectDir}`);

  // 2. Resolve binary
  out.appendLine("Looking for ff binary...");
  let binaryPath: string;
  try {
    binaryPath = await resolveBinaryPath();
    out.appendLine(`Found ff binary: ${binaryPath}`);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    out.appendLine(`ERROR: ${msg}`);
    vscode.window.showWarningMessage(`Feather-Flow: ${msg}`);
    registerEmptyTree(context);
    context.subscriptions.push(
      vscode.commands.registerCommand("featherflow.refreshIndex", () =>
        vscode.window.showWarningMessage(
          "Feather-Flow: `ff` binary not found."
        )
      )
    );
    return;
  }

  // 3. Version check (informational only)
  try {
    const version = await getVersion(binaryPath);
    out.appendLine(`ff version: ${version.raw}`);
  } catch {
    out.appendLine("WARNING: could not determine ff version");
  }

  // 4. Initialize project index
  const index = new ProjectIndex(binaryPath, projectDir);
  disposables.push(index);

  // Read project name from featherflow.yml
  const projectName = readProjectName(projectDir);
  if (projectName) {
    index.setProjectName(projectName);
    out.appendLine(`Project name: ${projectName}`);
  }

  out.appendLine("Initializing project index (running ff ls)...");
  try {
    await index.initialize();
    out.appendLine(`Index loaded: ${index.getModelCount()} models, ${index.getAllEntries().length} total entries`);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    out.appendLine(`ERROR initializing index: ${msg}`);
    vscode.window.showWarningMessage(
      `Feather-Flow: failed to initialize index — ${msg}`
    );
  }

  // 5. Register Definition Provider
  const defProvider = new DefinitionProvider(index);
  context.subscriptions.push(
    vscode.languages.registerDefinitionProvider(
      { language: "sql", scheme: "file" },
      defProvider
    )
  );

  // 6. Register Tree View
  const treeProvider = new TreeProvider(index);
  disposables.push(treeProvider);
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("featherflowModels", treeProvider)
  );

  // 7. Register Lineage Panel
  const lineageProvider = new LineageViewProvider(context.extensionUri, index);
  disposables.push(lineageProvider);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(
      LineageViewProvider.viewType,
      lineageProvider
    )
  );

  // 8. Create Status Bar
  const statusBar = new StatusBar(index);
  disposables.push(statusBar);

  // 9. Register refresh command
  context.subscriptions.push(
    vscode.commands.registerCommand("featherflow.refreshIndex", () =>
      index.refresh()
    )
  );

  // Track all disposables
  context.subscriptions.push(...disposables);
  out.appendLine("Feather-Flow extension activated successfully.");
}

/** Register an empty tree provider so the view doesn't show an error. */
function registerEmptyTree(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("featherflowModels", {
      getTreeItem: (el: vscode.TreeItem) => el,
      getChildren: () => [],
    })
  );
}

export function deactivate(): void {
  for (const d of disposables) {
    d.dispose();
  }
  disposables.length = 0;
  disposeOutputChannel();
  log?.dispose();
}

/**
 * Find the project directory containing featherflow.yml.
 *
 * Checks the `featherflow.projectDir` setting first, then searches
 * workspace folders for a featherflow.yml or featherflow.yaml file.
 */
async function findProjectDir(): Promise<string | undefined> {
  const out = getLog();
  const config = vscode.workspace.getConfiguration("featherflow");
  const configured = config.get<string>("projectDir", "").trim();
  const folders = vscode.workspace.workspaceFolders;

  if (configured) {
    // Resolve relative paths against the first workspace folder
    let resolved = configured;
    if (!path.isAbsolute(resolved) && folders && folders.length > 0) {
      resolved = path.resolve(folders[0].uri.fsPath, resolved);
    }
    out.appendLine(`Using configured projectDir: ${resolved}`);
    return resolved;
  }
  if (!folders) {
    out.appendLine("No workspace folders open.");
    return undefined;
  }

  out.appendLine(`Searching ${folders.length} workspace folder(s)...`);
  for (const folder of folders) {
    out.appendLine(`  Checking root: ${folder.uri.fsPath}`);
    const ymlPath = path.join(folder.uri.fsPath, "featherflow.yml");
    const yamlPath = path.join(folder.uri.fsPath, "featherflow.yaml");

    if (fs.existsSync(ymlPath)) {
      out.appendLine(`  Found: ${ymlPath}`);
      return folder.uri.fsPath;
    }
    if (fs.existsSync(yamlPath)) {
      out.appendLine(`  Found: ${yamlPath}`);
      return folder.uri.fsPath;
    }
  }

  // Deep search for monorepos / nested projects
  out.appendLine("  Not at root, searching recursively...");
  for (const folder of folders) {
    const uris = await vscode.workspace.findFiles(
      new vscode.RelativePattern(folder, "**/featherflow.{yml,yaml}"),
      "{**/node_modules/**,**/target/**}",
      1
    );
    if (uris.length > 0) {
      const dir = path.dirname(uris[0].fsPath);
      out.appendLine(`  Found: ${uris[0].fsPath}`);
      return dir;
    }
  }

  out.appendLine("  No featherflow.yml found anywhere in workspace.");
  return undefined;
}

/**
 * Read the project name from featherflow.yml without a full YAML parser.
 * Looks for the `name:` key on its own line.
 */
function readProjectName(projectDir: string): string | undefined {
  for (const filename of ["featherflow.yml", "featherflow.yaml"]) {
    const filePath = path.join(projectDir, filename);
    try {
      const content = fs.readFileSync(filePath, "utf-8");
      const match = content.match(/^name:\s*["']?([^"'\n]+?)["']?\s*$/m);
      if (match) {
        return match[1].trim();
      }
    } catch {
      // file not found, try next
    }
  }
  return undefined;
}
