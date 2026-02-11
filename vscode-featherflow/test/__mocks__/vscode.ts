/**
 * Minimal vscode module mock for unit testing outside the Extension Host.
 */

export const window = {
  createOutputChannel: () => ({
    appendLine: () => {},
    dispose: () => {},
  }),
  createStatusBarItem: () => ({
    text: "",
    command: "",
    tooltip: "",
    show: () => {},
    dispose: () => {},
  }),
  showWarningMessage: () => {},
  showTextDocument: async () => {},
  activeTextEditor: undefined,
  registerTreeDataProvider: () => ({ dispose: () => {} }),
  registerWebviewViewProvider: () => ({ dispose: () => {} }),
};

export const workspace = {
  getConfiguration: () => ({
    get: (_key: string, defaultValue?: unknown) => defaultValue ?? "",
  }),
  createFileSystemWatcher: () => ({
    onDidChange: () => ({ dispose: () => {} }),
    onDidCreate: () => ({ dispose: () => {} }),
    onDidDelete: () => ({ dispose: () => {} }),
    dispose: () => {},
  }),
  workspaceFolders: undefined,
  findFiles: async () => [],
  fs: {
    stat: async () => {
      throw new Error("not found");
    },
  },
};

export const languages = {
  registerDefinitionProvider: () => ({ dispose: () => {} }),
};

export const commands = {
  registerCommand: () => ({ dispose: () => {} }),
};

export class Uri {
  static file(path: string) {
    return { fsPath: path, scheme: "file" };
  }
}

export class Position {
  constructor(
    public line: number,
    public character: number
  ) {}
}

export class Range {
  constructor(
    public start: Position,
    public end: Position
  ) {}
}

export class Location {
  constructor(
    public uri: unknown,
    public range: unknown
  ) {}
}

export class EventEmitter {
  private listeners: Array<(...args: unknown[]) => void> = [];
  event = (listener: (...args: unknown[]) => void) => {
    this.listeners.push(listener);
    return { dispose: () => {} };
  };
  fire(...args: unknown[]) {
    for (const l of this.listeners) l(...args);
  }
  dispose() {
    this.listeners = [];
  }
}

export class MarkdownString {
  constructor(public value: string) {}
}

export class ThemeIcon {
  constructor(public id: string) {}
}

export class RelativePattern {
  constructor(
    public base: unknown,
    public pattern: string
  ) {}
}

export enum TreeItemCollapsibleState {
  None = 0,
  Collapsed = 1,
  Expanded = 2,
}

export class TreeItem {
  label?: string;
  collapsibleState?: TreeItemCollapsibleState;
  description?: string;
  iconPath?: unknown;
  command?: unknown;
  tooltip?: unknown;
  contextValue?: string;

  constructor(label: string, collapsibleState?: TreeItemCollapsibleState) {
    this.label = label;
    this.collapsibleState = collapsibleState;
  }
}

export enum StatusBarAlignment {
  Left = 1,
  Right = 2,
}

export enum ViewColumn {
  Beside = -2,
}
