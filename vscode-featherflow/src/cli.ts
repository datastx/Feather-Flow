/**
 * Binary discovery, version check, and CLI command execution.
 */

import { execFile } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import * as vscode from "vscode";
import type { LsModelEntry, VersionInfo } from "./types.js";

const execFileAsync = promisify(execFile);

/** Output channel for CLI stderr and diagnostics. */
let outputChannel: vscode.OutputChannel | undefined;

function getOutputChannel(): vscode.OutputChannel {
  if (!outputChannel) {
    outputChannel = vscode.window.createOutputChannel("Feather-Flow");
  }
  return outputChannel;
}

/**
 * Resolve the path to the `ff` binary.
 *
 * Checks the `featherflow.binaryPath` setting first, then falls back to
 * looking for `ff` on the system PATH.
 */
export async function resolveBinaryPath(): Promise<string> {
  const config = vscode.workspace.getConfiguration("featherflow");
  const configured = config.get<string>("binaryPath", "").trim();

  if (configured) {
    return configured;
  }

  // Check if `ff` is on PATH by running `which ff`
  try {
    const { stdout } = await execFileAsync("which", ["ff"]);
    const found = stdout.trim();
    if (found) {
      return found;
    }
  } catch {
    // not found on PATH
  }

  // VS Code often has a stripped-down PATH that doesn't include ~/.cargo/bin.
  // Try a login shell to pick up the user's full PATH.
  try {
    const shell = process.env.SHELL || "/bin/zsh";
    const { stdout } = await execFileAsync(shell, ["-lc", "which ff"], {
      timeout: 5000,
    });
    const found = stdout.trim();
    if (found) {
      return found;
    }
  } catch {
    // not found via login shell either
  }

  // Check well-known install locations directly
  const candidates = [
    path.join(os.homedir(), ".cargo", "bin", "ff"),
    "/usr/local/bin/ff",
    "/opt/homebrew/bin/ff",
  ];
  for (const candidate of candidates) {
    try {
      fs.accessSync(candidate, fs.constants.X_OK);
      return candidate;
    } catch {
      // not found at this path
    }
  }

  throw new Error(
    "Could not find the `ff` binary. Install Feather-Flow or set `featherflow.binaryPath` in settings."
  );
}

/**
 * Parse a version string like "ff 0.5.0" into structured version info.
 */
export function parseVersion(raw: string): VersionInfo {
  const match = raw.match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) {
    throw new Error(`Could not parse version from: ${raw}`);
  }
  return {
    raw: raw.trim(),
    major: parseInt(match[1], 10),
    minor: parseInt(match[2], 10),
    patch: parseInt(match[3], 10),
  };
}

/**
 * Get the version of the `ff` binary.
 */
export async function getVersion(bin: string): Promise<VersionInfo> {
  const { stdout } = await execFileAsync(bin, ["--version"], {
    timeout: 5000,
  });
  return parseVersion(stdout);
}

/**
 * Run an `ff` CLI command and parse the JSON output.
 */
export async function runCommand<T>(
  bin: string,
  projectDir: string,
  args: string[],
  timeout = 30_000
): Promise<{ data: T; exitCode: number }> {
  const fullArgs = ["-p", projectDir, ...args];
  const channel = getOutputChannel();
  channel.appendLine(`> ff ${fullArgs.join(" ")}`);

  try {
    const { stdout, stderr } = await execFileAsync(bin, fullArgs, { timeout });

    if (stderr.trim()) {
      channel.appendLine(stderr.trim());
    }

    const data = JSON.parse(stdout) as T;
    return { data, exitCode: 0 };
  } catch (err: unknown) {
    const error = err as Error & { code?: number; stderr?: string };
    if (error.stderr) {
      channel.appendLine(error.stderr);
    }
    throw new Error(`ff command failed: ${error.message}`);
  }
}

/**
 * Run `ff ls --output json` and return the parsed model entries.
 */
export async function ffLs(
  bin: string,
  projectDir: string
): Promise<LsModelEntry[]> {
  const { data } = await runCommand<LsModelEntry[]>(bin, projectDir, [
    "ls",
    "--output",
    "json",
  ]);
  return data;
}

/**
 * Dispose the output channel.
 */
export function disposeOutputChannel(): void {
  outputChannel?.dispose();
  outputChannel = undefined;
}
