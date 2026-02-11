/**
 * Go to Definition provider for Feather-Flow model references.
 *
 * When a user Ctrl+Clicks a table name in a FROM or JOIN clause inside a .sql
 * file, this provider resolves it to the model's .sql file path using the
 * project index.
 */

import * as vscode from "vscode";
import type { ProjectIndex } from "../projectIndex.js";

/**
 * Pattern that matches a FROM or JOIN keyword (possibly with type prefix)
 * at the end of text. Used to detect if the current word is in a table
 * reference context.
 */
const FROM_JOIN_PATTERN =
  /\b(?:FROM|(?:LEFT|RIGHT|INNER|FULL|CROSS)\s+(?:OUTER\s+)?JOIN)\s*$/i;

/**
 * Check whether the text leading up to a word position ends with a FROM/JOIN
 * keyword, indicating the word is a table reference.
 */
export function isTableReference(
  document: { getText(range?: { start: { line: number; character: number }; end: { line: number; character: number } }): string; lineAt(line: number): { text: string } },
  wordRange: { start: { line: number; character: number } }
): boolean {
  // Collect text from up to 5 lines before the word through the character
  // just before the word starts. This handles multi-line FROM/JOIN clauses.
  const startLine = Math.max(0, wordRange.start.line - 5);
  const precedingText = document.getText({
    start: { line: startLine, character: 0 },
    end: { line: wordRange.start.line, character: wordRange.start.character },
  });

  return FROM_JOIN_PATTERN.test(precedingText);
}

export class DefinitionProvider implements vscode.DefinitionProvider {
  constructor(private index: ProjectIndex) {}

  provideDefinition(
    document: vscode.TextDocument,
    position: vscode.Position,
    _token: vscode.CancellationToken
  ): vscode.Definition | undefined {
    const wordRange = document.getWordRangeAtPosition(
      position,
      /[a-zA-Z_][a-zA-Z0-9_]*/
    );
    if (!wordRange) {
      return undefined;
    }

    const word = document.getText(wordRange);

    // Only provide definitions for table references in FROM/JOIN context
    if (!isTableReference(document, wordRange)) {
      return undefined;
    }

    const model = this.index.getModelByName(word);
    if (!model?.path) {
      return undefined;
    }

    return new vscode.Location(
      vscode.Uri.file(model.path),
      new vscode.Position(0, 0)
    );
  }
}
