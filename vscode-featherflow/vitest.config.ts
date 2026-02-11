import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    alias: {
      // Stub out the vscode module for unit tests
      vscode: path.resolve(__dirname, "test/__mocks__/vscode.ts"),
    },
  },
});
