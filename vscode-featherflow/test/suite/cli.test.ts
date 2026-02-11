/**
 * Unit tests for CLI version parsing.
 */

import { describe, expect, it } from "vitest";
import { parseVersion } from "../../src/cli.js";

describe("parseVersion", () => {
  it("parses standard ff version output", () => {
    const v = parseVersion("ff 0.5.0");
    expect(v.major).toBe(0);
    expect(v.minor).toBe(5);
    expect(v.patch).toBe(0);
    expect(v.raw).toBe("ff 0.5.0");
  });

  it("parses version with leading text", () => {
    const v = parseVersion("featherflow version 1.2.3");
    expect(v.major).toBe(1);
    expect(v.minor).toBe(2);
    expect(v.patch).toBe(3);
  });

  it("parses bare version number", () => {
    const v = parseVersion("10.20.30");
    expect(v.major).toBe(10);
    expect(v.minor).toBe(20);
    expect(v.patch).toBe(30);
  });

  it("trims whitespace from raw", () => {
    const v = parseVersion("  ff 0.1.0\n");
    expect(v.raw).toBe("ff 0.1.0");
  });

  it("throws on invalid version string", () => {
    expect(() => parseVersion("no version here")).toThrow(
      "Could not parse version"
    );
  });

  it("throws on empty string", () => {
    expect(() => parseVersion("")).toThrow("Could not parse version");
  });
});
