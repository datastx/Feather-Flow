/**
 * TypeScript interfaces matching the Feather-Flow CLI JSON output.
 *
 * These types correspond to the `ModelInfo` struct in
 * `crates/ff-cli/src/commands/ls.rs` serialized via serde.
 */

/** A single entry from `ff ls --output json`. */
export interface LsModelEntry {
  /** Model/source/function name */
  name: string;
  /**
   * Resource type. Values: "model", "source", "function (scalar)", "function (table)".
   * Serialized from Rust field `resource_type` via `#[serde(rename = "type")]`.
   */
  type: string;
  /** Absolute path to the .sql file. Absent for sources. */
  path?: string;
  /**
   * Materialization strategy. Values: "view", "table", "incremental", "ephemeral".
   * Only present for models.
   */
  materialized?: string;
  /** Schema name. */
  schema?: string | null;
  /** Names of other models this resource depends on. */
  model_deps: string[];
  /** Names of external tables this resource depends on. */
  external_deps: string[];
}

/** Parsed version info from `ff --version`. */
export interface VersionInfo {
  raw: string;
  major: number;
  minor: number;
  patch: number;
}
