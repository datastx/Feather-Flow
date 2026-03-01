//! Shared formatting utilities wrapping the sqlfmt library.
//!
//! Formatting is a standalone CI / developer tool (`ff fmt`).  It never
//! runs automatically during `ff compile` or `ff run` — those pipelines
//! must produce byte-identical SQL regardless of format settings so that
//! formatting can never break execution.

use ff_core::config::{Dialect, FormatConfig};
use sqlfmt::report::Report;
use sqlfmt::Mode;
use std::path::PathBuf;

/// Build a sqlfmt [`Mode`] from Featherflow's [`FormatConfig`] and [`Dialect`].
pub(crate) fn build_sqlfmt_mode(config: &FormatConfig, dialect: Dialect) -> Mode {
    let dialect_name = match dialect {
        Dialect::DuckDb => "duckdb".to_string(),
        Dialect::Snowflake => "polyglot".to_string(),
    };

    Mode {
        line_length: config.line_length,
        dialect_name,
        no_jinjafmt: config.no_jinjafmt,
        quiet: true,
        no_progressbar: true,
        check: false,
        diff: false,
        fast: false,
        exclude: Vec::new(),
        encoding: "utf-8".to_string(),
        verbose: false,
        no_color: true,
        force_color: false,
        threads: 0,
        single_process: false,
        reset_cache: false,
    }
}

/// Run sqlfmt on a list of files, returning the report.
pub(crate) async fn format_files(files: &[PathBuf], mode: &Mode) -> Report {
    sqlfmt::run(files, mode).await
}

#[cfg(test)]
#[path = "format_helpers_test.rs"]
mod tests;
