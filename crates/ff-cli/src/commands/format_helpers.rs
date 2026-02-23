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
        // Quiet library-level output; the CLI handles its own reporting.
        quiet: true,
        no_progressbar: true,
        // Sensible defaults for library usage
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
mod tests {
    use super::*;
    use ff_core::config::{Dialect, FormatConfig};

    #[test]
    fn test_build_mode_duckdb_dialect() {
        let config = FormatConfig::default();
        let mode = build_sqlfmt_mode(&config, Dialect::DuckDb);
        assert_eq!(mode.dialect_name, "duckdb");
        assert_eq!(mode.line_length, 88);
        assert!(!mode.no_jinjafmt);
        assert!(mode.quiet);
    }

    #[test]
    fn test_build_mode_snowflake_dialect() {
        let config = FormatConfig::default();
        let mode = build_sqlfmt_mode(&config, Dialect::Snowflake);
        assert_eq!(mode.dialect_name, "polyglot");
    }

    #[test]
    fn test_build_mode_custom_line_length() {
        let config = FormatConfig {
            line_length: 120,
            ..Default::default()
        };
        let mode = build_sqlfmt_mode(&config, Dialect::DuckDb);
        assert_eq!(mode.line_length, 120);
    }

    #[test]
    fn test_build_mode_no_jinjafmt() {
        let config = FormatConfig {
            no_jinjafmt: true,
            ..Default::default()
        };
        let mode = build_sqlfmt_mode(&config, Dialect::DuckDb);
        assert!(mode.no_jinjafmt);
    }

    #[test]
    fn test_format_string_basic() {
        let mode = build_sqlfmt_mode(&FormatConfig::default(), Dialect::DuckDb);
        let result = sqlfmt::format_string("select 1", &mode);
        assert!(result.is_ok());
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn test_jinja_preservation() {
        let mode = build_sqlfmt_mode(&FormatConfig::default(), Dialect::DuckDb);
        let sql = "select {{ config() }}, id from orders";
        let result = sqlfmt::format_string(sql, &mode).unwrap();
        // The Jinja expression should survive formatting
        assert!(result.contains("{{ config() }}") || result.contains("{{config()}}"));
    }
}
