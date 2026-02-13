//! Build command implementation
//!
//! Orchestrates seed, run, and test phases in a single invocation.
//! With `--fail-fast`, stops at the first phase that fails.

use anyhow::Result;

use crate::cli::{BuildArgs, GlobalArgs, OutputFormat, RunArgs, SeedArgs, TestArgs};
use crate::commands::common::ExitCode;
use crate::commands::{run, seed, test};

/// Extract a structured exit code from an anyhow error.
///
/// Returns `Ok(None)` on success, `Ok(Some(code))` for structured `ExitCode`
/// failures, and propagates real errors via `Err`.
fn classify_phase_result(result: Result<()>) -> Result<Option<i32>> {
    match result {
        Ok(()) => Ok(None),
        Err(err) => match err.downcast_ref::<ExitCode>() {
            Some(ec) => Ok(Some(ec.0)),
            None => Err(err),
        },
    }
}

/// Track the highest-severity exit code across phases.
fn record_exit_code(worst: &mut Option<i32>, code: i32) {
    *worst = Some(worst.map_or(code, |c| c.max(code)));
}

/// Execute the build command: seed -> run -> test.
pub async fn execute(args: &BuildArgs, global: &GlobalArgs) -> Result<()> {
    let quiet = args.quiet || args.output == OutputFormat::Json;

    if !quiet {
        println!("Starting build...\n");
    }

    let mut worst_exit_code: Option<i32> = None;

    if !quiet {
        println!("=== Phase 1/3: Seed ===\n");
    }
    let seed_args = SeedArgs {
        seeds: None,
        full_refresh: args.full_refresh,
        show_columns: false,
    };
    if let Some(code) = classify_phase_result(seed::execute(&seed_args, global).await)? {
        record_exit_code(&mut worst_exit_code, code);
        if args.fail_fast {
            return Err(ExitCode(code).into());
        }
    }

    if !quiet {
        println!("\n=== Phase 2/3: Run ===\n");
    }
    let run_args = RunArgs {
        nodes: args.nodes.clone(),
        exclude: args.exclude.clone(),
        full_refresh: args.full_refresh,
        fail_fast: args.fail_fast,
        no_cache: false,
        defer: None,
        state: None,
        threads: args.threads,
        resume: false,
        retry_failed: false,
        state_file: None,
        output: args.output,
        quiet: args.quiet,
        smart: false,
        skip_static_analysis: args.skip_static_analysis,
    };
    if let Some(code) = classify_phase_result(run::execute(&run_args, global).await)? {
        record_exit_code(&mut worst_exit_code, code);
        if args.fail_fast {
            return Err(ExitCode(code).into());
        }
    }

    if !quiet {
        println!("\n=== Phase 3/3: Test ===\n");
    }
    let test_args = TestArgs {
        nodes: args.nodes.clone(),
        fail_fast: args.fail_fast,
        store_failures: args.store_failures,
        warn_only: false,
        threads: args.threads,
        output: args.output,
        quiet: args.quiet,
    };
    if let Some(code) = classify_phase_result(test::execute(&test_args, global).await)? {
        record_exit_code(&mut worst_exit_code, code);
    }

    if let Some(code) = worst_exit_code {
        if !quiet {
            println!("\nBuild completed with failures.");
        }
        return Err(ExitCode(code).into());
    }

    if !quiet {
        println!("\nBuild completed successfully.");
    }

    Ok(())
}
