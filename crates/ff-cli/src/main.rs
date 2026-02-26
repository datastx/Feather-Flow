//! Featherflow CLI - a dbt-like tool for SQL templating and execution

use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;

use cli::Cli;
use commands::{dt, run, run_macro};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result: Result<()> = match &cli.command {
        cli::Commands::Run(args) => run::execute(args, &cli.global).await,
        cli::Commands::RunMacro(args) => run_macro::execute(args, &cli.global).await,
        cli::Commands::Dt(args) => dt::execute(args, &cli.global).await,
    };

    if let Err(err) = result {
        // Check if this is an ExitCode (structured exit, not a real error)
        if let Some(exit_code) = err.downcast_ref::<commands::common::ExitCode>() {
            std::process::exit(exit_code.0);
        }
        // Real error — print and exit 1
        eprintln!("Error: {:?}", err);
        std::process::exit(1);
    }
}
