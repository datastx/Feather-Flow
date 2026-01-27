//! Featherflow CLI - a dbt-like tool for SQL templating and execution

use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;

use cli::Cli;
use commands::{
    clean, compile, docs, ls, parse, run, run_operation, seed, snapshot, source, test, validate,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        cli::Commands::Parse(args) => parse::execute(args, &cli.global).await,
        cli::Commands::Compile(args) => compile::execute(args, &cli.global).await,
        cli::Commands::Run(args) => run::execute(args, &cli.global).await,
        cli::Commands::Ls(args) => ls::execute(args, &cli.global).await,
        cli::Commands::Test(args) => test::execute(args, &cli.global).await,
        cli::Commands::Seed(args) => seed::execute(args, &cli.global).await,
        cli::Commands::Validate(args) => validate::execute(args, &cli.global).await,
        cli::Commands::Docs(args) => docs::execute(args, &cli.global).await,
        cli::Commands::Clean(args) => clean::execute(args, &cli.global).await,
        cli::Commands::Source(args) => source::execute(args, &cli.global).await,
        cli::Commands::Snapshot(args) => snapshot::execute(args, &cli.global).await,
        cli::Commands::RunOperation(args) => run_operation::execute(args, &cli.global).await,
    }
}
