//! Featherflow CLI - a dbt-like tool for SQL templating and execution

use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;
mod context;

use cli::Cli;
use commands::{compile, ls, parse, run, test};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        cli::Commands::Parse(args) => parse::execute(args, &cli.global).await,
        cli::Commands::Compile(args) => compile::execute(args, &cli.global).await,
        cli::Commands::Run(args) => run::execute(args, &cli.global).await,
        cli::Commands::Ls(args) => ls::execute(args, &cli.global).await,
        cli::Commands::Test(args) => test::execute(args, &cli.global).await,
    }
}
