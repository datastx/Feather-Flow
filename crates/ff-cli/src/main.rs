//! Featherflow CLI - a dbt-like tool for SQL templating and execution

use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;

use cli::Cli;
use commands::{
    analyze, build, clean, compile, docs, function, init, lineage, ls, meta, parse, rules, run,
    run_operation, seed, test, validate,
};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result: Result<()> = match &cli.command {
        cli::Commands::Init(args) => init::execute(args).await,
        cli::Commands::Parse(args) => parse::execute(args, &cli.global).await,
        cli::Commands::Compile(args) => compile::execute(args, &cli.global).await,
        cli::Commands::Run(args) => run::execute(args, &cli.global).await,
        cli::Commands::Ls(args) => ls::execute(args, &cli.global).await,
        cli::Commands::Test(args) => test::execute(args, &cli.global).await,
        cli::Commands::Seed(args) => seed::execute(args, &cli.global).await,
        cli::Commands::Validate(args) => validate::execute(args, &cli.global).await,
        cli::Commands::Docs(args) => docs::execute(args, &cli.global).await,
        cli::Commands::Clean(args) => clean::execute(args, &cli.global).await,
        cli::Commands::RunOperation(args) => run_operation::execute(args, &cli.global).await,
        cli::Commands::Lineage(args) => lineage::execute(args, &cli.global).await,
        cli::Commands::Analyze(args) => analyze::execute(args, &cli.global).await,
        cli::Commands::Function(args) => function::execute(args, &cli.global).await,
        cli::Commands::Build(args) => build::execute(args, &cli.global).await,
        cli::Commands::Rules(args) => rules::execute(args, &cli.global).await,
        cli::Commands::Meta(args) => meta::execute(args, &cli.global).await,
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
