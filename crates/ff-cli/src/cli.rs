//! CLI argument definitions using clap derive API

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Featherflow - A dbt-like CLI tool for SQL templating and execution
#[derive(Parser, Debug)]
#[command(name = "ff")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Global options
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Subcommand to execute
    #[command(subcommand)]
    pub command: Commands,
}

/// Global arguments available to all commands
#[derive(Args, Debug, Clone)]
pub struct GlobalArgs {
    /// Enable verbose output
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Path to project directory
    #[arg(short = 'p', long, global = true, default_value = ".")]
    pub project_dir: String,

    /// Override config file path
    #[arg(short, long, global = true)]
    pub config: Option<String>,

    /// Override target (database connection)
    #[arg(short, long, global = true)]
    pub target: Option<String>,
}

/// Available subcommands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Parse SQL files and output AST/dependencies
    Parse(ParseArgs),

    /// Compile Jinja templates to SQL
    Compile(CompileArgs),

    /// Execute compiled SQL against the database
    Run(RunArgs),

    /// List models and their dependencies
    Ls(LsArgs),

    /// Run schema tests
    Test(TestArgs),
}

/// Arguments for the parse command
#[derive(Args, Debug)]
pub struct ParseArgs {
    /// Model names to parse (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "deps")]
    pub output: ParseOutput,

    /// Override SQL dialect
    #[arg(short, long)]
    pub dialect: Option<String>,
}

/// Parse output formats
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseOutput {
    /// JSON AST output
    Json,
    /// Human-readable tree
    Pretty,
    /// Dependency list only
    Deps,
}

/// Arguments for the compile command
#[derive(Args, Debug)]
pub struct CompileArgs {
    /// Model names to compile (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Override output directory
    #[arg(short, long)]
    pub output_dir: Option<String>,

    /// Override/add variables as JSON
    #[arg(long)]
    pub vars: Option<String>,
}

/// Arguments for the run command
#[derive(Args, Debug)]
pub struct RunArgs {
    /// Model names to run (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// dbt-style selector (+model, model+)
    #[arg(short, long)]
    pub select: Option<String>,

    /// Drop and recreate all models
    #[arg(long)]
    pub full_refresh: bool,
}

/// Arguments for the ls command
#[derive(Args, Debug)]
pub struct LsArgs {
    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: LsOutput,

    /// dbt-style selector to filter models
    #[arg(short, long)]
    pub select: Option<String>,
}

/// List output formats
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsOutput {
    /// Table format
    Table,
    /// JSON output
    Json,
    /// Dependency tree
    Tree,
}

/// Arguments for the test command
#[derive(Args, Debug)]
pub struct TestArgs {
    /// Model names to test (comma-separated, default: all with tests)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Stop on first failure
    #[arg(long)]
    pub fail_fast: bool,
}
