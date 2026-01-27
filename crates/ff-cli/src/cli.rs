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

    /// Load CSV seed files into the database
    Seed(SeedArgs),

    /// Validate project without running
    Validate(ValidateArgs),

    /// Generate documentation from schema files
    Docs(DocsArgs),

    /// Remove generated artifacts
    Clean(CleanArgs),

    /// Source-related operations
    Source(SourceArgs),

    /// Execute snapshots (SCD Type 2)
    Snapshot(SnapshotArgs),

    /// Execute a standalone operation (macro that returns SQL)
    RunOperation(RunOperationArgs),
}

/// Arguments for the parse command
#[derive(Args, Debug)]
pub struct ParseArgs {
    /// Model names to parse (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "pretty")]
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

    /// Parse and validate only, don't write output files
    #[arg(long)]
    pub parse_only: bool,
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

    /// Exclude models matching this pattern
    #[arg(short, long)]
    pub exclude: Option<String>,

    /// Drop and recreate all models
    #[arg(long)]
    pub full_refresh: bool,

    /// Stop on first model failure
    #[arg(long)]
    pub fail_fast: bool,

    /// Skip manifest cache and force recompilation
    #[arg(long)]
    pub no_cache: bool,

    /// Defer to another manifest for unselected models
    #[arg(long)]
    pub defer: Option<String>,

    /// Path to manifest for state comparison (enables state:modified selector)
    #[arg(long)]
    pub state: Option<String>,

    /// Number of threads for parallel execution (default: 1)
    #[arg(long, default_value = "1")]
    pub threads: usize,
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

    /// Exclude models matching this pattern
    #[arg(short, long)]
    pub exclude: Option<String>,

    /// Filter by resource type
    #[arg(long, value_enum)]
    pub resource_type: Option<ResourceType>,
}

/// Resource types for filtering
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    /// Models only
    Model,
    /// Sources only
    Source,
    /// Seeds only
    Seed,
    /// Tests only
    Test,
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
    /// File paths only (one per line)
    Path,
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

    /// Store failing rows to target/test_failures/
    #[arg(long)]
    pub store_failures: bool,

    /// Treat test failures as warnings (exit 0)
    #[arg(long)]
    pub warn_only: bool,

    /// Number of threads for parallel test execution (default: 1)
    #[arg(long, default_value = "1")]
    pub threads: usize,
}

/// Arguments for the seed command
#[derive(Args, Debug)]
pub struct SeedArgs {
    /// Seed names to load (comma-separated, default: all)
    #[arg(short, long)]
    pub seeds: Option<String>,

    /// Drop and recreate all seed tables
    #[arg(long)]
    pub full_refresh: bool,

    /// Display inferred schema without loading
    #[arg(long)]
    pub show_columns: bool,
}

/// Arguments for the validate command
#[derive(Args, Debug)]
pub struct ValidateArgs {
    /// Model names to validate (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Enable strict mode (warnings become errors)
    #[arg(long)]
    pub strict: bool,
}

/// Arguments for the docs command
#[derive(Args, Debug)]
pub struct DocsArgs {
    /// Model names to generate docs for (comma-separated, default: all with schemas)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Output directory for documentation (default: target/docs)
    #[arg(short, long)]
    pub output: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "markdown")]
    pub format: DocsFormat,
}

/// Documentation output formats
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocsFormat {
    /// Markdown format
    Markdown,
    /// JSON format
    Json,
    /// HTML format
    Html,
}

/// Arguments for the clean command
#[derive(Args, Debug)]
pub struct CleanArgs {
    /// Show what would be deleted without actually deleting
    #[arg(long)]
    pub dry_run: bool,
}

/// Arguments for the source command
#[derive(Args, Debug)]
pub struct SourceArgs {
    /// Source subcommand
    #[command(subcommand)]
    pub command: SourceCommands,
}

/// Source subcommands
#[derive(Subcommand, Debug)]
pub enum SourceCommands {
    /// Check freshness of source data
    Freshness(FreshnessArgs),
}

/// Arguments for the freshness subcommand
#[derive(Args, Debug)]
pub struct FreshnessArgs {
    /// Source names to check (comma-separated, default: all with freshness config)
    #[arg(short, long)]
    pub sources: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: FreshnessOutput,
}

/// Freshness output formats
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessOutput {
    /// Table format
    Table,
    /// JSON output
    Json,
}

/// Arguments for the snapshot command
#[derive(Args, Debug)]
pub struct SnapshotArgs {
    /// Snapshot names to run (comma-separated, default: all)
    #[arg(long)]
    pub snapshots: Option<String>,

    /// dbt-style selector (+snapshot, snapshot+)
    #[arg(short, long)]
    pub select: Option<String>,
}

/// Arguments for the run-operation command
#[derive(Args, Debug)]
pub struct RunOperationArgs {
    /// Name of the macro to execute
    pub macro_name: String,

    /// Arguments to pass to the macro as JSON
    #[arg(long)]
    pub args: Option<String>,
}
