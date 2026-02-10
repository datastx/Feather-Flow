//! CLI argument definitions using clap derive API

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

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
    pub project_dir: PathBuf,

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
    /// Initialize a new Featherflow project
    Init(InitArgs),

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

    /// Check model freshness (SLA monitoring)
    Freshness(FreshnessArgs),

    /// Work with semantic layer metrics
    Metric(MetricArgs),

    /// Compare model output between databases
    Diff(DiffArgs),

    /// Show column-level lineage across models
    Lineage(LineageArgs),

    /// Analyze SQL models for potential issues
    Analyze(AnalyzeArgs),

    /// Manage user-defined functions (DuckDB macros)
    Function(FunctionArgs),
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

/// Output formats for run/test/compile commands (for CI integration)
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Human-readable text output (default)
    #[default]
    Text,
    /// Machine-readable JSON output
    Json,
}

/// Arguments for the compile command
#[derive(Args, Debug)]
pub struct CompileArgs {
    /// Model names to compile (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Override output directory
    #[arg(short = 'd', long)]
    pub output_dir: Option<String>,

    /// Override/add variables as JSON
    #[arg(long)]
    pub vars: Option<String>,

    /// Parse and validate only, don't write output files
    #[arg(long)]
    pub parse_only: bool,

    /// Skip DataFusion static analysis
    #[arg(long)]
    pub skip_static_analysis: bool,

    /// Print the DataFusion LogicalPlan for a model
    #[arg(long)]
    pub explain: Option<String>,

    /// Output format (text or json for CI integration)
    #[arg(short, long, value_enum, default_value = "text")]
    pub output: OutputFormat,
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

    /// Resume from a previous failed run
    #[arg(long)]
    pub resume: bool,

    /// Only retry failed models when resuming (skip pending)
    #[arg(long)]
    pub retry_failed: bool,

    /// Path to run state file for resume (default: target/run_state.json)
    #[arg(long)]
    pub state_file: Option<String>,

    /// Output format (text or json for CI integration)
    #[arg(short, long, value_enum, default_value = "text")]
    pub output: OutputFormat,

    /// Suppress progress indicators (useful for CI)
    #[arg(short, long)]
    pub quiet: bool,

    /// Smart build: skip models whose SQL, schema, and inputs haven't changed
    #[arg(long)]
    pub smart: bool,

    /// Skip DataFusion static analysis
    #[arg(long)]
    pub skip_static_analysis: bool,
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

    /// Filter models by owner (matches owner field or meta.owner)
    #[arg(long)]
    pub owner: Option<String>,

    /// Show downstream exposures that depend on the listed models
    #[arg(long)]
    pub downstream_exposures: bool,
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
    /// Functions only
    Function,
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

    /// Output format (text or json for CI integration)
    #[arg(short, long, value_enum, default_value = "text")]
    pub output: OutputFormat,

    /// Suppress progress indicators (useful for CI)
    #[arg(long)]
    pub quiet: bool,
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

    /// Validate schema contracts against a reference manifest
    #[arg(long)]
    pub contracts: bool,

    /// Path to reference manifest for contract validation (used with --contracts)
    #[arg(long, value_name = "FILE")]
    pub state: Option<String>,

    /// Enable governance checks (data classification completeness)
    #[arg(long)]
    pub governance: bool,
}

/// Arguments for the docs command
#[derive(Args, Debug)]
pub struct DocsArgs {
    /// Docs subcommand (serve, etc.)
    #[command(subcommand)]
    pub command: Option<DocsCommands>,

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

/// Docs subcommands
#[derive(Subcommand, Debug)]
pub enum DocsCommands {
    /// Launch interactive documentation server
    Serve(DocsServeArgs),
}

/// Arguments for the docs serve subcommand
#[derive(Args, Debug)]
pub struct DocsServeArgs {
    /// Port to serve on
    #[arg(long, default_value = "4040")]
    pub port: u16,

    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Filter models by selector
    #[arg(long)]
    pub select: Option<String>,

    /// Don't open browser automatically
    #[arg(long)]
    pub no_browser: bool,

    /// Export static site to directory instead of serving
    #[arg(long)]
    pub static_export: Option<String>,
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
    Freshness(SourceFreshnessArgs),
}

/// Arguments for the source freshness subcommand
#[derive(Args, Debug)]
pub struct SourceFreshnessArgs {
    /// Source names to check (comma-separated, default: all with freshness config)
    #[arg(short, long)]
    pub sources: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: FreshnessOutput,
}

/// Arguments for the model freshness command
#[derive(Args, Debug)]
pub struct FreshnessArgs {
    /// Model names to check (comma-separated, default: all with freshness config)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: FreshnessOutput,

    /// Write results to a JSON file
    #[arg(long)]
    pub write_json: bool,
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

/// Arguments for the metric command
#[derive(Args, Debug)]
pub struct MetricArgs {
    /// Metric name to show/execute
    pub name: Option<String>,

    /// List all metrics
    #[arg(short, long)]
    pub list: bool,

    /// Execute the metric query against the database
    #[arg(short, long)]
    pub execute: bool,

    /// Output format (text or json)
    #[arg(short, long, value_enum, default_value = "text")]
    pub output: OutputFormat,
}

/// Arguments for the init command
#[derive(Args, Debug)]
pub struct InitArgs {
    /// Project name (also used as directory name)
    #[arg(long)]
    pub name: String,

    /// Database file path (default: dev.duckdb)
    #[arg(long, default_value = "dev.duckdb")]
    pub database_path: String,
}

/// Arguments for the diff command
#[derive(Args, Debug)]
pub struct DiffArgs {
    /// Model name to compare
    pub model: String,

    /// Path to the comparison database
    #[arg(long, required = true)]
    pub compare_to: String,

    /// Specific columns to compare (comma-separated, default: all)
    #[arg(long)]
    pub columns: Option<String>,

    /// Primary key column(s) for matching rows (comma-separated)
    #[arg(long)]
    pub key: Option<String>,

    /// Maximum number of sample differences to show
    #[arg(long, default_value = "10")]
    pub sample_size: usize,

    /// Output format (text or json)
    #[arg(short, long, value_enum, default_value = "text")]
    pub output: OutputFormat,
}

/// Arguments for the lineage command
#[derive(Args, Debug)]
pub struct LineageArgs {
    /// Filter to a specific model
    #[arg(short, long)]
    pub model: Option<String>,

    /// Filter to a specific column (requires --model)
    #[arg(long, requires = "model")]
    pub column: Option<String>,

    /// Direction to trace lineage
    #[arg(short, long, value_enum, default_value = "both")]
    pub direction: LineageDirection,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: LineageOutput,

    /// Filter edges by data classification (e.g., pii, sensitive)
    #[arg(long)]
    pub classification: Option<String>,
}

/// Lineage trace direction
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageDirection {
    /// Trace upstream (sources)
    Upstream,
    /// Trace downstream (consumers)
    Downstream,
    /// Show both directions
    Both,
}

/// Lineage output formats
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageOutput {
    /// Human-readable table
    Table,
    /// JSON output
    Json,
    /// DOT graph for Graphviz
    Dot,
}

/// Arguments for the analyze command
#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    /// Model names to analyze (comma-separated, default: all)
    #[arg(short, long)]
    pub models: Option<String>,

    /// Run only specific passes (comma-separated, e.g., type_inference,nullability)
    #[arg(long)]
    pub pass: Option<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: AnalyzeOutput,

    /// Minimum severity to display
    #[arg(short, long, value_enum, default_value = "info")]
    pub severity: AnalyzeSeverity,
}

/// Analyze output formats
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeOutput {
    /// Human-readable table
    Table,
    /// JSON output
    Json,
}

/// Arguments for the function command
#[derive(Args, Debug)]
pub struct FunctionArgs {
    /// Function subcommand
    #[command(subcommand)]
    pub command: FunctionCommands,
}

/// Function subcommands
#[derive(Subcommand, Debug)]
pub enum FunctionCommands {
    /// List all user-defined functions in the project
    List(FunctionListArgs),

    /// Deploy functions to the database
    Deploy(FunctionDeployArgs),

    /// Show details about a function
    Show(FunctionShowArgs),

    /// Validate function definitions
    Validate(FunctionValidateArgs),

    /// Drop deployed functions from the database
    Drop(FunctionDropArgs),
}

/// Arguments for the function list subcommand
#[derive(Args, Debug)]
pub struct FunctionListArgs {
    /// Output format (text or json)
    #[arg(short, long, value_enum, default_value = "text")]
    pub output: OutputFormat,
}

/// Arguments for the function deploy subcommand
#[derive(Args, Debug)]
pub struct FunctionDeployArgs {
    /// Function names to deploy (comma-separated, default: all)
    #[arg(short, long)]
    pub functions: Option<String>,
}

/// Arguments for the function show subcommand
#[derive(Args, Debug)]
pub struct FunctionShowArgs {
    /// Name of the function to show
    pub name: String,

    /// Show generated CREATE MACRO SQL instead of YAML definition
    #[arg(long)]
    pub sql: bool,
}

/// Arguments for the function validate subcommand
#[derive(Args, Debug)]
pub struct FunctionValidateArgs {}

/// Arguments for the function drop subcommand
#[derive(Args, Debug)]
pub struct FunctionDropArgs {
    /// Function names to drop (comma-separated, default: all)
    #[arg(short, long)]
    pub functions: Option<String>,
}

/// Minimum severity filter for analyze output
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AnalyzeSeverity {
    /// Show all diagnostics
    Info,
    /// Show warnings and errors only
    Warning,
    /// Show errors only
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn verify_cli_args() {
        // Validates the entire command tree: short flag conflicts,
        // duplicate args, and other clap definition errors.
        Cli::command().debug_assert();
    }
}
