use super::*;
use clap::CommandFactory;

#[test]
fn verify_cli_args() {
    // Validates the entire command tree: short flag conflicts,
    // duplicate args, and other clap definition errors.
    Cli::command().debug_assert();
}
