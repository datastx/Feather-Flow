//! Docs command implementation - Generate and serve documentation

pub(super) mod data;
mod generate;
#[cfg(feature = "docs-serve")]
mod serve;

use anyhow::Result;

use crate::cli::{DocsArgs, GlobalArgs};

/// Execute the docs command, dispatching to subcommands
pub async fn execute(args: &DocsArgs, global: &GlobalArgs) -> Result<()> {
    match &args.command {
        Some(crate::cli::DocsCommands::Serve(serve_args)) => {
            #[cfg(feature = "docs-serve")]
            {
                serve::execute(serve_args, global).await
            }
            #[cfg(not(feature = "docs-serve"))]
            {
                let _ = serve_args;
                anyhow::bail!(
                    "The `docs serve` command requires the `docs-serve` feature.\n\
                     Rebuild with: cargo build -p ff-cli --features docs-serve"
                );
            }
        }
        None => generate::execute(args, global).await,
    }
}
