//! Deploy command implementation — seeds and functions in one place

use anyhow::Result;

use crate::cli::{DeployArgs, DeployCommands, GlobalArgs};
use crate::commands::{function, seed};

/// Execute the deploy command
pub(crate) async fn execute(args: &DeployArgs, global: &GlobalArgs) -> Result<()> {
    match &args.command {
        DeployCommands::Seeds(sub) => seed::execute(sub, global).await,
        DeployCommands::Functions(sub) => function::execute(sub, global).await,
    }
}
