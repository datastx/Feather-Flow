//! `ff dt` command — developer tooling dispatcher

use anyhow::Result;

use crate::cli::{DtArgs, DtCommands, GlobalArgs};
use crate::commands::{analyze, clean, compile, deploy, docs, fmt, init, lineage, ls};

/// Execute the dt (developer tooling) command.
pub(crate) async fn execute(args: &DtArgs, global: &GlobalArgs) -> Result<()> {
    match &args.command {
        DtCommands::Init(sub) => init::execute(sub).await,
        DtCommands::Compile(sub) => compile::execute(sub, global).await,
        DtCommands::Ls(sub) => ls::execute(sub, global).await,
        DtCommands::Clean(sub) => clean::execute(sub, global).await,
        DtCommands::Fmt(sub) => fmt::execute(sub, global).await,
        DtCommands::Lineage(sub) => lineage::execute(sub, global).await,
        DtCommands::Docs(sub) => docs::execute(sub, global).await,
        DtCommands::Deploy(sub) => deploy::execute(sub, global).await,
        DtCommands::Analyze(sub) => analyze::execute(sub, global).await,
    }
}
