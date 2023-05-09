use clap::{Args, Subcommand};

use dozer_types::constants::DEFAULT_CLOUD_TARGET_URL;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Cloud {
    #[arg(
    global = true,
    short = 't',
    long,
    default_value = DEFAULT_CLOUD_TARGET_URL
    )]
    pub target_url: String,
    #[command(subcommand)]
    pub command: CloudCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CloudCommands {
    Deploy,
    Update(AppCommand),
    Delete(AppCommand),
    List,
    Status(AppCommand),
    Monitor(AppCommand),
}

#[derive(Debug, Args, Clone)]
pub struct AppCommand {
    #[arg(short = 'a', long)]
    pub app_id: String,
}
