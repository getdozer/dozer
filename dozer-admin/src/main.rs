use clap::Parser;
use cli::{cli_process::CliProcess, load_config, types::Cli};
use errors::AdminError;
extern crate diesel;
pub mod cli;
pub mod db;
pub mod errors;
pub mod server;
pub mod services;
pub mod tests;
#[macro_use]
extern crate diesel_migrations;

#[tokio::main]
async fn main() -> Result<(), AdminError> {
    let cli = Cli::parse();
    let configuration = load_config(cli.config_path)?;
    let cli_process = CliProcess {
        config: configuration,
    };
    if let Some(cmd) = cli.cmd {
        match cmd {
            cli::types::Commands::Start => {
                cli_process
                    .start()
                    .await
                    .map_err(AdminError::FailedToRunCli)?;
            }
        }
    } else {
        cli_process
            .start()
            .await
            .map_err(AdminError::FailedToRunCli)?;
    }
    Ok(())
}
