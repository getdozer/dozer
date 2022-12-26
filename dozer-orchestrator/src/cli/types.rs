use clap::{Args, Parser, Subcommand};

use super::{DESCRIPTION, LOGO};

#[derive(Parser, Debug)]
#[command(author, version, name = "dozer")]
#[command(
    about = format!("{} \n {}", LOGO, DESCRIPTION),
    long_about = format!("{} \n {}", LOGO, DESCRIPTION),
)]
pub struct Cli {
    #[arg(short = 'c', long, default_value = "./dozer-config.yaml")]
    pub config_path: String,

    #[clap(subcommand)]
    pub cmd: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(arg_required_else_help = true)]
    Api(Api),
    App(App),
    Connector(Connector),
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Api {
    #[command(subcommand)]
    pub command: ApiCommands,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct App {
    #[command(subcommand)]
    pub command: AppCommands,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Connector {
    #[command(subcommand)]
    pub command: ConnectorCommands,
}

#[derive(Debug, Subcommand)]
pub enum ApiCommands {
    Run,
    #[command(
        author,
        version,
        about = "Generate master token",
        long_about = "Master Token can be used to create other run time tokens \
        that encapsulate different permissions."
    )]
    GenerateToken,
}

#[derive(Debug, Subcommand)]
pub enum AppCommands {
    Run,
}

#[derive(Debug, Subcommand)]
pub enum ConnectorCommands {
    Ls,
}
