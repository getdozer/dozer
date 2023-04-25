use clap::{Args, Parser, Subcommand};

use super::helper::{DESCRIPTION, LOGO};

use dozer_types::constants::DEFAULT_CONFIG_PATH;

#[derive(Parser, Debug)]
#[command(author, version, name = "dozer")]
#[command(
    about = format!("{} \n {}", LOGO, DESCRIPTION),
    long_about = None,
)]
pub struct Cli {
    #[arg(
        global = true,
        short = 'c',
        long,
        default_value = DEFAULT_CONFIG_PATH
    )]
    pub config_path: String,

    #[clap(subcommand)]
    pub cmd: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(about = "Initialize an app using a template")]
    Init,
    #[command(about = "Clean home directory")]
    Clean,
    #[command(
        about = "Initialize and lock schema definitions. Once initialized, schemas cannot be changed"
    )]
    Migrate(Migrate),
    #[command(about = "Deploy Dozer application")]
    Deploy(Deploy),

    #[command(about = "Run Api Server")]
    Api(Api),
    #[command(about = "Run App Server")]
    App(App),
    #[command(about = "Show Sources")]
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
pub struct Migrate {
    #[arg(short = 'f')]
    pub force: Option<Option<String>>,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Deploy {
    pub target_url: String,
    #[arg(short = 'u')]
    pub username: Option<String>,
    #[arg(short = 'p')]
    pub password: Option<String>,
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
