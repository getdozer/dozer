use clap::{Args, Parser, Subcommand};

use super::helper::{DESCRIPTION, LOGO};

#[cfg(feature = "cloud")]
use crate::cli::cloud::Cloud;
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
    #[arg(global = true, long, hide = true)]
    pub config_token: Option<String>,

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
    Build(Build),
    #[command(about = "Run App Server", hide = true)]
    App(App),
    #[command(about = "Run Api Server", hide = true)]
    Api(Api),
    #[command(about = "Run App or Api Server")]
    Run(Run),
    #[command(about = "Show Sources")]
    Connectors(ConnectorCommand),
    #[command(about = "Change security settings")]
    Security(Security),
    #[cfg(feature = "cloud")]
    #[command(about = "Deploy cloud applications")]
    Cloud(Cloud),
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Api {
    #[command(subcommand)]
    pub command: ApiCommands,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Build {
    #[arg(short = 'f')]
    pub force: Option<Option<String>>,
}

#[derive(Debug, Args)]
pub struct Run {
    #[command(subcommand)]
    pub command: RunCommands,
}

#[derive(Debug, Subcommand)]
pub enum RunCommands {
    Api,
    App,
}

#[derive(Debug, Args)]
pub struct Security {
    #[command(subcommand)]
    pub command: SecurityCommands,
}

#[derive(Debug, Subcommand)]
pub enum SecurityCommands {
    #[command(
        author,
        version,
        about = "Generate master token",
        long_about = "Master Token can be used to create other run time tokens \
        that encapsulate different permissions.",
        alias = "nx"
    )]
    GenerateToken,
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

#[derive(Debug, Args)]
pub struct ConnectorCommand {
    #[arg(short = 'f')]
    pub filter: Option<String>,
}
