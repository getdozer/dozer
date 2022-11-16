use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version)]
#[command(name = "dozer")]
#[command(
    about = "CLI to interact with dozer components",
    long_about = " Dozer lets you publish blazing fast data apis directly from your data sources. \
    Dozer on the fly moves, transforms, caches data and expose them as APIs in the form of gRPC and REST. \
    "
)]
pub struct Cli {
    #[arg(short = 'c', long, default_value = "./dozer-config.yaml")]
    pub config_path: String,

    #[clap(subcommand)]
    pub cmd: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Api(Api),
    App(App),
    Ps,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
struct Api {
    #[command(subcommand)]
    command: ApiCommands,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
struct App {
    #[command(subcommand)]
    command: AppCommands,
}

#[derive(Debug, Subcommand)]
enum ApiCommands {
    Start,
    Stop,
    Stats,
    Restart,
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
enum AppCommands {
    Start,
    Stop,
    Stats,
    Restart,
}
