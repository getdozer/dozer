use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, name = "dozer")]
#[command(
    about = "CLI to interact with dozer-admin components",
    long_about = "Dozer admin provide UI to config"
)]
pub struct Cli {
    #[arg(short = 'c', long, default_value = "./dozer-admin-config.yaml")]
    pub config_path: String,

    #[clap(subcommand)]
    pub cmd: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(arg_required_else_help = true)]
    Start,
}
