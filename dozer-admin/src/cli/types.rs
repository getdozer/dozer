use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, name = "dozer-admin")]
#[command(
    about = "CLI to interact with dozer-admin components",
    long_about = "Dozer admin provide UI to config"
)]
pub struct Cli {
    #[arg(short = 'c', long, default_value = "./dozer-admin-config.yaml")]
    pub config_path: String,

    #[arg(short = 'u', long, default_value = "./ui")]
    pub ui_path: String,

    #[clap(subcommand)]
    pub cmd: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Start,
}
