use clap::{Parser, Subcommand};
use dozer_ingestion::connectors::connector::TableInfo;
use dozer_types::models::connection::Connection;
use log::debug;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Dozer ingestion tool",
    long_about = "Connect to database and run cdc handler"
)]
pub struct Args {
    #[clap(subcommand)]
    pub cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
pub enum SubCommand {
    // Todo: implement tables support
    #[command(
        author,
        version,
        about = "Connect to database",
        long_about = "Connect to database and run cdc handler"
    )]
    Connect {
        #[arg(short = 'c', long, default_value = "default_ingestion")]
        config_name: String,
    },
    #[command(
        author,
        version,
        about = "Dump connection info",
        long_about = "Dump connection info"
    )]
    DumpConnectionInfo {
        #[arg(short = 'c', long, default_value = "default_ingestion")]
        config_name: String,
    },
    // Todo: Implement tables support
    #[command(
        author,
        version,
        about = "Create connection configuration file",
        long_about = "Create connection configuration file"
    )]
    CreateConnectionConfiguration {
        #[arg(short = 'c', long, default_value = "default_ingestion")]
        config_name: String,

        #[arg(short = 's', long, required = false)]
        storage_path: Option<String>,

        #[arg(short = 'n', long)]
        connection_name: String,

        #[arg(short = 'i', long, required = false)]
        connection_id: Option<String>,

        #[arg(short = 'u', long)]
        database_user: String,

        #[arg(short = 'p', long)]
        database_password: String,

        #[arg(short = 'o', long)]
        database_host: String,

        #[arg(short = 'r', long)]
        database_port: u32,

        #[arg(short = 'd', long)]
        database: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub storage_path: Option<String>,
    pub connection: Connection,
    pub tables: Option<Vec<TableInfo>>,
}

fn get_config_path(config_name: &String) -> String {
    "run_configs/".to_string() + config_name + ".yaml"
}

pub fn load_config(config_name: String) -> Config {
    let contents = fs::read_to_string(get_config_path(&config_name))
        .expect("Should have been able to read the file");

    serde_yaml::from_str(&contents).unwrap()
}

pub fn save_config(config_name: String, config: Config) {
    let content = serde_yaml::to_string(&config).expect("Should convert config to yaml");

    fs::write(get_config_path(&config_name), content)
        .expect("Should have been able to write the config file");

    debug!("Config file {}.yaml successfully created", config_name);
}
