use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::seq_no_resolver::SeqNoResolver;
use dozer_ingestion::connectors::storage::{RocksConfig, RocksStorage, Storage};

use crate::cli::{load_config, save_config, Args, Config, SubCommand};
use clap::Parser;
use dozer_types::models::connection::Authentication::PostgresAuthentication;
use dozer_types::models::connection::{Connection, DBType};
use dozer_types::log::debug;
use std::sync::{Arc, Mutex};
use std::time::Instant;

mod cli;

fn get_storage(configuration: &Config) -> RocksStorage {
    let storage_config = if let Some(path) = configuration.storage_path.clone() {
        RocksConfig { path }
    } else {
        RocksConfig::default()
    };

    Storage::new(storage_config)
}

fn main() {
    log4rs::init_file("log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    debug!(
        "
      ____   ___ __________ ____
     |  _ \\ / _ \\__  / ____|  _ \\
     | | | | | | |/ /|  _| | |_) |
     | |_| | |_| / /_| |___|  _ <
     |____/ \\___/____|_____|_| \\_\\n"
    );
    let args = Args::parse();

    match args.cmd {
        SubCommand::Connect { config_name } => {
            let configuration = load_config(config_name);
            let storage_client = Arc::new(get_storage(&configuration));

            let mut connector: Box<dyn Connector> = match configuration.connection.authentication {
                PostgresAuthentication {
                    user,
                    password: _,
                    host,
                    port,
                    database,
                } => {
                    let postgres_config = PostgresConfig {
                        name: configuration.connection.name,
                        tables: configuration.tables,
                        conn_str: format!(
                            "host={} port={} user={} dbname={}",
                            host, port, user, database
                        ),
                    };
                    debug!("Connecting to postgres database - {}", database);
                    Box::new(PostgresConnector::new(
                        configuration.connection.id.unwrap().parse().unwrap(),
                        postgres_config,
                    ))
                }
            };

            let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
            seq_resolver.init();
            let seq_no_resolver = Arc::new(Mutex::new(seq_resolver));

            connector.initialize(storage_client, None).unwrap();

            let before = Instant::now();
            const BACKSPACE: char = 8u8 as char;
            let mut iterator = connector.iterator(seq_no_resolver);
            let mut i = 0;
            loop {
                let _msg = iterator.next().unwrap();
                if i % 1000 == 0 {
                    debug!(
                        "{}\rCount: {}, Elapsed time: {:.2?}",
                        BACKSPACE,
                        i,
                        before.elapsed(),
                    );
                }
                i += 1;
            }
        }

        SubCommand::DumpConnectionInfo { config_name } => {
            let configuration = load_config(config_name);

            match configuration.connection.authentication {
                PostgresAuthentication {
                    user,
                    password: _,
                    host,
                    port,
                    database,
                } => {
                    debug!(
                        "\nhost={}\nport={}\nuser={}\ndbname={}",
                        host, port, user, database
                    );
                }
            }
        }

        SubCommand::CreateConnectionConfiguration {
            config_name,
            storage_path,
            connection_name,
            database,
            database_host,
            database_password,
            database_port,
            database_user,
            connection_id,
        } => {
            let config = Config {
                storage_path,
                connection: Connection {
                    db_type: DBType::Postgres,
                    authentication: PostgresAuthentication {
                        user: database_user,
                        password: database_password,
                        host: database_host,
                        port: database_port,
                        database,
                    },
                    name: connection_name,
                    id: connection_id,
                },
                tables: None,
            };

            save_config(config_name, config);
        }
    }
}
