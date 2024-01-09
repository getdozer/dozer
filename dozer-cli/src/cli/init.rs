use crate::errors::{CliError, OrchestrationError};
use dozer_types::constants::{DEFAULT_LAMBDAS_DIRECTORY, DEFAULT_QUERIES_DIRECTORY};
use dozer_types::log::warn;
use dozer_types::models::config::{default_cache_dir, default_home_dir, get_cache_dir};
use dozer_types::{
    constants::DEFAULT_CONFIG_PATH,
    log::info,
    models::ingestion_types::{
        EthConfig, EthFilter, EthLogConfig, EthProviderConfig, MongodbConfig, MySQLConfig,
        S3Details, S3Storage, SnowflakeConfig,
    },
    models::{
        config::Config,
        connection::{Connection, ConnectionConfig, PostgresConfig},
    },
    serde_yaml,
};
use rustyline::history::DefaultHistory;
use rustyline::{
    completion::{Completer, Pair},
    Context,
};
use rustyline::{error::ReadlineError, Editor};
use rustyline_derive::{Helper, Highlighter, Hinter, Validator};
use std::path::{Path, PathBuf};

#[derive(Helper, Highlighter, Hinter, Validator)]
pub struct InitHelper {}

impl Completer for InitHelper {
    type Candidate = Pair;
    fn complete(
        &self,
        line: &str,
        _pos: usize,
        _ctx: &Context,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let line = format!("{line}_");
        let mut tokens = line.split_whitespace();
        let mut last_token = String::from(tokens.next_back().unwrap());
        last_token.pop();
        let candidates: Vec<String> = vec![
            "Postgres".to_owned(),
            "Ethereum".to_owned(),
            "Snowflake".to_owned(),
            "MySQL".to_owned(),
            "S3".to_owned(),
            "MongoDB".to_owned(),
        ];
        let mut match_pair: Vec<Pair> = candidates
            .iter()
            .filter_map(|f| {
                if f.to_lowercase().starts_with(&last_token.to_lowercase()) {
                    Some(Pair {
                        display: f.to_owned(),
                        replacement: f.to_owned(),
                    })
                } else {
                    None
                }
            })
            .collect();
        if match_pair.is_empty() {
            match_pair = vec![Pair {
                display: "Postgres".to_owned(),
                replacement: "Postgres".to_owned(),
            }]
        }
        Ok((line.len() - last_token.len() - 1, match_pair))
    }
}

pub fn generate_connection(connection_name: &str) -> Connection {
    match connection_name {
        "Snowflake" | "snowflake" | "S" | "s" => {
            let snowflake_config = SnowflakeConfig {
                server: "<account_name>.<region_id>.snowflakecomputing.com".to_owned(),
                port: "443".to_owned(),
                user: "bob".to_owned(),
                password: "password".to_owned(),
                database: "database".to_owned(),
                schema: "schema".to_owned(),
                warehouse: "warehouse".to_owned(),
                driver: Some("SnowflakeDSIIDriver".to_owned()),
                role: "role".to_owned(),
                poll_interval_seconds: None,
            };
            let connection: Connection = Connection {
                name: "snowflake".to_owned(),
                config: ConnectionConfig::Snowflake(snowflake_config),
            };
            connection
        }
        "Ethereum" | "ethereum" | "E" | "e" => {
            let eth_filter = EthFilter {
                from_block: Some(0),
                to_block: None,
                addresses: vec![],
                topics: vec![],
            };
            let ethereum_config = EthConfig {
                provider: EthProviderConfig::Log(EthLogConfig {
                    wss_url: "wss://link".to_owned(),
                    filter: Some(eth_filter),
                    contracts: vec![],
                }),
            };
            let connection: Connection = Connection {
                name: "ethereum".to_owned(),
                config: ConnectionConfig::Ethereum(ethereum_config),
            };
            connection
        }
        "MySQL" | "MYSQL" | "mysql" | "Mysql" | "My" => {
            let mysql_config = MySQLConfig {
                url: "mysql://<user>:<password>@localhost:3306/<database>".to_owned(),
                server_id: Some((1).to_owned()),
            };
            let connection: Connection = Connection {
                name: "mysql".to_owned(),
                config: ConnectionConfig::MySQL(mysql_config),
            };
            connection
        }
        "S3" | "s3" => {
            let s3_details = S3Details {
                access_key_id: "<your_access_key_id>".to_owned(),
                secret_access_key: "<your_secret_access_key>".to_owned(),
                region: "<your_region>".to_owned(),
                bucket_name: "<your_bucket_name>".to_owned(),
            };
            let s3_config = S3Storage {
                details: s3_details,
                tables: vec![],
            };
            let connection: Connection = Connection {
                name: "s3".to_owned(),
                config: ConnectionConfig::S3Storage(s3_config),
            };
            connection
        }
        "MongoDB" | "mongodb" | "MONGODB" | "Mongodb" | "Mo" | "MO" => {
            let mongo_config = MongodbConfig {
                connection_string:
                    "mongodb://<username>:<password>@localhost:27017/<database_name>".to_owned(),
            };
            let connection: Connection = Connection {
                name: "mongodb".to_owned(),
                config: ConnectionConfig::MongoDB(mongo_config),
            };
            connection
        }
        _ => {
            let postgres_config = PostgresConfig {
                user: Some("postgres".to_owned()),
                password: Some("postgres".to_owned()),
                host: Some("localhost".to_owned()),
                port: Some(5432),
                database: Some("users".to_owned()),
                sslmode: None,
                connection_url: None,
                schema: None,
                batch_size: None,
            };
            let connection: Connection = Connection {
                name: "postgres".to_owned(),
                config: ConnectionConfig::Postgres(postgres_config),
            };
            connection
        }
    }
}
type Question = (
    String,
    Box<dyn Fn((String, &mut Config)) -> Result<(), OrchestrationError>>,
);
pub fn generate_config_repl() -> Result<(), OrchestrationError> {
    let mut rl = Editor::<InitHelper, DefaultHistory>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    rl.set_helper(Some(InitHelper {}));
    let mut default_config = Config {
        version: 1,
        ..Default::default()
    };
    let default_app_name = "quick-start-app";
    let questions: Vec<Question> = vec![
        (
            format!("question: App name ({:}): ", default_app_name),
            Box::new(move |(app_name, config)| {
                let app_name = app_name.trim();
                if app_name.is_empty() {
                    config.app_name = default_app_name.to_string();
                } else {
                    config.app_name = app_name.to_string();
                }
                Ok(())
            }),
        ),
        (
            format!("question: Data directory ({:}): ", default_home_dir()),
            Box::new(move |(home_dir, config)| {
                if home_dir.is_empty() {
                    config.home_dir = Some(default_home_dir());
                    config.cache_dir = Some(default_cache_dir());
                } else {
                    config.cache_dir = Some(get_cache_dir(&home_dir));
                    config.home_dir = Some(home_dir);
                }
                Ok(())
            }),
        ),
        (
            "question: Connection Type - one of: [P]ostgres, [E]thereum, [S]nowflake, [My]SQL, [S3]Storage, [Mo]ngoDB: "
                .to_string(),
            Box::new(move |(connection, config)| {
                let sample_connection = generate_connection(&connection);
                config.connections.push(sample_connection);

                Ok(())
            }),
        ),
        (
            format!("question: Config path ({:}): ", DEFAULT_CONFIG_PATH),
            Box::new(move |(yaml_path, config)| {
                let mut yaml_path = yaml_path.trim();
                if yaml_path.is_empty() {
                    yaml_path = DEFAULT_CONFIG_PATH;
                }
                let f = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(yaml_path)
                    .map_err(|e| {
                        OrchestrationError::CliError(CliError::FileSystem(
                            yaml_path.to_string().into(),
                            e,
                        ))
                    })?;
                serde_yaml::to_writer(f, &config)
                    .map_err(OrchestrationError::FailedToWriteConfigYaml)?;

                info!("Generating workspace: \n\
                \n- {} (main configuration)\n- ./queries (folder for sql queries)\n- ./lambdas (folder for lambda functions)
                \n• More details about our config: https://getdozer.io/docs/reference/configuration/introduction\
                \n• Connector & Sources: https://getdozer.io/docs/reference/configuration/connectors\
                \n• Endpoints: https://getdozer.io/docs/reference/configuration/endpoints/",
                   yaml_path.to_owned());

                let path = PathBuf::from(yaml_path);
                if let Some(dir) = path.parent() {
                    let queries_path = Path::new(dir).join(DEFAULT_QUERIES_DIRECTORY);
                    if let Err(_e) = std::fs::create_dir(queries_path) {
                        warn!("Cannot create queries directory");
                    }

                    let lambdas_path = Path::new(dir).join(DEFAULT_LAMBDAS_DIRECTORY);
                    if let Err(_e) = std::fs::create_dir(lambdas_path) {
                        warn!("Cannot create lambdas directory");
                    }
                }

                Ok(())
            }),
        ),
    ];
    let result = questions.iter().try_for_each(|(question, func)| {
        let readline = rl.readline(question);
        match readline {
            Ok(input) => func((input, &mut default_config)),
            Err(err) => Err(OrchestrationError::CliError(CliError::ReadlineError(err))),
        }
    });

    match result {
        Ok(_) => Ok(()),
        Err(e) => match e {
            OrchestrationError::CliError(CliError::ReadlineError(ReadlineError::Interrupted)) => {
                info!("Exiting..");
                Ok(())
            }
            OrchestrationError::CliError(CliError::ReadlineError(ReadlineError::Eof)) => {
                info!("CTRL-D - exiting...");
                Ok(())
            }
            _ => Err(e),
        },
    }
}
