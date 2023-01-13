use dozer_types::{
    ingestion_types::{EthConfig, EthFilter, SnowflakeConfig},
    log::info,
    models::{
        app_config::Config,
        connection::{Authentication, Connection, DBType, PostgresAuthentication},
    },
    serde_yaml,
};
use rustyline::{error::ReadlineError, Editor};

use crate::errors::{CliError, OrchestrationError};

fn sample_connection(connection_name: &str) -> Connection {
    match connection_name {
        "Snowflake" | "snowflake" => {
            let snowflake_config = SnowflakeConfig {
                server: "server".to_owned(),
                port: "443".to_owned(),
                user: "bob".to_owned(),
                password: "password".to_owned(),
                database: "database".to_owned(),
                schema: "schema".to_owned(),
                warehouse: "warehouse".to_owned(),
                driver: Some("SnowflakeDSIIDriver".to_owned()),
            };
            let connection: Connection = Connection {
                name: "snowflake".to_owned(),
                authentication: Some(Authentication::Snowflake(snowflake_config)),
                db_type: DBType::Ethereum as i32,
                ..Connection::default()
            };
            connection
        }
        "Ethereum" => {
            let eth_filter = EthFilter {
                from_block: Some(0),
                to_block: None,
                addresses: vec![],
                topics: vec![],
            };
            let ethereum_auth = EthConfig {
                filter: Some(eth_filter),
                wss_url: "wss://link".to_owned(),
                contracts: vec![],
            };
            let connection: Connection = Connection {
                name: "ethereum".to_owned(),
                authentication: Some(Authentication::Ethereum(ethereum_auth)),
                db_type: DBType::Ethereum as i32,
                ..Connection::default()
            };
            connection
        }
        _ => {
            let postgres_auth = PostgresAuthentication {
                user: "postgres".to_owned(),
                password: "postgres".to_owned(),
                host: "localhost".to_owned(),
                port: 5432,
                database: "users".to_owned(),
            };
            let connection: Connection = Connection {
                name: "postgres".to_owned(),
                authentication: Some(Authentication::Postgres(postgres_auth)),
                db_type: DBType::Postgres as i32,
                ..Connection::default()
            };
            connection
        }
    }
}
type Question = (
    &'static str,
    Box<dyn Fn((String, &mut Config)) -> Result<(), OrchestrationError>>,
);
pub fn init_simple_config_file_with_question() -> Result<(), OrchestrationError> {
    let mut rl = Editor::<()>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    let mut default_config = Config::default();
    let questions: Vec<Question> = vec![
        (
            "question: App name (quick-start-app): ",
            Box::new(move |(app_name, config)| {
                let app_name = app_name.trim();
                if app_name.is_empty() {
                    config.app_name = "quick-start-app".to_string();
                } else {
                    config.app_name = app_name.to_string();
                }
                Ok(())
            }),
        ),
        (
            "question: Home directior (./dozer): ",
            Box::new(move |(home_dir, config)| {
                if home_dir.is_empty() {
                    config.home_dir = "./dozer".to_string();
                } else {
                    config.home_dir = home_dir;
                }
                Ok(())
            }),
        ),
        (
            "question: Connection Type - one of: Postgres, Ethereum, Snowflake (Postgres): ",
            Box::new(move |(connection, config)| {
                let connections_available = vec!["Postgres", "Ethereum", "Snowflake"];
                if connections_available.contains(&connection.as_str()) {
                    let sample_connection = sample_connection(&connection);
                    config.connections.push(sample_connection);
                } else {
                    let sample_connection = sample_connection("Postgres");
                    config.connections.push(sample_connection);
                }
                Ok(())
            }),
        ),
        (
            "question: Config path (dozer-config.yaml): ",
            Box::new(move |(yaml_path, config)| {
                let mut yaml_path = yaml_path.trim();
                if yaml_path.is_empty() {
                    yaml_path = "dozer-config.yaml";
                }
                let f = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(yaml_path)
                    .map_err(|e| {
                        OrchestrationError::CliError(CliError::InternalError(Box::new(e)))
                    })?;
                serde_yaml::to_writer(f, &config)
                    .map_err(OrchestrationError::FailedToWriteConfigYaml)?;
                info!("Generating configuration at: {}", yaml_path.to_owned());
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
    if let Err(e) = result {
        return match e {
            OrchestrationError::CliError(CliError::ReadlineError(ReadlineError::Interrupted)) => {
                info!("Exiting..");
                Ok(())
            }
            OrchestrationError::CliError(CliError::ReadlineError(ReadlineError::Eof)) => {
                info!("CTRL-D - exiting...");
                Ok(())
            }
            _ => Err(e),
        };
    };
    result
}
