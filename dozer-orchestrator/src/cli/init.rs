use crate::errors::{CliError, OrchestrationError};
use dozer_types::{
    ingestion_types::{EthConfig, EthFilter, SnowflakeConfig},
    log::info,
    models::{
        app_config::Config,
        connection::{Authentication, Connection, DBType, PostgresAuthentication},
    },
    serde_yaml,
};
use rustyline::{
    completion::{Completer, Pair},
    Context,
};
use rustyline::{error::ReadlineError, Editor};
use rustyline_derive::{Helper, Highlighter, Hinter, Validator};

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
        "Ethereum" | "ethereum" | "E" | "e" => {
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
pub fn generate_config_repl() -> Result<(), OrchestrationError> {
    let mut rl = Editor::<InitHelper>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    rl.set_helper(Some(InitHelper {}));
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
            "question: Connection Type - one of: [P]ostgres, [E]thereum, [S]nowflake (Postgres): ",
            Box::new(move |(connection, config)| {
                let connections_available = vec!["Postgres", "Ethereum", "Snowflake"];
                if connections_available.contains(&connection.as_str()) {
                    let sample_connection = generate_connection(&connection);
                    config.connections.push(sample_connection);
                } else {
                    let sample_connection = generate_connection("Postgres");
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
                info!("\nGenerating configuration at: {}\n• More details about our config: https://getdozer.io/docs/reference/configuration/introduction\n• Connector & Sources: https://getdozer.io/docs/reference/configuration/connectors\n• Endpoints: https://getdozer.io/docs/reference/configuration/endpoints/",
                   yaml_path.to_owned());
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
