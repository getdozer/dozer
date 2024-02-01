use crate::errors::{CliError, OrchestrationError};
use super::init_downloader::fetch_latest_init_schema;
use dozer_types::constants::{DEFAULT_LAMBDAS_DIRECTORY, DEFAULT_QUERIES_DIRECTORY};
use dozer_types::log::warn;
use dozer_types::models::config::{default_cache_dir, default_home_dir, get_cache_dir};
use serde::Deserialize;
use std::env;

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
    serde_json,
};
use rustyline::history::DefaultHistory;
use rustyline::{
    completion::{Completer, Pair},
    Context,
};
use rustyline::{error::ReadlineError, Editor};
use rustyline_derive::{Helper, Highlighter, Hinter, Validator};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process;

#[derive(Debug, Deserialize)]
struct TemplateConfig {
    version: i32,
    app_name: String,
    home_dir: String,
    cache_dir: String,
    connections: Vec<ConnectionTemplate>,
}

#[derive(Debug, Deserialize)]
struct ConnectionTemplate {
    name: String,
    config: ConnectionConfigTemplate,
    #[serde(rename = "type")]
    connection_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ConnectionConfigTemplate {
    Snowflake(SnowflakeConfig),
    Ethereum(EthConfig),
    MySQL(MySQLConfig),
    // Add other connection types as needed
}

#[derive(Helper, Highlighter, Hinter, Validator)]
pub struct InitHelper {}

pub fn get_directory_path() -> String {
    let home_dir = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/{}", home_dir, ".dozer")
}

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

pub fn generate_connection(connection_template: &ConnectionTemplate) -> Connection {
    match &connection_template.connection_type.to_lowercase()[..] {
        "snowflake" | "s" => {
            let snowflake_config = match &connection_template.config {
                ConnectionConfigTemplate::Snowflake(config) => config.clone(),
                _ => unreachable!(), // Will never be reached if the template is correctly constructed
            };
            let connection: Connection = Connection {
                name: connection_template.name.clone(),
                config: ConnectionConfig::Snowflake(snowflake_config),
            };
            connection
        }
        "ethereum" | "e" => {
            let ethereum_config = match &connection_template.config {
                ConnectionConfigTemplate::Ethereum(config) => config.clone(),
                _ => unreachable!(),
            };
            let connection: Connection = Connection {
                name: connection_template.name.clone(),
                config: ConnectionConfig::Ethereum(ethereum_config),
            };
            connection
        }
        "mysql" | "my" => {
            let mysql_config = match &connection_template.config {
                ConnectionConfigTemplate::MySQL(config) => config.clone(),
                _ => unreachable!(),
            };
            let connection: Connection = Connection {
                name: connection_template.name.clone(),
                config: ConnectionConfig::MySQL(mysql_config),
            };
            connection
        }
        // Add other connection types as needed
        _ => {
            // Default case (e.g., Postgres)
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
                name: connection_template.name.clone(),
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
    if let Ok(downloaded_file_name) = fetch_latest_init_schema() {
        // Handle the success case
        info!("Downloaded file: {}", downloaded_file_name);
    
        // Now you can use `downloaded_file_name` in further processing
    } else {
        // Handle the error case
        info!("Error occurred during file download");
        process::exit(1);
    }

    // Read the template JSON file
    let mut template_json = String::new();
    let template_file_path = "path/to/your/template.json"; // Replace with your actual file path
    let mut template_file = File::open(template_file_path)
        .map_err(|e| OrchestrationError::CliError(CliError::FileSystem(template_file_path.into(), e)))?;
    template_file
        .read_to_string(&mut template_json)
        .map_err(|e| OrchestrationError::CliError(CliError::FileSystem(template_file_path.into(), e)))?;

    // Deserialize the JSON into a TemplateConfig struct
    let template_config: TemplateConfig = serde_json::from_str(&template_json)
        .map_err(|e| OrchestrationError::CliError(CliError::JsonError(e)))?;

    let mut rl = Editor::<InitHelper, DefaultHistory>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    rl.set_helper(Some(InitHelper {}));
    let mut default_config = Config {
        version: template_config.version,
        ..Default::default()
    };
    let questions: Vec<Question> = vec![
        (
            format!("question: App name ({:}): ", template_config.app_name),
            Box::new(move |(app_name, config)| {
                let app_name = app_name.trim();
                if app_name.is_empty() {
                    config.app_name = template_config.app_name.clone();
                } else {
                    config.app_name = app_name.to_string();
                }
                Ok(())
            }),
        ),
        (
            format!("question: Data directory ({:}): ", template_config.home_dir),
            Box::new(move |(home_dir, config)| {
                if home_dir.is_empty() {
                    config.home_dir = Some(template_config.home_dir.clone());
                    config.cache_dir = Some(template_config.cache_dir.clone());
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
                if let Some(connection_template) = template_config
                    .connections
                    .iter()
                    .find(|&tmpl| tmpl.name.eq_ignore_ascii_case(&connection))
                {
                    let sample_connection = generate_connection(connection_template);
                    config.connections.push(sample_connection);
                } else {
                    warn!("Connection type not found in the template.");
                }
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
