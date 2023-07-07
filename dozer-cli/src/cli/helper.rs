use crate::errors::OrchestrationError;
use crate::simple::SimpleOrchestrator as Dozer;
use crate::{errors::CliError, Orchestrator};

use crate::config_helper::combine_config;
use dozer_types::models::app_config::default_cache_max_map_size;
use dozer_types::prettytable::{row, Table};
use dozer_types::{models::app_config::Config, serde_yaml};
use handlebars::Handlebars;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn init_dozer(config_path: String, config_token: Option<String>) -> Result<Dozer, CliError> {
    let runtime = Runtime::new().map_err(CliError::FailedToCreateTokioRuntime)?;
    let mut config = runtime.block_on(load_config(&config_path, config_token))?;

    let cache_max_map_size = config
        .cache_max_map_size
        .unwrap_or_else(default_cache_max_map_size);
    let page_size = page_size::get() as u64;
    config.cache_max_map_size = Some(cache_max_map_size / page_size * page_size);

    Ok(Dozer::new(config, Arc::new(runtime)))
}

pub fn list_sources(
    config_path: &str,
    config_token: Option<String>,
    filter: Option<String>,
) -> Result<(), OrchestrationError> {
    let dozer = init_dozer(config_path.to_string(), config_token)?;
    let connection_map = dozer.list_connectors()?;
    let mut table_parent = Table::new();
    for (connection_name, (tables, schemas)) in connection_map {
        let mut first_table_found = false;

        for (table, schema) in tables.into_iter().zip(schemas) {
            let name = table.schema.map_or(table.name.clone(), |schema_name| {
                format!("{schema_name}.{}", table.name)
            });

            if filter
                .as_ref()
                .map_or(true, |name_part| name.contains(name_part))
            {
                if !first_table_found {
                    table_parent.add_row(row!["Connection", "Table", "Columns"]);
                    first_table_found = true;
                }
                let schema_table = schema.schema.print();

                table_parent.add_row(row![connection_name, name, schema_table]);
            }
        }

        if first_table_found {
            table_parent.add_empty_row();
        }
    }
    table_parent.printstd();
    Ok(())
}

async fn load_config(
    config_url_or_path: &str,
    config_token: Option<String>,
) -> Result<Config, CliError> {
    if config_url_or_path.starts_with("https://") || config_url_or_path.starts_with("http://") {
        load_config_from_http_url(config_url_or_path, config_token).await
    } else {
        load_config_from_file(config_url_or_path)
    }
}

async fn load_config_from_http_url(
    config_url: &str,
    config_token: Option<String>,
) -> Result<Config, CliError> {
    let client = reqwest::Client::new();
    let mut get_request = client.get(config_url);
    if let Some(token) = config_token {
        get_request = get_request.bearer_auth(token);
    }
    let response: reqwest::Response = get_request.send().await?.error_for_status()?;
    let contents = response.text().await?;
    parse_config(&contents)
}

pub fn load_config_from_file(config_path: &str) -> Result<Config, CliError> {
    let config_test = combine_config(config_path)?;
    parse_config(&config_test)
}

fn parse_config(config_template: &str) -> Result<Config, CliError> {
    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("config", config_template)
        .map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    let mut data = BTreeMap::new();

    for (key, value) in std::env::vars() {
        data.insert(key, value);
    }

    let config_str = handlebars
        .render("config", &data)
        .map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    let config: Config = serde_yaml::from_str(&config_str)
        .map_err(|e: serde_yaml::Error| CliError::FailedToParseYaml(Box::new(e)))?;

    Ok(config)
}

pub const LOGO: &str = r#"
.____   ___ __________ ____
|  _ \ / _ \__  / ____|  _ \
| | | | | | |/ /|  _| | |_) |
| |_| | |_| / /_| |___|  _ <
|____/ \___/____|_____|_| \_\
"#;

pub const DESCRIPTION: &str = r#"Open-source platform to build, publish and manage blazing-fast real-time data APIs in minutes. 

 If no sub commands are passed, dozer will bring up both app and api services.
"#;
