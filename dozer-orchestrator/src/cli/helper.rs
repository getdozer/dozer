use crate::errors::OrchestrationError;
use crate::simple::SimpleOrchestrator as Dozer;
use crate::{errors::CliError, Orchestrator};

use dozer_types::prettytable::{row, Table};
use dozer_types::{models::app_config::Config, serde_yaml};
use handlebars::Handlebars;
use std::{collections::BTreeMap, fs};

pub fn init_dozer(config_path: String) -> Result<Dozer, CliError> {
    let config = load_config(config_path)?;
    Ok(Dozer::new(config))
}

pub fn list_sources(config_path: &str) -> Result<(), OrchestrationError> {
    let dozer = init_dozer(config_path.to_string())?;
    let connection_map = dozer.list_connectors()?;
    let mut table_parent = Table::new();
    for (connection_name, (tables, schemas)) in connection_map {
        table_parent.add_row(row!["Connection", "Table", "Columns"]);

        for (table, schema) in tables.into_iter().zip(schemas) {
            let schema_table = schema.schema.print();

            let name = table.schema.map_or(table.name.clone(), |schema_name| {
                format!("{schema_name}.{}", table.name)
            });

            table_parent.add_row(row![connection_name, name, schema_table]);
        }
        table_parent.add_empty_row();
    }
    table_parent.printstd();
    Ok(())
}

pub fn load_config(config_path: String) -> Result<Config, CliError> {
    let contents = fs::read_to_string(config_path.clone())
        .map_err(|_| CliError::FailedToLoadFile(config_path))?;

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("config", contents)
        .map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    let mut data = BTreeMap::new();

    for (key, value) in std::env::vars() {
        data.insert(key, value);
    }

    let config_str = handlebars
        .render("config", &data)
        .map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    let config: Config =
        serde_yaml::from_str(&config_str).map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    // Create home_dir if not exists.
    let _res = fs::create_dir_all(&config.home_dir);

    Ok(config)
}

pub const LOGO: &str = "
____   ___ __________ ____
|  _ \\ / _ \\__  / ____|  _ \\
| | | | | | |/ /|  _| | |_) |
| |_| | |_| / /_| |___|  _ <
|____/ \\___/____|_____|_| \\_\\
";

pub const DESCRIPTION: &str = r#"Open-source platform to build, publish and manage blazing-fast real-time data APIs in minutes. 

If no sub commands are passed, dozer will bring up both app and api services.
"#;
