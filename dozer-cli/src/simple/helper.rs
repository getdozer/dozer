use crate::console_helper::PURPLE;
use crate::errors::OrchestrationError;
use crate::painted;
use dozer_types::log::info;
use dozer_types::models::api_config::ApiConfig;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::app_config::Config;
use dozer_types::prettytable::{row, Table};

pub fn validate_config(config: &Config) -> Result<(), OrchestrationError> {
    info!("Home dir: {}", painted!(&config.home_dir, PURPLE));
    if let Some(api_config) = &config.api {
        print_api_config(api_config)
    }

    validate_endpoints(&config.endpoints)?;

    print_api_endpoints(&config.endpoints);
    Ok(())
}

pub fn validate_endpoints(endpoints: &[ApiEndpoint]) -> Result<(), OrchestrationError> {
    if endpoints.is_empty() {
        return Err(OrchestrationError::EmptyEndpoints);
    }

    Ok(())
}

fn print_api_config(api_config: &ApiConfig) {
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Type", "IP", "Port"]);
    if let Some(rest_config) = &api_config.rest {
        table_parent.add_row(row!["REST", rest_config.host, rest_config.port]);
    }

    if let Some(grpc_config) = &api_config.grpc {
        table_parent.add_row(row!["GRPC", grpc_config.host, grpc_config.port]);
    }
    info!(
        "[API] {}\n{}",
        painted!("Configuration", PURPLE),
        table_parent
    );
}

pub fn print_api_endpoints(endpoints: &Vec<ApiEndpoint>) {
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Path", "Name"]);
    for endpoint in endpoints {
        table_parent.add_row(row![endpoint.path, endpoint.name]);
    }
    info!("[API] {}\n{}", painted!("Endpoints", PURPLE), table_parent);
}
