use crate::console_helper::get_colored_text;
use crate::console_helper::PURPLE;
use crate::errors::OrchestrationError;
use dozer_types::log::info;
use dozer_types::models::api_config::ApiConfig;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::config::default_home_dir;
use dozer_types::models::config::Config;
use dozer_types::prettytable::{row, Table};

pub fn validate_config(config: &Config) -> Result<(), OrchestrationError> {
    info!(
        "Home dir: {}",
        get_colored_text(
            &config.home_dir.clone().unwrap_or_else(default_home_dir),
            PURPLE
        )
    );
    print_api_config(&config.api);

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
    table_parent.add_row(row![
        "REST",
        api_config.rest.host.as_deref().unwrap_or("-"),
        api_config
            .rest
            .port
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_string())
    ]);

    table_parent.add_row(row![
        "GRPC",
        api_config.grpc.host.as_deref().unwrap_or("-"),
        api_config
            .grpc
            .port
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_string())
    ]);
    info!(
        "[API] {}\n{}",
        get_colored_text("Configuration", PURPLE),
        table_parent
    );
}

pub fn print_api_endpoints(endpoints: &Vec<ApiEndpoint>) {
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Path", "Name"]);
    for endpoint in endpoints {
        table_parent.add_row(row![endpoint.path, endpoint.name]);
    }
    info!(
        "[API] {}\n{}",
        get_colored_text("Endpoints", PURPLE),
        table_parent
    );
}
