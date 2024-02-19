use crate::console_helper::get_colored_text;
use crate::console_helper::PURPLE;
use crate::errors::OrchestrationError;
use dozer_types::log::info;
use dozer_types::models::config::default_home_dir;
use dozer_types::models::config::Config;
use dozer_types::models::sink::Sink;

pub fn validate_config(config: &Config) -> Result<(), OrchestrationError> {
    info!(
        "Data directory: {}",
        get_colored_text(
            &config.home_dir.clone().unwrap_or_else(default_home_dir),
            PURPLE
        )
    );
    validate_sinks(&config.sinks)?;

    Ok(())
}

pub fn validate_sinks(sinks: &[Sink]) -> Result<(), OrchestrationError> {
    if sinks.is_empty() {
        return Err(OrchestrationError::EmptySinks);
    }

    Ok(())
}
