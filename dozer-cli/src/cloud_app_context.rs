use crate::errors::CloudContextError;
use crate::errors::CloudContextError::{AppIdNotFound, FailedToGetDirectoryPath};
use dozer_types::models::cloud::Cloud;
use dozer_types::models::config::Config;
use dozer_types::serde_yaml;
use std::io::Write;
use std::{env, fs};

pub struct CloudAppContext {}

impl CloudAppContext {
    fn get_file_path() -> Result<String, CloudContextError> {
        Ok(format!(
            "{}/{}",
            env::current_dir()?
                .into_os_string()
                .into_string()
                .map_err(|_| FailedToGetDirectoryPath)?,
            "dozer-config.cloud.yaml"
        ))
    }

    pub fn get_app_id(config: &Option<Cloud>) -> Result<String, CloudContextError> {
        match &config {
            None => Err(AppIdNotFound),
            Some(cloud_config) => cloud_config.app_id.clone().ok_or(AppIdNotFound),
        }
    }

    pub fn save_app_id(app_id: String) -> Result<(), CloudContextError> {
        let file_path = Self::get_file_path()?;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)?;

        let config = Config {
            cloud: Some(Cloud {
                app_id: Some(app_id),
                ..Default::default()
            }),
            ..Default::default()
        };

        let config_string = serde_yaml::to_string(&config).unwrap();
        f.write_all(config_string.as_bytes())?;

        Ok(())
    }
}
