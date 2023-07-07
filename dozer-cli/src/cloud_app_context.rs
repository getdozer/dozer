use crate::errors::CloudContextError;
use crate::errors::CloudContextError::{
    AppIdNotFound, ContextFileNotFound, FailedToGetDirectoryPath, FailedToReadAppId,
};
use dozer_types::models::app_config::Config;
use dozer_types::models::cloud::Cloud;
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
            "dozer-cloud.yaml"
        ))
    }

    pub fn get_app_id() -> Result<String, CloudContextError> {
        let file_path = Self::get_file_path()?;
        match fs::metadata(&file_path) {
            Ok(_) => {
                let content = fs::read(file_path)?;
                match String::from_utf8(content) {
                    Ok(app_id) => {
                        let c: Config = serde_yaml::from_str(&app_id).map_err(|_| AppIdNotFound)?;
                        c.cloud.ok_or(AppIdNotFound)?.app_id.ok_or(AppIdNotFound)
                    }
                    Err(e) => Err(FailedToReadAppId(e)),
                }
            }
            Err(_) => Err(ContextFileNotFound),
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
