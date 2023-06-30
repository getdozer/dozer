use crate::errors::CloudContextError;
use crate::errors::CloudContextError::{
    ContextFileNotFound, FailedToGetDirectoryPath, FailedToReadAppId,
};
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
            ".dozer-cloud"
        ))
    }

    pub fn get_app_id() -> Result<String, CloudContextError> {
        let file_path = Self::get_file_path()?;
        match fs::metadata(&file_path) {
            Ok(_) => {
                let content = fs::read(file_path)?;
                match String::from_utf8(content) {
                    Ok(app_id) => Ok(app_id),
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

        f.write_all(app_id.as_bytes())?;

        Ok(())
    }
}
