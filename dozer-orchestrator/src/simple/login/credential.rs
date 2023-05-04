use crate::errors::CloudCredentialError;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json::{self};
use std::env;
use std::{fs, path::Path};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_types::serde")]

pub struct CredentialInfo {
    pub profile_name: String,
    pub email: Option<String>,
    pub client_id: String,
    pub client_secret: String,
    pub user_pool_name: String,
    pub region: String,
}

impl CredentialInfo {
    pub fn get_auth_endpoint(&self) -> String {
        format!(
            "https://{}.auth.{}.amazoncognito.com/oauth2/token",
            self.user_pool_name, self.region
        )
    }
    fn get_directory_path() -> String {
        let home_dir = match env::var("HOME") {
            Ok(val) => val,
            Err(e) => panic!("Could not get home directory: {}", e),
        };

        format!("{}/.dozer", home_dir)
    }

    fn get_file_name() -> &'static str {
        "credential.json"
    }

    fn get_file_path() -> String {
        let file_path = format!(
            "{}/{}",
            CredentialInfo::get_directory_path(),
            CredentialInfo::get_file_name()
        );
        file_path
    }

    pub fn save(&self) -> Result<(), CloudCredentialError> {
        let file_path: String = CredentialInfo::get_file_path();
        fs::create_dir_all(CredentialInfo::get_directory_path())
            .map_err(CloudCredentialError::FailedToCreateCredentialDirectory)?;
        let credential_info =
            serde_json::to_string(self).map_err(CloudCredentialError::SerializationError)?;
        let credential_path: &Path = Path::new(&file_path);
        fs::write(credential_path, credential_info)
            .map_err(CloudCredentialError::FailedToWriteCredentialFile)?;
        Ok(())
    }

    pub fn load() -> Result<CredentialInfo, CloudCredentialError> {
        let file_path = CredentialInfo::get_file_path();
        let credential_path = Path::new(&file_path);
        let credential_info_str = std::fs::read_to_string(credential_path).map_err(|e| {
            CloudCredentialError::FailedToReadCredentialFile(credential_path.to_path_buf(), e)
        })?;
        let credential_info: CredentialInfo = serde_json::from_str(&credential_info_str)
            .map_err(CloudCredentialError::SerializationError)?;
        Ok(credential_info)
    }
}
