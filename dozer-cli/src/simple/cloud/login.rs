use crate::errors::{CloudCredentialError, CloudLoginError};
use std::collections::HashMap;
use std::{env, fs, io};

use dozer_types::grpc_types::cloud::company_request::Criteria;
use dozer_types::grpc_types::cloud::dozer_public_client::DozerPublicClient;
use dozer_types::grpc_types::cloud::CompanyRequest;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json::{self, Value};
use dozer_types::serde_yaml;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_types::serde")]
pub struct CredentialInfo {
    pub profile_name: String,
    pub client_id: String,
    pub client_secret: String,
    pub target_url: String,
    pub auth_url: String,
}
const DOZER_FOLDER: &str = ".dozer";
const CREDENTIALS_FILE_NAME: &str = "credentials.yaml";

impl CredentialInfo {
    fn get_directory_path() -> String {
        let home_dir = env::var("HOME").unwrap_or_else(|_| ".".to_string());
        format!("{}/{}", home_dir, DOZER_FOLDER)
    }

    fn get_file_path() -> String {
        let file_path = format!(
            "{}/{}",
            CredentialInfo::get_directory_path(),
            CREDENTIALS_FILE_NAME
        );
        file_path
    }
    pub fn save(&self) -> Result<(), CloudCredentialError> {
        let file_path: String = CredentialInfo::get_file_path();
        fs::create_dir_all(CredentialInfo::get_directory_path())
            .map_err(CloudCredentialError::FailedToCreateDirectory)?;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_path)?;
        let mut current_credential_infos: Vec<CredentialInfo> = CredentialInfo::read_profile()?;
        current_credential_infos.append(&mut vec![self.clone()]);
        current_credential_infos.dedup_by_key(|key| key.to_owned().profile_name);
        serde_yaml::to_writer(f, &current_credential_infos)?;
        Ok(())
    }

    fn read_profile() -> Result<Vec<CredentialInfo>, CloudCredentialError> {
        let file_path = CredentialInfo::get_file_path();

        let file = std::fs::File::open(file_path)
            .map_err(|_e| CloudCredentialError::MissingCredentialFile)?;
        serde_yaml::from_reader::<std::fs::File, Vec<CredentialInfo>>(file)
            .map_err(CloudCredentialError::SerializationError)
    }

    pub fn load(name: Option<String>) -> Result<CredentialInfo, CloudCredentialError> {
        let credential_info: Vec<CredentialInfo> = CredentialInfo::read_profile()?;
        match name {
            Some(name) => {
                let credential_info = credential_info
                    .into_iter()
                    .find(|info| info.profile_name == name)
                    .ok_or(CloudCredentialError::MissingProfile)?;
                Ok(credential_info)
            }
            _ => credential_info
                .into_iter()
                .next()
                .ok_or(CloudCredentialError::MissingProfile),
        }
    }

    pub async fn get_access_token(&self) -> Result<TokenResponse, CloudCredentialError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(CloudCredentialError::HttpRequestError)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "Content-Type",
            "application/x-www-form-urlencoded".parse().unwrap(),
        );

        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("grant_type", "client_credentials");
        params.insert("client_id", self.client_id.as_str());
        params.insert("client_secret", self.client_secret.as_str());
        let request = client
            .request(reqwest::Method::POST, self.auth_url.to_owned())
            .headers(headers)
            .form(&params);
        let response = request
            .send()
            .await
            .map_err(CloudCredentialError::HttpRequestError)?;
        let json_response: Value = response
            .json()
            .await
            .map_err(CloudCredentialError::HttpRequestError)?;
        serde_json::from_value::<TokenResponse>(json_response)
            .map_err(CloudCredentialError::JsonSerializationError)
    }
}

pub struct LoginSvc {
    auth_url: String,
    target_url: String,
}
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: i32,
}
impl LoginSvc {
    pub async fn new(company_name: String, target_url: String) -> Result<Self, CloudLoginError> {
        let mut client = DozerPublicClient::connect(target_url.to_owned()).await?;
        let company_info = client
            .company_metadata(CompanyRequest {
                criteria: Some(Criteria::Name(company_name.to_owned())),
            })
            .await?;
        let company_info = company_info.into_inner();
        Ok(Self {
            auth_url: company_info.auth_url,
            target_url,
        })
    }
    pub async fn login(&self) -> Result<(), CloudLoginError> {
        self.login_by_credential().await
    }

    async fn login_by_credential(&self) -> Result<(), CloudLoginError> {
        let mut profile_name = String::new();
        println!("Please enter profile name:");
        io::stdin().read_line(&mut profile_name)?;
        profile_name = profile_name.trim().to_owned();

        let mut client_id = String::new();
        println!("Please enter your client_id:");
        io::stdin().read_line(&mut client_id)?;
        client_id = client_id.trim().to_owned();

        let mut client_secret = String::new();
        println!("Please enter your client_secret:");
        io::stdin().read_line(&mut client_secret)?;
        client_secret = client_secret.trim().to_owned();

        let credential_info = CredentialInfo {
            client_id,
            client_secret,
            profile_name,
            target_url: self.target_url.to_owned(),
            auth_url: self.auth_url.to_owned(),
        };
        credential_info.get_access_token().await?;
        credential_info.save()?;
        println!("Login success !");
        Ok(())
    }
}
