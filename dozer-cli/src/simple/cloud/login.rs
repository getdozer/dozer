use crate::errors::{CloudLoginError, OrchestrationError};
use std::collections::HashMap;
use std::{io, println as info};

use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json::{self, Value};

pub struct MyCompanyInfo {
    auth_endpoint: String,
}
pub struct LoginSvc {
    company_name: String,
}
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: i32,
}
impl LoginSvc {
    pub fn new(company_name: String) -> Self {
        Self { company_name }
    }
    pub fn login(&self) -> Result<(), OrchestrationError> {
        info!("Login");
        // fetch company info
        Ok(())
    }

    fn login_by_credential(&self) -> Result<(), CloudLoginError> {
        let mut profile_name = String::new();
        info!("Please enter login name:");
        io::stdin().read_line(&mut profile_name)?;
        profile_name = profile_name.trim().to_owned();

        let mut client_id = String::new();
        info!("Please enter your client_id:");
        io::stdin().read_line(&mut client_id)?;
        client_id = client_id.trim().to_owned();

        let mut client_secret = String::new();
        info!("Please enter your client_secret:");
        io::stdin().read_line(&mut client_secret)?;
        client_secret = client_secret.trim().to_owned();
        Ok(())
    }

    async fn get_access_token_request(
        client_id: String,
        client_secret: String,
        auth_endpoint: String,
    ) -> Result<TokenResponse, CloudLoginError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(CloudLoginError::HttpRequestError)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/x-www-form-urlencoded".parse()?);

        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("grant_type", "client_credentials");
        params.insert("client_id", client_id.as_str());
        params.insert("client_secret", client_secret.as_str());
        let request = client
            .request(reqwest::Method::POST, auth_endpoint)
            .headers(headers)
            .form(&params);
        let response = request
            .send()
            .await
            .map_err(CloudLoginError::HttpRequestError)?;
        let json_response: Value = response
            .json()
            .await
            .map_err(CloudLoginError::HttpRequestError)?;
        let result: TokenResponse =
            serde_json::from_value(json_response).map_err(CloudLoginError::SerializationError)?;
        Ok(result)
    }
}
