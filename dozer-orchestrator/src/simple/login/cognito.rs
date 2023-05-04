use super::credential::CredentialInfo;
use crate::errors::CloudCredentialError;
use crate::simple::helper::{cli_select_option, listen_with_timeout};
use dozer_types::grpc_types::cloud::CompanyResponse;
use dozer_types::indicatif::{ProgressBar, ProgressStyle};
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json::{self, Value};
use reqwest::header::HeaderValue;
use reqwest::{Client, Response, Url};
use std::collections::HashMap;
use std::io::{self};
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::println as info;
#[derive(Serialize)]
#[serde(crate = "dozer_types::serde")]

struct AuthRequest<'a> {
    #[serde(rename = "ClientId")]
    client_id: &'a str,
    #[serde(rename = "AuthFlow")]
    auth_flow: &'a str,
    #[serde(rename = "AuthParameters")]
    auth_parameters: AuthParams<'a>,
}

#[derive(Serialize)]
#[serde(crate = "dozer_types::serde")]

struct AuthParams<'a> {
    #[serde(rename = "USERNAME")]
    username: &'a str,
    #[serde(rename = "PASSWORD")]
    password: &'a str,
}
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: i32,
}

pub struct CognitoSvc {
    pub user: Option<CredentialInfo>,
    cli_client_id: String,
    region: String,
    user_pool_name: String,
    callback_url: String,
    redirect_url: String,
}

impl CognitoSvc {
    fn get_progress(&self) -> Result<ProgressBar, CloudCredentialError> {
        let pb: ProgressBar = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("Logging in... {spinner:.blue}")
                .map_err(CloudCredentialError::FailedToSetProgressStyle)?
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        Ok(pb)
    }

    fn get_auth_endpoint(&self) -> String {
        format!(
            "https://{}.auth.{}.amazoncognito.com/oauth2/token",
            self.user_pool_name, self.region
        )
    }

    async fn fetch_user_info_with_access_token(
        region: String,
        access_token: String,
        user_pool_name: &str,
    ) -> Result<CredentialInfo, CloudCredentialError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-Amz-Target",
            "AWSCognitoIdentityProviderService.GetUser".parse()?,
        );
        headers.insert("Content-Type", "application/x-amz-json-1.1".parse()?);

        let data = format!("{{\"AccessToken\": \"{}\"}}", access_token);
        let request = client
            .request(
                reqwest::Method::POST,
                format!("https://cognito-idp.{}.amazonaws.com", region),
            )
            .headers(headers)
            .body(data);
        let response = request
            .send()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let json_response: Value = response
            .json()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let profile_name = json_response["Username"].as_str().unwrap().to_owned();
        let attributes = json_response["UserAttributes"]
            .as_array()
            .unwrap()
            .to_owned();
        let client_id = attributes
            .iter()
            .cloned()
            .find(|x| x["Name"] == "custom:client_id")
            .unwrap()["Value"]
            .as_str()
            .unwrap()
            .to_owned();
        let client_secret = attributes
            .iter()
            .cloned()
            .find(|x| x["Name"] == "custom:client_secret")
            .unwrap()["Value"]
            .as_str()
            .unwrap()
            .to_owned();
        let email = attributes
            .into_iter()
            .find(|x| x["Name"] == "email")
            .unwrap()["Value"]
            .as_str()
            .unwrap()
            .to_owned();

        Ok(CredentialInfo {
            profile_name,
            email: Some(email),
            client_id,
            client_secret,
            user_pool_name: user_pool_name.to_owned(),
            region,
        })
    }

    async fn fetch_user_info(
        user_pool_name: &str,
        region: &str,
        token: TokenResponse,
    ) -> Result<CredentialInfo, CloudCredentialError> {
        let user_info_url = format!(
            "https://{}.auth.{}.amazoncognito.com/oauth2/userInfo",
            user_pool_name, region
        );
        let user_info: Value = reqwest::Client::new()
            .get(user_info_url)
            .header(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {}", token.access_token))
                    .map_err(CloudCredentialError::FailedToAddAuthorizationHeader)?,
            )
            .send()
            .await
            .map_err(CloudCredentialError::FailedToGetUserInfo)?
            .json::<Value>()
            .await
            .map_err(CloudCredentialError::FailedToGetUserInfo)?;
        let user = CredentialInfo {
            profile_name: user_info["username"].as_str().unwrap().to_owned(),
            email: user_info["email"].as_str().map(|s| s.to_owned()),
            client_id: user_info["custom:client_id"].as_str().unwrap().to_owned(),
            client_secret: user_info["custom:client_secret"]
                .as_str()
                .unwrap()
                .to_owned(),
            user_pool_name: user_pool_name.to_owned(),
            region: region.to_string(),
        };
        Ok(user)
    }

    fn extract_authorization_code(
        input_reader: BufReader<TcpStream>,
    ) -> Result<String, CloudCredentialError> {
        for line in input_reader.lines().flatten() {
            let code_pos: Option<_> = line.find("code=");
            if let Some(pos) = code_pos {
                let authorization_code = line[pos + 5..pos + 5 + 36].to_string();
                return Ok(authorization_code);
            }
        }
        Err(CloudCredentialError::FailedToExtractAuthorizationCode)
    }

    async fn exchange_authorization_code(
        auth_endpoint: &str,
        client_id: &str,
        code: &str,
        redirect_uri: &str,
    ) -> Result<TokenResponse, CloudCredentialError> {
        let client = Client::new();
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("grant_type", "authorization_code");
        params.insert("client_id", client_id);
        params.insert("code", code);
        params.insert("redirect_uri", redirect_uri);
        let response: Response = client
            .post(auth_endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&params)
            .send()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;

        let json_response: Value = response
            .json()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let result: TokenResponse = serde_json::from_value(json_response)
            .map_err(CloudCredentialError::SerializationError)?;
        Ok(result)
    }

    async fn get_access_token_request(
        client_id: String,
        client_secret: String,
        auth_endpoint: String,
    ) -> Result<TokenResponse, CloudCredentialError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
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
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let json_response: Value = response
            .json()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let result: TokenResponse = serde_json::from_value(json_response)
            .map_err(CloudCredentialError::SerializationError)?;
        Ok(result)
    }

    async fn login_by_usr_pwd_request(
        client_id: String,
        user_name: String,
        password: String,
        region: String,
    ) -> Result<TokenResponse, CloudCredentialError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-Amz-Target",
            "AWSCognitoIdentityProviderService.InitiateAuth".parse()?,
        );
        headers.insert("Content-Type", "application/x-amz-json-1.1".parse()?);
        let data = format!( "{{\"AuthFlow\": \"USER_PASSWORD_AUTH\",\"AuthParameters\": {{\"PASSWORD\": \"{}\",\"USERNAME\": \"{}\"}},\"ClientId\": \"{}\"}}", password, user_name,client_id);
        let request = client
            .request(
                reqwest::Method::POST,
                format!("https://cognito-idp.{}.amazonaws.com", region),
            )
            .headers(headers)
            .body(data);
        let response = request
            .send()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let response = response
            .error_for_status()
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let json_response: Value = response
            .json()
            .await
            .map_err(CloudCredentialError::FailedToGetAccessToken)?;
        let access_token = json_response["AuthenticationResult"]["AccessToken"]
            .as_str()
            .unwrap()
            .to_owned();
        let token_type = json_response["AuthenticationResult"]["TokenType"]
            .as_str()
            .unwrap()
            .to_owned();
        let expires_in = json_response["AuthenticationResult"]["ExpiresIn"]
            .as_i64()
            .unwrap() as i32;
        Ok(TokenResponse {
            access_token,
            token_type,
            expires_in,
        })
    }
}

impl CognitoSvc {
    pub fn new(company_info: CompanyResponse) -> Self {
        Self {
            user: None,
            cli_client_id: company_info.cli_client_id.to_owned(),
            region: company_info.region.to_owned(),
            user_pool_name: company_info.user_pool_name.to_owned(),
            callback_url: company_info.callback_url.to_owned(),
            redirect_url: company_info.redirect_url,
        }
    }

    pub async fn get_access_token() -> Result<TokenResponse, CloudCredentialError> {
        let user = CredentialInfo::load()?;
        let access_token_response: TokenResponse = CognitoSvc::get_access_token_request(
            user.client_id.to_owned(),
            user.client_secret.to_owned(),
            user.to_owned().get_auth_endpoint(),
        )
        .await?;
        Ok(access_token_response)
    }

    pub async fn login(&mut self) -> Result<(), CloudCredentialError> {
        let options = vec!["browser", "input credential", "username and password"];
        let selected_option = cli_select_option(options);
        info!("\n");
        match selected_option {
            "browser" => self.login_browser().await,
            "input credential" => self.login_by_credential().await,
            _ => self.login_by_usr_pwd().await,
        }
    }

    async fn login_by_credential(&mut self) -> Result<(), CloudCredentialError> {
        let mut profile_name = String::new();
        info!("Please enter your profile name:");
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

        let _: TokenResponse = CognitoSvc::get_access_token_request(
            client_id.to_owned(),
            client_secret.to_owned(),
            self.get_auth_endpoint(),
        )
        .await?;
        let new_credential = CredentialInfo {
            profile_name,
            email: None,
            client_id,
            client_secret,
            user_pool_name: self.user_pool_name.to_owned(),
            region: self.region.to_owned(),
        };
        new_credential.save()?;
        info!("Saved profile {}", new_credential.profile_name);
        Ok(())
    }

    async fn login_by_usr_pwd(&mut self) -> Result<(), CloudCredentialError> {
        let mut user_name = String::new();
        info!("Please enter username:");
        io::stdin()
            .read_line(&mut user_name)
            .expect("Failed to read line");
        user_name = user_name.trim().to_owned();

        let mut password = String::new();
        info!("Please enter your password:");
        io::stdin()
            .read_line(&mut password)
            .expect("Failed to read line");
        password = password.trim().to_owned();
        let pb: ProgressBar = self.get_progress()?;
        pb.inc(1);
        let token = CognitoSvc::login_by_usr_pwd_request(
            self.cli_client_id.to_owned(),
            user_name.to_owned(),
            password.to_owned(),
            self.region.to_owned(),
        )
        .await?;
        let user_info = CognitoSvc::fetch_user_info_with_access_token(
            self.region.to_owned(),
            token.access_token.to_owned(),
            &self.user_pool_name,
        )
        .await?;

        user_info.save()?;
        pb.finish_and_clear();
        info!("Logged in as {}", user_info.profile_name);
        Ok(())
    }

    async fn login_browser(&mut self) -> Result<(), CloudCredentialError> {
        info!("open browser to login");
        let pb: ProgressBar = self.get_progress()?;
        pb.inc(1);
        // Construct the Cognito Identity Provider authentication URL and open the browser
        let auth_url: Url = Url::parse(&self.redirect_url)
            .map_err(|e| CloudCredentialError::FailedToParseUrl(Box::new(e)))?;
        // Open browser to start authentication flow
        webbrowser::open(auth_url.as_str()).map_err(CloudCredentialError::FailedToOpenBrowser)?;
        // Listen for the callback on http://localhost:3000/auth/cli/callback
        let stream = listen_with_timeout(&self.callback_url, 60)?;
        let reader = BufReader::new(stream);
        let authorization_code = CognitoSvc::extract_authorization_code(reader)?;

        let token = CognitoSvc::exchange_authorization_code(
            format!(
                "https://{}.auth.{}.amazoncognito.com/oauth2/token",
                self.user_pool_name, self.region
            )
            .as_str(),
            self.cli_client_id.as_str(),
            &authorization_code,
            &self.callback_url,
        )
        .await?;
        let user_info =
            CognitoSvc::fetch_user_info(&self.user_pool_name, &self.region, token.to_owned())
                .await?;

        user_info.save()?;
        self.user = Some(user_info.to_owned());
        pb.finish_and_clear();
        info!("Logged in as {}", user_info.profile_name);
        Ok(())
    }
}
