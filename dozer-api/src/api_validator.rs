use actix_web::{dev::ServiceRequest, Error};
use actix_web_httpauth::extractors::{
    bearer::{self, BearerAuth},
    AuthenticationError,
};
use jsonwebtoken::errors::ErrorKind;

use crate::api_auth::ApiAuth;

pub async fn validate(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    // eprintln!("{:?}", credentials);
    // credentials.token();
    // Ok(req)
    Ok(req)
    // if  == "mF_9.B5f-4.1JqM" {

    // } else {
    //     let api_auth = req.app_data::<ApiAuth>().unwrap();
    //     api_auth.validate_token(credentials.token());
    //     Ok(req)
    // }
}
