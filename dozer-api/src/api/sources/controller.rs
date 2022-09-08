use actix_web::{error, post, web, Error, HttpResponse};
use futures::StreamExt;
use valico::json_schema;
extern crate oas3;
extern crate serde_json;
extern crate valico;
use serde_json::{Value};
use crate::{services::validation, models::SourceSetting};

#[post("/sources")]
async fn create_source(input: web::Json<Value>) -> Result<HttpResponse, Error> {
    let result = validation::validation::<SourceSetting>(serde_json::to_value(input).unwrap());
    Ok(HttpResponse::Ok().json(result))
}
