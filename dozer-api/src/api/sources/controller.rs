use actix_web::{post, web, Error, HttpResponse};
use serde_json::{Value};
use crate::{services::validation, models::SourceSetting};

#[post("/sources")]
async fn create_source(input: web::Json<Value>) -> Result<HttpResponse, Error> {
    let result = validation::validation::<SourceSetting>(serde_json::to_value(input).unwrap());
    Ok(HttpResponse::Ok().json(result))
}
