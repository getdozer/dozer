use actix_web::{error, post, web, Error, HttpResponse};
use futures::StreamExt;
use valico::json_schema;
extern crate oas3;
extern crate serde_json;
extern crate valico;
use serde_json::{Value};

use std::{fs::File};

const MAX_SIZE: usize = 262_144; // max payload size is 256k

#[post("/sources")]
async fn create_source(mut payload: web::Payload) -> Result<HttpResponse, Error> {
    // payload is a stream of Bytes objects
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }
    let valid = serde_json::from_slice(&body)?;
    let raw_json: Value =
        serde_json::from_reader(File::open("src/models/json-schema/SourceSetting.json").unwrap())
            .unwrap();
    let mut scope = json_schema::Scope::new();
    let schema = scope.compile_and_return(raw_json.clone(), false).unwrap();
    let validation_state = schema.validate(&valid);

    Ok(HttpResponse::Ok().json(validation_state)) // <- send response
}
