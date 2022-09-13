use crate::services::connections;
use crate::services::validation;
use crate::{db::pool::DbPool, models::ConnectionRequest};
use actix_web::{
    get, post,
    web::{self, Data, Json},
    Error, HttpResponse,
};

use dozer_shared::types::TableInfo;
use serde_json::Value;

#[get("/connections")]
async fn index(pool: web::Data<DbPool>) -> Json<String> {
    return Json("Hello".to_string());
}

#[post("/connections")]
async fn create_connection(pool: Data<DbPool>, input: Json<Value>) -> Result<HttpResponse, Error> {
    let conn = pool.get().expect("couldn't get db connection from pool");
    let result = validation::validation::<ConnectionRequest>(serde_json::to_value(input).unwrap());
    let inserted_result = connections::create_connection(&conn, result.unwrap().into()).unwrap();
    println!("=== inserted_result {:?}", inserted_result);
    Ok(HttpResponse::Ok().json(inserted_result))
}

#[post("/connections/test")]
async fn test_connection(input: Json<Value>) -> Result<HttpResponse, Error> {
    let result = validation::validation::<ConnectionRequest>(serde_json::to_value(input).unwrap());
    let temp_value:Vec<TableInfo> = crate::services::connections::test_connection(result.unwrap())
        .await
        .unwrap();

    Ok(HttpResponse::Ok().json(temp_value))
}
