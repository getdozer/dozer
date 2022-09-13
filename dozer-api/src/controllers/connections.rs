use crate::models::ConnectionResponse;
use crate::services::connections;
use crate::services::validation;
use crate::{db::pool::DbPool, models::ConnectionRequest};
use actix_web::{
    get, post,
    web::{self, Data, Json},
    Error, HttpResponse,
};

use dozer_ingestion::connectors::postgres;
use serde_json::Value;

#[get("/connections")]
async fn index(pool: web::Data<DbPool>) -> Json<String> {
    return Json("Hello".to_string());
    // let conn = pool.get().expect("couldn't get db connection from pool");
    // let connections = web::block(move || get_connections(&*conn))
    //     .await
    //     .map_err(|e| {
    //         eprintln!("{}", e);
    //         HttpResponse::InternalServerError().finish()
    //     });

    // match connections {
    //     Ok(connection) => Ok(HttpResponse::Ok().json(connection)),
    //     _ => Ok(HttpResponse::BadRequest().finish())
    // }
}

#[post("/connections")]
async fn create_connection(pool: Data<DbPool>, input: Json<Value>) -> Result<HttpResponse, Error> {
    let conn = pool.get().expect("couldn't get db connection from pool");
    let result = validation::validation::<ConnectionRequest>(serde_json::to_value(input).unwrap());
    // Ok(HttpResponse::Ok().json(result))
    // let connection_created = web::block(move || connections::services::create_connection(&conn, result.unwrap().into()))
    //     .await
    //     .map_err(|e| {
    //         eprintln!("{}", e);
    //         HttpResponse::InternalServerError().finish()
    //     });
    let inserted_result = connections::create_connection(&conn, result.unwrap().into()).unwrap();
    println!("=== inserted_result {:?}", inserted_result);
    Ok(HttpResponse::Ok().json(ConnectionResponse::from(inserted_result)))
    // connections::services::create_connection(pool, result.unwrap()).await;
    // match connection_created {
    //     Ok(connection) => Ok(HttpResponse::Ok().json(connection.unwrap().)),
    //     _ => Ok(HttpResponse::BadRequest().finish())
    // }
}

#[post("/connections/test")]
async fn test_connection(input: Json<Value>) -> Result<HttpResponse, Error> {
    let result = validation::validation::<ConnectionRequest>(serde_json::to_value(input).unwrap());
    let temp_value = ingestion_client::test_connection(result.unwrap())
        .await
        .unwrap();
    let grpc_result: &dozer_shared::ingestion::ConnectionResponse = temp_value.get_ref();
    let table_info = grpc_result.response.clone().unwrap();
    match table_info {
        Response::Error(err) => Ok(HttpResponse::Ok().json(err)),
        Response::Success(details) => Ok(HttpResponse::Ok().json(details)),
    }
}
