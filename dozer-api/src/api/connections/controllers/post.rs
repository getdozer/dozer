use crate::api::connections;
use crate::db::models::connection;
use crate::models::ConnectionResponse;
use crate::services::{validation};
use crate::{db::pool::DbPool, models::ConnectionRequest};
use actix_web::{
    post,
    web::{Data, Json},
    Error, HttpResponse,
    web
};
use serde_json::Value;
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
   let inserted_result =  connections::services::create_connection(&conn, result.unwrap().into()).unwrap();
   println!("=== inserted_result {:?}", inserted_result);
    Ok(HttpResponse::Ok().json(ConnectionResponse::from(inserted_result)))
    // connections::services::create_connection(pool, result.unwrap()).await;
    // match connection_created {
    //     Ok(connection) => Ok(HttpResponse::Ok().json(connection.unwrap().)),
    //     _ => Ok(HttpResponse::BadRequest().finish())
    // }
}
