use actix_web::{get, post,Error, HttpResponse, web::{self, Json}};
use serde_json::Value;

use crate::{db::pool::DbPool};

use crate::services::validation;
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