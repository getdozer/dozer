use actix_web::{get, Error, HttpResponse, web};

use crate::db::pool::DbPool;

use super::services::get_connections;

#[get("/connections")]
async fn index(pool: web::Data<DbPool>) -> Result<HttpResponse, Error> {
    let conn = pool.get().expect("couldn't get db connection from pool");
    let connections = web::block(move || get_connections(&*conn))
        .await
        .map_err(|e| {
            eprintln!("{}", e);
            HttpResponse::InternalServerError().finish()
        });
        
    match connections {
        Ok(connection) => Ok(HttpResponse::Ok().json(connection)),
        _ => Ok(HttpResponse::BadRequest().finish())
    }
}
