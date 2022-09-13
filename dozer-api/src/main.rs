#[macro_use]
extern crate diesel;

mod controllers;
pub mod db;
pub mod lib;
pub mod models;
mod routes;
pub mod services;

// use actix_web::middleware::ErrorHandlers;
use actix_web::{
    get, post,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use db::pool::establish_connection;
#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    std::env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();

    let connection = establish_connection();

    HttpServer::new(move || {
        App::new()
            // .wrap(
            //     ErrorHandlers::new()
            //         .handler(http::StatusCode::METHOD_NOT_ALLOWED, error::render_405)
            //         .handler(http::StatusCode::NOT_FOUND, error::render_404)
            //         .handler(http::StatusCode::INTERNAL_SERVER_ERROR, error::render_500)
            //         .handler(http::StatusCode::BAD_REQUEST, error::render_400),
            // )
            .app_data(Data::new(connection.clone()))
            .service(hello)
            .service(echo)
            .configure(routes::routes)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("0.0.0.0", 3001))?
    .run()
    .await
}
