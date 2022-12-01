extern crate diesel;
pub mod db;
pub mod server;
pub mod services;
pub mod tests;
#[macro_use]
extern crate diesel_migrations;
#[tokio::main]
async fn main() {
    server::get_server().await.unwrap();
}
