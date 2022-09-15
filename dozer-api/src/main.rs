#[macro_use]
extern crate diesel;
pub mod db;
pub mod lib;
pub mod server;
pub mod services;
#[tokio::main]
async fn main() {
    server::get_server().await.unwrap();
}
