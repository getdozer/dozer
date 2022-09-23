extern crate diesel;
pub mod server;
pub mod services;
pub mod persistent;

#[tokio::main]
async fn main() {
    server::get_server().await.unwrap();
}
