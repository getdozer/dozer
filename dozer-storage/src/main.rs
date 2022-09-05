mod storage_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    storage_server::get_server().await.unwrap();
    Ok(())
}
