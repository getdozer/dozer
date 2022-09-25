mod registry;
mod storage;
use dozer_types::schema_registry::{context, get_client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = get_client().await?;

    let pong = client.ping(context::current()).await?;
    println!("{:?}", pong);

    Ok(())
}
