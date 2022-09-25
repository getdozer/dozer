mod registry;
mod storage;
use registry::serve;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    serve().await
}
