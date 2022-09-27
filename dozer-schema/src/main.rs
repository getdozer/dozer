mod registry;
mod storage;
use registry::_serve;
use storage::RocksConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = RocksConfig::_target();
    _serve(Some(config)).await
}
