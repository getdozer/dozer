pub mod registry;
pub mod storage;

pub async fn run() -> anyhow::Result<()> {
    use crate::storage::RocksConfig;
    use registry::_serve;
    let config = RocksConfig::_target();
    _serve(Some(config)).await
}
