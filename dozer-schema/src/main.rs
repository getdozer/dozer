mod registry;
mod storage;

use dozer_types::log4rs;
use registry::_serve;
use storage::RocksConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log4rs::init_file("log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let config = RocksConfig::_target();
    _serve(Some(config)).await
}
