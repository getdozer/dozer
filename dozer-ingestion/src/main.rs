use dozer_ingestion::{
    connectors::{ethereum::connector::EthConnector, Connector},
    errors::ConnectorError,
    ingestion::{IngestionConfig, Ingestor},
};
use dozer_types::{
    ingestion_types::{EthConfig, EthFilter},
    log::info,
};
use std::thread;
fn main() -> Result<(), ConnectorError> {
    dozer_tracing::init_telemetry(false).unwrap();

    // SET ENV variable `ETH_WSS_URL`
    let wss_url = std::env::var("ETH_WSS_URL").unwrap_or("wss://localhost:8545".to_string());

    // SET ENV variable `ETH_HTTPS_URL`
    let https_url = std::env::var("ETH_HTTPS_URL").unwrap_or("http://localhost:8545".to_string());

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let t = thread::spawn(move || -> Result<(), ConnectorError> {
        let mut eth_connector = EthConnector::new(
            1,
            EthConfig {
                name: "all_eth_events".to_string(),
                filter: Some(EthFilter::default()),
                wss_url,
                https_url,
            },
        );
        eth_connector.initialize(ingestor, None)?;
        eth_connector.start()
    });

    let mut idx = 0;
    loop {
        if idx > 10 {
            break;
        }
        let msg = iterator.write().next();

        if let Some(msg) = msg {
            info!("{:?}", msg);
        } else {
            break;
        }
        idx += 1;
    }

    t.join().unwrap()?;
    Ok(())
}
