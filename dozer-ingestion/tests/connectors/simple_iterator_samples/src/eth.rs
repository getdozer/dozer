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

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let t = thread::spawn(move || -> Result<(), ConnectorError> {
        let mut eth_connector = EthConnector::new(
            1,
            EthConfig {
                name: "all_eth_events".to_string(),
                filter: Some(EthFilter::default()),
                wss_url,
            },
        );
        eth_connector.initialize(ingestor, None)?;
        eth_connector.start()
    });

    loop {
        let msg = iterator.write().next();

        if let Some(msg) = msg {
            info!("{:?}", msg);
        } else {
            break;
        }
    }

    t.join().unwrap()?;
    Ok(())
}
