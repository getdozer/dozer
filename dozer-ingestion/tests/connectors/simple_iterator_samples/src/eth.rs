use dozer_ingestion::{
    connectors::{ethereum::connector::EthConnector, Connector},
    errors::ConnectorError,
    ingestion::{IngestionConfig, Ingestor},
};
use dozer_types::{
    ingestion_types::{EthConfig, EthFilter},
    log::info,
    log4rs,
};
use std::thread;
fn main() -> Result<(), ConnectorError> {
    log4rs::init_file("./log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));
    let wss_url = "wss://localhost:8545".to_string();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let t = thread::spawn(move || -> Result<(), ConnectorError> {
        let mut eth_connector = EthConnector::new(
            1,
            EthConfig {
                name: "all_eth_events".to_string(),
                filter: EthFilter::default(),
                wss_url,
            },
        );
        eth_connector.initialize(ingestor, None)?;
        eth_connector.start()
    });

    loop {
        let msg = iterator.next();

        if let Some(msg) = msg {
            info!("{:?}", msg);
        } else {
            break;
        }
    }

    t.join().unwrap()?;
    Ok(())
}
