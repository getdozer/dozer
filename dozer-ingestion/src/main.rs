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
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};
fn main() -> Result<(), ConnectorError> {
    log4rs::init_file("./log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));
    let wss_url = "wss://wiser-smart-sound.ethereum-goerli.discover.quiknode.pro/c94c6154019a91660db7f9d2b718622d3355e471/".to_string();
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let t = thread::spawn(move || -> Result<(), ConnectorError> {
        let mut eth_connector = EthConnector::new(
            1,
            EthConfig {
                name: "all_eth_logs".to_string(),
                filter: EthFilter::default(),
                wss_url,
            },
        );

        eth_connector.initialize(ingestor, None)?;
        eth_connector.start(r)
    });

    loop {
        if !running.load(Ordering::SeqCst) {
            info!("Exiting Connector on Ctrl-C");
            break;
        }
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
