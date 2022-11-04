use dozer_ingestion::connectors::{
    connector::Connector,
    ethereum::connector::{EthConfig, EthConnector, EthFilter},
    seq_no_resolver::SeqNoResolver,
    storage::{RocksConfig, Storage},
};
use dozer_types::{errors::connector::ConnectorError, log::info, log4rs};
use std::sync::{Arc, Mutex};
fn main() -> Result<(), ConnectorError> {
    log4rs::init_file("./log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));
    let wss_url = "wss://wiser-smart-sound.ethereum-goerli.discover.quiknode.pro/c94c6154019a91660db7f9d2b718622d3355e471/";
    let mut eth_connector = EthConnector::new(
        1,
        EthConfig {
            name: "all_eth_events",
            filter: EthFilter::default(),
            wss_url,
        },
    );

    let storage_config = RocksConfig::default();
    let storage_client = Arc::new(Storage::new(storage_config));

    let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
    seq_resolver.init();
    let seq_no_resolver = Arc::new(Mutex::new(seq_resolver));

    eth_connector.initialize(storage_client, None).unwrap();

    let mut iterator = eth_connector.iterator(seq_no_resolver);

    loop {
        let msg = iterator.next();

        if let Some(msg) = msg {
            info!("{:?}", msg);
        } else {
            break;
        }
    }

    Ok(())
}
