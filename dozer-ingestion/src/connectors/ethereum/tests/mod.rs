use crate::{
    connectors::{ethereum::connector::EthConnector, Connector},
    errors::ConnectorError,
    ingestion::{IngestionConfig, Ingestor},
};
use core::panic;
use dozer_types::{
    ingestion_types::{EthConfig, EthContract, EthFilter},
    log::{debug, info},
    parking_lot::RwLock,
};
use hex_literal::hex;
use std::{process, sync::Arc, thread};
use tokio::runtime::Runtime;
use web3::{contract::Contract, transports::WebSocket};

use super::helper;

#[test]
fn test_eth_iterator() {
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        process::exit(1);
    }));
    let wss_url = "ws://localhost:8545".to_string();

    let contract = Runtime::new()
        .unwrap()
        .block_on(async { deploy_contract(wss_url.clone()).await });

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let ingestor_pr = ingestor.clone();

    // let t = thread::spawn(move || {
    //     info!("Initializing with WSS: {}", wss_url);
    //     get_eth_producer(wss_url, ingestor_pr, contract).unwrap();
    // });
    // t.join().unwrap();

    // let mut idx = 0;
    // loop {
    //     if idx > 1 {
    //         info!("exiting after {}", idx);
    //         process::exit(1);
    //     }
    //     let msg = iterator.write().next();

    //     if let Some(msg) = msg {
    //         info!("{:?}", msg);
    //     } else {
    //         break;
    //     }
    //     idx += 1;
    // }
}

async fn deploy_contract(wss_url: String) -> Contract<WebSocket> {
    let web3 = helper::get_wss_client(&wss_url).await.unwrap();

    let my_account = hex!("d028d24f16a8893bd078259d413372ac01580769").into();
    // Get the contract bytecode for instance from Solidity compiler
    let bytecode = include_str!("./contracts/CustomEvent.code").trim_end();
    let abi = include_bytes!("./contracts/CustomEvent.json");
    // Deploying a contract
    let res = Contract::deploy(web3.eth(), abi)
        .unwrap()
        .confirmations(0)
        .execute(bytecode, (), my_account)
        .await;

    res.unwrap()
}

fn get_eth_producer(
    wss_url: String,
    ingestor: Arc<RwLock<Ingestor>>,
    contract: Contract<WebSocket>,
) -> Result<(), ConnectorError> {
    let address = format!("{:?}", contract.address());
    let mut eth_connector = EthConnector::new(
        1,
        EthConfig {
            filter: Some(EthFilter {
                from_block: Some(0),
                to_block: None,
                addresses: vec![address.clone()],
                topics: vec![],
            }),
            contracts: vec![EthContract {
                name: "custom_event".to_string(),
                address,
                abi: include_str!("./contracts/CustomEvent.json")
                    .trim_end()
                    .to_string(),
            }],
            wss_url,
        },
    );

    let schemas = eth_connector.get_schemas(None)?;
    for (name, schema) in schemas {
        info!("Schema: {}", name);
        schema.print().printstd();
    }

    eth_connector.initialize(ingestor, None)?;
    eth_connector.start()
}
