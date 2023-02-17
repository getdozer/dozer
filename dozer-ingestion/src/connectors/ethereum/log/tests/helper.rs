use std::{collections::HashSet, thread, time::Duration};

use crate::{
    connectors::{
        ethereum::{helper, EthLogProvider},
        Connector,
    },
    errors::ConnectorError,
    ingestion::{IngestionConfig, Ingestor},
};

use dozer_types::{
    ingestion_types::{EthConfig, EthContract, EthFilter, EthLogConfig, EthProviderConfig},
    log::info,
    types::Operation,
};

use tokio::runtime::Runtime;
use web3::{
    contract::{Contract, Options},
    transports::WebSocket,
    types::H160,
};

pub async fn deploy_contract(wss_url: String, my_account: H160) -> Contract<WebSocket> {
    let web3 = helper::get_wss_client(&wss_url).await.unwrap();
    // Get the contract bytecode for instance from Solidity compiler
    let bytecode = include_str!("./contracts/CustomEvent.code").trim_end();
    let abi = include_bytes!("./contracts/CustomEvent.json");
    // Deploying a contract
    let builder = Contract::deploy(web3.eth(), abi).unwrap();
    let contract = builder
        .confirmations(0)
        .options(Options::with(|opt| {
            opt.gas = Some(3_000_000.into());
        }))
        .execute(bytecode, (), my_account)
        .await
        .unwrap();

    contract
        .call("test", (), my_account, Options::default())
        .await
        .unwrap();
    contract
}

pub fn get_eth_producer(
    wss_url: String,
    ingestor: Ingestor,
    contract: Contract<WebSocket>,
) -> Result<(), ConnectorError> {
    let address = format!("{:?}", contract.address());
    let eth_connector = EthLogProvider::new(
        1,
        wss_url,
        EthLogConfig {
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
        },
        "eth_test".to_string(),
    );

    let schemas = eth_connector.get_schemas(None)?;
    for (name, schema, _) in schemas {
        info!("Schema: {}, Id: {}", name, schema.identifier.unwrap().id);
        // schema.print().printstd();
    }

    eth_connector.start(None, &ingestor, None)
}

pub fn run_eth_sample(wss_url: String, my_account: H160) -> (Contract<WebSocket>, Vec<Operation>) {
    dozer_tracing::init_telemetry(false).unwrap();
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
    }));

    let contract = Runtime::new()
        .unwrap()
        .block_on(async { deploy_contract(wss_url.clone(), my_account).await });

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    let cloned_contract = contract.clone();
    let _t = thread::spawn(move || {
        info!("Initializing with WSS: {}", wss_url);
        get_eth_producer(wss_url, ingestor, cloned_contract).unwrap();
    });

    let mut msgs = vec![];
    let mut op_index = HashSet::new();
    while let Some(msg) = iterator.next_timeout(Duration::from_millis(400)) {
        // Duplicates are to be expected in ethereum connector
        let ((_, seq_no), op) = msg;
        if op_index.insert(seq_no) {
            msgs.push(op);
        }
    }
    (contract, msgs)
}
