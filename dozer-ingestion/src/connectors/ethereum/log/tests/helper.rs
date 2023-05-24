use std::{collections::HashSet, time::Duration};

use crate::{
    connectors::{
        ethereum::{helper, EthLogConnector},
        Connector,
    },
    errors::ConnectorError,
    ingestion::{IngestionConfig, Ingestor},
};

use dozer_types::{
    ingestion_types::{
        EthContract, EthFilter, EthLogConfig, IngestionMessage, IngestionMessageKind,
    },
    log::info,
    types::Operation,
};

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

pub async fn get_eth_producer(
    wss_url: String,
    ingestor: Ingestor,
    contract: Contract<WebSocket>,
) -> Result<(), ConnectorError> {
    let address = format!("{:?}", contract.address());
    let eth_connector = EthLogConnector::new(
        1,
        EthLogConfig {
            wss_url,
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

    let (table_infos, schemas) = eth_connector.list_all_schemas().await?;
    for (table_info, schema) in table_infos.iter().zip(schemas) {
        info!(
            "Schema: {}, Id: {}",
            table_info.name,
            schema.schema.identifier.unwrap().id
        );
    }

    eth_connector.start(&ingestor, table_infos).await
}

pub async fn run_eth_sample(
    wss_url: String,
    my_account: H160,
) -> (Contract<WebSocket>, Vec<Operation>) {
    dozer_tracing::init_telemetry(None, None);
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
    }));

    let contract = deploy_contract(wss_url.clone(), my_account).await;

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    let cloned_contract = contract.clone();
    let _t = tokio::spawn(async move {
        info!("Initializing with WSS: {}", wss_url);
        get_eth_producer(wss_url, ingestor, cloned_contract)
            .await
            .unwrap();
    });

    let mut msgs = vec![];
    let mut op_index = HashSet::new();
    while let Some(IngestionMessage {
        identifier,
        kind: IngestionMessageKind::OperationEvent(op),
    }) = iterator.next_timeout(Duration::from_millis(400))
    {
        // Duplicates are to be expected in ethereum connector
        if op_index.insert(identifier.seq_in_tx) {
            msgs.push(op);
        }
    }
    (contract, msgs)
}
