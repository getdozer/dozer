use std::{sync::Arc, time::Duration};

use dozer_ingestion_connector::{
    dozer_types::{
        errors::internal::BoxedError,
        log::info,
        models::ingestion_types::{EthContract, EthFilter, EthLogConfig, IngestionMessage},
        types::Operation,
    },
    test_util::spawn_connector,
    tokio::runtime::Runtime,
    Connector, TableInfo,
};
use web3::{
    contract::{Contract, Options},
    transports::WebSocket,
    types::H160,
};

use crate::{helper, EthLogConnector};

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

pub async fn get_eth_tables(
    wss_url: String,
    contract: Contract<WebSocket>,
) -> Result<(EthLogConnector, Vec<TableInfo>), BoxedError> {
    let address = format!("{:?}", contract.address());
    let mut eth_connector = EthLogConnector::new(
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

    let (table_infos, _) = eth_connector.list_all_schemas().await?;
    for table_info in table_infos.iter() {
        info!("Schema: {}", table_info.name);
    }
    Ok((eth_connector, table_infos))
}

pub async fn run_eth_sample(
    runtime: Arc<Runtime>,
    wss_url: String,
    my_account: H160,
) -> (Contract<WebSocket>, Vec<Operation>) {
    dozer_tracing::init_telemetry(None, &Default::default());
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
    }));

    let contract = deploy_contract(wss_url.clone(), my_account).await;

    let (connector, tables) = get_eth_tables(wss_url, contract.clone()).await.unwrap();
    let (mut iterator, _) = spawn_connector(runtime, connector, tables);

    let mut msgs = vec![];
    while let Some(IngestionMessage::OperationEvent {
        table_index: 0, op, ..
    }) = iterator.next_timeout(Duration::from_millis(400)).await
    {
        msgs.push(op);
    }
    (contract, msgs)
}
