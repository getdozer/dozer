use core::time;
use std::collections::HashMap;
use std::sync::Arc;

use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::log::{debug, info, trace, warn};
use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    EthFilter, IngestionMessage,
};
use dozer_ingestion_connector::futures::future::BoxFuture;
use dozer_ingestion_connector::futures::{FutureExt, StreamExt};
use dozer_ingestion_connector::{tokio, Ingestor, TableToIngest};
use web3::transports::WebSocket;
use web3::types::{Log, H256};
use web3::Web3;

use crate::log::Error;
use crate::{helper as conn_helper, EthLogConnector};

use super::connector::ContractTuple;
use super::helper;

const MAX_RETRIES: usize = 3;

pub struct EthDetails<'a> {
    wss_url: String,
    filter: EthFilter,
    ingestor: &'a Ingestor,
    contracts: HashMap<String, ContractTuple>,
    pub tables: Vec<TableToIngest>,
    pub schema_map: HashMap<H256, usize>,
    pub conn_name: String,
}

impl<'a> EthDetails<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        wss_url: String,
        filter: EthFilter,
        ingestor: &'a Ingestor,
        contracts: HashMap<String, ContractTuple>,
        tables: Vec<TableToIngest>,
        schema_map: HashMap<H256, usize>,
        conn_name: String,
    ) -> Self {
        EthDetails {
            wss_url,
            filter,
            ingestor,
            contracts,
            tables,
            schema_map,
            conn_name,
        }
    }
}

pub async fn run(details: Arc<EthDetails<'_>>) -> Result<(), BoxedError> {
    let client = conn_helper::get_wss_client(&details.wss_url).await?;

    // Get current block no.
    let latest_block_no = client.eth().block_number().await?.as_u64();

    let block_end = match details.filter.to_block {
        None => latest_block_no,
        Some(block_no) => block_no,
    };

    // Default to current block if from_block is not specified
    let block_start = details.filter.from_block.unwrap_or(block_end);

    fetch_logs(
        details.clone(),
        client.clone(),
        block_start,
        block_end,
        0,
        MAX_RETRIES,
    )
    .await?;

    let changes_handler_filter = match details.filter.to_block {
        None => {
            // Create a filter from the last block to check for changes
            let mut filter = details.filter.clone();
            filter.from_block = Some(latest_block_no);
            Some(filter)
        }
        Some(block_end) => {
            if block_end > latest_block_no {
                // Create a filter from the last block to defined end of blocks.
                // It can be used to fetch future records with limiting it by `block_to`

                let mut filter = details.filter.clone();
                filter.from_block = Some(latest_block_no);
                filter.to_block = Some(block_end);
                Some(filter)
            } else {
                None
            }
        }
    };

    if let Some(filter) = changes_handler_filter {
        debug!(
            "[{}] Fetching from block ..: {}",
            details.conn_name, block_end
        );

        let filter = client
            .eth_filter()
            .create_logs_filter(EthLogConnector::build_filter(&filter))
            .await?;

        let stream = filter.stream(time::Duration::from_secs(1));

        tokio::pin!(stream);

        loop {
            let msg = stream.next().await;

            let msg = msg.ok_or(Error::EmptyMessage)??;

            process_log(details.clone(), msg).await;
        }
    } else {
        info!("[{}] Reading reached block_to limit", details.conn_name);
    }
    Ok(())
}

pub fn fetch_logs(
    details: Arc<EthDetails>,
    client: Web3<WebSocket>,
    block_start: u64,
    block_end: u64,
    depth: usize,
    retries_left: usize,
) -> BoxFuture<'_, Result<(), BoxedError>> {
    let filter = details.filter.clone();
    let depth_str = (0..depth)
        .map(|_| " ".to_string())
        .collect::<Vec<String>>()
        .join("");
    async move {
        let mut applied_filter = filter.clone();
        applied_filter.from_block = Some(block_start);
        applied_filter.to_block = Some(block_end);
        let res = client.eth().logs(EthLogConnector::build_filter(&applied_filter)).await;

        match res {
            Ok(logs) => {
                debug!("[{}] {} Fetched: {} , block_start: {},block_end: {}, depth: {}", details.conn_name, depth_str, logs.len(), block_start, block_end, depth);
                for msg in logs {
                    process_log(
                        details.clone(),
                        msg,
                    ).await;
                }
                Ok(())
            },
            Err(e) => match e {
                web3::Error::Rpc(rpc_error) => {
                    // Infura returns a RpcError if the no of records are more than 10000
                    // { code: ServerError(-32005), message: "query returned more than 10000 results", data: None }
                    // break it down into half on each error and exit after 10 errors in a specific branch
                    if rpc_error.code.code() == -32005 {
                        debug!("[{}] {} More than 10000 records, block_start: {},block_end: {}, depth: {}", details.conn_name, depth_str, block_start, block_end, depth);
                        if depth > 100 {
                            Err(Error::EthTooManyRecurisions(depth).into())
                        } else {
                            let middle = (block_start + block_end) / 2;
                            debug!("[{}] {} Splitting in two calls block_start: {}, middle: {}, block_end: {}", details.conn_name, depth_str,block_start, block_end, middle);
                            fetch_logs(
                                details.clone(),
                                client.clone(),
                                block_start,
                                middle,
                                depth + 1,
                                MAX_RETRIES
                            )
                            .await?;

                            fetch_logs(
                                details,
                                client.clone(),
                                middle + 1,
                                block_end,
                                depth + 1,
                                MAX_RETRIES
                            )
                            .await?;
                            Ok(())
                        }
                    } else {
                        Err(rpc_error.into())
                    }
                }
                e => {
                    if retries_left == 0 {
                        Err(e.into())
                    } else {
                        warn!("[{}] Retrying to fetch logs", details.conn_name);
                        fetch_logs(details, client, block_start, block_end, depth, retries_left - 1).await?;
                        Ok(())
                    }
                },
            },
        }
    }
    .boxed()
}

async fn process_log(details: Arc<EthDetails<'_>>, msg: Log) {
    // Filter pending logs. log.log_index is None for pending State
    if msg.log_index.is_some() {
        if let Some((table_index, op)) = helper::map_log_to_event(msg.to_owned(), details.clone()) {
            trace!("Writing log : {:?}", op);
            // Write eth_log record
            if details
                .ingestor
                .handle_message(IngestionMessage::OperationEvent {
                    table_index,
                    op,
                    id: None,
                })
                .await
                .is_err()
            {
                // If receiving end is closed, exit
                return;
            }
        } else {
            trace!("Ignoring log : {:?}", msg);
        }

        // write event record optionally

        let op = helper::decode_event(msg, details.contracts.to_owned(), details.tables.clone());
        if let Some((table_index, op)) = op {
            trace!("Writing event : {:?}", op);
            // if receiving end is closed, ignore
            let _ = details
                .ingestor
                .handle_message(IngestionMessage::OperationEvent {
                    table_index,
                    op,
                    id: None,
                })
                .await;
        } else {
            trace!("Writing event : {:?}", op);
        }
    }
}
