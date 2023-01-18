use core::time;
use std::collections::HashMap;
use std::sync::Arc;

use crate::ingestion::Ingestor;
use crate::{
    connectors::{ethereum::helper, TableInfo},
    errors::ConnectorError,
};
use dozer_types::ingestion_types::{EthFilter, IngestionMessage};
use dozer_types::log::{debug, trace};
use dozer_types::parking_lot::RwLock;

use futures::StreamExt;

use futures::future::{BoxFuture, FutureExt};

use web3::transports::WebSocket;
use web3::types::{Log, H256};
use web3::Web3;

use super::connector::{ContractTuple, EthConnector};

pub struct EthDetails {
    wss_url: String,
    filter: EthFilter,
    ingestor: Arc<RwLock<Ingestor>>,
    contracts: HashMap<String, ContractTuple>,
    pub tables: Option<Vec<TableInfo>>,
    pub schema_map: HashMap<H256, usize>,
    from_seq: Option<(u64, u64)>,
}

impl EthDetails {
    pub fn new(
        wss_url: String,
        filter: EthFilter,
        ingestor: Arc<RwLock<Ingestor>>,
        contracts: HashMap<String, ContractTuple>,
        tables: Option<Vec<TableInfo>>,
        schema_map: HashMap<H256, usize>,
        from_seq: Option<(u64, u64)>,
    ) -> Self {
        EthDetails {
            wss_url,
            filter,
            ingestor,
            contracts,
            tables,
            schema_map,
            from_seq
        }
    }
}

#[allow(unreachable_code)]
pub async fn run(details: Arc<EthDetails>) -> Result<(), ConnectorError> {
    let client = helper::get_wss_client(&details.wss_url)
        .await
        .map_err(ConnectorError::EthError)?;

    // Get current block no.
    let block_end = client
        .eth()
        .block_number()
        .await
        .map_err(ConnectorError::EthError)?
        .as_u64();

    // Default to current block if from_block is not specified
    let block_start = match (details.from_seq, details.filter.from_block) {
        (Some((0, _)), Some(block_no)) | (None, Some(block_no)) => block_no,
        (Some((0, _)), None) | (None, None) => block_end,
        (Some((lsn, _)), _) => lsn + 1,
    };

    fetch_logs(details.clone(), client.clone(), block_start, block_end, 0).await?;

    // Create a filter from the last block to check for changes
    let mut filter = details.filter.clone();
    filter.from_block = Some(block_end);

    debug!("Fetching from block ..: {}", block_end);

    let filter = client
        .eth_filter()
        .create_logs_filter(EthConnector::build_filter(&filter))
        .await
        .map_err(ConnectorError::EthError)?;

    let stream = filter.stream(time::Duration::from_secs(1));

    tokio::pin!(stream);

    loop {
        let msg = stream.next().await;

        let msg = msg
            .map_or(Err(ConnectorError::EmptyMessage), Ok)?
            .map_err(ConnectorError::EthError)?;

        process_log(details.clone(), msg)?;
    }
    Ok(())
}

pub fn fetch_logs(
    details: Arc<EthDetails>,
    client: Web3<WebSocket>,
    block_start: u64,
    block_end: u64,
    depth: usize,
) -> BoxFuture<'static, Result<(), ConnectorError>> {
    let filter = details.filter.clone();
    let depth_str = (0..depth)
        .map(|_| " ".to_string())
        .collect::<Vec<String>>()
        .join("");
    async move {
        let mut applied_filter = filter.clone();
        applied_filter.from_block = Some(block_start);
        applied_filter.to_block = Some(block_end);
        let res = client.eth().logs(EthConnector::build_filter(&applied_filter)).await;

        match res {
            Ok(logs) => {
                debug!(" {} Fetched: {} , block_start: {},block_end: {}, depth: {}", depth_str, logs.len(), block_start, block_end, depth);
                for msg in logs {
                    process_log(
                        details.clone(),
                        msg,
                    )?;
                }
                Ok(())
            },
            Err(e) => match &e {
                web3::Error::Rpc(rpc_error) => {
                    // Infura returns a RpcError if the no of records are more than 10000
                    // { code: ServerError(-32005), message: "query returned more than 10000 results", data: None }
                    // break it down into half on each error and exit after 10 errors in a specific branch
                    if rpc_error.code.code() == -32005 {
                        debug!("{} More than 10000 records, block_start: {},block_end: {}, depth: {}", depth_str, block_start, block_end, depth);                
                        if depth > 100 {
                            Err(ConnectorError::EthTooManyRecurisions(depth))
                        } else {
                            let middle = (block_start + block_end) / 2;
                            debug!("{} Splitting in two calls block_start: {}, middle: {}, block_end: {}", depth_str,block_start, block_end, middle);
                            fetch_logs(
                                details.clone(),
                                client.clone(),
                                block_start,
                                middle,
                                depth + 1,
                            )
                            .await?;

                            fetch_logs(
                                details,
                                client.clone(),
                                middle + 1,
                                block_end,
                                depth + 1,
                            )
                            .await?;
                            Ok(())
                        }
                    } else {
                        Err(ConnectorError::EthError(e))
                    }
                }
                e => Err(ConnectorError::EthError(e.to_owned())),
            },
        }
    }
    .boxed()
}

fn process_log(details: Arc<EthDetails>, msg: Log) -> Result<(), ConnectorError> {
    // Filter pending logs. log.log_index is None for pending State
    if msg.log_index.is_none() {
        Ok(())
    } else {
        if let Some(op) = helper::map_log_to_event(msg.to_owned(), details.clone()) {
            trace!("Writing log : {:?}", op);
            // Write eth_log record
            details
                .ingestor
                .write()
                .handle_message(((msg.block_number.expect("expected for non pending").as_u64(), 0), IngestionMessage::OperationEvent(op)))
                .map_err(ConnectorError::IngestorError)?;
        } else {
            trace!("Ignoring log : {:?}", msg);
        }

        // write event record optionally

        let op = helper::decode_event(
            msg,
            details.contracts.to_owned(),
            details.tables.clone(),
            details.schema_map.clone(),
        );
        if let Some(op) = op {
            trace!("Writing event : {:?}", op);
            details
                .ingestor
                .write()
                .handle_message(((0, 0), IngestionMessage::OperationEvent(op)))
                .map_err(ConnectorError::IngestorError)?;
        } else {
            trace!("Writing event : {:?}", op);
        }

        Ok(())
    }
}
