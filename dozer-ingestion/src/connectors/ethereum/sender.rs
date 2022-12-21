use core::time;
use std::{sync::Arc};


use crate::ingestion::Ingestor;
use crate::{
    connectors::{ethereum::helper, TableInfo},
    errors::ConnectorError,
};
use dozer_types::ingestion_types::{EthFilter, IngestionMessage};
use dozer_types::log::info;
use dozer_types::parking_lot::RwLock;

use futures::StreamExt;


use futures::future::{BoxFuture, FutureExt};

use web3::ethabi::Contract;
use web3::transports::WebSocket;
use web3::types::{Log};
use web3::Web3;

use super::connector::EthConnector;

pub struct EthSender {
  wss_url: String,
    filter: EthFilter,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    contract: Option<Contract>,
    tables: Option<Vec<TableInfo>>,
}

impl EthSender {
  #[allow(unreachable_code)]
pub async fn new(
    wss_url: String,
    filter: EthFilter,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    contract: Option<Contract>,
    tables: Option<Vec<TableInfo>>,
) -> Self {
  EthSender { wss_url, filter, ingestor, connector_id, contract, tables }
}

#[allow(unreachable_code)]
pub async fn run(&self) -> Result<(), ConnectorError> {
    let client = helper::get_wss_client(&self.wss_url).await.unwrap();

    // Get current block no.
    let block_end = client
        .eth()
        .block_number()
        .await
        .map_err(ConnectorError::EthError)?
        .as_u64();

    let block_start = self.filter.from_block();

    let logs = self.fetch_logs(client.clone(), block_start, block_end, 0).await?;

    for msg in logs {
        self.process_log(
            msg,
        )?;
    }
    // }

    // Create a filter from the last block to check for changes
    let mut filter = self.filter.clone();
    filter.from_block = Some(block_end);

    info!("Fetching from block ..: {}", block_end);

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

        self.process_log(
            msg,
        )?;
    }
    Ok(())
}

pub fn fetch_logs(
  &self,
    client: Web3<WebSocket>,
    
    block_start: u64,
    block_end: u64,
    depth: usize,
) -> BoxFuture<'static, Result<Vec<Log>, ConnectorError>> {
    async move {
        let mut applied_filter = self.filter.clone();
        applied_filter.from_block = Some(block_start);
        applied_filter.to_block = Some(block_end);
        let res = client.eth().logs(EthConnector::build_filter(&applied_filter)).await;

        match res {
            Ok(res) => Ok(res),
            Err(e) => match &e {
                web3::Error::Rpc(rpc_error) => {
                    // Infura returns a RpcError if the no of records are more than 10000
                    // { code: ServerError(-32005), message: "query returned more than 10000 results", data: None }
                    // break it down into half on each error and exit after 10 errors in a specific branch

                    
                    if rpc_error.code.code() == -32005 {
                        let depth_str = (0..depth).map(|_| " ".to_string()).collect::<Vec<String>>().join("");
                        info!("{} More than 10000 records, block_start: {},block_end: {}, depth: {}", depth_str, block_start, block_end, depth);
                        
                        if depth > 100 {
                            Err(ConnectorError::EthTooManyRecurisions(depth))
                        } else {
                            let middle = (block_start + block_end) / 2;
                            info!("{} Splitting in two calls block_start: {}, middle: {}, block_end: {}", depth_str,block_start, block_end, middle);
                            let left = self.fetch_logs(
                                client.clone(),
                                
                                block_start,
                                middle,
                                depth + 1,
                            )
                            .await?;

                            let right = self.fetch_logs(
                                client.clone(),           
                                middle,
                                block_end,
                                depth + 1,
                            )
                            .await?;

                            Ok([left, right].concat())
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

fn process_log(
  &self,
    msg: Log,
) -> Result<(), ConnectorError> {
    if let Some(op) = helper::map_log_to_event(msg.to_owned()) {
        // Write eth_log record
        self.ingestor
            .write()
            .handle_message((self.connector_id, IngestionMessage::OperationEvent(op)))
            .map_err(ConnectorError::IngestorError)?;

        // write event record optionally
        if let Some(ref contract) = self.contract {
            let op = helper::decode_event(msg.to_owned(), contract.to_owned(), self.tables);
            if let Some(op) = op {
                self.ingestor
                    .write()
                    .handle_message((self.connector_id, IngestionMessage::OperationEvent(op)))
                    .map_err(ConnectorError::IngestorError)?;
            }
        }
    }
    Ok(())
}

}
