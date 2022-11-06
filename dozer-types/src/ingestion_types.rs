use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    errors::internal::BoxedError,
    types::{Commit, OperationEvent, Schema},
};

#[derive(Clone, Debug)]
pub enum IngestionOperation {
    OperationEvent(OperationEvent),
    SchemaUpdate(Schema),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    Begin(),
    OperationEvent(OperationEvent),
    Schema(Schema),
    Commit(Commit),
}

#[derive(Error, Debug)]
pub enum IngestorError {
    #[error("Failed to send message on channel")]
    ChannelError(#[from] BoxedError),
}

pub trait IngestorForwarder: Send + Sync {
    fn forward(&self, msg: (u64, IngestionOperation)) -> Result<(), IngestorError>;
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct EthFilter {
    // Starting block
    pub from_block: Option<u64>,
    pub addresses: Vec<String>,
    pub topics: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct EthConfig {
    pub name: String,
    pub filter: EthFilter,
    pub wss_url: String,
}
