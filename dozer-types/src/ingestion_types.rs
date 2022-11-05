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
