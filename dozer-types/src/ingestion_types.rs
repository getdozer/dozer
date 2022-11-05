use serde::{Deserialize, Serialize};

use crate::types::{Commit, OperationEvent, Schema};

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
pub trait IngestorForwarder: Send + Sync {
    fn forward(&self, msg: (u64, IngestionOperation));
}
