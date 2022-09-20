use std::sync::Arc;

use super::storage::RocksStorage;
use dozer_shared::types::{OperationEvent, Schema};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    OperationEvent(OperationEvent),
    Schema(Schema),
}
pub trait IngestorForwarder: Send + Sync {
    fn forward(&self, msg: OperationEvent);
}

pub struct ChannelForwarder {
    pub sender: crossbeam::channel::Sender<OperationEvent>,
}

impl IngestorForwarder for ChannelForwarder {
    fn forward(&self, event: OperationEvent) {
        let send_res = self.sender.send(event);
        match send_res {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e.to_string())
            }
        }
    }
}

pub struct Ingestor {
    pub storage_client: Arc<RocksStorage>,
    pub sender: Arc<Box<dyn IngestorForwarder>>,
}

impl Ingestor {
    pub fn new(
        storage_client: Arc<RocksStorage>,
        sender: Arc<Box<dyn IngestorForwarder + 'static>>,
    ) -> Self {
        Self {
            storage_client,
            sender,
        }
    }

    pub fn handle_message(&self, message: IngestionMessage) {
        match message {
            IngestionMessage::OperationEvent(event) => {
                self.storage_client.insert_operation_event(&event);
                // self.sender.forward(event);
            }
            IngestionMessage::Schema(schema) => {
                self.storage_client.insert_schema(&schema);
            }
        }
    }
}
