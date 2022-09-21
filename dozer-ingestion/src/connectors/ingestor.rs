use std::sync::Arc;

use super::storage::RocksStorage;
use dozer_shared::types::{OperationEvent, Schema};
use serde::{Deserialize, Serialize};
use crate::connectors::writer::{BatchedRocksDbWriter, Writer};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    OperationEvent(OperationEvent),
    Schema(Schema),
    Commit()
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
    writer: BatchedRocksDbWriter
}

impl Ingestor {
    pub fn new(
        storage_client: Arc<RocksStorage>,
        sender: Arc<Box<dyn IngestorForwarder + 'static>>,
    ) -> Self {
        Self {
            storage_client,
            sender,
            writer: BatchedRocksDbWriter::new()
        }
    }

    pub fn handle_message(&mut self, message: IngestionMessage) {
        match message {
            IngestionMessage::OperationEvent(event) => {
                let (key, encoded) =
                    self.storage_client.map_operation_event(&event);
                self.writer.insert(key.as_ref(), encoded);
                self.sender.forward(event);
            }
            IngestionMessage::Schema(schema) => {
                let (key, encoded) = self.storage_client.map_schema(&schema);
                self.writer.insert(key.as_ref(), encoded);
                // self.sender.forward(schema);
            },
            IngestionMessage::Commit() => {
                self.writer.commit(&self.storage_client);
            }
        }
    }
}

unsafe impl Sync for Ingestor {}
