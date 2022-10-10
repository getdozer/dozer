use std::sync::Arc;
use std::time::Instant;

use super::storage::RocksStorage;
use crate::connectors::writer::{BatchedRocksDbWriter, Writer};
use dozer_schema::registry::{_get_client, context};
use dozer_types::types::{OperationEvent, Schema};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    Begin(),
    OperationEvent(OperationEvent),
    Schema(Schema),
    Commit(),
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
    writer: BatchedRocksDbWriter,
    timer: Instant,
}

impl Ingestor {
    pub fn new(
        storage_client: Arc<RocksStorage>,
        sender: Arc<Box<dyn IngestorForwarder + 'static>>,
    ) -> Self {
        Self {
            storage_client,
            sender,
            writer: BatchedRocksDbWriter::new(),
            timer: Instant::now(),
        }
    }

    pub fn handle_message(&mut self, message: IngestionMessage) {
        match message {
            IngestionMessage::OperationEvent(event) => {
                let (key, encoded) = self.storage_client.map_operation_event(&event);
                self.writer.insert(key.as_ref(), encoded);
                self.sender.forward(event);
            }
            IngestionMessage::Schema(schema) => {
                // TODO: fix usage of schema registry update
                // let schema_update = Runtime::new()
                //     .unwrap()
                //     .block_on(async {
                //         let client = _get_client().await.unwrap();
                //         client.insert(context::current(), schema).await
                //     });

                // if let Err(_) = schema_update {
                //     println!("Igoring schema updated error");
                // }
            }
            IngestionMessage::Commit() => {
                self.writer.commit(&self.storage_client);
                println!("Batch processing took: {:.2?}", self.timer.elapsed());
            }
            IngestionMessage::Begin() => {
                self.writer.begin();
                self.timer = Instant::now();
            }
        }
    }
}

unsafe impl Sync for Ingestor {}
