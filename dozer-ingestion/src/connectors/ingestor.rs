use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::storage::RocksStorage;
use crate::connectors::writer::{BatchedRocksDbWriter, Writer};
use dozer_schema::registry::{_get_client, context};
use dozer_types::types::{Commit, OperationEvent, Schema};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use crate::connectors::seq_no_resolver::SeqNoResolver;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    Begin(),
    OperationEvent(OperationEvent),
    Schema(Schema),
    Commit(Commit),
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
    pub seq_storage_client: Arc<RocksStorage>,
    pub sender: Arc<Box<dyn IngestorForwarder>>,
    writer: BatchedRocksDbWriter,
    seq_writer: BatchedRocksDbWriter,
    timer: Instant,
    seq_no_resolver: Arc<Mutex<SeqNoResolver>>
}

impl Ingestor {
    pub fn new(
        storage_client: Arc<RocksStorage>,
        seq_storage_client: Arc<RocksStorage>,
        sender: Arc<Box<dyn IngestorForwarder + 'static>>,
        seq_no_resolver: Arc<Mutex<SeqNoResolver>>
    ) -> Self {
        Self {
            storage_client,
            seq_storage_client,
            sender,
            writer: BatchedRocksDbWriter::new(),
            seq_writer: BatchedRocksDbWriter::new(),
            timer: Instant::now(),
            seq_no_resolver
        }
    }

    pub fn handle_message(&mut self, message: IngestionMessage) {
        match message {
            IngestionMessage::OperationEvent(mut event) => {
                let seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
                event.seq_no = seq_no as u64;

                let (key, encoded) = self.storage_client.map_operation_event(&event);
                self.writer.insert(key.as_ref(), encoded);
                self.sender.forward(event);
            }
            IngestionMessage::Schema(schema) => {
                let _seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
                // Runtime::new()
                //     .unwrap()
                //     .block_on(async {
                //         let client = _get_client().await.unwrap();
                //         client.insert(context::current(), schema).await
                //     })
                //     .unwrap();
            }
            IngestionMessage::Commit(mut event) => {
                let seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
                event.seq_no = seq_no as u64;
                let (seq_key, seq_encoded) = self.storage_client.map_ingestion_seq_message(
                    &seq_no,
                    &1,
                    &event.lsn
                );
                self.seq_writer.insert(seq_key.as_ref(), seq_encoded);

                let (commit_key, commit_encoded) = self.storage_client.map_ingestion_checkpoint_message(
                    &seq_no,
                    &1
                );
                self.seq_writer.insert(commit_key.as_ref(), commit_encoded);

                self.writer.commit(&self.storage_client);
                self.seq_writer.commit(&self.seq_storage_client);
                self.seq_writer.begin();

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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use crossbeam::channel::unbounded;
    use rocksdb::{DB, Options};
    use dozer_types::types::{Operation, Record};
    use crate::connectors::ingestor::{ChannelForwarder, Ingestor, IngestorForwarder};
    use crate::connectors::ingestor::IngestionMessage::{Begin, Commit, OperationEvent, Schema};
    use crate::connectors::seq_no_resolver::SeqNoResolver;
    use crate::connectors::storage::{RocksConfig, RocksStorage, Storage};

    #[tokio::test]
    async fn test_message_handle() {
        DB::destroy(&Options::default(), "target/ingestion-message-handler-test".to_string()).unwrap();
        DB::destroy(&Options::default(), "target/sequence-test".to_string()).unwrap();

        let storage_config = RocksConfig {
            path: "target/ingestion-message-handler-test".to_string(),
        };
        let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));

        let (tx, _rx) = unbounded::<dozer_types::types::OperationEvent>();
        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));

        let mut lsn_storage_config = RocksConfig::default();
        lsn_storage_config.path = "target/sequence-test".to_string();
        let lsn_storage_client: Arc<RocksStorage> = Arc::new(Storage::new(lsn_storage_config));
        let mut seq_resolver = SeqNoResolver::new(Arc::clone(&lsn_storage_client));
        seq_resolver.init();
        let seq_no_resolver = Arc::new(Mutex::new(seq_resolver));

        let mut ingestor = Ingestor::new(storage_client.clone(), lsn_storage_client.clone(), forwarder, seq_no_resolver);

        // Expected seq no - 1
        let schema_message = dozer_types::types::Schema {
            identifier: None,
            fields: vec![],
            values: vec![],
            primary_index: vec![],
            secondary_indexes: vec![]
        };

        // Expected seq no - 2
        let operation_event_message = dozer_types::types::OperationEvent {
            seq_no: 0,
            operation: Operation::Insert {
                new: Record {
                    schema_id: None,
                    values: vec![]
                }
            }
        };

        // Expected seq no - 3
        let commit_message = dozer_types::types::Commit {
            seq_no: 0,
            lsn: 3
        };

        ingestor.handle_message(Begin());
        ingestor.handle_message(Schema(schema_message));
        ingestor.handle_message(OperationEvent(operation_event_message));
        ingestor.handle_message(Commit(commit_message));

        let mut expected_seq = vec![
            lsn_storage_client.map_ingestion_seq_message(&(3 as usize), &1, &3),
            lsn_storage_client.map_ingestion_checkpoint_message(&(3 as usize), &1)
        ].into_iter();
        let db = lsn_storage_client.get_db();
        let mut seq_iterator = db.raw_iterator();
        seq_iterator.seek_to_first();
        while seq_iterator.valid() {
            let key = seq_iterator.key().unwrap();
            let value = seq_iterator.value().unwrap();

            let (expected_key, expected_value) = expected_seq.next().unwrap();

            assert_eq!(expected_key, key);
            assert_eq!(expected_value, value);

            seq_iterator.next();
        }
    }
}
