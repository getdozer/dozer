use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::storage::RocksStorage;
use crate::connectors::writer::{BatchedRocksDbWriter, Writer};
// use dozer_schema::registry::{_get_client, context};
use dozer_types::types::{Commit, OperationEvent, Schema};
use serde::{Deserialize, Serialize};
// use tokio::runtime::Runtime;
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
    pub sender: Arc<Box<dyn IngestorForwarder>>,
    writer: BatchedRocksDbWriter,
    timer: Instant,
    seq_no_resolver: Arc<Mutex<SeqNoResolver>>
}

impl Ingestor {
    pub fn new(
        storage_client: Arc<RocksStorage>,
        sender: Arc<Box<dyn IngestorForwarder + 'static>>,
        seq_no_resolver: Arc<Mutex<SeqNoResolver>>
    ) -> Self {
        Self {
            storage_client,
            sender,
            writer: BatchedRocksDbWriter::new(),
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
            IngestionMessage::Schema(_schema) => {
                let _seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
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
            IngestionMessage::Commit(event) => {
                let seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
                let (commit_key, commit_encoded) = self.storage_client.map_commit_message(
                    &1,
                    &seq_no,
                    &event.lsn
                );
                self.writer.insert(commit_key.as_ref(), commit_encoded);
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
        let storage_config = RocksConfig::default();
        DB::destroy(&Options::default(), &storage_config.path).unwrap();

        let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));

        let (tx, _rx) = unbounded::<dozer_types::types::OperationEvent>();
        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));

        let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
        seq_resolver.init();
        let seq_no_resolver = Arc::new(Mutex::new(seq_resolver));

        let mut ingestor = Ingestor::new(storage_client.clone(), forwarder, seq_no_resolver);

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
        let operation_event_message2 = dozer_types::types::OperationEvent {
            seq_no: 0,
            operation: Operation::Insert {
                new: Record {
                    schema_id: None,
                    values: vec![]
                }
            }
        };
        
        // Expected seq no - 4
        let commit_message = dozer_types::types::Commit {
            seq_no: 0,
            lsn: 412142432
        };

        ingestor.handle_message(Begin());
        ingestor.handle_message(Schema(schema_message));
        ingestor.handle_message(OperationEvent(operation_event_message.clone()));
        ingestor.handle_message(OperationEvent(operation_event_message2.clone()));
        ingestor.handle_message(Commit(commit_message.clone()));

        let mut expected_event = operation_event_message;
        expected_event.seq_no = 2;
        let mut expected_event2 = operation_event_message2;
        expected_event2.seq_no = 3;
        let mut expected_op_event_message = vec![
            storage_client.map_operation_event(&expected_event),
            storage_client.map_operation_event(&expected_event2),
            storage_client.map_commit_message(&1, &4, &commit_message.lsn),
        ].into_iter();

        let db = storage_client.get_db();
        let mut seq_iterator = db.raw_iterator();

        seq_iterator.seek_to_first();
        while seq_iterator.valid() {
            let key = seq_iterator.key().unwrap();
            let value = seq_iterator.value().unwrap();

            let (expected_key, expected_value) = expected_op_event_message.next().unwrap();

            assert_eq!(expected_key, key);
            assert_eq!(expected_value, value);

            seq_iterator.next();
        }
    }
}
