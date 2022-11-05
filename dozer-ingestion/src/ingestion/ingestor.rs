use crossbeam::channel::{unbounded, Receiver};
use dozer_types::ingestion_types::{
    IngestionMessage, IngestionOperation, IngestorError, IngestorForwarder,
};
use dozer_types::log::{debug, warn};
use dozer_types::parking_lot::RwLock;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::seq_no_resolver::SeqNoResolver;
use super::storage::RocksStorage;
use super::writer::{BatchedRocksDbWriter, Writer};
use super::IngestionConfig;

pub struct ChannelForwarder {
    pub sender: crossbeam::channel::Sender<(u64, IngestionOperation)>,
}

impl IngestorForwarder for ChannelForwarder {
    fn forward(&self, event: (u64, IngestionOperation)) -> Result<(), IngestorError> {
        let send_res = self.sender.send(event);
        match send_res {
            Ok(_) => Ok(()),
            Err(e) => Err(IngestorError::ChannelError(Box::new(e))),
        }
    }
}
pub struct IngestionIterator {
    pub rx: Receiver<(u64, IngestionOperation)>,
}

impl Iterator for IngestionIterator {
    type Item = (u64, IngestionOperation);
    fn next(&mut self) -> Option<Self::Item> {
        let msg = self.rx.recv();
        match msg {
            Ok(msg) => Some(msg),
            Err(e) => {
                warn!("IngestionIterator: Error in receiving {:?}", e.to_string());
                None
            }
        }
    }
}

pub struct Ingestor {
    pub storage_client: Arc<RocksStorage>,
    pub sender: Arc<Box<dyn IngestorForwarder>>,
    writer: BatchedRocksDbWriter,
    timer: Instant,
    seq_no_resolver: Arc<Mutex<SeqNoResolver>>,
}

impl Ingestor {
    pub fn initialize_channel(
        config: IngestionConfig,
    ) -> (Arc<RwLock<Ingestor>>, IngestionIterator) {
        let (tx, rx) = unbounded::<(u64, IngestionOperation)>();
        let sender: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let ingestor = Arc::new(RwLock::new(Self::new(config, sender)));

        let iterator = IngestionIterator { rx };
        (ingestor, iterator)
    }
    pub fn new(config: IngestionConfig, sender: Arc<Box<dyn IngestorForwarder + 'static>>) -> Self {
        Self {
            storage_client: config.storage_client,
            sender,
            writer: BatchedRocksDbWriter::new(),
            timer: Instant::now(),
            seq_no_resolver: config.seq_resolver,
        }
    }

    pub fn handle_message(
        &mut self,
        (connector_id, message): (u64, IngestionMessage),
    ) -> Result<(), IngestorError> {
        match message {
            IngestionMessage::OperationEvent(mut event) => {
                let seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
                event.seq_no = seq_no as u64;

                let (key, encoded) = self.storage_client.map_operation_event(&event);
                self.writer.insert(key.as_ref(), encoded);
                self.sender
                    .forward((connector_id, IngestionOperation::OperationEvent(event)))?;
            }
            IngestionMessage::Schema(schema) => {
                let _seq_no: u64 = self.seq_no_resolver.lock().unwrap().get_next_seq_no() as u64;
                self.sender
                    .forward((connector_id, IngestionOperation::SchemaUpdate(schema)))?;
            }
            IngestionMessage::Commit(event) => {
                let seq_no = self.seq_no_resolver.lock().unwrap().get_next_seq_no();
                let (commit_key, commit_encoded) = self
                    .storage_client
                    .map_commit_message(&1, &seq_no, &event.lsn);
                self.writer.insert(commit_key.as_ref(), commit_encoded);
                self.writer.commit(&self.storage_client);

                debug!("Batch processing took: {:.2?}", self.timer.elapsed());
            }
            IngestionMessage::Begin() => {
                self.writer.begin();
                self.timer = Instant::now();
            }
        }
        Ok(())
    }
}

unsafe impl Sync for Ingestor {}

#[cfg(test)]
mod tests {
    use crate::ingestion::IngestionConfig;

    use super::IngestionMessage::{Begin, Commit, OperationEvent, Schema};
    use super::{ChannelForwarder, IngestionOperation, Ingestor, IngestorForwarder};
    use crossbeam::channel::unbounded;
    use dozer_types::types::{Operation, Record};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_message_handle() {
        let config = IngestionConfig::default();
        let storage_client = config.storage_client.clone();
        let (tx, _rx) = unbounded::<(u64, IngestionOperation)>();
        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let mut ingestor = Ingestor::new(config, forwarder);

        // Expected seq no - 1
        let schema_message = dozer_types::types::Schema {
            identifier: None,
            fields: vec![],
            values: vec![],
            primary_index: vec![],
            secondary_indexes: vec![],
        };

        // Expected seq no - 2
        let operation_event_message = dozer_types::types::OperationEvent {
            seq_no: 0,
            operation: Operation::Insert {
                new: Record::new(None, vec![]),
            },
        };

        // Expected seq no - 3
        let operation_event_message2 = dozer_types::types::OperationEvent {
            seq_no: 0,
            operation: Operation::Insert {
                new: Record::new(None, vec![]),
            },
        };

        // Expected seq no - 4
        let commit_message = dozer_types::types::Commit {
            seq_no: 0,
            lsn: 412142432,
        };

        ingestor.handle_message((1, Begin())).unwrap();
        ingestor
            .handle_message((1, Schema(schema_message)))
            .unwrap();
        ingestor
            .handle_message((1, OperationEvent(operation_event_message.clone())))
            .unwrap();
        ingestor
            .handle_message((1, OperationEvent(operation_event_message2.clone())))
            .unwrap();
        ingestor
            .handle_message((1, Commit(commit_message)))
            .unwrap();

        let mut expected_event = operation_event_message;
        expected_event.seq_no = 2;
        let mut expected_event2 = operation_event_message2;
        expected_event2.seq_no = 3;

        let mut expected_op_event_message = vec![
            storage_client.map_operation_event(&expected_event),
            storage_client.map_operation_event(&expected_event2),
            storage_client.map_commit_message(&1, &4, &commit_message.lsn),
        ]
        .into_iter();

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
