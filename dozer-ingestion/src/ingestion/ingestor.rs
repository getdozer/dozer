use crossbeam::channel::{unbounded, Receiver};
use dozer_types::ingestion_types::{
    IngestionMessage, IngestionOperation, IngestorError, IngestorForwarder,
};
use dozer_types::log::{debug, warn};
use dozer_types::parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::IngestionConfig;

#[derive(Debug)]
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
#[derive(Debug)]
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
impl IngestionIterator {
    pub fn next_timeout(&mut self, timeout: Duration) -> Option<(u64, IngestionOperation)> {
        let msg = self.rx.recv_timeout(timeout);
        match msg {
            Ok(msg) => Some(msg),
            Err(e) => {
                warn!("IngestionIterator: Error in receiving {:?}", e.to_string());
                None
            }
        }
    }
}

#[derive(Debug)]
pub struct Ingestor {
    pub sender: Arc<Box<dyn IngestorForwarder>>,
    timer: Instant,
}

impl Ingestor {
    pub fn initialize_channel(
        config: IngestionConfig,
    ) -> (Arc<RwLock<Ingestor>>, Arc<RwLock<IngestionIterator>>) {
        let (tx, rx) = unbounded::<(u64, IngestionOperation)>();
        let sender: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let ingestor = Arc::new(RwLock::new(Self::new(config, sender)));

        let iterator = Arc::new(RwLock::new(IngestionIterator { rx }));
        (ingestor, iterator)
    }
    pub fn new(
        _config: IngestionConfig,
        sender: Arc<Box<dyn IngestorForwarder + 'static>>,
    ) -> Self {
        Self {
            sender,
            timer: Instant::now(),
        }
    }

    pub fn handle_message(
        &mut self,
        (connector_id, message): (u64, IngestionMessage),
    ) -> Result<(), IngestorError> {
        match message {
            IngestionMessage::OperationEvent(event) => {
                self.sender
                    .forward((connector_id, IngestionOperation::OperationEvent(event)))?;
            }
            IngestionMessage::Commit(_event) => {
                debug!("Batch processing took: {:.2?}", self.timer.elapsed());
            }
            IngestionMessage::Begin() => {
                self.timer = Instant::now();
            }
        }
        Ok(())
    }
}

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
        let (tx, rx) = unbounded::<(u64, IngestionOperation)>();
        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let mut ingestor = Ingestor::new(config, forwarder);

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

        let table_name = "test".to_string();
        ingestor.handle_message((1, Begin())).unwrap();
        ingestor
            .handle_message((1, OperationEvent(operation_event_message.clone())))
            .unwrap();
        ingestor
            .handle_message((1, OperationEvent(operation_event_message2.clone())))
            .unwrap();
        ingestor
            .handle_message((1, Commit(commit_message)))
            .unwrap();

        let expected_op_event_message = vec![
            IngestionOperation::OperationEvent(operation_event_message),
            IngestionOperation::OperationEvent(operation_event_message2),
        ]
        .into_iter();

        for x in expected_op_event_message {
            let msg = rx.recv().unwrap();
            assert_eq!(x, msg.1);
        }
    }
}
