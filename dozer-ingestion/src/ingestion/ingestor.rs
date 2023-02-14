use crossbeam::channel::{bounded, Receiver};
use dozer_types::ingestion_types::{IngestionMessage, IngestorError, IngestorForwarder};
use dozer_types::log::warn;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Operation;
use std::sync::Arc;
use std::time::Duration;

use super::IngestionConfig;

#[derive(Debug)]
pub struct ChannelForwarder {
    pub sender: crossbeam::channel::Sender<((u64, u64), Operation)>,
}

impl IngestorForwarder for ChannelForwarder {
    fn forward(&self, event: ((u64, u64), Operation)) -> Result<(), IngestorError> {
        let send_res = self.sender.send(event);
        match send_res {
            Ok(_) => Ok(()),
            Err(e) => Err(IngestorError::ChannelError(Box::new(e))),
        }
    }
}
#[derive(Debug)]
pub struct IngestionIterator {
    pub rx: Receiver<((u64, u64), Operation)>,
}

impl Iterator for IngestionIterator {
    type Item = ((u64, u64), Operation);
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
    pub fn next_timeout(&mut self, timeout: Duration) -> Option<((u64, u64), Operation)> {
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
}

impl Ingestor {
    pub fn initialize_channel(
        config: IngestionConfig,
    ) -> (Arc<RwLock<Ingestor>>, Arc<RwLock<IngestionIterator>>) {
        let (tx, rx) = bounded(config.forwarder_channel_cap);
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
        Self { sender }
    }

    pub fn handle_message(
        &mut self,
        ((lsn, seq_no), message): ((u64, u64), IngestionMessage),
    ) -> Result<(), IngestorError> {
        match message {
            IngestionMessage::OperationEvent(op) => {
                self.sender.forward(((lsn, seq_no), op))?;
            }
            IngestionMessage::Commit(_event) => {}
            IngestionMessage::Begin() => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::ingestion::IngestionConfig;

    use super::IngestionMessage::{Begin, Commit, OperationEvent};
    use super::{ChannelForwarder, Ingestor, IngestorForwarder};
    use crossbeam::channel::unbounded;
    use dozer_types::types::{Operation, Record};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_message_handle() {
        let config = IngestionConfig::default();
        let (tx, rx) = unbounded();
        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let mut ingestor = Ingestor::new(config, forwarder);

        // Expected seq no - 2
        let operation = Operation::Insert {
            new: Record::new(None, vec![], None),
        };

        // Expected seq no - 3
        let operation2 = Operation::Insert {
            new: Record::new(None, vec![], None),
        };

        // Expected seq no - 4
        let commit_message = dozer_types::types::Commit {
            seq_no: 0,
            lsn: 412142432,
        };

        ingestor.handle_message(((1, 1), Begin())).unwrap();
        ingestor
            .handle_message(((1, 2), OperationEvent(operation.clone())))
            .unwrap();
        ingestor
            .handle_message(((1, 3), OperationEvent(operation2.clone())))
            .unwrap();
        ingestor
            .handle_message(((1, 4), Commit(commit_message)))
            .unwrap();

        let expected_op_event_message = vec![operation, operation2].into_iter();

        for x in expected_op_event_message {
            let msg = rx.recv().unwrap();
            assert_eq!(x, msg.1);
        }
    }
}
